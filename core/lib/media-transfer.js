/**
 * Media Transfer - Chunked media transfer over Hyperswarm sockets
 *
 * Splits media files into 64KB chunks and sends them via FramedSocket.
 * Reassembles incoming chunks and emits 'complete' when all arrive.
 */

const EventEmitter = require('bare-events')
const b4a = require('b4a')
const config = require('./config')

const CHUNK_SIZE = config.MEDIA_CHUNK_SIZE
const DEFAULT_MAX_MEDIA_BYTES = config.MEDIA_MAX_BYTES
const DEFAULT_MAX_ACTIVE_TRANSFERS = 4
const DEFAULT_TRANSFER_TIMEOUT_MS = 60000
const DEFAULT_MAX_CONCURRENT_SENDS = 2
const DEFAULT_MAX_QUEUED_SENDS = 64
const { mediaTiming } = require('./media-timing')

function diag (...args) { /* no-op; media-transfer errors are non-fatal */ }

function positiveIntegerOption (value, fallback, name) {
  if (value === undefined) return fallback
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(name + ' must be a positive integer')
  }
  return value
}

class MediaTransfer extends EventEmitter {
  constructor (mediaStore, opts = {}) {
    super()
    this._generation = 0
    this.mediaStore = mediaStore
    this.activeTransfers = new Map() // hashHex -> { chunks, received, receivedBytes, totalChunks, timer }
    this._pendingSends = new Map() // authenticated recipient -> Map<hash, job>
    this._activeRecipients = new Set()
    this._mediaResults = new Map()
    this._completedProactive = new Map() // bounded, single-use first-request credits
    this._sendData = new Map() // active uploads only; shared read per hash
    this._sendQueue = []
    this._activeSendCount = 0
    this._activeBytes = 0
    this._maxMediaBytes = positiveIntegerOption(
      opts.maxMediaBytes, DEFAULT_MAX_MEDIA_BYTES, 'maxMediaBytes')
    this._maxActiveTransfers = positiveIntegerOption(
      opts.maxActiveTransfers, DEFAULT_MAX_ACTIVE_TRANSFERS, 'maxActiveTransfers')
    this._transferTimeoutMs = positiveIntegerOption(
      opts.transferTimeoutMs, DEFAULT_TRANSFER_TIMEOUT_MS, 'transferTimeoutMs')
    this._maxConcurrentSends = positiveIntegerOption(
      opts.maxConcurrentSends, DEFAULT_MAX_CONCURRENT_SENDS, 'maxConcurrentSends')
    this._maxQueuedSends = positiveIntegerOption(
      opts.maxQueuedSends, DEFAULT_MAX_QUEUED_SENDS, 'maxQueuedSends')
  }

  resetIdentity () {
    this._generation++
    this._resetting = true
    for (const [hash, transfer] of this.activeTransfers) this._dropTransfer(hash, transfer)
    const jobs = [...this._pendingSends.values()].flatMap(pending => [...pending.values()])
    for (const job of jobs) job.cancel.emit('cancel')
    for (const key of this._completedProactive.keys()) this._forgetProactive(key)
    this._mediaResults.clear()
    this._sendData.clear()
    this._resetting = false
    return Promise.allSettled(jobs.map(job => job.promise))
  }

  /**
   * (Re)arm the stall timer for an in-flight transfer.
   * @private
   */
  _armTimeout (hashHex) {
    const transfer = this.activeTransfers.get(hashHex)
    if (!transfer) return
    if (transfer.timer) clearTimeout(transfer.timer)
    transfer.timer = setTimeout(() => {
      // Only fire if still incomplete and still the active transfer.
      if (this.activeTransfers.get(hashHex) === transfer) {
        this._dropTransfer(hashHex, transfer)
        diag('Media transfer stalled, evicting: ' + hashHex.substring(0, 12))
        this.emit('timeout', hashHex)
      }
    }, this._transferTimeoutMs)
    // Don't let a pending transfer timer keep the runtime alive.
    if (transfer.timer && typeof transfer.timer.unref === 'function') transfer.timer.unref()
  }

  /**
   * Clear the stall timer for a transfer (on completion or eviction).
   * @private
   */
  _clearTimeout (transfer) {
    if (transfer && transfer.timer) {
      clearTimeout(transfer.timer)
      transfer.timer = null
    }
  }

  _dropTransfer (hashHex, transfer) {
    if (!transfer || this.activeTransfers.get(hashHex) !== transfer) return
    this._clearTimeout(transfer)
    this.activeTransfers.delete(hashHex)
    this._activeBytes = Math.max(0, this._activeBytes - transfer.receivedBytes)
  }

  _drainSendQueue () {
    if (this._resetting) return
    while (this._activeSendCount < this._maxConcurrentSends) {
      // One active upload per recipient: another socket cannot bypass fairness.
      const index = this._sendQueue.findIndex(job => !this._activeRecipients.has(job.recipient))
      if (index < 0) return
      const job = this._sendQueue.splice(index, 1)[0]
      job.active = true
      this._activeSendCount++
      this._activeRecipients.add(job.recipient)
      Promise.resolve().then(() => this._upload(job)).then(
        () => this._finishSend(job, null), err => this._finishSend(job, err))
    }
  }

  _uploadState (hash, state) {
    if (state === 'failed') {
      if (!this._mediaResults.has(hash) && this._mediaResults.size >= 512) {
        this._mediaResults.delete(this._mediaResults.keys().next().value)
      }
      this._mediaResults.set(hash, true)
    }
    const pending = [...this._pendingSends.values()].map(jobs => jobs.get(hash)).filter(Boolean)
    const aggregate = this._mediaResults.get(hash) ? 'failed'
      : pending.some(job => job.active) ? 'sending'
        : pending.length ? 'queued' : state
    this.emit('upload_state', hash, aggregate)
  }

  _finishSend (job, err) {
    if (job.finished) return
    job.finished = true
    if (job.socket.socket && job.socket.socket.removeListener) {
      job.socket.socket.removeListener('close', job.onClose)
    }
    job.cancel.removeAllListeners()
    if (job.active) {
      this._activeSendCount--
      this._activeRecipients.delete(job.recipient)
    } else {
      const index = this._sendQueue.indexOf(job)
      if (index >= 0) this._sendQueue.splice(index, 1)
    }
    const pending = this._pendingSends.get(job.recipient)
    if (pending) {
      pending.delete(job.hashHex)
      if (!pending.size) this._pendingSends.delete(job.recipient)
    }
    if (!err) this._rememberProactive(job)
    this._uploadState(job.hashHex, err ? 'failed' : 'queued_socket')
    if (err) job.reject(err)
    else job.resolve(true)
    this._drainSendQueue()
  }

  _forgetProactive (key) {
    const entry = this._completedProactive.get(key)
    if (!entry) return
    if (entry.socket.socket && entry.socket.socket.removeListener) {
      entry.socket.socket.removeListener('close', entry.onClose)
    }
    this._completedProactive.delete(key)
  }

  _rememberProactive (job) {
    if (!job.proactive || job.requestObserved || !job.socket.peerId) return
    const key = job.socket.peerId + ':' + job.hashHex
    this._forgetProactive(key)
    if (this._completedProactive.size >= 512) this._forgetProactive(this._completedProactive.keys().next().value)
    const entry = { socket: job.socket, onClose: () => this._forgetProactive(key) }
    this._completedProactive.set(key, entry)
    if (job.socket.socket && job.socket.socket.on) job.socket.socket.on('close', entry.onClose)
  }

  sendMedia (socket, hashHex, proactive = false) {
    // Production FramedSockets are bound to the Noise identity by P2PManager.
    // Object fallback supports embedders without allowing two known peers to merge.
    const recipient = socket.peerId || socket
    let pending = this._pendingSends.get(recipient)
    if (pending && pending.has(hashHex)) return pending.get(hashHex).promise
    if (socket.peerId) this._forgetProactive(socket.peerId + ':' + hashHex)
    if (this._sendQueue.length >= this._maxQueuedSends) {
      this._uploadState(hashHex, 'failed')
      return Promise.resolve(false)
    }
    if (![...this._pendingSends.values()].some(jobs => jobs.has(hashHex))) {
      this._mediaResults.delete(hashHex)
      if (this._mediaResults.size >= 512) this._mediaResults.delete(this._mediaResults.keys().next().value)
      this._mediaResults.set(hashHex, false)
    }
    if (!pending) this._pendingSends.set(recipient, pending = new Map())
    const job = { socket, hashHex, recipient, proactive, requestObserved: false, active: false, finished: false,
      cancelled: false, generation: this._generation, cancel: new EventEmitter() }
    job.promise = new Promise((resolve, reject) => { job.resolve = resolve; job.reject = reject })
    job.onClose = () => job.cancel.emit('cancel')
    job.cancel.on('cancel', () => {
      job.cancelled = true
      if (!job.active) this._finishSend(job, new Error('Media transfer cancelled'))
    })
    if (socket.socket && socket.socket.on) socket.socket.on('close', job.onClose)
    pending.set(hashHex, job)
    this._sendQueue.push(job)
    this._uploadState(hashHex, 'queued')
    this._drainSendQueue()
    return job.promise
  }

  async _upload (job) {
    const check = () => {
      if (job.generation !== this._generation) throw new Error('Media identity changed')
      if (job.cancelled) throw new Error('Media transfer cancelled')
      if (job.socket._destroyed || (job.socket.socket &&
          (job.socket.socket.destroyed || job.socket.socket.writable === false))) throw new Error('Media socket closed')
    }
    check()
    const timing = mediaTiming()
    let cached = this._sendData.get(job.hashHex)
    if (!cached) {
      const data = this.mediaStore.getMedia(job.hashHex)
      if (!data) throw new Error('Media not found')
      if (data.length < 1 || data.length > this._maxMediaBytes) throw new Error('Media size is outside the transferable range')
      cached = { data, users: 0 }
      this._sendData.set(job.hashHex, cached)
    }
    cached.users++
    const data = cached.data
    timing('file_read', data.length)
    try {
      const hash = b4a.from(job.hashHex, 'hex')
      const count = Math.ceil(data.length / CHUNK_SIZE)
      this._uploadState(job.hashHex, 'sending')
      for (let index = 0; index < count; index++) {
        check()
        const chunk = data.subarray(index * CHUNK_SIZE, Math.min((index + 1) * CHUNK_SIZE, data.length))
        if (job.socket.writeChunkAsync) {
          await job.socket.writeChunkAsync(hash, index, count, chunk, job.cancel, this._transferTimeoutMs, () => {
            if (index === 0) timing('first_byte_queued', chunk.length)
            if (index === count - 1) timing('last_byte_queued', data.length)
          })
        } else if (job.socket.writeChunk(hash, index, count, chunk) === false) {
          throw new Error('Media socket rejected chunk')
        }
        // Yield even on sockets with a large high-water mark.
        if (index % 4 === 3) await new Promise(resolve => setTimeout(resolve, 0))
      }
      check()
      // This is socket acceptance only. No remote verification is implied.
      this.emit('sent', job.hashHex, count)
    } finally {
      if (--cached.users === 0 && this._sendData.get(job.hashHex) === cached) this._sendData.delete(job.hashHex)
    }
  }

  async sendMediaToAll (sockets, hashHex) {
    const results = await Promise.allSettled(sockets.map(socket => this.sendMedia(socket, hashHex, true)))
    return results.every(result => result.status === 'fulfilled' && result.value === true)
  }

  /**
   * Handle an incoming chunk from a peer
   * @param {Buffer} hashBuf - Media hash (32 bytes)
   * @param {number} chunkIndex - Chunk index
   * @param {number} totalChunks - Total number of chunks
   * @param {Buffer} chunkData - Chunk data
   */
  onChunkReceived (hashBuf, chunkIndex, totalChunks, chunkData) {
    if (!b4a.isBuffer(hashBuf) || hashBuf.length !== 32) return false
    if (!Number.isSafeInteger(totalChunks) || totalChunks < 1 ||
        totalChunks > Math.ceil(this._maxMediaBytes / CHUNK_SIZE)) return false
    if (!Number.isSafeInteger(chunkIndex) || chunkIndex < 0 || chunkIndex >= totalChunks) return false
    if (!b4a.isBuffer(chunkData) || chunkData.length < 1 || chunkData.length > CHUNK_SIZE) return false
    if (this._activeBytes + chunkData.length > this._maxMediaBytes) return false

    const hashHex = b4a.toString(hashBuf, 'hex')

    if (!this.activeTransfers.has(hashHex)) {
      if (this.activeTransfers.size >= this._maxActiveTransfers) return false
      this.activeTransfers.set(hashHex, {
        chunks: new Array(totalChunks).fill(null),
        received: 0,
        receivedBytes: 0,
        totalChunks,
        timer: null
      })
    }

    const transfer = this.activeTransfers.get(hashHex)
    if (transfer.totalChunks !== totalChunks) return false

    // Don't process duplicate chunks
    if (transfer.chunks[chunkIndex]) return false
    if (transfer.receivedBytes + chunkData.length > this._maxMediaBytes) return false

    transfer.chunks[chunkIndex] = b4a.from(chunkData)
    transfer.received++
    transfer.receivedBytes += chunkData.length
    this._activeBytes += chunkData.length

    // Fresh bytes arrived — reset the stall timer.
    this._armTimeout(hashHex)

    // Emit progress
    this.emit('progress', hashHex, transfer.received / transfer.totalChunks)

    // Check if complete
    if (transfer.received === transfer.totalChunks) {
      const fullData = b4a.concat(transfer.chunks)
      this._dropTransfer(hashHex, transfer)

      // Verify integrity: reassembled data must match the claimed hash
      const verificationTiming = mediaTiming()
      const actualHash = this.mediaStore.hash(fullData)
      const actualHashHex = b4a.toString(actualHash, 'hex')
      if (actualHashHex !== hashHex) {
        diag('Media integrity check failed: expected', hashHex.substring(0, 12), 'got', actualHashHex.substring(0, 12))
        this.emit('error', hashHex, new Error('Media integrity verification failed'))
        return false
      }

      verificationTiming('receiver_verified', fullData.length)
      this.emit('complete', hashHex, fullData)
    }
    return true
  }

  /**
   * Handle a media request from a peer
   * @param {Buffer} hashBuf - Media hash (32 bytes)
   * @param {FramedSocket} framedSocket - Requesting peer's socket
   */
  async handleRequest (hashBuf, framedSocket, attempt = 0) {
    if (!b4a.isBuffer(hashBuf) || hashBuf.length !== 32) return false
    if (!framedSocket || typeof framedSocket.writeChunk !== 'function') return false
    const hashHex = b4a.toString(hashBuf, 'hex')
    if (!this.mediaStore.hasMedia(hashHex)) {
      diag('Media request for unknown hash:', hashHex.substring(0, 12))
      return false
    }

    const pending = this._pendingSends.get(framedSocket.peerId || framedSocket)
    const active = pending && pending.get(hashHex)
    if (active) { active.requestObserved = true; return active.promise }
    const key = framedSocket.peerId + ':' + hashHex
    const completed = this._completedProactive.has(key)
    this._forgetProactive(key)
    // A first request can cross the already-completed proactive write in flight.
    // Consume one credit, not a time-based ban. Explicit retries and legacy
    // requests always start fresh; close/cancel/identity change erase credits.
    if (attempt === 1 && completed) return true
    return this.sendMedia(framedSocket, hashHex)
  }

  /**
   * Cancel an active transfer
   * @param {string} mediaHashHex - Media hash (hex)
   */
  cancelTransfer (mediaHashHex) {
    for (const key of this._completedProactive.keys()) {
      if (key.endsWith(':' + mediaHashHex)) this._forgetProactive(key)
    }
    for (const pending of this._pendingSends.values()) {
      const job = pending.get(mediaHashHex)
      if (job) job.cancel.emit('cancel')
    }
    this.cancelDownload(mediaHashHex)
  }

  cancelDownload (mediaHashHex) {
    const transfer = this.activeTransfers.get(mediaHashHex)
    if (transfer) {
      this._dropTransfer(mediaHashHex, transfer)
      this.emit('cancelled', mediaHashHex)
    }
  }

  /**
   * Get transfer progress
   * @param {string} mediaHashHex - Media hash (hex)
   * @returns {number|null} Progress (0-1) or null if not active
   */
  getProgress(mediaHashHex) {
    const transfer = this.activeTransfers.get(mediaHashHex)
    if (!transfer) return null
    return transfer.received / transfer.totalChunks
  }
}

module.exports = { MediaTransfer, CHUNK_SIZE }
