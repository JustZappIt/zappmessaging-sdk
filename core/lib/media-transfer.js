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
    this.mediaStore = mediaStore
    this.activeTransfers = new Map() // hashHex -> { chunks, received, receivedBytes, totalChunks, timer }
    this._pendingSends = new WeakMap() // framedSocket -> Set<hashHex>, queued or active
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
    while (this._activeSendCount < this._maxConcurrentSends && this._sendQueue.length > 0) {
      const job = this._sendQueue.shift()
      this._activeSendCount++

      Promise.resolve()
        .then(() => this.sendMedia(job.framedSocket, job.hashHex))
        .then(
          () => this._finishSend(job, null),
          (err) => this._finishSend(job, err)
        )
    }
  }

  _finishSend (job, err) {
    job.pending.delete(job.hashHex)
    this._activeSendCount--
    if (err) job.reject(err)
    else job.resolve(true)
    this._drainSendQueue()
  }

  /**
   * Send a media file to a single framed socket
   * @param {FramedSocket} framedSocket - Target socket
   * @param {string} mediaHashHex - Media hash (hex)
   */
  async sendMedia(framedSocket, mediaHashHex) {
    const data = this.mediaStore.getMedia(mediaHashHex)
    if (!data) {
      throw new Error('Media not found: ' + mediaHashHex)
    }
    if (data.length < 1 || data.length > this._maxMediaBytes) {
      throw new Error('Media size is outside the transferable range')
    }

    const hashBuf = b4a.from(mediaHashHex, 'hex')
    const totalChunks = Math.ceil(data.length / CHUNK_SIZE)

    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE
      const end = Math.min(start + CHUNK_SIZE, data.length)
      const chunk = data.subarray(start, end)
      framedSocket.writeChunk(hashBuf, i, totalChunks, chunk)

      // Yield to event loop every 4 chunks to avoid blocking
      if (i > 0 && i % 4 === 0) {
        await new Promise(r => setTimeout(r, 1))
      }
    }

    this.emit('sent', mediaHashHex, totalChunks)
  }

  /**
   * Send media to multiple framed sockets (all peers in a conversation)
   * @param {Array<FramedSocket>} framedSockets - Target sockets
   * @param {string} mediaHashHex - Media hash (hex)
   */
  async sendMediaToAll(framedSockets, mediaHashHex) {
    const data = this.mediaStore.getMedia(mediaHashHex)
    if (!data) {
      throw new Error('Media not found: ' + mediaHashHex)
    }
    if (data.length < 1 || data.length > this._maxMediaBytes) {
      throw new Error('Media size is outside the transferable range')
    }

    const hashBuf = b4a.from(mediaHashHex, 'hex')
    const totalChunks = Math.ceil(data.length / CHUNK_SIZE)

    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE
      const end = Math.min(start + CHUNK_SIZE, data.length)
      const chunk = data.subarray(start, end)

      for (const framed of framedSockets) {
        try {
          framed.writeChunk(hashBuf, i, totalChunks, chunk)
        } catch (err) {
          diag('Failed to send chunk to peer:', err)
        }
      }

      if (i > 0 && i % 4 === 0) {
        await new Promise(r => setTimeout(r, 1))
      }
    }

    this.emit('sent', mediaHashHex, totalChunks)
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
      const actualHash = this.mediaStore.hash(fullData)
      const actualHashHex = b4a.toString(actualHash, 'hex')
      if (actualHashHex !== hashHex) {
        diag('Media integrity check failed: expected', hashHex.substring(0, 12), 'got', actualHashHex.substring(0, 12))
        this.emit('error', hashHex, new Error('Media integrity verification failed'))
        return false
      }

      this.emit('complete', hashHex, fullData)
    }
    return true
  }

  /**
   * Handle a media request from a peer
   * @param {Buffer} hashBuf - Media hash (32 bytes)
   * @param {FramedSocket} framedSocket - Requesting peer's socket
   */
  async handleRequest (hashBuf, framedSocket) {
    if (!b4a.isBuffer(hashBuf) || hashBuf.length !== 32) return false
    if (!framedSocket || typeof framedSocket.writeChunk !== 'function') return false
    const hashHex = b4a.toString(hashBuf, 'hex')
    if (!this.mediaStore.hasMedia(hashHex)) {
      diag('Media request for unknown hash:', hashHex.substring(0, 12))
      return false
    }

    let pending = this._pendingSends.get(framedSocket)
    if (!pending) {
      pending = new Set()
      this._pendingSends.set(framedSocket, pending)
    }
    if (pending.has(hashHex) || this._sendQueue.length >= this._maxQueuedSends) return false

    pending.add(hashHex)
    return new Promise((resolve, reject) => {
      this._sendQueue.push({ framedSocket, hashHex, pending, resolve, reject })
      this._drainSendQueue()
    })
  }

  /**
   * Cancel an active transfer
   * @param {string} mediaHashHex - Media hash (hex)
   */
  cancelTransfer (mediaHashHex) {
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
