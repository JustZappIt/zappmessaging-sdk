/**
 * Media Requests - pulls the bytes behind inbound media messages
 *
 * Media bytes are pushed once, only to sockets connected at send time, so an
 * offline recipient (the normal doorbell case) never got them. Every inbound
 * media record we lack bytes for becomes a request here, served from two
 * sources at once: the author (or any connected peer in the conversation)
 * streams chunks over the live socket, and when the record says where the
 * bytes sit in the author's media core, that range is downloaded from the
 * blind peer. Whichever finishes first wins and the other is cancelled.
 *
 * Both sources retry when a peer (re)connects, when the mirror comes up, or
 * when an in-flight transfer stalls, each under its own attempt cap and
 * cooldown. Mirror fetches are single-flight per mediaId and bounded in
 * number; a phone never keeps ciphertext it has decoded or given up on.
 */

const b4a = require('b4a')
const { isMediaId, isPeerId } = require('./media-id')
const mediaBlobs = require('./media-blobs')
const { createDiagnosticLogger } = require('./diagnostics')

const diag = createDiagnosticLogger('MEDIA')

const DEFAULTS = {
  maxAttempts: 12,
  cooldownMs: 10000,
  maxPending: 512,
  fetchConcurrency: 2,
  fetchTimeoutMs: 60000
}

function peerIdOrNull (value) {
  return isPeerId(value) ? value : null
}

// The range a record says its image occupies in the author's media core.
function descriptorOf (message) {
  if (!message || !isPeerId(message.mediaCoreKey)) return null
  const range = {
    coreKey: message.mediaCoreKey,
    offset: message.mediaBlockOffset,
    n: message.mediaBlockLength,
    byteLength: Number.isSafeInteger(message.mediaSize) && message.mediaSize > 0 ? message.mediaSize : null
  }
  return mediaBlobs.isValidRange(range) ? range : null
}

function rejected (message) {
  const err = new Error(message)
  err.code = 'MEDIA_RANGE_INVALID'
  return err
}

class MediaRequests {
  /**
   * @param {() => object} getDependencies - Resolved on every use, because
   *   the mirror is rebuilt on swarm restart and the IPC handler can attach
   *   after the first replicated record: { chatStore, mediaStore,
   *   mediaTransfer, p2pManager, hypercoreManager, blindMirror, ipcHandler }
   */
  constructor (getDependencies, opts = {}) {
    this._deps = getDependencies
    this._requests = new Map() // mediaId -> { conversationId, senderId, attempts, lastAt, descriptor, mirrorAttempts, mirrorFailedAt }
    this._fetches = new Map() // mediaId -> { core, cancelled } while a mirror fetch is in flight
    this._maxAttempts = opts.maxAttempts || DEFAULTS.maxAttempts
    this._cooldownMs = opts.cooldownMs || DEFAULTS.cooldownMs
    this._maxPending = opts.maxPending || DEFAULTS.maxPending
    this._fetchConcurrency = opts.fetchConcurrency || DEFAULTS.fetchConcurrency
    this._fetchTimeoutMs = opts.fetchTimeoutMs || DEFAULTS.fetchTimeoutMs
  }

  /** The pending request for a hash, which is what authorizes its chunks. */
  get (mediaId) {
    return this._requests.get(mediaId)
  }

  get size () {
    return this._requests.size
  }

  /**
   * Ask for the bytes behind a media message we do not have. Safe to call on
   * every ingestion; repeats are throttled per source.
   */
  request (conversationId, message, peerId) {
    try {
      if (typeof conversationId !== 'string' || conversationId.length === 0) return
      if (!message || message.isFromMe || !isMediaId(message.mediaId)) return
      const { chatStore, mediaStore, mediaTransfer, p2pManager } = this._deps()
      if (!mediaStore || !p2pManager) return
      if (mediaStore.hasMedia(message.mediaId) && chatStore &&
          chatStore.hasAuthorizedMediaReference(conversationId, message.mediaId)) {
        return
      }

      const mediaId = message.mediaId
      const now = Date.now()
      const senderId = peerIdOrNull(peerId) || peerIdOrNull(message.senderId)
      const entry = this._requests.get(mediaId) ||
        { conversationId, senderId, attempts: 0, lastAt: 0, descriptor: null, mirrorAttempts: 0, mirrorFailedAt: 0 }
      // A transfer's authorization scope cannot move when another conversation
      // references the same hash. Its chunks prove possession only in this scope.

      if (!this._requests.has(mediaId) && this._requests.size >= this._maxPending) {
        const oldestMediaId = this._requests.keys().next().value
        this._requests.delete(oldestMediaId)
        if (mediaTransfer) mediaTransfer.cancelTransfer(oldestMediaId)
        this._cancelFetch(oldestMediaId)
      }
      this._requests.set(mediaId, entry)
      if (!entry.descriptor) entry.descriptor = descriptorOf(message)
      this._pumpFetches()

      if (entry.attempts >= this._maxAttempts) return
      if (entry.lastAt && (now - entry.lastAt) < this._cooldownMs) return

      const sent = p2pManager.requestMedia(entry.conversationId, mediaId, entry.senderId)
      if (sent) {
        entry.attempts += 1
        entry.lastAt = now
      }
      // If no socket was available the entry is retained so peer_online retries.
    } catch (err) {
      diag('request failed: ' + (err.message || err))
    }
  }

  /** Re-request everything a conversation is still missing. */
  async requestMissing (conversationId, peerId = null) {
    try {
      const { chatStore, mediaStore } = this._deps()
      if (!chatStore || !mediaStore) return
      const messages = await chatStore.getMessages(conversationId, 50)
      for (const m of messages) {
        if (m && isMediaId(m.mediaId) && !m.isFromMe &&
            (!mediaStore.hasMedia(m.mediaId) || !chatStore.hasAuthorizedMediaReference(conversationId, m.mediaId))) {
          const entry = this._requests.get(m.mediaId)
          const onlinePeerId = peerIdOrNull(peerId)
          // A fresh author connection is a meaningful new opportunity. Reset a
          // prior cap, but never replace the author with an unrelated group peer.
          if (entry && onlinePeerId && onlinePeerId === peerIdOrNull(m.senderId)) {
            entry.attempts = 0
            entry.lastAt = 0
            entry.mirrorAttempts = 0
            entry.mirrorFailedAt = 0
          }
          this.request(conversationId, m, m.senderId)
        }
      }
    } catch (err) {
      diag('requestMissing failed: ' + (err.message || err))
    }
  }

  /** Re-request everything every conversation is still missing. */
  async requestMissingEverywhere () {
    try {
      const { chatStore } = this._deps()
      if (!chatStore) return
      for (const conv of await chatStore.listConversations()) {
        await this.requestMissing(conv.id)
      }
    } catch (err) {
      diag('requestMissingEverywhere failed: ' + (err.message || err))
    }
  }

  /**
   * A partial live download stalled or was corrupt. Its buffers are already
   * evicted; re-request from the author so it can resume from zero.
   */
  retry (mediaId) {
    const entry = this._requests.get(mediaId)
    if (!entry) return
    entry.lastAt = 0 // bypass the cooldown for an immediate retry
    this.request(entry.conversationId, { mediaId, senderId: entry.senderId, isFromMe: false }, entry.senderId)
  }

  /**
   * The one completion path for image bytes, whether a peer streamed them or
   * the blind peer served them: hash verified by the caller, saved once,
   * authorized only for the conversation that asked.
   */
  complete (hashHex, fullData) {
    try {
      const request = this._requests.get(hashHex)
      if (!request) return
      const { chatStore, mediaStore, mediaTransfer } = this._deps()
      // Determine extension from first bytes (magic number detection)
      let ext = 'jpg'
      if (fullData.length >= 4) {
        if (fullData[0] === 0x89 && fullData[1] === 0x50) ext = 'png'
        else if (fullData[0] === 0x47 && fullData[1] === 0x49) ext = 'gif'
        else if (fullData[0] === 0x00 && fullData[1] === 0x00 && fullData[2] === 0x00) ext = 'mp4'
      }

      const filePath = mediaStore.saveMediaWithHash(fullData, hashHex, ext)
      // Never grant other conversations merely because their peer named the
      // same hash.
      if (chatStore) chatStore.authorizeReceivedMedia(request.conversationId, hashHex)
      diag('Media transfer complete: ' + hashHex.substring(0, 12) + ' (' + fullData.length + ' bytes) -> ' + ext)

      // Downloaded — stop tracking it, and stop the other source if it is
      // still running.
      this._requests.delete(hashHex)
      this._cancelFetch(hashHex)
      if (mediaTransfer) mediaTransfer.cancelTransfer(hashHex)

      // Update all messages with this mediaId to have the local path
      if (chatStore) chatStore.updateMediaPath(hashHex, filePath)

      this._pushEvent('media.transfer_complete', {
        mediaId: hashHex,
        mediaLocalPath: filePath,
        mediaSize: fullData.length
      })
    } catch (err) {
      diag('Failed to save received media: ' + (err.message || err))
    }
  }

  /** Stop every mirror fetch; pending requests stay and restart on the next trigger. */
  cancelFetches () {
    for (const mediaId of [...this._fetches.keys()]) this._cancelFetch(mediaId)
  }

  // Start mirror fetches for pending requests that carry a descriptor, oldest
  // first, up to the concurrency cap.
  _pumpFetches () {
    const { hypercoreManager, mediaStore, blindMirror } = this._deps()
    if (!hypercoreManager || !mediaStore || !blindMirror || !blindMirror.enabled) return
    const now = Date.now()
    for (const [mediaId, entry] of this._requests) {
      if (this._fetches.size >= this._fetchConcurrency) return
      if (!entry.descriptor || this._fetches.has(mediaId)) continue
      if (entry.mirrorAttempts >= this._maxAttempts) continue
      if (entry.mirrorFailedAt && (now - entry.mirrorFailedAt) < this._cooldownMs) continue
      this._startFetch(mediaId, entry)
    }
  }

  _startFetch (mediaId, entry) {
    const fetch = { core: null, cancelled: false }
    this._fetches.set(mediaId, fetch)
    this._fetch(mediaId, entry, fetch)
      .then((bytes) => { if (bytes) this.complete(mediaId, bytes) })
      .catch((err) => diag('Mirror fetch failed unexpectedly: ' + (err.message || err)))
      .finally(() => {
        if (this._fetches.get(mediaId) === fetch) this._fetches.delete(mediaId)
        this._pumpFetches()
      })
  }

  // Resolves with verified bytes, or null after logging and bookkeeping. The
  // session is closed and its range cleared either way.
  async _fetch (mediaId, entry, fetch) {
    const { conversationId, senderId, descriptor } = entry
    const { p2pManager, mediaStore } = this._deps()
    let core = null
    try {
      core = await p2pManager.openRemoteMediaCore(conversationId, senderId, descriptor.coreKey)
      if (!core) throw rejected('media core unavailable')
      if (fetch.cancelled) return null
      fetch.core = core
      let downloaded = 0
      const bytes = await mediaBlobs.fetch(core, descriptor, {
        timeoutMs: this._fetchTimeoutMs,
        onBlock: () => {
          downloaded++
          this._pushEvent('media.transfer_progress', { mediaId, progress: downloaded / descriptor.n })
        }
      })
      if (b4a.toString(mediaStore.hash(bytes), 'hex') !== mediaId) throw rejected('media core bytes do not hash to mediaId')
      return bytes
    } catch (err) {
      if (fetch.cancelled) return null
      if (err.code === 'MEDIA_RANGE_INVALID') {
        // The record's descriptor cannot produce this image; only the live
        // path is left for it.
        entry.descriptor = null
        diag('Mirror fetch rejected media=' + mediaId.substring(0, 12) + ': ' + (err.message || err))
      } else {
        entry.mirrorAttempts += 1
        entry.mirrorFailedAt = Date.now()
        diag('Mirror fetch failed media=' + mediaId.substring(0, 12) +
          ' attempt=' + entry.mirrorAttempts + ': ' + (err.code || err.message || err))
      }
      return null
    } finally {
      if (core) {
        if (!fetch.cancelled) await core.clear(descriptor.offset, descriptor.offset + descriptor.n).catch(() => {})
        await core.close().catch(() => {})
      }
    }
  }

  _cancelFetch (mediaId) {
    const fetch = this._fetches.get(mediaId)
    if (!fetch) return
    fetch.cancelled = true
    this._fetches.delete(mediaId)
    if (fetch.core) fetch.core.close().catch(() => {})
  }

  _pushEvent (type, payload) {
    const { ipcHandler } = this._deps()
    if (ipcHandler) ipcHandler.pushEvent(type, payload)
  }
}

module.exports = { MediaRequests, DEFAULTS }
