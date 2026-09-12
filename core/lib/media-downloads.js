// Bounded receiver request lifecycle, including requests that get no first byte.
const { mediaTiming } = require('./media-timing')
class MediaDownloads {
  constructor ({ request, cancel, state, maxPending = 512, concurrency = 4, maxAttempts = 12,
    timeoutMs = 10000, retryMs = 1000 }) {
    Object.assign(this, { request, cancel, state, maxPending, concurrency, maxAttempts, timeoutMs, retryMs })
    this._entries = new Map()
    this._activeHashes = new Map()
    this.active = 0
    this.closed = false
    this._draining = false
  }
  get size () { return this._entries.size }
  get (hash, conversationId) {
    if (conversationId !== undefined) return this._entries.get(JSON.stringify([hash, conversationId]))
    return this._activeHashes.get(hash) || [...this._entries.values()].find(entry => entry.hash === hash)
  }
  enqueue (hash, conversationId, senderId, restart = false) {
    if (this.closed) return false
    const key = JSON.stringify([hash, conversationId])
    let entry = this._entries.get(key)
    if (!entry) {
      if (this.size >= this.maxPending) {
        const evict = [...this._entries.values()].find(value => value.failed)
        if (!evict) { this.state(hash, 'failed', conversationId); return false }
        this._remove(evict)
      }
      entry = { key, hash, conversationId, senderId, attempts: 0, active: false, failed: false,
        timer: null, timing: mediaTiming(), waitingPeer: false }
      this._entries.set(key, entry)
    }
    // Each conversation must independently prove possession of the bytes.
    // A second reference queues a separate scope, never replaces the first.
    if (restart && (entry.failed || entry.waitingPeer)) {
      if (entry.failed) { entry.failed = false; entry.attempts = 0 }
      clearTimeout(entry.timer)
      entry.timer = null
    }
    this._drain()
    return true
  }
  _arm (entry, ms, fn) {
    clearTimeout(entry.timer)
    entry.timer = setTimeout(() => {
      entry.timer = null
      if (this._entries.get(entry.key) === entry && !this.closed) fn()
    }, ms)
    if (entry.timer.unref) entry.timer.unref()
  }
  _release (entry) {
    if (!entry.active) return
    entry.active = false
    this._activeHashes.delete(entry.hash)
    this.active--
  }
  _drain () {
    if (this.closed || this._draining) return
    this._draining = true
    try {
      for (const entry of this._entries.values()) {
        if (this.active >= this.concurrency) break
        if (entry.failed || entry.active || entry.timer || this._activeHashes.has(entry.hash)) continue
        entry.active = true
        entry.waitingPeer = false
        this._activeHashes.set(entry.hash, entry)
        this.active++
        entry.attempts++
        entry.timing('download_queue_wait')
        this.state(entry.hash, 'queued', entry.conversationId)
        // Register scope and deadline before the write, including synchronous chunks.
        this._arm(entry, this.timeoutMs, () => this._retryEntry(entry))
        let sent = false
        try { sent = !!this.request(entry.conversationId, entry.hash, entry.senderId, entry.attempts) } catch (_) {}
        if (this._entries.get(entry.key) !== entry || !entry.active) continue
        entry.waitingPeer = !sent
        entry.timing(sent ? 'download_requested' : 'download_no_peer')
        if (!sent) {
          // No socket was written: retain its retry deadline but let another
          // conversation with this hash use the single hash reassembler now.
          this._release(entry)
          this.state(entry.hash, 'waiting_peer', entry.conversationId)
        }
      }
    } finally { this._draining = false }
  }
  progress (hash) {
    const entry = this._activeHashes.get(hash)
    if (!entry) return
    if (!entry.receiving) {
      entry.receiving = true
      entry.waitingPeer = false
      entry.timing('download_first_chunk')
      this.state(hash, 'downloading', entry.conversationId)
    }
    this._arm(entry, this.timeoutMs, () => this._retryEntry(entry))
  }
  retry (hash) {
    const entry = this._activeHashes.get(hash)
    if (entry) this._retryEntry(entry)
  }
  _retryEntry (entry) {
    entry.timing('download_retry_wait')
    if (entry.active) this.cancel(entry.hash)
    this._release(entry)
    entry.receiving = false
    clearTimeout(entry.timer)
    entry.timer = null
    if (entry.attempts >= this.maxAttempts) {
      entry.failed = true
      this.state(entry.hash, 'failed', entry.conversationId)
    } else {
      this._arm(entry, this.retryMs, () => this._drain())
    }
    this._drain()
  }
  _remove (entry) {
    clearTimeout(entry.timer)
    this._release(entry)
    this._entries.delete(entry.key)
  }
  complete (hash) {
    const entry = this._activeHashes.get(hash)
    if (!entry) return false
    this._remove(entry)
    this._drain()
    return true
  }
  delete (hash) {
    const entries = [...this._entries.values()].filter(entry => entry.hash === hash)
    if (entries.some(entry => entry.active)) this.cancel(hash)
    for (const entry of entries) this._remove(entry)
    this._drain()
    return entries.length > 0
  }
  clear () {
    this.closed = true
    for (const hash of this._activeHashes.keys()) this.cancel(hash)
    for (const entry of this._entries.values()) clearTimeout(entry.timer)
    this._entries.clear()
    this._activeHashes.clear()
    this.active = 0
    this.closed = false
  }
}
module.exports = { MediaDownloads }
