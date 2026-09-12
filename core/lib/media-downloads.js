// Bounded receiver request lifecycle, including requests that get no first byte.
const { mediaTiming } = require('./media-timing')
class MediaDownloads extends Map {
  constructor ({ request, cancel, state, maxPending = 512, concurrency = 4, maxAttempts = 12,
    timeoutMs = 10000, retryMs = 1000 }) {
    super()
    Object.assign(this, { request, cancel, state, maxPending, concurrency, maxAttempts, timeoutMs, retryMs })
    this.active = 0
    this.closed = false
  }
  enqueue (hash, conversationId, senderId, restart = false) {
    if (this.closed) return false
    let entry = this.get(hash)
    if (!entry) {
      if (this.size >= this.maxPending) {
        const evict = [...this].find(([, value]) => value.failed)
        if (!evict) { this.state(hash, 'failed', conversationId); return false }
        this.delete(evict[0])
      }
      entry = { conversationId, senderId, attempts: 0, active: false, failed: false, timer: null, timing: mediaTiming(), waitingPeer: false }
      this.set(hash, entry)
    }
    // Never move an existing hash's authorization scope to another conversation.
    if (restart && entry.failed) { entry.failed = false; entry.attempts = 0 }
    if (restart && entry.active && entry.waitingPeer) {
      clearTimeout(entry.timer)
      entry.timer = null
      entry.active = false
      this.active--
    }
    this._drain()
    return true
  }
  _arm (hash, entry, ms, fn) {
    clearTimeout(entry.timer)
    entry.timer = setTimeout(() => {
      entry.timer = null
      if (this.get(hash) === entry && !this.closed) fn()
    }, ms)
    if (entry.timer.unref) entry.timer.unref()
  }
  _drain () {
    if (this.closed) return
    for (const [hash, entry] of this) {
      if (this.active >= this.concurrency) break
      if (entry.failed || entry.active || entry.timer) continue
      entry.active = true
      this.active++
      entry.attempts++
      entry.timing('download_queue_wait')
      this.state(hash, 'queued', entry.conversationId)
      // Register deadline before the write, including synchronous first chunks.
      this._arm(hash, entry, this.timeoutMs, () => this.retry(hash))
      try {
        const sent = this.request(entry.conversationId, hash, entry.senderId, entry.attempts)
        entry.waitingPeer = !sent
        entry.timing(sent ? 'download_requested' : 'download_no_peer')
        if (!sent && this.get(hash) === entry) this.state(hash, 'waiting_peer', entry.conversationId)
      } catch (_) { /* deadline retries */ }
    }
  }
  progress (hash) {
    const entry = this.get(hash)
    if (entry && entry.active) {
      if (!entry.receiving) {
        entry.receiving = true
        entry.waitingPeer = false
        entry.timing('download_first_chunk')
        this.state(hash, 'downloading', entry.conversationId)
      }
      this._arm(hash, entry, this.timeoutMs, () => this.retry(hash))
    }
  }
  retry (hash) {
    const entry = this.get(hash)
    if (!entry || !entry.active) return
    entry.timing('download_retry_wait')
    this.cancel(hash)
    entry.active = false
    entry.receiving = false
    this.active--
    clearTimeout(entry.timer)
    entry.timer = null
    if (entry.attempts >= this.maxAttempts) {
      entry.failed = true
      this.state(hash, 'failed', entry.conversationId)
    } else {
      this._arm(hash, entry, this.retryMs, () => this._drain())
    }
    this._drain()
  }
  delete (hash) {
    const entry = this.get(hash)
    if (!entry) return false
    clearTimeout(entry.timer)
    if (entry.active) this.active--
    const result = super.delete(hash)
    this._drain()
    return result
  }
  clear () {
    this.closed = true
    for (const [hash, entry] of this) { clearTimeout(entry.timer); this.cancel(hash) }
    super.clear()
    this.active = 0
    this.closed = false
  }
}
module.exports = { MediaDownloads }
