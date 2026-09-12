const crypto = require('hypercore-crypto')
const b4a = require('b4a')
let sink = null
const stages = new Set(['file_read', 'first_byte_queued', 'last_byte_queued', 'connection_prepared',
  'download_queue_wait', 'download_requested', 'download_no_peer', 'download_first_chunk', 'download_retry_wait',
  'message_append', 'media_prepared', 'receiver_verified', 'receiver_persisted', 'receiver_ui_event'])
function setMediaTimingSink (value) { sink = typeof value === 'function' ? value : null }
function mediaTiming () {
  if (!sink) return () => {}
  const id = b4a.toString(crypto.randomBytes(8), 'hex')
  const start = Date.now()
  let previous = start
  return (stage, bytes = 0) => {
    if (!sink || !stages.has(stage)) return
    const now = Date.now()
    const event = { id, stage, durationMs: Math.max(0, now - previous), elapsedMs: Math.max(0, now - start),
      bytes: Number.isSafeInteger(bytes) && bytes >= 0 ? bytes : 0 }
    previous = now
    try { sink(event) } catch (_) { /* Diagnostics never affect transfers. */ }
  }
}
module.exports = { mediaTiming, setMediaTimingSink }
