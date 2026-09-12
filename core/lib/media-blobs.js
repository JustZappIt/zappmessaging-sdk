/**
 * Media bytes as consecutive blocks of a conversation's media Hypercore.
 *
 * put() appends one image and returns where it landed; download() pulls that
 * range back out of a core the blind peer replicated. Both are plain
 * functions over a core session: authorization, key derivation and mirror
 * registration stay in hypercore-manager / p2p-manager.
 */

const b4a = require('b4a')
const config = require('./config')

const MEDIA_BLOCK_SIZE = 256 * 1024
const MAX_MEDIA_BLOCKS = Math.ceil(config.MEDIA_MAX_BYTES / MEDIA_BLOCK_SIZE)
const DEFAULT_FETCH_TIMEOUT_MS = 60000

function mediaError (code, message) {
  const err = new Error(message)
  err.code = code
  return err
}

function blockCount (byteLength) {
  return Math.ceil(byteLength / MEDIA_BLOCK_SIZE)
}

/**
 * Whether {offset, n, byteLength} could describe an image in a media core.
 * byteLength is optional; when present it must need exactly n blocks.
 */
function isValidRange ({ offset, n, byteLength } = {}) {
  if (!Number.isSafeInteger(offset) || offset < 0) return false
  if (!Number.isSafeInteger(n) || n < 1 || n > MAX_MEDIA_BLOCKS) return false
  if (byteLength == null) return true
  return Number.isSafeInteger(byteLength) && byteLength >= 1 && blockCount(byteLength) === n
}

/**
 * Append bytes as MEDIA_BLOCK_SIZE blocks in one batch. The offset comes from
 * the append result, so concurrent puts on one core never overlap.
 * @returns {Promise<{offset: number, n: number}>}
 */
async function put (core, bytes) {
  if (!b4a.isBuffer(bytes) || bytes.length < 1 || bytes.length > config.MEDIA_MAX_BYTES) {
    throw mediaError('MEDIA_RANGE_INVALID',
      'media must be between 1 byte and ' + config.MEDIA_MAX_BYTES + ' bytes')
  }
  const blocks = []
  for (let start = 0; start < bytes.length; start += MEDIA_BLOCK_SIZE) {
    blocks.push(bytes.subarray(start, Math.min(start + MEDIA_BLOCK_SIZE, bytes.length)))
  }
  const { length } = await core.append(blocks)
  return { offset: length - blocks.length, n: blocks.length }
}

/**
 * Download blocks [offset, offset + n) and return them as one buffer.
 *
 * Returns `bytes`, which rejects with code MEDIA_RANGE_INVALID when the range
 * or the bytes behind it cannot be an image (never retried),
 * MEDIA_FETCH_TIMEOUT when no block arrives for timeoutMs, and
 * MEDIA_FETCH_CANCELLED after `cancel()` or when the session closes
 * underneath it. The session stays open in every case, so the caller can
 * clear whatever landed and close it on one path.
 */
function download (core, range, { onBlock = null, timeoutMs = DEFAULT_FETCH_TIMEOUT_MS } = {}) {
  if (!isValidRange(range)) {
    const bytes = Promise.reject(mediaError('MEDIA_RANGE_INVALID', 'invalid media block range'))
    bytes.catch(() => {})
    return { bytes, cancel () {} }
  }
  const { offset, n, byteLength } = range
  const end = offset + n

  const pending = core.download({ start: offset, end })
  let timer = null
  let timedOut = false
  let oversized = false
  let cancelled = false
  const arm = () => {
    if (timer) clearTimeout(timer)
    timer = setTimeout(() => {
      timedOut = true
      pending.destroy()
    }, timeoutMs)
    if (typeof timer.unref === 'function') timer.unref()
  }
  const onDownload = (index, blockBytes) => {
    if (index < offset || index >= end) return
    if (blockBytes > MEDIA_BLOCK_SIZE) {
      oversized = true
      pending.destroy()
      return
    }
    arm()
    if (onBlock && !cancelled) onBlock(index)
  }

  const bytes = (async () => {
    core.on('download', onDownload)
    arm()
    let complete
    try {
      complete = await pending.done()
    } finally {
      clearTimeout(timer)
      core.off('download', onDownload)
    }

    if (oversized) throw mediaError('MEDIA_RANGE_INVALID', 'media block larger than MEDIA_BLOCK_SIZE')
    if (!complete) {
      if (timedOut) throw mediaError('MEDIA_FETCH_TIMEOUT', 'no media block arrived for ' + timeoutMs + 'ms')
      throw mediaError('MEDIA_FETCH_CANCELLED', 'media fetch cancelled')
    }
    if (end > core.length) throw mediaError('MEDIA_RANGE_INVALID', 'media block range past the end of the core')

    const blocks = []
    for (let index = offset; index < end; index++) blocks.push(await core.get(index))
    const assembled = b4a.concat(blocks)
    if (assembled.length > config.MEDIA_MAX_BYTES || (byteLength != null && assembled.length !== byteLength)) {
      throw mediaError('MEDIA_RANGE_INVALID', 'media bytes do not match the descriptor')
    }
    return assembled
  })()

  return {
    bytes,
    cancel () {
      cancelled = true
      pending.destroy()
    }
  }
}

module.exports = { MEDIA_BLOCK_SIZE, MAX_MEDIA_BLOCKS, isValidRange, put, download }
