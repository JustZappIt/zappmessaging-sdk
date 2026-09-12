/**
 * Media bytes as consecutive blocks of a conversation's media Hypercore.
 *
 * put() appends one image and returns where it landed; fetch() pulls that
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
 * Rejects with code MEDIA_RANGE_INVALID when the range or the bytes behind it
 * cannot be an image (never retried), MEDIA_FETCH_TIMEOUT when no block
 * arrives for timeoutMs, and MEDIA_FETCH_CANCELLED when the session is closed
 * underneath it — closing the session is how a caller cancels.
 */
async function fetch (core, range, { onBlock = null, timeoutMs = DEFAULT_FETCH_TIMEOUT_MS } = {}) {
  if (!isValidRange(range)) throw mediaError('MEDIA_RANGE_INVALID', 'invalid media block range')
  const { offset, n, byteLength } = range
  const end = offset + n

  const download = core.download({ start: offset, end })
  let timer = null
  let timedOut = false
  let oversized = false
  const arm = () => {
    if (timer) clearTimeout(timer)
    timer = setTimeout(() => {
      timedOut = true
      download.destroy()
    }, timeoutMs)
    if (typeof timer.unref === 'function') timer.unref()
  }
  const onDownload = (index, blockBytes) => {
    if (index < offset || index >= end) return
    if (blockBytes > MEDIA_BLOCK_SIZE) {
      oversized = true
      download.destroy()
      return
    }
    arm()
    if (onBlock) onBlock(index)
  }

  core.on('download', onDownload)
  arm()
  let complete
  try {
    complete = await download.done()
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
  const bytes = b4a.concat(blocks)
  if (bytes.length > config.MEDIA_MAX_BYTES || (byteLength != null && bytes.length !== byteLength)) {
    throw mediaError('MEDIA_RANGE_INVALID', 'media bytes do not match the descriptor')
  }
  return bytes
}

module.exports = { MEDIA_BLOCK_SIZE, MAX_MEDIA_BLOCKS, isValidRange, put, fetch }
