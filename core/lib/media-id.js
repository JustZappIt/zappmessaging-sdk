const MEDIA_ID_PATTERN = /^[0-9a-f]{64}$/

/**
 * Media IDs are canonical lowercase 32-byte content hashes.
 *
 * @param {unknown} value
 * @returns {value is string}
 */
function isMediaId (value) {
  return typeof value === 'string' && MEDIA_ID_PATTERN.test(value)
}

/**
 * Noise identity keys have the same wire shape as media hashes but are kept as
 * a separate domain validator at call sites.
 *
 * @param {unknown} value
 * @returns {value is string}
 */
function isPeerId (value) {
  return typeof value === 'string' && MEDIA_ID_PATTERN.test(value)
}

module.exports = { isMediaId, isPeerId }
