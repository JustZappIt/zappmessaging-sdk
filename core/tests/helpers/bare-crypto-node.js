/**
 * Node adapter for the bare-crypto subset the core uses (see node-compat.js).
 * bare-crypto's pbkdf2 is synchronous (pbkdf2Sync aliases it), unlike
 * node:crypto's callback form — hence the explicit mapping.
 */
const crypto = require('crypto')

module.exports = {
  createHash: (algorithm) => crypto.createHash(algorithm),
  createHmac: (algorithm, key) => crypto.createHmac(algorithm, key),
  randomBytes: (size) => crypto.randomBytes(size),
  pbkdf2: (password, salt, iterations, keylen, digest) =>
    crypto.pbkdf2Sync(password, salt, iterations, keylen, digest)
}
