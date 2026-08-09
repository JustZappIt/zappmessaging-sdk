/**
 * Mnemonic - BIP-39 ↔ entropy + standards-compliant Ed25519 seed derivation.
 *
 * The 24-word phrase is a real BIP-39 mnemonic: 256 bits of entropy plus an
 * 8-bit SHA-256 checksum, picked from the BIP-39 English wordlist. The
 * mnemonic ↔ entropy round-trip is pure (entropyToMnemonic / mnemonicToEntropy).
 *
 * To produce the Ed25519 seed used by Hyperswarm, we run the mnemonic through
 * BIP-39 PBKDF2 (yielding the standard 64-byte BIP-39 seed) and then through
 * SLIP-0010's Ed25519 master derivation (`HMAC-SHA512("ed25519 seed", ...)`).
 * The first 32 bytes of that HMAC are the Ed25519 seed.
 *
 * Domain separation: this seed differs from the EVM secp256k1 key (BIP-32
 * `HMAC-SHA512("Bitcoin seed", ...)`) and from Zcash's ZIP-32 derivation,
 * even though all three start from the same BIP-39 seed.
 *
 * Historical note: pre-2026-05 builds used the raw mnemonic entropy as the
 * Ed25519 seed directly, with no PBKDF2 or SLIP-0010 in between. Any phrase
 * minted on an old build derives a different Ed25519 keypair under this code,
 * so prior testers must re-onboard to back up their identity by mnemonic.
 */

const crypto = require('bare-crypto')
const b4a = require('b4a')
const WORDLIST = require('./bip39-wordlist')

const ENTROPY_BYTES = 32
const WORD_COUNT = 24
const BITS_PER_WORD = 11
const CHECKSUM_BITS = 8
const PBKDF2_ITERATIONS = 2048
const PBKDF2_KEYLEN = 64
const ED25519_SEED_BYTES = 32
const SLIP10_ED25519_KEY = b4a.from('ed25519 seed', 'utf8')

function bytesToBits(bytes) {
  let bits = ''
  for (let i = 0; i < bytes.byteLength; i++) {
    bits += bytes[i].toString(2).padStart(8, '0')
  }
  return bits
}

function bitsToBytes(bits) {
  const bytes = b4a.alloc(bits.length / 8)
  for (let i = 0; i < bytes.byteLength; i++) {
    bytes[i] = parseInt(bits.slice(i * 8, (i + 1) * 8), 2)
  }
  return bytes
}

function sha256(buf) {
  const h = crypto.createHash('sha256')
  h.update(buf)
  return h.digest()
}

function hmacSha512(key, data) {
  const h = crypto.createHmac('sha512', key)
  h.update(data)
  return h.digest()
}

/**
 * Encode 32 bytes of entropy as a 24-word BIP-39 mnemonic with an SHA-256
 * checksum.
 * @param {Buffer} entropy 32-byte buffer
 * @returns {string} space-separated mnemonic
 */
function entropyToMnemonic(entropy) {
  if (!b4a.isBuffer(entropy) || entropy.byteLength !== ENTROPY_BYTES) {
    throw new Error(`Entropy must be a ${ENTROPY_BYTES}-byte buffer`)
  }

  const checksum = sha256(entropy)
  const bits = bytesToBits(entropy) + bytesToBits(checksum.subarray(0, 1))

  const words = []
  for (let i = 0; i < WORD_COUNT; i++) {
    const idx = parseInt(bits.slice(i * BITS_PER_WORD, (i + 1) * BITS_PER_WORD), 2)
    words.push(WORDLIST[idx])
  }
  return words.join(' ')
}

/**
 * Decode a 24-word BIP-39 mnemonic into its 32 bytes of entropy. Verifies the
 * SHA-256 checksum.
 * @param {string} mnemonic
 * @returns {Buffer} 32-byte entropy
 */
function mnemonicToEntropy(mnemonic) {
  const words = mnemonic.trim().toLowerCase().split(/\s+/)
  if (words.length !== WORD_COUNT) {
    throw new Error(`Mnemonic must be exactly ${WORD_COUNT} words`)
  }

  let bits = ''
  for (const word of words) {
    const idx = WORDLIST.indexOf(word)
    if (idx === -1) throw new Error('Unknown word: ' + word)
    bits += idx.toString(2).padStart(BITS_PER_WORD, '0')
  }

  const entropyBits = bits.slice(0, ENTROPY_BYTES * 8)
  const checksumBits = bits.slice(ENTROPY_BYTES * 8)
  const entropy = bitsToBytes(entropyBits)

  const expectedChecksum = bytesToBits(sha256(entropy).subarray(0, CHECKSUM_BITS / 8))
  if (checksumBits !== expectedChecksum) {
    throw new Error('Invalid checksum - please verify your recovery phrase')
  }
  return entropy
}

/**
 * Derive the 32-byte Ed25519 seed from a 24-word BIP-39 mnemonic per BIP-39
 * (PBKDF2-HMAC-SHA512, 2048 iterations, "mnemonic" salt) followed by
 * SLIP-0010 Ed25519 master-key derivation.
 *
 * Optional passphrase follows BIP-39 — appended to the PBKDF2 salt. Defaults
 * to empty so it composes with the wallet's mnemonic without an extra prompt.
 *
 * @param {string} mnemonic
 * @param {string} [passphrase='']
 * @returns {Buffer} 32-byte Ed25519 seed
 */
function mnemonicToEd25519Seed(mnemonic, passphrase = '') {
  // Validate (this also normalises whitespace + case) and recover the entropy.
  // We don't use the entropy itself for derivation — BIP-39 explicitly takes
  // the normalised wordlist string as input — but checksum failure should
  // throw the same error class the user sees on direct entropy validation.
  mnemonicToEntropy(mnemonic)

  const normalisedMnemonic = mnemonic.trim().toLowerCase().split(/\s+/).join(' ').normalize('NFKD')
  const normalisedPassphrase = ('mnemonic' + passphrase).normalize('NFKD')

  const bipSeed = crypto.pbkdf2(
    b4a.from(normalisedMnemonic, 'utf8'),
    b4a.from(normalisedPassphrase, 'utf8'),
    PBKDF2_ITERATIONS,
    PBKDF2_KEYLEN,
    'sha512',
  )

  const master = hmacSha512(SLIP10_ED25519_KEY, bipSeed)
  return master.subarray(0, ED25519_SEED_BYTES)
}

/**
 * Validate a mnemonic phrase without converting.
 * @param {string} mnemonic
 * @returns {boolean}
 */
function validateMnemonic(mnemonic) {
  try {
    mnemonicToEntropy(mnemonic)
    return true
  } catch {
    return false
  }
}

module.exports = {
  entropyToMnemonic,
  mnemonicToEntropy,
  mnemonicToEd25519Seed,
  validateMnemonic,
}
