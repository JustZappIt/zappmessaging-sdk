/**
 * Identity - Manages cryptographic identity for P2P authentication
 *
 * Uses hypercore-crypto to generate valid Ed25519 keypairs required
 * by Hyperswarm's Noise protocol handshake.
 *
 * The on-disk source of truth is the 32 bytes of BIP-39 entropy. The Ed25519
 * keypair is re-derived on every load via PBKDF2 + SLIP-0010 (see mnemonic.js),
 * so the user's recovery phrase is the only secret we need to preserve. There
 * is no legacy-format fallback: an identity.json that lacks valid `entropy` is
 * treated as "no identity" and load() returns false, so the caller can route
 * the user through onboarding (which clears stores before creating a new key).
 * Silently regenerating here would bind a fresh keypair to the old on-disk
 * conversations — visible old data, unreachable on the wire.
 *
 * At-rest encryption: the entropy is the fund-controlling Zcash/EVM master
 * seed, so it is never written in plaintext when the native layer supplies a
 * file key (--identity-file-key, a 32-byte key the platform keeps wrapped in
 * Android Keystore / iOS Keychain). The entropy field is then secretbox-
 * encrypted (XSalsa20-Poly1305) under that key. A plaintext legacy file is
 * transparently rewritten encrypted on first load. Decrypt failure or a
 * missing key for an encrypted file fails closed — load() returns false and
 * the user recovers from the 24-word phrase, the standard wallet path.
 * Without a supplied key (tests, CLI) the legacy plaintext format is kept.
 */

const crypto = require('hypercore-crypto')
const sodium = require('sodium-universal')
const b4a = require('b4a')
const path = require('bare-path')
const mnemonic = require('./mnemonic')
const { getDataDir, readJSON, writeJSON, fileExists } = require('./storage')
const { IDENTITY_FILE_KEY } = require('./config')

function diag (...args) { /* no-op; identity logging goes through index.js diag */ }

const SEED_BYTES = 32
const FILE_KEY_BYTES = 32

class Identity {
  constructor(opts = {}) {
    this.keyPair = null
    this.displayName = ''
    this.createdAt = null
    // 32 bytes of BIP-39 entropy. Always set after load() / create() /
    // restoreFromMnemonic() succeeds.
    this.bipEntropy = null
    this.storagePath = path.join(getDataDir(), 'identity.json')
    // 32-byte at-rest encryption key for the entropy, normally injected by
    // the native layer via --identity-file-key. Null (tests, CLI) keeps the
    // legacy plaintext format.
    this.fileKey = opts.fileKey !== undefined
      ? opts.fileKey
      : (IDENTITY_FILE_KEY ? b4a.from(IDENTITY_FILE_KEY, 'hex') : null)
    if (this.fileKey && this.fileKey.byteLength !== FILE_KEY_BYTES) {
      throw new Error('Identity fileKey must be 32 bytes')
    }
  }

  /**
   * Get public key as hex string
   * @returns {string|null} Hex-encoded public key
   */
  get publicKeyHex() {
    if (!this.keyPair) return null
    return b4a.toString(this.keyPair.publicKey, 'hex')
  }

  /**
   * Recover the BIP-39 mnemonic for this identity.
   * @returns {string} 24-word mnemonic
   * @throws if the identity has no entropy (constructor default before
   *         load/create — should never happen on a live keypair)
   */
  exportMnemonic() {
    if (!this.bipEntropy) throw new Error('No mnemonic: identity has no entropy')
    return mnemonic.entropyToMnemonic(this.bipEntropy)
  }

  /**
   * Restore the identity from a 24-word BIP-39 mnemonic. Derives the Ed25519
   * seed via PBKDF2 + SLIP-0010 and stores the recovered entropy so that the
   * mnemonic round-trips on the next exportMnemonic() call.
   */
  async restoreFromMnemonic(mnemonicPhrase, displayName = '') {
    const entropy = mnemonic.mnemonicToEntropy(mnemonicPhrase)
    const seed = mnemonic.mnemonicToEd25519Seed(mnemonicPhrase)
    this.keyPair = crypto.keyPair(seed)
    this.bipEntropy = entropy
    this.displayName = displayName
    this.createdAt = Date.now()
    await this.save()
    return this.keyPair
  }

  /**
   * Create a new identity. Generates fresh BIP-39 entropy, derives the
   * Ed25519 seed through the same path as restore, and persists the entropy
   * so the mnemonic can be backed up.
   */
  async create(displayName = '') {
    const entropy = crypto.randomBytes(SEED_BYTES)
    const mnemonicPhrase = mnemonic.entropyToMnemonic(entropy)
    const seed = mnemonic.mnemonicToEd25519Seed(mnemonicPhrase)
    this.keyPair = crypto.keyPair(seed)
    this.bipEntropy = entropy
    this.displayName = displayName || ''
    this.createdAt = Date.now()
    await this.save()
    return this.keyPair
  }

  async updateDisplayName(displayName) {
    if (!this.keyPair) throw new Error('No identity')

    const previousDisplayName = this.displayName
    this.displayName = displayName
    try {
      await this.save()
    } catch (error) {
      this.displayName = previousDisplayName
      throw error
    }
  }

  /**
   * Load identity from storage. Returns true only when a valid entropy-bearing
   * identity is on disk; returns false for missing file, unparsable JSON, no
   * entropy, malformed entropy, or failed keypair validation. The caller is
   * expected to surface "no identity" and route through onboarding, which
   * wipes stores before creating a new keypair.
   * @returns {boolean} True if identity was loaded successfully
   */
  async load() {
    try {
      if (!fileExists(this.storagePath)) return false

      const stored = readJSON(this.storagePath)
      if (!stored) return false

      let entropy = null
      let migrateToEncrypted = false

      if (stored.entropyEnc) {
        // v2: entropy secretbox-encrypted under the native-held file key.
        // Fail closed when the key is missing or wrong — the caller routes
        // the user through restore-from-mnemonic, like any unreadable
        // identity. Regenerating or guessing here would be worse.
        if (!this.fileKey) {
          diag('Encrypted identity present but no --identity-file-key supplied')
          return false
        }
        entropy = decryptEntropy(stored.entropyEnc, this.fileKey)
        if (!entropy) {
          diag('Identity decrypt failed (wrong or rotated file key)')
          return false
        }
      } else if (stored.entropy) {
        // Legacy plaintext file.
        entropy = b4a.from(stored.entropy, 'hex')
        migrateToEncrypted = !!this.fileKey
      } else {
        return false
      }

      if (entropy.byteLength !== SEED_BYTES) return false

      const mnemonicPhrase = mnemonic.entropyToMnemonic(entropy)
      const seed = mnemonic.mnemonicToEd25519Seed(mnemonicPhrase)
      this.keyPair = crypto.keyPair(seed)
      this.bipEntropy = entropy
      this.displayName = stored.displayName || ''
      this.createdAt = stored.createdAt || null

      if (!this.validateKeyPair()) {
        // The on-disk entropy parsed but the derived keypair is unusable. Wipe
        // the in-memory state so we don't half-publish a broken identity.
        this.keyPair = null
        this.bipEntropy = null
        return false
      }

      if (migrateToEncrypted) {
        // One-time upgrade: rewrite the plaintext file encrypted. Best-effort —
        // a failed rewrite must not fail the load; the next save() retries.
        try {
          await this.save()
        } catch (err) {
          diag('Identity encrypt-migration failed:', err)
        }
      }

      return true
    } catch (error) {
      diag('Failed to load identity:', error)
      return false
    }
  }

  /**
   * Save identity to storage. Only the BIP-39 entropy is persisted as secret
   * material; the Ed25519 keypair is re-derived deterministically on load via
   * PBKDF2 + SLIP-0010. The public key is denormalised for fast read.
   *
   * With a file key the entropy is written secretbox-encrypted (`entropyEnc`,
   * hex of nonce||ciphertext) and no plaintext `entropy` field exists.
   */
  async save() {
    if (!this.keyPair) throw new Error('No identity to save')
    if (!this.bipEntropy) throw new Error('No identity entropy to save')

    const record = {
      publicKey: b4a.toString(this.keyPair.publicKey, 'hex'),
      displayName: this.displayName,
      createdAt: this.createdAt || Date.now()
    }

    if (this.fileKey) {
      record.version = 2
      record.entropyEnc = encryptEntropy(this.bipEntropy, this.fileKey)
    } else {
      record.entropy = b4a.toString(this.bipEntropy, 'hex')
    }

    writeJSON(this.storagePath, record)
  }

  /**
   * Validate that the keypair is a valid Ed25519 keypair
   * @returns {boolean} True if keypair is valid
   */
  validateKeyPair() {
    if (!this.keyPair) return false

    try {
      if (crypto.validateKeyPair) {
        return crypto.validateKeyPair(this.keyPair)
      }

      return (
        this.keyPair.publicKey &&
        this.keyPair.secretKey &&
        this.keyPair.publicKey.byteLength === 32 &&
        this.keyPair.secretKey.byteLength === 64
      )
    } catch (err) {
      diag('Keypair validation error:', err)
      return false
    }
  }
}

/**
 * Encrypt 32 bytes of entropy with crypto_secretbox (XSalsa20-Poly1305).
 * @returns {string} hex of nonce(24) || ciphertext(entropy+16 MAC)
 */
function encryptEntropy (entropy, key) {
  const nonce = crypto.randomBytes(sodium.crypto_secretbox_NONCEBYTES)
  const cipher = b4a.alloc(entropy.byteLength + sodium.crypto_secretbox_MACBYTES)
  sodium.crypto_secretbox_easy(cipher, entropy, nonce, key)
  return b4a.toString(nonce, 'hex') + b4a.toString(cipher, 'hex')
}

/**
 * Decrypt an `entropyEnc` blob. Returns the entropy buffer, or null on any
 * malformed input or authentication failure (wrong key, tampered file).
 */
function decryptEntropy (encHex, key) {
  try {
    const blob = b4a.from(encHex, 'hex')
    const nonceBytes = sodium.crypto_secretbox_NONCEBYTES
    if (blob.byteLength <= nonceBytes + sodium.crypto_secretbox_MACBYTES) return null
    const nonce = blob.subarray(0, nonceBytes)
    const cipher = blob.subarray(nonceBytes)
    const plain = b4a.alloc(cipher.byteLength - sodium.crypto_secretbox_MACBYTES)
    if (!sodium.crypto_secretbox_open_easy(plain, cipher, nonce, key)) return null
    return plain
  } catch (err) {
    return null
  }
}

module.exports = { Identity }
