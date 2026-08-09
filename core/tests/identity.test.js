/**
 * Unit tests for identity.js
 */

const { test } = require('node:test')
const assert = require('node:assert')
const path = require('path')
const fs = require('fs')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const { Identity } = require('../lib/identity')
const { getDataDir } = require('../lib/storage')

test('Identity creation generates valid Ed25519 keypair', async () => {
  const identity = new Identity()
  const keyPair = await identity.create('Test User')
  
  assert.ok(keyPair, 'KeyPair should be generated')
  assert.ok(keyPair.publicKey, 'Public key should exist')
  assert.ok(keyPair.secretKey, 'Secret key should exist')
  assert.strictEqual(keyPair.publicKey.byteLength, 32, 'Public key should be 32 bytes')
  assert.strictEqual(keyPair.secretKey.byteLength, 64, 'Secret key should be 64 bytes')
  assert.strictEqual(identity.displayName, 'Test User', 'Display name should be set')
  
  // Cleanup
  fs.unlinkSync(identity.storagePath)
})

test('publicKeyHex returns hex-encoded public key', async () => {
  const identity = new Identity()
  await identity.create('Test User')
  
  const hex = identity.publicKeyHex
  assert.ok(hex, 'Public key hex should exist')
  assert.strictEqual(hex.length, 64, 'Hex string should be 64 characters (32 bytes)')
  assert.ok(/^[0-9a-f]+$/.test(hex), 'Hex string should only contain hex characters')
  
  // Cleanup
  fs.unlinkSync(identity.storagePath)
})

test('restoreFromMnemonic recreates identity correctly', async () => {
  const identity1 = new Identity()
  await identity1.create('Original User')
  const originalMnemonic = identity1.exportMnemonic()
  const originalPublicKey = identity1.publicKeyHex

  const identity2 = new Identity()
  identity2.storagePath = path.join(getDataDir(), 'identity-restored-' + Date.now() + '.json')
  await identity2.restoreFromMnemonic(originalMnemonic, 'Restored User')

  assert.strictEqual(identity2.publicKeyHex, originalPublicKey, 'Public keys should match')
  assert.strictEqual(identity2.displayName, 'Restored User', 'Display name should be updated')

  fs.unlinkSync(identity1.storagePath)
  fs.unlinkSync(identity2.storagePath)
})

test('save and load persist identity correctly', async () => {
  const identity1 = new Identity()
  identity1.storagePath = path.join(getDataDir(), 'identity-persist-' + Date.now() + '.json')
  await identity1.create('Persistent User')
  const originalPublicKey = identity1.publicKeyHex
  
  // Load in new instance
  const identity2 = new Identity()
  identity2.storagePath = identity1.storagePath
  const loaded = await identity2.load()
  
  assert.ok(loaded, 'Identity should be loaded')
  assert.strictEqual(identity2.publicKeyHex, originalPublicKey, 'Public keys should match')
  assert.strictEqual(identity2.displayName, 'Persistent User', 'Display name should match')
  
  // Cleanup
  fs.unlinkSync(identity1.storagePath)
})

test('updateDisplayName persists without changing the identity key', async () => {
  const identity = new Identity()
  identity.storagePath = path.join(getDataDir(), 'identity-update-' + Date.now() + '.json')
  await identity.create('Original User')
  const publicKey = identity.publicKeyHex

  await identity.updateDisplayName('Updated User')

  const reloaded = new Identity()
  reloaded.storagePath = identity.storagePath
  await reloaded.load()
  assert.strictEqual(reloaded.publicKeyHex, publicKey)
  assert.strictEqual(reloaded.displayName, 'Updated User')

  fs.unlinkSync(identity.storagePath)
})

test('updateDisplayName restores the in-memory value when persistence fails', async () => {
  const identity = new Identity()
  identity.storagePath = path.join(getDataDir(), 'identity-update-failure-' + Date.now() + '.json')
  await identity.create('Original User')
  identity.save = async () => { throw new Error('write failed') }

  await assert.rejects(identity.updateDisplayName('Updated User'), /write failed/)
  assert.strictEqual(identity.displayName, 'Original User')

  fs.unlinkSync(identity.storagePath)
})

test('load returns false for non-existent identity', async () => {
  const identity = new Identity()
  identity.storagePath = path.join(getDataDir(), 'non-existent-' + Date.now() + '.json')
  
  const loaded = await identity.load()
  assert.strictEqual(loaded, false, 'Load should return false for non-existent file')
})

test('validateKeyPair detects valid keypairs', async () => {
  const identity = new Identity()
  await identity.create('Test User')
  
  const isValid = identity.validateKeyPair()
  assert.ok(isValid, 'Valid keypair should pass validation')
  
  // Cleanup
  fs.unlinkSync(identity.storagePath)
})

test('validateKeyPair detects invalid keypairs', () => {
  const identity = new Identity()
  
  // No keypair
  assert.strictEqual(identity.validateKeyPair(), false, 'Should return false for no keypair')
  
  // Invalid keypair
  identity.keyPair = {
    publicKey: b4a.alloc(32),
    secretKey: b4a.alloc(32) // Wrong size
  }
  assert.strictEqual(identity.validateKeyPair(), false, 'Should return false for invalid keypair')
})

test('restoreFromMnemonic throws on invalid mnemonic', async () => {
  const identity = new Identity()
  try {
    await identity.restoreFromMnemonic('not a valid mnemonic phrase')
    assert.fail('Should throw for invalid mnemonic')
  } catch (err) {
    assert.ok(/24 words|Unknown word|checksum/i.test(err.message),
      'Error should mention BIP-39 validation: ' + err.message)
  }
})

test('exportMnemonic throws when no identity loaded', () => {
  const identity = new Identity()

  try {
    identity.exportMnemonic()
    assert.fail('Should throw error when no identity loaded')
  } catch (err) {
    assert.ok(err.message.includes('No mnemonic'), 'Error should mention no mnemonic')
  }
})

test('restoreFromMnemonic round-trips through exportMnemonic', async () => {
  const { entropyToMnemonic } = require('../lib/mnemonic')
  const entropy = crypto.randomBytes(32)
  const original = entropyToMnemonic(entropy)

  const identity = new Identity()
  identity.storagePath = path.join(getDataDir(), 'identity-bip39-' + Date.now() + '.json')
  await identity.restoreFromMnemonic(original, 'BIP39 User')

  assert.strictEqual(identity.exportMnemonic(), original, 'Exported mnemonic must match what was restored')
  assert.ok(identity.publicKeyHex, 'Identity must derive a public key')

  fs.unlinkSync(identity.storagePath)
})

test('create() persists entropy so the mnemonic is exportable', async () => {
  const identity = new Identity()
  identity.storagePath = path.join(getDataDir(), 'identity-create-' + Date.now() + '.json')
  await identity.create('Fresh User')

  const mnemonic = identity.exportMnemonic()
  assert.ok(mnemonic, 'Fresh identity must have an exportable mnemonic')
  assert.strictEqual(mnemonic.split(' ').length, 24, 'Mnemonic must be 24 words')

  // Reload from disk to confirm entropy persists across save/load.
  const reloaded = new Identity()
  reloaded.storagePath = identity.storagePath
  await reloaded.load()
  assert.strictEqual(reloaded.exportMnemonic(), mnemonic, 'Reloaded identity must export the same mnemonic')

  fs.unlinkSync(identity.storagePath)
})

test('load() returns false when identity.json lacks entropy (no silent regenerate)', async () => {
  // A corrupted/hand-edited identity.json with no entropy field used to silently fall
  // through to create(), binding a fresh keypair to the on-disk conversation data and
  // leaving the user with visible old contacts but no working network identity. load()
  // now returns false and leaves the in-memory state empty so the caller can route
  // through the proper onboarding/wipe path.
  const storagePath = path.join(getDataDir(), 'identity-noentropy-' + Date.now() + '.json')
  fs.writeFileSync(storagePath, JSON.stringify({
    publicKey: 'deadbeef'.repeat(8),
    displayName: 'Stale User',
    createdAt: Date.now()
  }))

  const identity = new Identity()
  identity.storagePath = storagePath
  const loaded = await identity.load()

  assert.strictEqual(loaded, false, 'Load must report failure on missing entropy')
  assert.strictEqual(identity.keyPair, null, 'No keypair must be populated')
  assert.strictEqual(identity.bipEntropy, null, 'No entropy must be populated')

  fs.unlinkSync(storagePath)
})

test('load() returns false on invalid entropy length', async () => {
  const storagePath = path.join(getDataDir(), 'identity-shortentropy-' + Date.now() + '.json')
  fs.writeFileSync(storagePath, JSON.stringify({
    entropy: 'abcd'.repeat(4), // 8 bytes — not 32
    displayName: 'Short Entropy',
    createdAt: Date.now()
  }))

  const identity = new Identity()
  identity.storagePath = storagePath
  const loaded = await identity.load()

  assert.strictEqual(loaded, false, 'Load must report failure on malformed entropy')
  assert.strictEqual(identity.keyPair, null)
  assert.strictEqual(identity.bipEntropy, null)

  fs.unlinkSync(storagePath)
})

// --- At-rest encryption (--identity-file-key) ---

const FILE_KEY = b4a.from('11'.repeat(32), 'hex')
const OTHER_KEY = b4a.from('22'.repeat(32), 'hex')

test('save() with a file key writes no plaintext entropy and round-trips', async () => {
  const identity = new Identity({ fileKey: FILE_KEY })
  identity.storagePath = path.join(getDataDir(), 'identity-enc-' + Date.now() + '.json')
  await identity.create('Encrypted User')

  const raw = fs.readFileSync(identity.storagePath, 'utf8')
  const stored = JSON.parse(raw)
  assert.strictEqual(stored.entropy, undefined, 'No plaintext entropy field on disk')
  assert.ok(stored.entropyEnc, 'Encrypted entropy present')
  assert.strictEqual(stored.version, 2, 'Encrypted files are marked version 2')
  const entropyHex = b4a.toString(identity.bipEntropy, 'hex')
  assert.ok(!raw.includes(entropyHex), 'Raw file must not contain the entropy hex anywhere')

  const reloaded = new Identity({ fileKey: FILE_KEY })
  reloaded.storagePath = identity.storagePath
  assert.strictEqual(await reloaded.load(), true, 'Load with the same key must succeed')
  assert.strictEqual(reloaded.publicKeyHex, identity.publicKeyHex)
  assert.strictEqual(reloaded.exportMnemonic(), identity.exportMnemonic())

  fs.unlinkSync(identity.storagePath)
})

test('load() migrates a legacy plaintext identity to encrypted in place', async () => {
  const plain = new Identity({ fileKey: null })
  plain.storagePath = path.join(getDataDir(), 'identity-migrate-' + Date.now() + '.json')
  await plain.create('Legacy User')
  assert.ok(JSON.parse(fs.readFileSync(plain.storagePath, 'utf8')).entropy, 'Starts plaintext')

  const migrating = new Identity({ fileKey: FILE_KEY })
  migrating.storagePath = plain.storagePath
  assert.strictEqual(await migrating.load(), true, 'Plaintext file must still load')

  const after = JSON.parse(fs.readFileSync(plain.storagePath, 'utf8'))
  assert.strictEqual(after.entropy, undefined, 'Plaintext entropy removed by migration')
  assert.ok(after.entropyEnc, 'Encrypted entropy written by migration')

  const reloaded = new Identity({ fileKey: FILE_KEY })
  reloaded.storagePath = plain.storagePath
  assert.strictEqual(await reloaded.load(), true, 'Migrated file must load with the key')
  assert.strictEqual(reloaded.publicKeyHex, plain.publicKeyHex, 'Same identity after migration')

  fs.unlinkSync(plain.storagePath)
})

test('load() fails closed on a wrong file key', async () => {
  const identity = new Identity({ fileKey: FILE_KEY })
  identity.storagePath = path.join(getDataDir(), 'identity-wrongkey-' + Date.now() + '.json')
  await identity.create('Keyed User')

  const wrong = new Identity({ fileKey: OTHER_KEY })
  wrong.storagePath = identity.storagePath
  assert.strictEqual(await wrong.load(), false, 'Wrong key must not load')
  assert.strictEqual(wrong.keyPair, null)
  assert.strictEqual(wrong.bipEntropy, null)

  fs.unlinkSync(identity.storagePath)
})

test('load() fails closed when the file key is missing for an encrypted file', async () => {
  const identity = new Identity({ fileKey: FILE_KEY })
  identity.storagePath = path.join(getDataDir(), 'identity-nokey-' + Date.now() + '.json')
  await identity.create('Keyed User')

  const keyless = new Identity({ fileKey: null })
  keyless.storagePath = identity.storagePath
  assert.strictEqual(await keyless.load(), false, 'Encrypted file without key must not load')
  assert.strictEqual(keyless.keyPair, null)

  fs.unlinkSync(identity.storagePath)
})

test('Identity rejects a malformed file key', () => {
  assert.throws(() => new Identity({ fileKey: b4a.from('deadbeef', 'hex') }), /32 bytes/)
})
