/**
 * Unit tests for mnemonic.js
 */

const { test } = require('node:test')
const assert = require('node:assert')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const {
  entropyToMnemonic,
  mnemonicToEntropy,
  mnemonicToEd25519Seed,
  validateMnemonic,
} = require('../lib/mnemonic')

test('entropyToMnemonic generates 24-word phrase', () => {
  const entropy = crypto.randomBytes(32)
  const mnemonic = entropyToMnemonic(entropy)

  const words = mnemonic.split(' ')
  assert.strictEqual(words.length, 24, 'Mnemonic should have 24 words')

  words.forEach(word => {
    assert.strictEqual(word, word.toLowerCase(), 'Words should be lowercase')
  })
})

test('mnemonicToEntropy round-trips entropyToMnemonic', () => {
  for (let i = 0; i < 10; i++) {
    const entropy = crypto.randomBytes(32)
    const mnemonic = entropyToMnemonic(entropy)
    const recovered = mnemonicToEntropy(mnemonic)
    assert.ok(b4a.equals(entropy, recovered), `Entropy ${i} should be recoverable`)
  }
})

test('mnemonicToEd25519Seed is deterministic for a given mnemonic', () => {
  const entropy = crypto.randomBytes(32)
  const mnemonic = entropyToMnemonic(entropy)
  const a = mnemonicToEd25519Seed(mnemonic)
  const b = mnemonicToEd25519Seed(mnemonic)
  assert.ok(b4a.equals(a, b), 'Same mnemonic must yield same Ed25519 seed')
  assert.strictEqual(a.byteLength, 32, 'Ed25519 seed must be 32 bytes')
})

test('mnemonicToEd25519Seed differs from raw entropy (PBKDF2 + SLIP-0010 applied)', () => {
  // Guards against accidentally reverting to the pre-2026-05 "raw entropy as
  // seed" behaviour. The two MUST differ for the new derivation to be in effect.
  const entropy = crypto.randomBytes(32)
  const mnemonic = entropyToMnemonic(entropy)
  const ed25519Seed = mnemonicToEd25519Seed(mnemonic)
  assert.ok(!b4a.equals(entropy, ed25519Seed), 'Ed25519 seed must NOT equal raw entropy')
})

test('mnemonicToEd25519Seed is case- and whitespace-insensitive', () => {
  const entropy = crypto.randomBytes(32)
  const mnemonic = entropyToMnemonic(entropy)
  const upperMnemonic = mnemonic.toUpperCase()
  const spacedMnemonic = '  ' + mnemonic.split(' ').join('   ') + '  '

  const seed1 = mnemonicToEd25519Seed(mnemonic)
  const seed2 = mnemonicToEd25519Seed(upperMnemonic)
  const seed3 = mnemonicToEd25519Seed(spacedMnemonic)

  assert.ok(b4a.equals(seed1, seed2), 'Uppercase mnemonic must yield the same seed')
  assert.ok(b4a.equals(seed1, seed3), 'Whitespace-padded mnemonic must yield the same seed')
})

test('mnemonicToEd25519Seed applies the passphrase per BIP-39', () => {
  const entropy = crypto.randomBytes(32)
  const mnemonic = entropyToMnemonic(entropy)
  const noPass = mnemonicToEd25519Seed(mnemonic)
  const withPass = mnemonicToEd25519Seed(mnemonic, 'TREZOR')
  assert.ok(!b4a.equals(noPass, withPass), 'Different passphrases must yield different seeds')
})

test('validateMnemonic accepts valid phrases', () => {
  const entropy = crypto.randomBytes(32)
  const mnemonic = entropyToMnemonic(entropy)
  assert.ok(validateMnemonic(mnemonic), 'Valid mnemonic should pass validation')
})

test('validateMnemonic rejects invalid phrases', () => {
  assert.strictEqual(validateMnemonic('word word word'), false, 'Wrong word count rejected')
  assert.strictEqual(validateMnemonic('invalid '.repeat(24).trim()), false, 'Invalid words rejected')
  assert.strictEqual(validateMnemonic(''), false, 'Empty rejected')
})

test('mnemonicToEntropy throws for wrong word count', () => {
  try {
    mnemonicToEntropy('word word word')
    assert.fail('Should throw for wrong word count')
  } catch (err) {
    assert.ok(err.message.includes('24 words'), 'Error should mention 24 words')
  }
})

test('mnemonicToEntropy throws for unknown words', () => {
  try {
    mnemonicToEntropy('invalidword '.repeat(24).trim())
    assert.fail('Should throw for unknown words')
  } catch (err) {
    assert.ok(err.message.includes('Unknown word'), 'Error should mention unknown word')
  }
})

test('mnemonicToEntropy throws for invalid checksum', () => {
  const entropy = crypto.randomBytes(32)
  const mnemonic = entropyToMnemonic(entropy)
  const words = mnemonic.split(' ')
  // Replace last word with a different valid word — flips the checksum.
  words[23] = words[23] === 'abandon' ? 'ability' : 'abandon'
  const corrupted = words.join(' ')

  try {
    mnemonicToEntropy(corrupted)
    assert.fail('Should throw for invalid checksum')
  } catch (err) {
    assert.ok(err.message.includes('checksum'), 'Error should mention checksum')
  }
})

test('entropyToMnemonic throws for invalid entropy size', () => {
  try {
    entropyToMnemonic(b4a.alloc(16))
    assert.fail('Should throw for invalid entropy size')
  } catch (err) {
    assert.ok(err.message.includes('32-byte'), 'Error should mention 32-byte requirement')
  }
})

test('entropyToMnemonic throws for non-buffer input', () => {
  try {
    entropyToMnemonic('not a buffer')
    assert.fail('Should throw for non-buffer input')
  } catch (err) {
    assert.ok(err.message.includes('32-byte'), 'Error should mention buffer requirement')
  }
})
