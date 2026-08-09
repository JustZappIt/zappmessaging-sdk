/**
 * Identity golden vectors: the cross-platform parity oracle.
 *
 * Chat identity is derived from the BIP-39 wallet seed *inside the JS worklet*
 * (core/lib/identity.js -> core/lib/mnemonic.js), never in Kotlin or Swift.
 * Both the Android and iOS worklet bundles are packed from the same
 * core/index.js, so the derivation is identical by construction.
 *
 * "By construction" is not a test. This file makes it one.
 *
 * Run:   npx bare test/identity-golden-vectors.js
 *
 * The printed pubkeys are the assertions the Android and iOS apps must each
 * reproduce for the same phrase. If a platform disagrees, the bug is in that
 * platform's bundle/addon linkage, not in the protocol.
 *
 * Derivation under test:
 *   phrase -> NFKD -> PBKDF2-HMAC-SHA512(2048, 64B, salt="mnemonic")   [BIP-39]
 *          -> HMAC-SHA512(key="ed25519 seed")                          [SLIP-0010]
 *          -> first 32 bytes = Ed25519 seed
 *          -> hypercore-crypto.keyPair()                               [sodium crypto_sign_seed_keypair]
 *
 * Vectors are standard BIP-39 test phrases. They are NOT real wallets.
 */

const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const mnemonic = require('../core/lib/mnemonic')

// Standard BIP-39 (Trezor) test vectors, 24-word / 256-bit entropy only —
// Zapp rejects anything else, matching the Zcash wallet seed length.
// Public, well-known phrases. They hold no funds.
const VECTORS = [
  {
    name: 'all-zero entropy (0x00 * 32)',
    phrase: 'abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon '
          + 'abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon art'
  },
  {
    name: 'all-ones entropy (0xff * 32)',
    phrase: 'zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo vote'
  },
  {
    name: 'legal winner (0x7f * 32)',
    phrase: 'legal winner thank year wave sausage worth useful legal winner thank year '
          + 'wave sausage worth useful legal winner thank year wave sausage worth title'
  },
  {
    name: 'letter advice (0x80 * 32)',
    phrase: 'letter advice cage absurd amount doctor acoustic avoid letter advice cage absurd '
          + 'amount doctor acoustic avoid letter advice cage absurd amount doctor acoustic bless'
  }
]

let failures = 0

console.log('')
console.log('  Zapp chat-identity golden vectors')
console.log('  =================================')
console.log('  Every platform must reproduce these exactly.')
console.log('')

for (const v of VECTORS) {
  let seed, keyPair
  try {
    seed = mnemonic.mnemonicToEd25519Seed(v.phrase)
    keyPair = crypto.keyPair(seed)
  } catch (err) {
    console.log(`  FAIL  ${v.name}`)
    console.log(`        ${err.message}`)
    failures++
    continue
  }

  const seedHex = b4a.toString(seed, 'hex')
  const pubHex = b4a.toString(keyPair.publicKey, 'hex')

  // Structural invariants. A silently-wrong derivation usually still produces
  // *a* key, so check the shape as well as printing the value.
  if (seed.byteLength !== 32) {
    console.log(`  FAIL  ${v.name}: ed25519 seed is ${seed.byteLength}B, expected 32B`)
    failures++
    continue
  }
  if (keyPair.publicKey.byteLength !== 32) {
    console.log(`  FAIL  ${v.name}: pubkey is ${keyPair.publicKey.byteLength}B, expected 32B`)
    failures++
    continue
  }

  // Determinism: re-derive and compare. Catches accidental randomness.
  const again = crypto.keyPair(mnemonic.mnemonicToEd25519Seed(v.phrase))
  if (!b4a.equals(again.publicKey, keyPair.publicKey)) {
    console.log(`  FAIL  ${v.name}: derivation is NOT deterministic`)
    failures++
    continue
  }

  console.log(`  ${v.name}`)
  console.log(`    phrase : ${v.phrase}`)
  console.log(`    seed   : ${seedHex}`)
  console.log(`    pubkey : ${pubHex}`)
  console.log('')
}

// A bad checksum must throw, not silently derive. If checksum validation is
// skipped, a typo'd recovery phrase yields a valid-looking but WRONG identity —
// the user would be reachable at an address nobody has.
// 24x"abandon" is well-formed and correct length; only the checksum is wrong
// (the zero-entropy phrase must end in "art").
const badChecksum = Array(24).fill('abandon').join(' ')
try {
  mnemonic.mnemonicToEd25519Seed(badChecksum)
  console.log('  FAIL  bad checksum was accepted (should have thrown)')
  failures++
} catch (err) {
  console.log('  ok    bad checksum rejected')
}

// 12-word phrases must be rejected. Zapp is 24-word only (256-bit entropy),
// matching the Zcash seed. A 12-word phrase silently accepted would derive a
// different identity than the wallet it is supposed to be bound to.
try {
  mnemonic.mnemonicToEd25519Seed('abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about')
  console.log('  FAIL  12-word phrase was accepted (Zapp is 24-word only)')
  failures++
} catch (err) {
  console.log('  ok    12-word phrase rejected (24-word only)')
}

// Passphrase must change the key. If it is ignored, two users with the same
// seed but different passphrases would collide onto one identity.
const base = crypto.keyPair(mnemonic.mnemonicToEd25519Seed(VECTORS[0].phrase))
const withPass = crypto.keyPair(mnemonic.mnemonicToEd25519Seed(VECTORS[0].phrase, 'TREZOR'))
if (b4a.equals(base.publicKey, withPass.publicKey)) {
  console.log('  FAIL  passphrase is ignored in derivation')
  failures++
} else {
  console.log('  ok    passphrase changes the derived key')
}

console.log('')
if (failures > 0) {
  console.log(`  ${failures} FAILURE(S)`)
  process.exit(1)
}
console.log('  all checks passed')
console.log('')
