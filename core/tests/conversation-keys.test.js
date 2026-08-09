/**
 * Unit tests for conversation-keys.js — v2 shared-secret key derivation.
 */

const { test } = require('node:test')
const assert = require('node:assert')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const { deriveDirectKey, deriveGroupKey } = require('../lib/conversation-keys')

function makeIdentity (seedByte) {
  return crypto.keyPair(b4a.alloc(32, seedByte))
}

const alice = makeIdentity(1)
const bob = makeIdentity(2)
const mallory = makeIdentity(3)
const aliceHex = b4a.toString(alice.publicKey, 'hex')
const bobHex = b4a.toString(bob.publicKey, 'hex')
const CONV = 'dm_' + 'ab'.repeat(16)

test('direct key is symmetric: both sides derive the identical key', () => {
  const kA = deriveDirectKey(alice, bobHex, CONV)
  const kB = deriveDirectKey(bob, aliceHex, CONV)
  assert.strictEqual(kA.byteLength, 32)
  assert.ok(b4a.equals(kA, kB), 'A→B and B→A must agree')
})

test('direct key binds the conversation id and the epoch', () => {
  const base = deriveDirectKey(alice, bobHex, CONV)
  const otherConv = deriveDirectKey(alice, bobHex, 'dm_' + 'cd'.repeat(16))
  const rotated = deriveDirectKey(alice, bobHex, CONV, 1)
  assert.ok(!b4a.equals(base, otherConv), 'Different conversation → different key')
  assert.ok(!b4a.equals(base, rotated), 'Different epoch → different key')
  assert.ok(b4a.equals(rotated, deriveDirectKey(bob, aliceHex, CONV, 1)), 'Rotated key stays symmetric')
})

test('a third party cannot derive the pair key from public keys alone', () => {
  const kAB = deriveDirectKey(alice, bobHex, CONV)
  // Mallory knows both public keys but holds neither secret — the closest
  // she can compute are her own ECDH keys with each party.
  const kMA = deriveDirectKey(mallory, aliceHex, CONV)
  const kMB = deriveDirectKey(mallory, bobHex, CONV)
  assert.ok(!b4a.equals(kAB, kMA), 'Mallory↔Alice must differ from Alice↔Bob')
  assert.ok(!b4a.equals(kAB, kMB), 'Mallory↔Bob must differ from Alice↔Bob')
})

test('direct key is not the legacy conversationId hash', () => {
  const legacy = crypto.data(b4a.from('zapp-enc:' + CONV))
  assert.ok(!b4a.equals(deriveDirectKey(alice, bobHex, CONV), legacy),
    'v2 must not collapse to the v1 public derivation')
})

test('direct key derivation fails closed on malformed inputs', () => {
  assert.throws(() => deriveDirectKey(alice, 'ff'.repeat(8), CONV), /bad peer public key/)
  assert.throws(() => deriveDirectKey({ publicKey: alice.publicKey, secretKey: b4a.alloc(32) }, bobHex, CONV), /bad identity keypair/)
  assert.throws(() => deriveDirectKey(null, bobHex, CONV), /bad identity keypair/)
})

test('group key depends only on groupId (+epoch), never a local conversation id', () => {
  const groupId = b4a.toString(crypto.randomBytes(32), 'hex')
  const k1 = deriveGroupKey(groupId)
  const k2 = deriveGroupKey(groupId)
  assert.strictEqual(k1.byteLength, 32)
  assert.ok(b4a.equals(k1, k2), 'Deterministic per groupId')
  assert.ok(!b4a.equals(k1, deriveGroupKey(b4a.toString(crypto.randomBytes(32), 'hex'))),
    'Different group → different key')
  assert.ok(!b4a.equals(k1, deriveGroupKey(groupId, 1)), 'Epoch rotates the key')
})

test('group key requires a full 32-byte groupId', () => {
  assert.throws(() => deriveGroupKey('abcd'), /bad groupId/)
  assert.throws(() => deriveGroupKey(null), /bad groupId/)
})
