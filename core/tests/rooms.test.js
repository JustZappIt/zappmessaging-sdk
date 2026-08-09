/**
 * Unit tests for rooms.js
 */

const { test } = require('node:test')
const assert = require('node:assert')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const {
  deriveDirectChatTopic,
  deriveGroupChatTopic,
  derivePersonalTopic,
  generateGroupId
} = require('../lib/rooms')

test('deriveDirectChatTopic generates 32-byte topic', () => {
  const keyA = b4a.toString(crypto.randomBytes(32), 'hex')
  const keyB = b4a.toString(crypto.randomBytes(32), 'hex')
  const topic = deriveDirectChatTopic(keyA, keyB)
  assert.ok(topic, 'Topic should be generated')
  assert.strictEqual(topic.byteLength, 32, 'Topic should be 32 bytes')
})

test('deriveDirectChatTopic is order-independent', () => {
  const keyA = b4a.toString(crypto.randomBytes(32), 'hex')
  const keyB = b4a.toString(crypto.randomBytes(32), 'hex')
  
  const topic1 = deriveDirectChatTopic(keyA, keyB)
  const topic2 = deriveDirectChatTopic(keyB, keyA)
  
  assert.ok(b4a.equals(topic1, topic2), 'Topic should be same regardless of key order')
})

test('deriveDirectChatTopic is deterministic', () => {
  const keyA = b4a.toString(crypto.randomBytes(32), 'hex')
  const keyB = b4a.toString(crypto.randomBytes(32), 'hex')
  
  const topic1 = deriveDirectChatTopic(keyA, keyB)
  const topic2 = deriveDirectChatTopic(keyA, keyB)
  
  assert.ok(b4a.equals(topic1, topic2), 'Same keys should produce same topic')
})

test('deriveGroupChatTopic generates 32-byte topic', () => {
  const groupId = b4a.toString(crypto.randomBytes(32), 'hex')
  const topic = deriveGroupChatTopic(groupId)
  assert.ok(topic, 'Topic should be generated')
  assert.strictEqual(topic.byteLength, 32, 'Topic should be 32 bytes')
})

test('deriveGroupChatTopic is deterministic', () => {
  const groupId = b4a.toString(crypto.randomBytes(32), 'hex')
  const topic1 = deriveGroupChatTopic(groupId)
  const topic2 = deriveGroupChatTopic(groupId)
  assert.ok(b4a.equals(topic1, topic2), 'Same group ID should produce same topic')
})

test('derivePersonalTopic generates 32-byte topic from Buffer', () => {
  const publicKey = crypto.randomBytes(32)
  const topic = derivePersonalTopic(publicKey)
  assert.ok(topic, 'Topic should be generated')
  assert.strictEqual(topic.byteLength, 32, 'Topic should be 32 bytes')
})

test('derivePersonalTopic generates 32-byte topic from hex string', () => {
  const publicKey = b4a.toString(crypto.randomBytes(32), 'hex')
  const topic = derivePersonalTopic(publicKey)
  assert.ok(topic, 'Topic should be generated')
  assert.strictEqual(topic.byteLength, 32, 'Topic should be 32 bytes')
})

test('derivePersonalTopic is deterministic', () => {
  const publicKey = crypto.randomBytes(32)
  const topic1 = derivePersonalTopic(publicKey)
  const topic2 = derivePersonalTopic(publicKey)
  assert.ok(b4a.equals(topic1, topic2), 'Same public key should produce same topic')
})

test('derivePersonalTopic produces same result for Buffer and hex', () => {
  const publicKey = crypto.randomBytes(32)
  const publicKeyHex = b4a.toString(publicKey, 'hex')
  
  const topic1 = derivePersonalTopic(publicKey)
  const topic2 = derivePersonalTopic(publicKeyHex)
  
  assert.ok(b4a.equals(topic1, topic2), 'Buffer and hex should produce same topic')
})

test('generateGroupId returns 64-character hex string', () => {
  const groupId = generateGroupId()
  assert.ok(groupId, 'Group ID should be generated')
  assert.strictEqual(groupId.length, 64, 'Group ID should be 64 hex characters (32 bytes)')
  assert.ok(/^[0-9a-f]+$/.test(groupId), 'Group ID should be valid hex')
})

test('generateGroupId produces unique IDs', () => {
  const id1 = generateGroupId()
  const id2 = generateGroupId()
  assert.notStrictEqual(id1, id2, 'Each call should produce a unique ID')
})
