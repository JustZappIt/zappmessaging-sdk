const { test } = require('node:test')
const assert = require('node:assert')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const { getInboundPushTopics } = require('../lib/push-topics')

test('recipient inbound topic matches the sender writer discovery key', () => {
  const conversationId = 'dm-1'
  const writerPublicKey = b4a.toString(crypto.keyPair().publicKey, 'hex')
  const senderCoreKey = crypto.keyPair().publicKey
  const senderDiscoveryKey = crypto.discoveryKey(senderCoreKey)
  const remoteCores = new Map([
    [conversationId, new Map([
      [writerPublicKey, { key: senderCoreKey, discoveryKey: senderDiscoveryKey }]
    ])]
  ])

  const topics = getInboundPushTopics(remoteCores, conversationId)

  assert.deepStrictEqual(topics, [{
    topic: b4a.toString(senderDiscoveryKey, 'hex'),
    writerPublicKey
  }])
})

test('conversations awaiting a remote writer have no inbound topics', () => {
  assert.deepStrictEqual(getInboundPushTopics(new Map(), 'dm-1'), [])
})
