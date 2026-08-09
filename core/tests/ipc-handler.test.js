const { test } = require('node:test')
const assert = require('node:assert')
const { validateDirectParticipant } = require('../lib/direct-recipient')
const { IPCHandler } = require('../lib/ipc-handler')

const ownKey = 'aa'.repeat(32)
const peerKey = 'bb'.repeat(32)

test('direct participant validation normalizes a remote key', () => {
  assert.strictEqual(validateDirectParticipant(ownKey, [`0x${peerKey.toUpperCase()}`]), peerKey)
})

test('direct participant validation rejects the local identity with a stable code', () => {
  assert.throws(
    () => validateDirectParticipant(ownKey, [`0x${ownKey.toUpperCase()}`]),
    error => error.code === 'OWN_PUBLIC_KEY'
  )
})

test('direct participant validation distinguishes missing and malformed keys', () => {
  assert.throws(
    () => validateDirectParticipant(ownKey, []),
    error => error.code === 'MISSING_PARTICIPANT'
  )
  assert.throws(
    () => validateDirectParticipant(ownKey, ['not-a-public-key']),
    error => error.code === 'INVALID_PUBLIC_KEY'
  )
})

test('conversation creation rejects unsupported types with a stable code', async () => {
  const handler = Object.create(IPCHandler.prototype)
  await assert.rejects(
    handler.handleConversation('create', { type: 'unsupported', participants: [] }),
    error => error.code === 'UNSUPPORTED_CONVERSATION_TYPE'
  )
})

test('outgoing status uses relay acknowledgement instead of a socket write', () => {
  const handler = Object.create(IPCHandler.prototype)
  const relayed = []
  const events = []
  handler.chatStore = {
    markRelayedByIds: (conversationId, messageIds) => {
      relayed.push([conversationId, messageIds])
      return messageIds
    }
  }
  handler.pushEvent = (type, payload) => events.push([type, payload])

  const relayedMessage = { id: 'relayed-message', status: 'queued' }
  handler._recordOutgoingStatus(
    'conversation',
    relayedMessage,
    { sent: false, relay: 'request_acknowledged' }
  )

  assert.strictEqual(relayedMessage.status, 'sent')
  assert.deepStrictEqual(relayed, [['conversation', ['relayed-message']]])
  assert.strictEqual(events[0][1].status, 'sent')

  const liveOnlyMessage = { id: 'live-only-message', status: 'queued' }
  handler._recordOutgoingStatus(
    'conversation',
    liveOnlyMessage,
    { sent: true, relay: 'pending' }
  )

  assert.strictEqual(liveOnlyMessage.status, 'queued')
  assert.strictEqual(relayed.length, 1)
  assert.strictEqual(events[1][1].status, 'queued')
})

test('relay status persistence failure does not fail an otherwise durable send', () => {
  const handler = Object.create(IPCHandler.prototype)
  const events = []
  handler.chatStore = {
    markRelayedByIds: () => {
      throw new Error('disk full')
    }
  }
  handler.pushEvent = (type, payload) => events.push([type, payload])

  const message = { id: 'relayed-message', status: 'queued' }
  const status = handler._recordOutgoingStatus(
    'conversation',
    message,
    { sent: false, relay: 'request_acknowledged' }
  )

  assert.strictEqual(status, 'sent')
  assert.strictEqual(message.status, 'sent')
  assert.deepStrictEqual(events, [[
    'message.status',
    {
      messageId: 'relayed-message',
      conversationId: 'conversation',
      status: 'sent'
    }
  ]])
})
