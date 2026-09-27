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

function envelopeHarness () {
  const handler = Object.create(IPCHandler.prototype)
  const responses = []
  const events = []
  handler.sendResponse = (id, success, data, error) => responses.push({ id, success, data, error })
  handler.pushEvent = (type, payload) => events.push({ type, payload })
  handler.routeMessage = async (type, payload) => ({ echoed: type, payload })
  return { handler, responses, events }
}

test('a line that is valid JSON but not a request envelope is dropped with a controlled event', async () => {
  const { handler, responses, events } = envelopeHarness()

  for (const line of ['null', '42', '"text"', '[]', '{}', '{"id":7,"type":"identity.get"}']) {
    await assert.doesNotReject(handler._processLine(line))
  }

  assert.strictEqual(responses.length, 0)
  assert.strictEqual(events.length, 6)
  assert.ok(events.every(e => e.type === 'ipc.error' && e.payload.code === 'INVALID_ENVELOPE'))
})

test('unparsable IPC input reports MALFORMED_FRAME instead of rejecting', async () => {
  const { handler, responses, events } = envelopeHarness()
  await assert.doesNotReject(handler._processLine('{not json'))
  assert.strictEqual(responses.length, 0)
  assert.deepStrictEqual(events.map(e => e.payload.code), ['MALFORMED_FRAME'])
})

test('a request with an id but an invalid type or payload gets an error response', async () => {
  const { handler, responses } = envelopeHarness()

  await handler._processLine(JSON.stringify({ id: 'r1', type: 5 }))
  await handler._processLine(JSON.stringify({ id: 'r2', type: 'identity.get', payload: 'nope' }))
  await handler._processLine(JSON.stringify({ id: 'r3', type: 'identity.get', payload: [1] }))

  assert.deepStrictEqual(responses.map(r => [r.id, r.success, r.error.code]), [
    ['r1', false, 'INVALID_ENVELOPE'],
    ['r2', false, 'INVALID_ENVELOPE'],
    ['r3', false, 'INVALID_ENVELOPE']
  ])
})

test('a well-formed request is routed and a null payload defaults to an empty object', async () => {
  const { handler, responses } = envelopeHarness()
  await handler._processLine(JSON.stringify({ id: 'ok', type: 'identity.get', payload: null }))
  assert.deepStrictEqual(responses, [
    { id: 'ok', success: true, data: { echoed: 'identity.get', payload: {} }, error: null }
  ])
})

test('a response that cannot be written is downgraded to an error response', async () => {
  const { handler, responses } = envelopeHarness()
  let attempts = 0
  handler.sendResponse = (id, success, data, error) => {
    attempts++
    if (success) throw new Error('pipe closed')
    responses.push({ id, success, error })
  }
  await assert.doesNotReject(handler._processLine(JSON.stringify({ id: 'w', type: 'x.y' })))
  assert.strictEqual(attempts, 2)
  assert.deepStrictEqual(responses, [{ id: 'w', success: false, error: { code: 'RESPONSE_FAILED', message: 'Response could not be delivered' } }])
})

test('_handleIPCData never surfaces an unhandled rejection for a bad frame', async () => {
  const { handler, events } = envelopeHarness()
  handler._recvBuf = Buffer.alloc(0)
  handler._skippingOversizedFrame = false
  handler._maxRecvBufSize = 1024

  const rejections = []
  const onRejection = reason => rejections.push(reason)
  process.on('unhandledRejection', onRejection)
  try {
    handler._handleIPCData(Buffer.from('null\n{"id":"a","type":"identity.get"}\n'))
    await new Promise(resolve => setImmediate(resolve))
  } finally {
    process.off('unhandledRejection', onRejection)
  }

  assert.deepStrictEqual(rejections, [])
  assert.deepStrictEqual(events.map(e => e.payload.code), ['INVALID_ENVELOPE'])
})

function identityHarness ({ wipeFailure = null, installFailure = null } = {}) {
  const handler = Object.create(IPCHandler.prototype)
  const calls = []
  handler.readReceiptsEnabled = false
  handler.ensureBlindMirror = async () => { calls.push('mirror') }
  handler.p2pManager = {
    stop: async () => { calls.push('stop') },
    start: async keyPair => { calls.push('start:' + keyPair) }
  }
  handler.chatStore = {
    clearAll: async () => {
      calls.push('chat.clear')
      if (wipeFailure) throw wipeFailure
    }
  }
  handler.contactStore = { clearAll: async () => { calls.push('contacts.clear') } }
  handler.identity = {
    keyPair: 'old-key',
    publicKeyHex: 'old-key',
    displayName: 'Old',
    exportMnemonic: () => 'words',
    create: async displayName => {
      calls.push('install')
      if (installFailure) throw installFailure
      handler.identity.keyPair = 'new-key'
      handler.identity.publicKeyHex = 'new-key'
      handler.identity.displayName = displayName
    },
    restoreFromMnemonic: async (phrase, displayName) => {
      calls.push('install')
      handler.identity.keyPair = 'restored-key'
      handler.identity.publicKeyHex = GOLDEN_PUBLIC_KEY
      handler.identity.displayName = displayName
    }
  }
  return { handler, calls }
}

const GOLDEN_PHRASE = 'abandon '.repeat(23) + 'art'
const GOLDEN_PUBLIC_KEY = '7afa7190d9f5daeaa45d9650ed3ce7c0973bb0e35f7361bf858389a8cf1c3f3c'

test('identity.create wipes both stores before installing and starting the new key', async () => {
  const { handler, calls } = identityHarness()
  const result = await handler.handleIdentity('create', { displayName: 'New' })
  assert.deepStrictEqual(calls, ['stop', 'chat.clear', 'contacts.clear', 'install', 'start:new-key', 'mirror'])
  assert.deepStrictEqual(result, { publicKey: 'new-key', displayName: 'New', seedPhrase: 'words' })
  assert.strictEqual(handler.readReceiptsEnabled, true)
})

test('a failed wipe aborts the identity change and brings the previous identity back online', async () => {
  const failure = Object.assign(new Error('simulated permission failure'), { code: 'EACCES' })
  const { handler, calls } = identityHarness({ wipeFailure: failure })

  await assert.rejects(handler.handleIdentity('create', { displayName: 'New' }), { code: 'EACCES' })

  // The contact store is still attempted, the new key is never installed, and
  // the old key's transport is restored so the caller can retry.
  assert.deepStrictEqual(calls, ['stop', 'chat.clear', 'contacts.clear', 'start:old-key', 'mirror'])
  assert.strictEqual(handler.identity.publicKeyHex, 'old-key')
  assert.strictEqual(handler.readReceiptsEnabled, false, 'the previous preference is restored')
})

test('a failed install after a successful wipe restarts the previous identity', async () => {
  const { handler, calls } = identityHarness({ installFailure: new Error('disk full') })
  await assert.rejects(handler.handleIdentity('create', { displayName: 'New' }), /disk full/)
  assert.deepStrictEqual(calls, ['stop', 'chat.clear', 'contacts.clear', 'install', 'start:old-key', 'mirror'])
})

test('restore rejects a malformed phrase before anything destructive happens', async () => {
  const { handler, calls } = identityHarness()
  await assert.rejects(
    handler.handleMigration('restore_from_seed_phrase', { seedPhrase: 'not a phrase', displayName: 'x' }),
    /24 words/
  )
  assert.deepStrictEqual(calls, [])
  assert.strictEqual(handler.identity.publicKeyHex, 'old-key')
})

test('restore of a different seed wipes both stores; the same seed is idempotent', async () => {
  const { handler, calls } = identityHarness()

  const first = await handler.handleMigration('restore_from_seed_phrase', { seedPhrase: GOLDEN_PHRASE, displayName: 'Golden' })
  assert.deepStrictEqual(first, { publicKey: GOLDEN_PUBLIC_KEY, displayName: 'Golden' })
  assert.deepStrictEqual(calls, ['stop', 'chat.clear', 'contacts.clear', 'install', 'start:restored-key', 'mirror'])

  calls.length = 0
  const again = await handler.handleMigration('restore_from_seed_phrase', { seedPhrase: GOLDEN_PHRASE, displayName: 'Renamed' })
  assert.deepStrictEqual(again, { publicKey: GOLDEN_PUBLIC_KEY, displayName: 'Golden' })
  assert.deepStrictEqual(calls, ['mirror'], 'a same-seed republish must not stop, wipe, or reinstall')
})

test('contacts.update rejects malformed payloads before touching the store', async () => {
  const handler = Object.create(IPCHandler.prototype)
  let touched = false
  handler.contactStore = { updateContact: async () => { touched = true } }

  await assert.rejects(handler.handleContacts('update', { publicKey: ownKey, updates: null }), /Invalid updates/)
  await assert.rejects(handler.handleContacts('update', { publicKey: ownKey, updates: ['name'] }), /Invalid updates/)
  await assert.rejects(handler.handleContacts('update', { publicKey: ownKey, updates: { name: 42 } }), /Invalid name/)
  await assert.rejects(handler.handleContacts('update', { publicKey: 'short', updates: { name: 'ok' } }), /Invalid publicKey/)
  assert.strictEqual(touched, false)

  assert.deepStrictEqual(await handler.handleContacts('update', { publicKey: ownKey, updates: { name: 'ok' } }), { success: true })
  assert.strictEqual(touched, true)
})

test('message.send and media.send_message forward the quoted message type to the store', async () => {
  const handler = Object.create(IPCHandler.prototype)
  const stored = []
  handler.identity = { publicKeyHex: ownKey, displayName: 'Me' }
  handler.chatStore = {
    getConversation: async () => ({ id: 'conversation', type: 'group' }),
    addMessage: async (conversationId, data) => { stored.push(data); return { id: 'persisted', ...data } },
    markRelayedByIds: () => []
  }
  handler.p2pManager = { sendToConversationDurably: async () => ({ sent: true, relay: 'pending' }) }
  handler.pushEvent = () => {}
  handler.mediaTransfer = null
  handler.mediaStore = {}
  handler._mediaCoreDescriptor = async () => ({})

  const reply = { replyToId: 'quoted', replyToSenderName: 'alice', replyToContent: 'beach', replyToContentType: 'image/jpeg' }
  await handler.handleMessage('send', { conversationId: 'conversation', content: 'nice', ...reply })
  await handler.handleMedia('send_message', { conversationId: 'conversation', contentType: 'image/jpeg', mediaId: 'ff'.repeat(32), ...reply })
  await handler.handleMessage('send', { conversationId: 'conversation', content: 'plain' })

  assert.strictEqual(stored.length, 3)
  assert.strictEqual(stored[0].replyToContentType, 'image/jpeg')
  assert.strictEqual(stored[1].replyToContentType, 'image/jpeg')
  assert.strictEqual(stored[2].replyToContentType, null)
})
