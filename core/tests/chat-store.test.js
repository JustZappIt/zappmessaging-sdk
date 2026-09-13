/**
 * Unit tests for chat-store.js
 */

const { test } = require('node:test')
const assert = require('node:assert')
const path = require('path')
const fs = require('fs')
const { ChatStore } = require('../lib/chat-store')
const { getDataDir } = require('../lib/storage')

test('ChatStore initializes correctly', () => {
  const store = new ChatStore()
  
  assert.ok(store, 'Store should be created')
  assert.ok(store.conversations instanceof Map, 'Conversations should be a Map')
  assert.ok(store.storagePath.includes('chats'), 'Storage path should include chats')
})

test('createConversation creates direct conversation', async () => {
  const store = new ChatStore()
  const participantIds = ['pubkey1', 'pubkey2']
  
  const conv = await store.createConversation('direct', participantIds, { displayName: 'Test Chat' })
  
  assert.ok(conv, 'Conversation should be created')
  assert.strictEqual(conv.type, 'direct', 'Type should be direct')
  assert.strictEqual(conv.displayName, 'Test Chat', 'Display name should match')
  assert.deepEqual(conv.participantIds, participantIds, 'Participants should match')
  
  // Cleanup
  await store.deleteConversation(conv.id)
})

test('createConversation creates group conversation', async () => {
  const store = new ChatStore()
  const participantIds = ['pubkey1', 'pubkey2', 'pubkey3']
  const groupId = 'test-group-id'
  
  const conv = await store.createConversation('group', participantIds, { 
    groupId,
    displayName: 'Test Group',
    creatorKey: 'pubkey1'
  })
  
  assert.ok(conv, 'Conversation should be created')
  assert.strictEqual(conv.type, 'group', 'Type should be group')
  assert.strictEqual(conv.groupId, groupId, 'Group ID should match')
  assert.strictEqual(conv.creatorKey, 'pubkey1', 'Creator key should match')
  
  // Cleanup
  await store.deleteConversation(conv.id)
})

test('createConversation rejects unsupported conversation types', async () => {
  const store = new ChatStore()
  await assert.rejects(
    store.createConversation('unsupported', []),
    /Unsupported conversation type/
  )
})

test('createConversation deduplicates direct chats', async () => {
  const store = new ChatStore()
  const participantIds = ['pubkey1', 'pubkey2']
  
  const conv1 = await store.createConversation('direct', participantIds)
  const conv2 = await store.createConversation('direct', participantIds)
  
  assert.strictEqual(conv1.id, conv2.id, 'Should return same conversation')
  
  // Cleanup
  await store.deleteConversation(conv1.id)
})

test('listConversations returns sorted conversations', async () => {
  const store = new ChatStore()
  
  const conv1 = await store.createConversation('direct', ['p1'])
  const conv2 = await store.createConversation('direct', ['p2'])
  
  // Add messages to set timestamps
  await store.addMessage(conv1.id, { senderId: 'p1', content: 'First' })
  await new Promise(r => setTimeout(r, 10))
  await store.addMessage(conv2.id, { senderId: 'p2', content: 'Second' })
  
  const list = await store.listConversations()
  
  assert.ok(Array.isArray(list), 'Should return array')
  assert.strictEqual(list[0].id, conv2.id, 'Most recent should be first')
  
  // Cleanup
  await store.deleteConversation(conv1.id)
  await store.deleteConversation(conv2.id)
})

test('getConversation retrieves conversation by ID', async () => {
  const store = new ChatStore()
  
  const created = await store.createConversation('direct', ['p1'])
  const retrieved = await store.getConversation(created.id)
  
  assert.deepEqual(retrieved, created, 'Retrieved conversation should match')
  
  // Cleanup
  await store.deleteConversation(created.id)
})

test('getConversation returns null for non-existent ID', async () => {
  const store = new ChatStore()
  
  const result = await store.getConversation('non-existent')
  assert.strictEqual(result, null, 'Should return null')
})

test('updateConversation updates fields', async () => {
  const store = new ChatStore()
  
  const conv = await store.createConversation('direct', ['p1'])
  const updated = await store.updateConversation(conv.id, { displayName: 'Updated Name' })
  
  assert.strictEqual(updated.displayName, 'Updated Name', 'Display name should be updated')
  
  // Cleanup
  await store.deleteConversation(conv.id)
})

test('deleteConversation removes conversation and messages', async () => {
  const store = new ChatStore()
  
  const conv = await store.createConversation('direct', ['p1'])
  await store.addMessage(conv.id, { senderId: 'p1', content: 'Test' })
  
  const deleted = await store.deleteConversation(conv.id)
  
  assert.strictEqual(deleted, true, 'Should return true')
  assert.strictEqual(store.conversations.has(conv.id), false, 'Conversation should be removed')
  
  const messagesPath = path.join(store.storagePath, `${conv.id}.json`)
  assert.strictEqual(fs.existsSync(messagesPath), false, 'Messages file should be deleted')
})

test('addMessage adds message to conversation', async () => {
  const store = new ChatStore()
  
  const conv = await store.createConversation('direct', ['p1'])
  const message = await store.addMessage(conv.id, {
    senderId: 'p1',
    senderName: 'User 1',
    content: 'Hello!',
    isFromMe: false
  })
  
  assert.ok(message, 'Message should be created')
  assert.ok(message.id, 'Message should have ID')
  assert.strictEqual(message.content, 'Hello!', 'Content should match')
  assert.strictEqual(message.senderId, 'p1', 'Sender ID should match')
  
  // Cleanup
  await store.deleteConversation(conv.id)
})

test('addMessage updates conversation last message', async () => {
  const store = new ChatStore()
  
  const conv = await store.createConversation('direct', ['p1'])
  await store.addMessage(conv.id, { senderId: 'p1', content: 'Test message' })
  
  const updated = await store.getConversation(conv.id)
  assert.strictEqual(updated.lastMessage, 'Test message', 'Last message should be updated')
  assert.ok(updated.lastMessageTimestamp, 'Timestamp should be set')
  
  // Cleanup
  await store.deleteConversation(conv.id)
})

test('addMessage handles media messages', async () => {
  const store = new ChatStore()
  
  const conv = await store.createConversation('direct', ['p1'])
  await store.addMessage(conv.id, {
    senderId: 'p1',
    content: '',
    contentType: 'image/jpeg',
    mediaId: 'hash123'
  })
  
  const updated = await store.getConversation(conv.id)
  assert.strictEqual(updated.lastMessage, '[Photo]', 'Should show [Photo] for images')
  
  // Cleanup
  await store.deleteConversation(conv.id)
})

test('addMessage clamps hostile media metadata before persistence', async () => {
  const store = new ChatStore()
  const conv = await store.createConversation('direct', ['hostile-peer'])

  const message = await store.addMessage(conv.id, {
    senderId: 'hostile-peer',
    contentType: 'image/jpeg',
    mediaSize: 1e19,
    mediaWidth: -50,
    mediaHeight: 'not-a-number'
  })

  assert.strictEqual(message.mediaSize, 0x7fffffff)
  assert.strictEqual(message.mediaWidth, 0)
  assert.strictEqual(message.mediaHeight, null)

  const persisted = await store.getMessages(conv.id)
  assert.strictEqual(persisted[0].mediaSize, 0x7fffffff)
  assert.strictEqual(persisted[0].mediaWidth, 0)
  assert.strictEqual(persisted[0].mediaHeight, null)

  await store.deleteConversation(conv.id)
})

test('getMessages retrieves messages', async () => {
  const store = new ChatStore()
  
  const conv = await store.createConversation('direct', ['p1'])
  await store.addMessage(conv.id, { senderId: 'p1', content: 'Message 1' })
  await store.addMessage(conv.id, { senderId: 'p1', content: 'Message 2' })
  
  const messages = await store.getMessages(conv.id)
  
  assert.strictEqual(messages.length, 2, 'Should return 2 messages')
  assert.strictEqual(messages[0].content, 'Message 1', 'First message should match')
  
  // Cleanup
  await store.deleteConversation(conv.id)
})

test('getMessages respects limit', async () => {
  const store = new ChatStore()
  
  const conv = await store.createConversation('direct', ['p1'])
  for (let i = 0; i < 10; i++) {
    await store.addMessage(conv.id, { senderId: 'p1', content: `Message ${i}` })
  }
  
  const messages = await store.getMessages(conv.id, 5)
  
  assert.strictEqual(messages.length, 5, 'Should return only 5 messages')
  assert.strictEqual(messages[0].content, 'Message 5', 'Should return last 5 messages')
  
  // Cleanup
  await store.deleteConversation(conv.id)
})

test('updateMediaPath updates all matching messages', async () => {
  const store = new ChatStore()
  
  const conv = await store.createConversation('direct', ['p1'])
  await store.addMessage(conv.id, { senderId: 'p1', mediaId: 'hash123' })
  
  store.updateMediaPath('hash123', '/path/to/media.jpg')
  
  const messages = await store.getMessages(conv.id)
  assert.strictEqual(messages[0].mediaLocalPath, '/path/to/media.jpg', 'Media path should be updated')
  
  // Cleanup
  await store.deleteConversation(conv.id)
})

test('outgoing delivery status persists monotonically', async () => {
  const store = new ChatStore()
  const conv = await store.createConversation('direct', ['peer'])
  const first = await store.addMessage(conv.id, {
    senderId: 'me',
    content: 'First',
    isFromMe: true
  })
  const second = await store.addMessage(conv.id, {
    senderId: 'me',
    content: 'Second',
    isFromMe: true
  })

  assert.strictEqual(first.status, 'queued')
  assert.deepEqual(store.getPendingOutgoingMessages(conv.id).map(message => message.id), [first.id, second.id])
  assert.deepEqual(store.markRelayedByIds(conv.id, [first.id]), [first.id])
  assert.deepEqual(store.getPendingOutgoingMessages(conv.id).map(message => message.id), [first.id, second.id])
  assert.deepEqual(store.markDeliveredUpTo(conv.id, second.id), [first.id, second.id])
  assert.deepEqual(store.markReadUpTo(conv.id, first.id), [first.id])
  assert.deepEqual(store.markDeliveredUpTo(conv.id, second.id), [])

  const messages = await store.getMessages(conv.id)
  assert.strictEqual(messages[0].status, 'read')
  assert.strictEqual(messages[1].status, 'delivered')
  assert.deepEqual(store.getPendingOutgoingMessages(conv.id), [])
  await store.deleteConversation(conv.id)
})

test('receipt status updates fail closed on an unreadable message file', async () => {
  const store = new ChatStore()
  const conv = await store.createConversation('direct', ['peer'])
  const messagesPath = path.join(store.storagePath, `${conv.id}.json`)
  fs.writeFileSync(messagesPath, '{not-json')

  assert.throws(() => store.markDeliveredUpTo(conv.id, 'message-1'), /unreadable/)
  assert.throws(() => store.markRelayedByIds(conv.id, ['message-1']), /unreadable/)
  assert.throws(() => store.markReadUpTo(conv.id, 'message-1'), /unreadable/)

  await store.deleteConversation(conv.id)
})

test('addMessage inserts a late-arriving older message chronologically', async () => {
  const store = new ChatStore()
  const conv = await store.createConversation('direct', ['p1'])
  const base = Date.now()

  await store.addMessage(conv.id, { id: 'm-old', senderId: 'p1', content: 'old', timestamp: base - 60000 })
  await store.addMessage(conv.id, { id: 'm-new', senderId: 'p1', content: 'new', timestamp: base })
  // Blind-peer catch-up: a message from between the two arrives last
  await store.addMessage(conv.id, { id: 'm-mid', senderId: 'p1', content: 'mid', timestamp: base - 30000 })

  const messages = await store.getMessages(conv.id)
  assert.deepEqual(messages.map(m => m.id), ['m-old', 'm-mid', 'm-new'], 'Messages should be chronological')

  await store.deleteConversation(conv.id)
})

test('same-timestamp messages keep arrival order', async () => {
  const store = new ChatStore()
  const conv = await store.createConversation('direct', ['p1'])
  const ts = Date.now()

  await store.addMessage(conv.id, { id: 'z-first', senderId: 'p1', content: '1', timestamp: ts })
  await store.addMessage(conv.id, { id: 'a-second', senderId: 'p1', content: '2', timestamp: ts })

  const messages = await store.getMessages(conv.id)
  assert.deepEqual(messages.map(m => m.id), ['z-first', 'a-second'], 'Ties should keep arrival order, not id order')

  await store.deleteConversation(conv.id)
})

test('far-future peer timestamps cannot pin conversation ordering', async () => {
  const store = new ChatStore()
  const conv = await store.createConversation('direct', ['p1'])
  const base = Date.now()
  const hostileFuture = base + (365 * 24 * 60 * 60 * 1000)

  const hostile = await store.addMessage(conv.id, {
    id: 'future',
    senderId: 'p1',
    content: 'future',
    timestamp: hostileFuture
  })
  assert.ok(hostile.timestamp < hostileFuture, 'Unreasonable future timestamp should be replaced')

  await store.addMessage(conv.id, {
    id: 'next',
    senderId: 'p1',
    content: 'next',
    timestamp: base + 1000
  })

  const messages = await store.getMessages(conv.id)
  assert.deepEqual(messages.map(m => m.id), ['future', 'next'])
  assert.strictEqual(store.getLatestIncomingMessageId(conv.id), 'next')

  const updated = await store.getConversation(conv.id)
  assert.strictEqual(updated.lastMessage, 'next')
  assert.strictEqual(updated.lastMessageTimestamp, base + 1000)

  await store.deleteConversation(conv.id)
})

test('late catch-up insert does not clobber the conversation preview', async () => {
  const store = new ChatStore()
  const conv = await store.createConversation('direct', ['p1'])
  const base = Date.now()

  await store.addMessage(conv.id, { senderId: 'p1', content: 'newest', timestamp: base })
  await store.addMessage(conv.id, { senderId: 'p1', content: 'stale catch-up', timestamp: base - 60000 })

  const updated = await store.getConversation(conv.id)
  assert.strictEqual(updated.lastMessage, 'newest', 'Preview should stay on the newest message')
  assert.strictEqual(updated.lastMessageTimestamp, base, 'Preview timestamp should not move backwards')

  await store.deleteConversation(conv.id)
})

test('getLatestIncomingMessage tracks chronology, not arrival', async () => {
  const store = new ChatStore()
  const conv = await store.createConversation('direct', ['p1'])
  const base = Date.now()

  await store.addMessage(conv.id, { id: 'newer', senderId: 'p1', content: 'newer', timestamp: base })
  await store.addMessage(conv.id, { id: 'older', senderId: 'p1', content: 'older', timestamp: base - 60000 })

  const latest = store.getLatestIncomingMessage(conv.id)
  assert.strictEqual(latest.id, 'newer', 'A late catch-up must not become the read-receipt watermark')

  await store.deleteConversation(conv.id)
})

test('getMessages heals a misordered legacy file on disk', async () => {
  const store = new ChatStore()
  const conv = await store.createConversation('direct', ['p1'])
  const base = Date.now()
  const messagesPath = path.join(store.storagePath, `${conv.id}.json`)

  // Simulate a file persisted before chronological insertion existed
  fs.writeFileSync(messagesPath, JSON.stringify([
    { id: 'b', conversationId: conv.id, senderId: 'p1', content: 'second', timestamp: base - 10000 },
    { id: 'c', conversationId: conv.id, senderId: 'p1', content: 'third', timestamp: base },
    { id: 'a', conversationId: conv.id, senderId: 'p1', content: 'first', timestamp: base - 20000 }
  ]))

  const messages = await store.getMessages(conv.id)
  assert.deepEqual(messages.map(m => m.id), ['a', 'b', 'c'], 'Read should return chronological order')

  const persisted = JSON.parse(fs.readFileSync(messagesPath, 'utf8'))
  assert.deepEqual(persisted.map(m => m.id), ['a', 'b', 'c'], 'Healed order should be written back')

  await store.deleteConversation(conv.id)
})

test('addMessage heals a misordered legacy file before chronological insert', async () => {
  const store = new ChatStore()
  const conv = await store.createConversation('direct', ['p1'])
  const messagesPath = path.join(store.storagePath, `${conv.id}.json`)

  // Background replication can add a message before the app opens this room
  // and calls getMessages(), so insertion itself must heal the legacy file.
  fs.writeFileSync(messagesPath, JSON.stringify([
    { id: 'a', conversationId: conv.id, senderId: 'p1', content: 'first', timestamp: 100 },
    { id: 'c', conversationId: conv.id, senderId: 'p1', content: 'newest', timestamp: 300 },
    { id: 'b', conversationId: conv.id, senderId: 'p1', content: 'second', timestamp: 200 }
  ]))

  await store.addMessage(conv.id, {
    id: 'between',
    senderId: 'p1',
    content: 'between',
    timestamp: 250
  })

  const persisted = JSON.parse(fs.readFileSync(messagesPath, 'utf8'))
  assert.deepEqual(persisted.map(m => m.id), ['a', 'b', 'between', 'c'])
  assert.strictEqual(store.getLatestIncomingMessageId(conv.id), 'c')

  const updated = await store.getConversation(conv.id)
  assert.strictEqual(updated.lastMessage, 'newest')
  assert.strictEqual(updated.lastMessageTimestamp, 300)

  await store.deleteConversation(conv.id)
})

test('clearAll attempts every file, then reports the failure with memory reloaded from disk', async () => {
  const store = new ChatStore()
  const conv = await store.createConversation('direct', ['wipe-a', 'wipe-b'], { displayName: 'Wipe' })
  await store.addMessage(conv.id, { content: 'hello', senderId: 'wipe-a', contentType: 'text/plain' })
  const indexPath = path.join(store.storagePath, 'index.json')
  const messagesPath = path.join(store.storagePath, `${conv.id}.json`)
  assert.ok(fs.existsSync(indexPath) && fs.existsSync(messagesPath))

  const realUnlink = fs.unlinkSync
  fs.unlinkSync = file => {
    if (file === indexPath) throw Object.assign(new Error('simulated I/O failure'), { code: 'EIO' })
    return realUnlink(file)
  }
  try {
    await assert.rejects(store.clearAll(), { code: 'EIO' })
  } finally {
    fs.unlinkSync = realUnlink
  }

  assert.strictEqual(fs.existsSync(messagesPath), false, 'the deletable files must still be removed')
  assert.ok(fs.existsSync(indexPath))
  assert.ok(store.conversations.has(conv.id), 'memory must match the surviving index')

  await store.clearAll()
  assert.strictEqual(store.conversations.size, 0)
  assert.strictEqual(fs.existsSync(indexPath), false)
})

test('clearAll treats a missing chats directory as already clean', async () => {
  const store = new ChatStore()
  await store.clearAll()
  fs.rmSync(store.storagePath, { recursive: true, force: true })
  await store.clearAll()
  assert.strictEqual(store.conversations.size, 0)
  store.ensureStorageDir()
})
