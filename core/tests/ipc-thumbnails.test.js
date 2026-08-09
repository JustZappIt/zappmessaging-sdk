/**
 * Tests for lazy thumbnail handling (1 MB IPC buffer discard fix): message.list
 * strips inline base64 thumbnails and message.get_thumbnail fetches them one at
 * a time, keeping list frames well under the native receive cap.
 */

globalThis.Bare = { argv: [], IPC: { on: () => {} } }

const { test } = require('node:test')
const assert = require('node:assert')
const EventEmitter = require('node:events')
const path = require('node:path')
const fs = require('node:fs')
const { IPCHandler } = require('../lib/ipc-handler')
const { ChatStore, MAX_THUMBNAIL_DATA_BYTES } = require('../lib/chat-store')
const { getDataDir } = require('../lib/storage')

const MY = 'aa'.repeat(32)
const PEER = 'bb'.repeat(32)
const THUMB = 'data:image/jpeg;base64,' + 'Zm9vYmFy'.repeat(20)

let _seq = 0

async function seededHandler () {
  // Isolate storage: node --test runs files in parallel processes sharing the
  // real chats/ dir, so point this ChatStore at a unique subdir to avoid
  // racing the other suites.
  const isolatedDir = path.join(getDataDir(), `test-thumbs-${process.pid}-${_seq++}`)
  const chatStore = new ChatStore()
  chatStore.storagePath = isolatedDir
  chatStore.ensureStorageDir()
  chatStore.conversations.clear()
  const convId = 'conv'
  await chatStore.createConversationWithId(convId, 'direct', [PEER], {})
  await chatStore.addMessage(convId, { id: 'm-text', senderId: PEER, content: 'hi', isFromMe: false })
  await chatStore.addMessage(convId, {
    id: 'm-media', senderId: PEER, content: 'photo', isFromMe: false,
    mediaId: 'cc'.repeat(32), thumbnailData: THUMB
  })

  const p2pManager = new EventEmitter()
  const handler = new IPCHandler({
    identity: { publicKeyHex: MY, displayName: 'Me' },
    chatStore,
    contactStore: {},
    p2pManager
  })
  handler.pushEvent = () => {}
  handler._sendDeliveryReceiptIfNeeded = () => {} // sidestep receipt plumbing
  process.stdin.removeAllListeners('data')
  process.stdin.pause()

  const cleanup = () => fs.rmSync(isolatedDir, { recursive: true, force: true })
  return { handler, chatStore, convId, cleanup }
}

test('message.list strips inline thumbnails and flags them instead', async () => {
  const { handler, chatStore, convId, cleanup } = await seededHandler()
  try {
    const { messages } = await handler.handleMessage('list', { conversationId: convId })
    const media = messages.find(m => m.id === 'm-media')
    const text = messages.find(m => m.id === 'm-text')

    assert.ok(media, 'media message present')
    assert.strictEqual(media.thumbnailData, undefined, 'base64 thumbnail is not inlined in the list frame')
    assert.strictEqual(media.hasThumbnail, true, 'media message flagged as having a thumbnail')
    assert.ok(!('hasThumbnail' in text) || text.hasThumbnail !== true, 'text message not flagged')

    const serialized = JSON.stringify({ messages })
    assert.ok(!serialized.includes(THUMB), 'list frame must not carry the thumbnail bytes')
  } finally {
    cleanup()
  }
})

test('message.get_thumbnail returns the base64 for one message', async () => {
  const { handler, chatStore, convId, cleanup } = await seededHandler()
  try {
    const got = await handler.handleMessage('get_thumbnail', { conversationId: convId, messageId: 'm-media' })
    assert.strictEqual(got.thumbnailData, THUMB, 'exact thumbnail returned for the media message')

    const none = await handler.handleMessage('get_thumbnail', { conversationId: convId, messageId: 'm-text' })
    assert.strictEqual(none.thumbnailData, null, 'a message without a thumbnail returns null')

    const missing = await handler.handleMessage('get_thumbnail', { conversationId: convId, messageId: 'nope' })
    assert.strictEqual(missing.thumbnailData, null, 'unknown message id returns null')
  } finally {
    cleanup()
  }
})

test('the stored record keeps its thumbnail after a list (strip is non-destructive)', async () => {
  const { handler, chatStore, convId, cleanup } = await seededHandler()
  try {
    await handler.handleMessage('list', { conversationId: convId })
    // The strip returns shallow copies; on-disk data must be untouched.
    const stillThere = await chatStore.getMessageThumbnail(convId, 'm-media')
    assert.strictEqual(stillThere, THUMB, 'stripping the list response must not drop the persisted thumbnail')
  } finally {
    cleanup()
  }
})

test('oversized peer thumbnails are dropped before persistence and hydration', async () => {
  const { handler, chatStore, convId, cleanup } = await seededHandler()
  try {
    const oversized = 'x'.repeat(MAX_THUMBNAIL_DATA_BYTES + 1)
    const added = await chatStore.addMessage(convId, {
      id: 'm-hostile', senderId: PEER, content: 'photo', isFromMe: false,
      mediaId: 'dd'.repeat(32), thumbnailData: oversized
    })
    assert.strictEqual(added.thumbnailData, null, 'oversized thumbnail is not persisted')

    const { messages } = await handler.handleMessage('list', { conversationId: convId })
    const hostile = messages.find(m => m.id === 'm-hostile')
    assert.ok(hostile, 'message remains visible')
    assert.strictEqual(hostile.hasThumbnail, undefined, 'native will not schedule hydration')

    const got = await handler.handleMessage('get_thumbnail', {
      conversationId: convId,
      messageId: 'm-hostile'
    })
    assert.strictEqual(got.thumbnailData, null, 'thumbnail response remains bounded')
  } finally {
    cleanup()
  }
})
