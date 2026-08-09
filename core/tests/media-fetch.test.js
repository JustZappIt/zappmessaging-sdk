/**
 * Unit tests for receiver-driven media fetch (media permanently lost to offline
 * peers). Covers p2p-manager.requestMedia routing and the media-transfer stall
 * timeout.
 */

const { test } = require('node:test')
const assert = require('node:assert')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const { P2PManager } = require('../lib/p2p-manager')
const { MediaTransfer, CHUNK_SIZE } = require('../lib/media-transfer')
const { MediaStore } = require('../lib/media-store')

const MEDIA_ID = b4a.toString(crypto.randomBytes(32), 'hex')
const AUTHOR = b4a.toString(crypto.randomBytes(32), 'hex')

// Fake framed socket that records writeRequest calls.
function fakeFramed (writeResult = true) {
  const requests = []
  return { requests, writeRequest (hashBuf) { requests.push(b4a.toString(hashBuf, 'hex')); return writeResult } }
}

function wireSocket (manager, peerId, framed) {
  const rawSocket = {}
  manager.allPeerConnections.set(peerId, [rawSocket])
  manager.framedSockets.set(rawSocket, framed)
  return rawSocket
}

test('requestMedia writes a request frame to the authoring peer', () => {
  const manager = new P2PManager()
  const framed = fakeFramed()
  wireSocket(manager, AUTHOR, framed)

  const ok = manager.requestMedia('conv-1', MEDIA_ID, AUTHOR)
  assert.strictEqual(ok, true)
  assert.deepStrictEqual(framed.requests, [MEDIA_ID], 'Author socket got exactly one request for the media hash')
})

test('requestMedia falls back to conversation sockets when the author is not directly connected', () => {
  const manager = new P2PManager()
  // Author has no global connection; a different peer shares the conversation.
  const peerFramed = fakeFramed()
  const rawSocket = {}
  manager.framedSockets.set(rawSocket, peerFramed)
  manager.conversations.set('conv-1', {
    peers: new Map([[b4a.toString(crypto.randomBytes(32), 'hex'), { connections: [rawSocket] }]])
  })

  const ok = manager.requestMedia('conv-1', MEDIA_ID, AUTHOR)
  assert.strictEqual(ok, true)
  assert.deepStrictEqual(peerFramed.requests, [MEDIA_ID], 'Fell back to a conversation socket')
})

test('requestMedia does not duplicate a request when author is also a conversation socket', () => {
  const manager = new P2PManager()
  const framed = fakeFramed()
  const rawSocket = wireSocket(manager, AUTHOR, framed)
  manager.conversations.set('conv-1', { peers: new Map([[AUTHOR, { connections: [rawSocket] }]]) })

  manager.requestMedia('conv-1', MEDIA_ID, AUTHOR)
  assert.strictEqual(framed.requests.length, 1, 'The same socket is only asked once')
})

test('requestMedia does not fan out after the author accepts the request', () => {
  const manager = new P2PManager()
  const authorFramed = fakeFramed()
  wireSocket(manager, AUTHOR, authorFramed)

  const fallbackFramed = fakeFramed()
  const fallbackSocket = {}
  manager.framedSockets.set(fallbackSocket, fallbackFramed)
  manager.conversations.set('conv-1', {
    peers: new Map([['fallback', { connections: [fallbackSocket] }]])
  })

  assert.strictEqual(manager.requestMedia('conv-1', MEDIA_ID, AUTHOR), true)
  assert.deepStrictEqual(authorFramed.requests, [MEDIA_ID])
  assert.deepStrictEqual(fallbackFramed.requests, [])
})

test('requestMedia falls back when every author socket rejects the write', () => {
  const manager = new P2PManager()
  const authorFramed = fakeFramed(false)
  wireSocket(manager, AUTHOR, authorFramed)

  const fallbackFramed = fakeFramed()
  const fallbackSocket = {}
  manager.framedSockets.set(fallbackSocket, fallbackFramed)
  manager.conversations.set('conv-1', {
    peers: new Map([['fallback', { connections: [fallbackSocket] }]])
  })

  assert.strictEqual(manager.requestMedia('conv-1', MEDIA_ID, AUTHOR), true)
  assert.deepStrictEqual(authorFramed.requests, [MEDIA_ID])
  assert.deepStrictEqual(fallbackFramed.requests, [MEDIA_ID])
})

test('requestMedia returns false when no peer is connected', () => {
  const manager = new P2PManager()
  assert.strictEqual(manager.requestMedia('conv-1', MEDIA_ID, AUTHOR), false)
})

test('requestMedia rejects a malformed media id', () => {
  const manager = new P2PManager()
  const framed = fakeFramed()
  wireSocket(manager, AUTHOR, framed)
  assert.strictEqual(manager.requestMedia('conv-1', 'not-a-hash', AUTHOR), false)
  assert.strictEqual(manager.requestMedia('conv-1', 'ab', AUTHOR), false)
  assert.strictEqual(manager.requestMedia('conv-1', 'g'.repeat(64), AUTHOR), false)
  assert.strictEqual(manager.requestMedia('', MEDIA_ID, AUTHOR), false)
  assert.strictEqual(manager.requestMedia('conv-1', MEDIA_ID, 'bad-peer'), false)
  assert.strictEqual(framed.requests.length, 0)
})

test('media requests from one peer are rate limited', () => {
  const manager = new P2PManager()
  for (let i = 0; i < 60; i++) {
    assert.strictEqual(manager._isMediaRequestRateLimited(AUTHOR), false)
  }
  assert.strictEqual(manager._isMediaRequestRateLimited(AUTHOR), true)
})

test('incoming chunks reject hostile metadata before allocating buffers', () => {
  const transfer = new MediaTransfer(new MediaStore(), {
    maxMediaBytes: CHUNK_SIZE * 2,
    maxActiveTransfers: 1
  })
  const hashBuf = crypto.randomBytes(32)

  assert.strictEqual(transfer.onChunkReceived(hashBuf, 0, 0, b4a.from('x')), false)
  assert.strictEqual(transfer.onChunkReceived(hashBuf, 0, 3, b4a.from('x')), false)
  assert.strictEqual(transfer.onChunkReceived(hashBuf, 2, 2, b4a.from('x')), false)
  assert.strictEqual(transfer.onChunkReceived(b4a.alloc(31), 0, 1, b4a.from('x')), false)
  assert.strictEqual(transfer.onChunkReceived(hashBuf, 0, 1, b4a.alloc(CHUNK_SIZE + 1)), false)
  assert.strictEqual(transfer.activeTransfers.size, 0)
  assert.strictEqual(transfer._activeBytes, 0)
})

test('incoming chunks cap concurrent transfers and reject shape changes', () => {
  const transfer = new MediaTransfer(new MediaStore(), { maxActiveTransfers: 1 })
  const firstHash = crypto.randomBytes(32)
  const secondHash = crypto.randomBytes(32)

  assert.strictEqual(transfer.onChunkReceived(firstHash, 0, 2, b4a.from('first')), true)
  assert.strictEqual(transfer.onChunkReceived(firstHash, 1, 3, b4a.from('changed')), false)
  assert.strictEqual(transfer.onChunkReceived(secondHash, 0, 2, b4a.from('second')), false)
  assert.strictEqual(transfer.activeTransfers.size, 1)

  transfer.cancelTransfer(b4a.toString(firstHash, 'hex'))
  assert.strictEqual(transfer.activeTransfers.size, 0)
  assert.strictEqual(transfer._activeBytes, 0)
})

test('media transfer options reject invalid limits', () => {
  assert.throws(
    () => new MediaTransfer(new MediaStore(), { transferTimeoutMs: 0 }),
    /positive integer/
  )
})

test('sendMedia rejects files above the configured receive limit', async () => {
  const store = new MediaStore()
  const { hashHex } = store.saveMedia(b4a.from('oversized'), 'jpg')
  const transfer = new MediaTransfer(store, { maxMediaBytes: 4 })

  await assert.rejects(
    transfer.sendMedia({ writeChunk () {} }, hashHex),
    /transferable range/
  )
  store.deleteMedia(hashHex)
})

test('duplicate requests cannot start concurrent uploads of the same blob', async () => {
  const store = new MediaStore()
  const { hashHex } = store.saveMedia(b4a.from('payload'), 'jpg')
  const hashBuf = b4a.from(hashHex, 'hex')
  const transfer = new MediaTransfer(store)
  const framed = { writeChunk () {} }

  let release
  transfer.sendMedia = () => new Promise((resolve) => { release = resolve })
  const first = transfer.handleRequest(hashBuf, framed)
  await Promise.resolve()

  assert.strictEqual(await transfer.handleRequest(hashBuf, framed), false)
  release()
  assert.strictEqual(await first, true)
  store.deleteMedia(hashHex)
})

test('media uploads use a bounded queue', async () => {
  const store = new MediaStore()
  const hashes = ['one', 'two', 'three'].map((value) =>
    store.saveMedia(b4a.from(value), 'jpg').hashHex)
  const transfer = new MediaTransfer(store, {
    maxConcurrentSends: 1,
    maxQueuedSends: 1
  })
  const framed = { writeChunk () {} }
  const releases = []
  let active = 0
  let maxActive = 0

  transfer.sendMedia = async () => {
    active++
    maxActive = Math.max(maxActive, active)
    await new Promise((resolve) => releases.push(resolve))
    active--
  }

  const first = transfer.handleRequest(b4a.from(hashes[0], 'hex'), framed)
  await Promise.resolve()
  const second = transfer.handleRequest(b4a.from(hashes[1], 'hex'), framed)
  assert.strictEqual(await transfer.handleRequest(b4a.from(hashes[2], 'hex'), framed), false)

  releases.shift()()
  assert.strictEqual(await first, true)
  await Promise.resolve()
  releases.shift()()
  assert.strictEqual(await second, true)
  assert.strictEqual(maxActive, 1)

  for (const hashHex of hashes) store.deleteMedia(hashHex)
})

test('a stalled media transfer emits timeout and evicts its buffers', () => {
  return new Promise((resolve) => {
    const transfer = new MediaTransfer(new MediaStore(), { transferTimeoutMs: 20 })
    const hashBuf = crypto.randomBytes(32)
    const hashHex = b4a.toString(hashBuf, 'hex')

    // The stall timer is unref'd (must not keep the worklet alive in
    // production); hold the loop open here so node:test doesn't exit first.
    const keepAlive = setInterval(() => {}, 1000)

    transfer.on('timeout', (timedOutHash) => {
      clearInterval(keepAlive)
      assert.strictEqual(timedOutHash, hashHex)
      assert.strictEqual(transfer.activeTransfers.has(hashHex), false, 'Partial transfer evicted')
      resolve()
    })

    // First of three chunks arrives, then the peer goes silent.
    transfer.onChunkReceived(hashBuf, 0, 3, b4a.from('chunk-0'))
    assert.ok(transfer.activeTransfers.has(hashHex), 'Transfer is active after the first chunk')
  })
})

test('a completing transfer clears its stall timer (no late timeout)', async () => {
  const transfer = new MediaTransfer(new MediaStore(), { transferTimeoutMs: 20 })
  const payload = b4a.from('single chunk payload')
  const hashBuf = transfer.mediaStore.hash(payload) // real hash so completion passes integrity
  const hashHex = b4a.toString(hashBuf, 'hex')

  let timedOut = false
  transfer.on('timeout', () => { timedOut = true })

  const done = new Promise((resolve) => transfer.on('complete', resolve))
  transfer.onChunkReceived(hashBuf, 0, 1, payload)
  await done

  assert.strictEqual(transfer.activeTransfers.has(hashHex), false, 'Completed transfer removed')
  await new Promise((r) => setTimeout(r, 40)) // past the timeout window
  assert.strictEqual(timedOut, false, 'A completed transfer must not fire a stale timeout')
})
