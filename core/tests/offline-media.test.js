/**
 * Offline image delivery end to end on the phone side: the descriptor on the
 * record, the sender's media-core append inside the production send_message
 * handler, and the receiver pulling the range from a mirror while the author
 * is unreachable. The mirror is a plain third Corestore, which is what a
 * blind peer is underneath. Only the swarm is faked.
 */

const { test } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const Corestore = require('corestore')
const { ChatStore } = require('../lib/chat-store')
const { MediaStore } = require('../lib/media-store')
const { MediaTransfer } = require('../lib/media-transfer')
const { HypercoreManager } = require('../lib/hypercore-manager')
const { P2PManager } = require('../lib/p2p-manager')
const { IPCHandler } = require('../lib/ipc-handler')
const { MediaRequests } = require('../lib/media-requests')
const { createPeerReceiver } = require('../lib/peer-receiver')
const { normalizePeerRecord } = require('../lib/peer-record')
const mediaBlobs = require('../lib/media-blobs')

globalThis.Bare = { argv: [], IPC: { on () {} } }

const alice = crypto.keyPair(b4a.alloc(32, 1))
const bob = crypto.keyPair(b4a.alloc(32, 2))
const hex = keyPair => b4a.toString(keyPair.publicKey, 'hex')
const CONV = ChatStore.directChatId(hex(alice), hex(bob))
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))

function temporary (t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'zapp-offline-media-'))
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }))
  return dir
}

function link (a, b) {
  const s1 = a.replicate(true)
  const s2 = b.replicate(false)
  s1.pipe(s2).pipe(s1)
  return () => { s1.destroy(); s2.destroy() }
}

async function until (condition, ms = 5000) {
  const deadline = Date.now() + ms
  while (!condition()) {
    if (Date.now() > deadline) throw new Error('timed out waiting')
    await sleep(10)
  }
}

// One phone: stores, hypercores and a P2P manager with no network, plus the
// production IPC handler and media request pipeline wired the way index.js
// wires them.
async function phone (t, self, peer, opts = {}) {
  const dir = temporary(t)
  const identity = { publicKeyHex: hex(self), displayName: 'Me', keyPair: self }
  const chatStore = new ChatStore()
  chatStore.storagePath = path.join(dir, 'chats')
  fs.mkdirSync(chatStore.storagePath)
  chatStore.conversations.clear()
  chatStore.leftConversations.clear()
  await chatStore.createConversationWithId(CONV, 'direct', [hex(peer)])
  const mediaStore = new MediaStore()
  mediaStore.mediaDir = path.join(dir, 'media')
  mediaStore._ensureDir()
  const mediaTransfer = new MediaTransfer(mediaStore)
  mediaTransfer.on('error', () => {})
  const hypercoreManager = new HypercoreManager({ dataDir: dir })
  hypercoreManager.setKeyContext({
    getIdentityKeyPair: () => self,
    getConversation: id => chatStore.conversations.get(id) || null
  })
  await hypercoreManager.initialize()
  const p2pManager = new P2PManager({
    hypercoreManager,
    isParticipant: (id, key) => chatStore.isPeerAuthorized(id, key),
    getConversation: id => chatStore.conversations.get(id)
  })
  p2pManager.keyPair = self
  p2pManager.swarm = { join: () => ({ flushed: async () => {}, destroy: async () => {} }) }
  const blindMirror = { enabled: true, registered: [], addLocalMediaCore () {}, addRemoteMediaCore (...args) { this.registered.push(args) } }
  p2pManager.blindMirror = blindMirror
  const events = []
  const ipcHandler = new IPCHandler({
    identity, chatStore, contactStore: { getContact: async () => null }, p2pManager, mediaStore, mediaTransfer, hypercoreManager
  })
  process.stdin.removeAllListeners('data')
  process.stdin.pause()
  ipcHandler.pushEvent = (type, payload) => events.push({ type, payload })
  const mediaRequests = new MediaRequests(
    () => ({ chatStore, mediaStore, mediaTransfer, p2pManager, hypercoreManager, blindMirror, ipcHandler }),
    { cooldownMs: 1, fetchTimeoutMs: 200, ...opts })
  mediaTransfer.on('complete', (hash, bytes) => mediaRequests.complete(hash, bytes))
  const receive = createPeerReceiver(() => ({
    chatStore, p2pManager, ipcHandler, identity,
    processReceipt: async () => {},
    onMessage: (id, stored, record, from) => mediaRequests.request(id, record, from)
  }))
  t.after(async () => { mediaRequests.cancelFetches(); await hypercoreManager.close() })
  return { dir, identity, chatStore, mediaStore, mediaTransfer, hypercoreManager, p2pManager, blindMirror, ipcHandler, mediaRequests, receive, events }
}

const imageBytes = () => b4a.concat([b4a.from([0x89, 0x50, 0x4e, 0x47]), crypto.randomBytes(700_000)])

// Alice sends an image through the production handler; returns the stored
// row, and a mirror store that replicated her media core before she left.
// With `partial`, the mirror is missing the last block, so a receiver's fetch
// lands every other block and then waits.
async function sentByAlice (t, image, { partial = false } = {}) {
  const a = await phone(t, alice, bob)
  const prepared = await a.ipcHandler.handleMedia('prepare_send', { filePath: writeTemp(a.dir, image), extension: 'png' })
  const { message } = await a.ipcHandler.handleMedia('send_message', {
    conversationId: CONV, mediaId: prepared.mediaId, mediaSize: prepared.mediaSize, contentType: 'image/png',
    mediaLocalPath: prepared.mediaLocalPath, mediaWidth: 10, mediaHeight: 10
  })
  const mirror = new Corestore(temporary(t))
  t.after(() => mirror.close())
  const mirrorCore = mirror.get({ key: b4a.from(message.mediaCoreKey, 'hex') })
  const unlink = link(a.hypercoreManager.store, mirror)
  const end = message.mediaBlockOffset + message.mediaBlockLength
  await mirrorCore.download({ start: 0, end: partial ? end - 1 : end }).done()
  unlink()
  await a.hypercoreManager.close()
  // Alice's phone comes back long enough for the relay to finish pulling.
  const completeMirror = async () => {
    const sender = new Corestore(path.join(a.dir, 'corestore'))
    t.after(() => sender.close())
    const unlinkSender = link(sender, mirror)
    await mirrorCore.download({ start: 0, end }).done()
    unlinkSender()
  }
  return { message, mirror, mirrorCore, completeMirror }
}

function writeTemp (dir, bytes) {
  const file = path.join(dir, 'outgoing.png')
  fs.writeFileSync(file, bytes)
  return file
}

const wire = (message, extra = {}) => {
  const { mediaLocalPath, mediaTransferState, mediaAuthorized, status, isFromMe, conversationId, ...record } = message
  return { ...record, ...extra }
}

test('peer records carry a whole descriptor or none', () => {
  const base = { id: 'img', mediaId: 'ab'.repeat(32), mediaSize: 5, timestamp: 1 }
  const descriptor = { mediaCoreKey: '0x' + 'CD'.repeat(32), mediaBlockOffset: 0, mediaBlockLength: 1 }

  const record = normalizePeerRecord({ ...base, ...descriptor }, hex(alice))
  assert.equal(record.mediaCoreKey, 'cd'.repeat(32), 'key is normalized like every other key')
  assert.equal(record.mediaBlockOffset, 0)
  assert.equal(record.mediaBlockLength, 1)
  assert.equal(normalizePeerRecord(base, hex(alice)).mediaCoreKey, undefined)

  for (const broken of [
    { ...base, mediaCoreKey: descriptor.mediaCoreKey },
    { ...base, mediaBlockOffset: 0, mediaBlockLength: 1 },
    { ...base, ...descriptor, mediaCoreKey: 'zz'.repeat(32) },
    { ...base, ...descriptor, mediaBlockOffset: -1 },
    { ...base, ...descriptor, mediaBlockOffset: 1.5 },
    { ...base, ...descriptor, mediaBlockLength: 0 },
    { ...base, ...descriptor, mediaBlockLength: '1' },
    { id: 'no-media', timestamp: 1, ...descriptor }
  ]) {
    assert.throws(() => normalizePeerRecord(broken, hex(alice)), { code: 'INVALID_PEER_RECORD' })
  }
})

test('the chat store persists the descriptor and finds our own for a resend', async t => {
  const { chatStore } = await phone(t, bob, alice)
  const mediaId = 'ab'.repeat(32)
  const descriptor = { mediaCoreKey: 'cd'.repeat(32), mediaBlockOffset: 3, mediaBlockLength: 2 }
  await chatStore.addMessage(CONV, { id: 'theirs', senderId: hex(alice), mediaId, ...descriptor })
  await chatStore.addMessage(CONV, { id: 'mine', senderId: hex(bob), isFromMe: true, mediaId, mediaCoreKey: 'ef'.repeat(32), mediaBlockOffset: 0, mediaBlockLength: 2 })
  await chatStore.addMessage(CONV, { id: 'plain', senderId: hex(alice), mediaId })

  const rows = await chatStore.getMessages(CONV)
  assert.deepEqual(rows.map(r => [r.mediaCoreKey, r.mediaBlockOffset, r.mediaBlockLength]), [
    ['cd'.repeat(32), 3, 2], ['ef'.repeat(32), 0, 2], [null, null, null]
  ])
  assert.deepEqual(chatStore.findOwnMediaDescriptor(CONV, mediaId),
    { mediaCoreKey: 'ef'.repeat(32), mediaBlockOffset: 0, mediaBlockLength: 2 }, 'a peer\'s descriptor is never ours')
  assert.equal(chatStore.findOwnMediaDescriptor(CONV, 'ff'.repeat(32)), null)
  await assert.rejects(chatStore.addMessage(CONV, { id: 'partial', senderId: hex(bob), mediaId, mediaCoreKey: 'cd'.repeat(32) }),
    { code: 'INVALID_PEER_RECORD' })
})

test('send_message appends the image to the media core and describes it on the row and the wire', async t => {
  const a = await phone(t, alice, bob)
  const image = imageBytes()
  const prepared = await a.ipcHandler.handleMedia('prepare_send', { filePath: writeTemp(a.dir, image), extension: 'png' })
  const send = () => a.ipcHandler.handleMedia('send_message', {
    conversationId: CONV, mediaId: prepared.mediaId, mediaSize: prepared.mediaSize, contentType: 'image/png',
    mediaLocalPath: prepared.mediaLocalPath
  })

  const { message } = await send()
  const mediaCore = a.hypercoreManager.localMediaCores.get(CONV)
  assert.equal(message.mediaCoreKey, b4a.toString(mediaCore.key, 'hex'))
  assert.equal(message.mediaBlockOffset, 0)
  assert.equal(message.mediaBlockLength, Math.ceil(image.length / mediaBlobs.MEDIA_BLOCK_SIZE))
  assert.equal(mediaCore.length, message.mediaBlockLength)
  assert.ok(b4a.equals(await mediaBlobs.download(mediaCore, { offset: 0, n: mediaCore.length, byteLength: image.length }).bytes, image))

  const [row] = await a.chatStore.getMessages(CONV)
  assert.equal(row.mediaCoreKey, message.mediaCoreKey)
  const messageCore = a.hypercoreManager.localCores.get(CONV)
  const record = await messageCore.get(messageCore.length - 1)
  assert.equal(record.mediaCoreKey, message.mediaCoreKey)
  assert.equal(record.mediaBlockOffset, 0)
  assert.equal(record.mediaBlockLength, message.mediaBlockLength)
  assert.equal(record.mediaLocalPath, undefined)

  const { message: again } = await send()
  assert.equal(mediaCore.length, message.mediaBlockLength, 'a resend reuses the blocks already in the core')
  assert.equal(again.mediaBlockOffset, 0)
  assert.equal(again.mediaBlockLength, message.mediaBlockLength)
})

test('a media core failure leaves the live path untouched', async t => {
  const a = await phone(t, alice, bob)
  a.hypercoreManager.getOrCreateLocalMediaCore = async () => { throw new Error('disk full') }
  const prepared = await a.ipcHandler.handleMedia('prepare_send', { filePath: writeTemp(a.dir, imageBytes()), extension: 'png' })
  const { message } = await a.ipcHandler.handleMedia('send_message', {
    conversationId: CONV, mediaId: prepared.mediaId, mediaSize: prepared.mediaSize, contentType: 'image/png'
  })
  assert.equal(message.mediaId, prepared.mediaId)
  assert.equal(message.mediaCoreKey, null)
  assert.equal(a.events.filter(e => e.type === 'message.status').length, 1)
})

test('a record with a descriptor completes through the mirror while the author is unreachable', async t => {
  const image = imageBytes()
  const { message, mirror } = await sentByAlice(t, image)
  const b = await phone(t, bob, alice)
  const unlink = link(mirror, b.hypercoreManager.store)
  t.after(unlink)

  await b.receive(CONV, hex(alice), wire(message))
  assert.equal(b.mediaRequests.get(message.mediaId).descriptor.n, message.mediaBlockLength)
  await until(() => b.events.some(e => e.type === 'media.transfer_complete'))

  const completions = b.events.filter(e => e.type === 'media.transfer_complete')
  assert.equal(completions.length, 1)
  assert.equal(completions[0].payload.mediaId, message.mediaId)
  assert.equal(completions[0].payload.mediaSize, image.length)
  assert.ok(completions[0].payload.mediaLocalPath.endsWith('.png'))
  assert.ok(b4a.equals(b.mediaStore.getMedia(message.mediaId), image))
  const progress = b.events.filter(e => e.type === 'media.transfer_progress').map(e => e.payload.progress)
  assert.equal(progress.length, message.mediaBlockLength)
  assert.equal(progress[progress.length - 1], 1)
  const [row] = await b.chatStore.getMessages(CONV)
  assert.equal(row.mediaLocalPath, completions[0].payload.mediaLocalPath)
  assert.equal(b.chatStore.canServeMedia(message.mediaId, hex(alice)), true, 'verified bytes authorize this conversation')
  assert.equal(b.mediaRequests.get(message.mediaId), undefined)
  assert.equal(b.blindMirror.registered.length, 1, 'the sender\'s media core was registered with the relay for the fetch')

  await until(() => b.mediaRequests._fetches.size === 0)
  const again = await b.hypercoreManager.openRemoteMediaCore(CONV, hex(alice), message.mediaCoreKey)
  assert.equal(await again.has(message.mediaBlockOffset, message.mediaBlockOffset + message.mediaBlockLength), false,
    'the ciphertext copy is cleared after a verified save')
  await again.close()

  await b.receive(CONV, hex(alice), wire(message, { id: 'again' }))
  assert.equal(b.events.filter(e => e.type === 'media.transfer_complete').length, 1, 'bytes on disk are never fetched twice')
})

test('a record without a descriptor still only waits for the live path', async t => {
  const b = await phone(t, bob, alice)
  const mediaId = 'ab'.repeat(32)
  await b.receive(CONV, hex(alice), { id: 'plain', mediaId, mediaSize: 5, timestamp: 1 })
  assert.equal(b.mediaRequests.get(mediaId).descriptor, null)
  assert.equal(b.mediaRequests._fetches.size, 0)
  assert.equal(b.events.filter(e => e.type.startsWith('media.')).length, 0)
})

// Every block of the range must be gone: a receiver never keeps ciphertext.
async function assertRangeCleared (phone, message) {
  const core = await phone.hypercoreManager.openRemoteMediaCore(CONV, hex(alice), message.mediaCoreKey)
  try {
    for (let i = message.mediaBlockOffset; i < message.mediaBlockOffset + message.mediaBlockLength; i++) {
      assert.equal(await core.has(i), false, 'block ' + i + ' was left behind')
    }
  } finally {
    await core.close()
  }
}

test('a mirror without the blocks times out, counts an attempt and retries by itself after the cooldown', async t => {
  const image = imageBytes()
  const { message, mirror } = await sentByAlice(t, image)
  const b = await phone(t, bob, alice, { cooldownMs: 300 })
  const keepAlive = setInterval(() => {}, 1000)
  t.after(() => clearInterval(keepAlive))

  await b.receive(CONV, hex(alice), wire(message))
  const entry = b.mediaRequests.get(message.mediaId)
  assert.equal(b.mediaRequests._fetches.size, 1)
  await until(() => entry.mirrorAttempts === 1)
  assert.ok(entry.mirrorFailedAt > 0)
  assert.ok(entry.descriptor, 'a timeout is not a verdict on the descriptor')
  assert.equal(b.events.filter(e => e.type === 'media.transfer_complete').length, 0)
  await until(() => b.mediaRequests._fetches.size === 0)

  // The relay becomes reachable; no message, peer or mirror event follows.
  const unlink = link(mirror, b.hypercoreManager.store)
  t.after(unlink)
  await until(() => b.events.some(e => e.type === 'media.transfer_complete'))
  assert.equal(entry.mirrorAttempts, 1, 'the cooldown timer retried once and succeeded')
  assert.ok(b4a.equals(b.mediaStore.getMedia(message.mediaId), image))
})

test('bytes that cannot be saved count as a failed mirror attempt instead of a refetch loop', async t => {
  const image = imageBytes()
  const { message, mirror } = await sentByAlice(t, image)
  const b = await phone(t, bob, alice, { cooldownMs: 60000 })
  const unlink = link(mirror, b.hypercoreManager.store)
  t.after(unlink)
  let opens = 0
  const open = b.p2pManager.openRemoteMediaCore.bind(b.p2pManager)
  b.p2pManager.openRemoteMediaCore = (...args) => { opens++; return open(...args) }
  b.mediaStore.saveMediaWithHash = () => { throw Object.assign(new Error('no space left on device'), { code: 'ENOSPC' }) }

  await b.receive(CONV, hex(alice), wire(message))
  const entry = b.mediaRequests.get(message.mediaId)
  await until(() => entry.mirrorAttempts === 1)
  await until(() => b.mediaRequests._fetches.size === 0)
  await sleep(300)
  assert.equal(opens, 1, 'the image was fetched once')
  assert.equal(b.events.filter(e => e.type === 'media.transfer_complete').length, 0)
  assert.equal(b.mediaRequests.get(message.mediaId), entry, 'the request stays for a later attempt')
  assert.ok(entry.descriptor)
  await assertRangeCleared(b, message)
})

test('the mirror coming up gives an image its attempts back', async t => {
  const image = imageBytes()
  const { message, mirror } = await sentByAlice(t, image)
  const b = await phone(t, bob, alice)
  const unlink = link(mirror, b.hypercoreManager.store)
  t.after(unlink)

  b.blindMirror.enabled = false
  await b.receive(CONV, hex(alice), wire(message))
  const entry = b.mediaRequests.get(message.mediaId)
  assert.equal(b.mediaRequests._fetches.size, 0, 'no mirror, no fetch')
  entry.mirrorAttempts = 12
  b.blindMirror.enabled = true
  await b.mediaRequests.requestMissingEverywhere()
  assert.equal(entry.mirrorAttempts, 0)
  await until(() => b.events.some(e => e.type === 'media.transfer_complete'))
  assert.ok(b4a.equals(b.mediaStore.getMedia(message.mediaId), image))
})

test('a descriptor whose bytes do not hash to the mediaId is dropped without touching the live path', async t => {
  const { message, mirror } = await sentByAlice(t, imageBytes())
  const b = await phone(t, bob, alice)
  const unlink = link(mirror, b.hypercoreManager.store)
  t.after(unlink)

  // Same core, wrong image: the blocks decode but are not the ones named.
  const otherId = b4a.toString(b.mediaStore.hash(imageBytes()), 'hex')
  await b.receive(CONV, hex(alice), wire(message, { mediaId: otherId }))
  const entry = b.mediaRequests.get(otherId)
  await until(() => entry.descriptor === null)
  assert.equal(entry.mirrorAttempts, 0)
  assert.equal(b.events.filter(e => e.type === 'media.transfer_complete').length, 0)
  assert.equal(b.mediaRequests.get(otherId), entry, 'the live request stays pending')
})

test('whichever source finishes first wins, the other is cancelled and its blocks are cleared', async t => {
  const image = imageBytes()
  const { message, mirror } = await sentByAlice(t, image, { partial: true })
  const b = await phone(t, bob, alice, { fetchTimeoutMs: 60000 })
  const keepAlive = setInterval(() => {}, 1000)
  t.after(() => clearInterval(keepAlive))
  const unlink = link(mirror, b.hypercoreManager.store)
  t.after(unlink)

  await b.receive(CONV, hex(alice), wire(message))
  const fetch = b.mediaRequests._fetches.get(message.mediaId)
  // The mirror lands all but the last block; then the author's socket overtakes it.
  await until(() => b.events.filter(e => e.type === 'media.transfer_progress').length === message.mediaBlockLength - 1)

  const chunkSize = 64 * 1024
  const total = Math.ceil(image.length / chunkSize)
  const hash = b4a.from(message.mediaId, 'hex')
  for (let i = 0; i < total; i++) {
    b.mediaTransfer.onChunkReceived(hash, i, total, image.subarray(i * chunkSize, (i + 1) * chunkSize))
  }
  assert.equal(b.events.filter(e => e.type === 'media.transfer_complete').length, 1)
  assert.equal(b.mediaRequests._fetches.size, 0)
  assert.equal(fetch.cancelled, true)
  assert.equal(b.mediaRequests.get(message.mediaId), undefined)
  await fetch.settled
  assert.equal(fetch.core.closed, true)
  await assertRangeCleared(b, message)
  assert.equal(b.events.filter(e => e.type === 'media.transfer_complete').length, 1)
})

test('cancelling every fetch clears and closes before resolving', async t => {
  const image = imageBytes()
  const { message, mirror, completeMirror } = await sentByAlice(t, image, { partial: true })
  const b = await phone(t, bob, alice, { fetchTimeoutMs: 60000 })
  const keepAlive = setInterval(() => {}, 1000)
  t.after(() => clearInterval(keepAlive))
  const unlink = link(mirror, b.hypercoreManager.store)
  t.after(unlink)

  await b.receive(CONV, hex(alice), wire(message))
  const fetch = b.mediaRequests._fetches.get(message.mediaId)
  await until(() => b.events.filter(e => e.type === 'media.transfer_progress').length === message.mediaBlockLength - 1)
  await b.mediaRequests.cancelFetches()
  assert.equal(fetch.core.closed, true)
  assert.equal(b.mediaRequests._fetches.size, 0, 'nothing restarts until the mirror is back')
  assert.ok(b.mediaRequests.get(message.mediaId).descriptor, 'the request survives for the next mirror')
  await assertRangeCleared(b, message)

  // The relay finishes pulling and the mirror comes back: the request
  // finishes without any other trigger.
  await completeMirror()
  await b.mediaRequests.requestMissingEverywhere()
  await until(() => b.events.some(e => e.type === 'media.transfer_complete'))
  assert.ok(b4a.equals(b.mediaStore.getMedia(message.mediaId), image))
})
