'use strict'

// Exercise index.js's actual wiring. Replace only external swarm/mirror I/O;
// IPC, identity, message validation, storage and lifecycle handlers are real.
const { test } = require('node:test')
const assert = require('node:assert/strict')
const Module = require('node:module')
const EventEmitter = require('node:events')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const { ChatStore } = require('../lib/chat-store')
const tick = () => new Promise(resolve => setImmediate(resolve))
let nextReady = null
const mirrors = []
class Mirror {
  constructor (swarm) { this.swarm = swarm; mirrors.push(this); this.wait = nextReady; nextReady = null }
  async ready () { if (this.wait) await this.wait }
  async close () { this.closed = true }
  cancelNotifications () {}
  addLocalCore () {}
  addRemoteCore () {}
  getDebugInfo () { return { blindPeerCount: 0 } }
}
class Swarm extends EventEmitter {
  constructor (opts) {
    super()
    this.keyPair = opts.keyPair
    this.connections = new Set()
    this.dht = { ready: async () => {}, ping: async () => { throw new Error('unavailable') } }
  }
  join () { return { flushed: async () => {}, destroy: async () => {} } }
  async destroy () {
    for (const socket of this.connections) socket.destroy()
  }
}
const originalLoad = Module._load
Module._load = function (name, ...args) {
  if (name === 'hyperswarm') return Swarm
  if (name === './lib/blind-mirror') return { BlindMirror: Mirror }
  return originalLoad.call(this, name, ...args)
}
let worklet
try { worklet = require('../index') } finally { Module._load = originalLoad }
globalThis.Bare = { argv: [], IPC: { on () {}, write () {} } }

test('worklet production wiring follows a mirror init overtaken by restart and subsequent identity restoration', async t => {
  await worklet.initialize()
  t.after(async () => {
    process.stdin.removeAllListeners('data')
    process.stdin.pause()
    await worklet.shutdown()
  })
  const { ipcHandler: ipc, p2pManager: p2p, identity, chatStore } = worklet.getInstances()
  const events = []
  ipc.pushEvent = (type, payload) => events.push({ type, payload })
  const initial = await ipc.handleIdentity('create', { displayName: 'First' })
  const first = ipc.blindMirror
  assert.equal(first.swarm, p2p.swarm)
  let release
  nextReady = new Promise(resolve => { release = resolve })
  await p2p.stop()
  await p2p.start(identity.keyPair)
  const initializing = mirrors.at(-1)
  assert.notEqual(initializing, first)
  await p2p.stop()
  await p2p.start(identity.keyPair)
  release()
  await ipc.handleMigration('restore_from_seed_phrase', { seedPhrase: initial.seedPhrase })
  assert.equal(initializing.closed, true)
  assert.equal(first.closed, true)
  assert.notEqual(p2p.blindMirror, initializing)
  assert.equal(p2p.blindMirror, ipc.blindMirror)
  assert.equal(ipc.blindMirror.swarm, p2p.swarm)
  const second = await ipc.handleIdentity('create', { displayName: 'Second' })
  assert.notEqual(second.publicKey, initial.publicKey)
  assert.equal(p2p.blindMirror.swarm.keyPair, identity.keyPair)
  const peer = crypto.keyPair(b4a.alloc(32, 9))
  const peerHex = b4a.toString(peer.publicKey, 'hex')
  const id = ChatStore.directChatId(peerHex, identity.publicKeyHex)
  const socket = new EventEmitter()
  socket.remotePublicKey = peer.publicKey
  socket.writable = true
  socket.write = () => true
  socket.destroy = () => { socket.destroyed = true; socket.emit('close') }
  p2p.swarm.connections.add(socket)
  p2p.swarm.emit('connection', socket, { publicKey: peer.publicKey })
  const framed = p2p.framedSockets.get(socket)
  await framed.onMessage({ type: 'direct_invite', senderKey: peerHex, conversationId: id, bootstrapReply: true })
  await framed.onMessage({ id: 'after-restore', conversationId: id, content: 'works', timestamp: Date.now() })
  assert.equal((await chatStore.getMessages(id)).length, 1)
  assert.equal(events.filter(event => event.type === 'message.received').length, 1)
  assert.equal(events.filter(event => event.type === 'conversation.invite_received').length, 1)
  assert.ok(events.some(event => event.type === 'connection.status' && event.payload.online))
  const outgoing = await ipc.handleMessage('send', { conversationId: id, content: 'reply' })
  await framed.onMessage({ type: '__receipt', conversationId: id, kind: 'delivered', upTo: outgoing.message.id, to: identity.publicKeyHex })
  assert.ok(events.some(event => event.type === 'message.status' && event.payload.status === 'delivered'))
  const mediaBytes = b4a.from('verified coalesced image bytes')
  const mediaHash = crypto.data(mediaBytes)
  const { FramedSocket } = require('../lib/socket-framing')
  const frames = []
  const writerSocket = new EventEmitter()
  writerSocket.write = frame => { frames.push(frame); return true }
  const writer = new FramedSocket(writerSocket)
  writer.writeJSON({ id: 'coalesced-media', conversationId: id, contentType: 'image/jpeg', mediaId: b4a.toString(mediaHash, 'hex') })
  writer.writeChunk(mediaHash, 0, 1, mediaBytes)
  socket.emit('data', b4a.concat(frames))
  for (let i = 0; i < 100 && !events.some(event => event.type === 'media.transfer_complete'); i++) await tick()
  const completion = events.find(event => event.type === 'media.transfer_complete')
  assert.ok(completion)
  assert.equal(chatStore.hasAuthorizedMediaReference(id, completion.payload.mediaId), true)
  const row = (await chatStore.getMessages(id)).find(message => message.id === 'coalesced-media')
  assert.equal(row.mediaLocalPath, completion.payload.mediaLocalPath)
  assert.equal(row.mediaTransferState, 'complete')
  await tick()
})

test('worklet receives identical media from separate conversations without blocking or sharing authorization', async t => {
  await worklet.initialize()
  t.after(async () => {
    process.stdin.removeAllListeners('data')
    process.stdin.pause()
    await worklet.shutdown()
  })
  const { ipcHandler: ipc, p2pManager: p2p, identity, chatStore, mediaTransfer } = worklet.getInstances()
  const events = []
  ipc.pushEvent = (type, payload) => events.push({ type, payload })
  await ipc.handleIdentity('create', { displayName: 'Same hash receiver' })
  const peers = [10, 11].map(n => crypto.keyPair(b4a.alloc(32, n)))
  const peerIds = peers.map(peer => b4a.toString(peer.publicKey, 'hex'))
  const ids = peerIds.map(peer => ChatStore.directChatId(peer, identity.publicKeyHex))
  const bytes = b4a.from('same image, separately authorized senders')
  const hash = crypto.data(bytes), mediaId = b4a.toString(hash, 'hex')
  const requests = []
  let firstOnline = false
  p2p.requestMedia = (conversationId, requestedHash, senderId) => {
    requests.push({ conversationId, requestedHash, senderId })
    return conversationId === ids[1] || firstOnline
  }
  for (let i = 0; i < peers.length; i++) {
    const socket = new EventEmitter()
    socket.remotePublicKey = peers[i].publicKey
    socket.writable = true
    socket.write = () => true
    socket.destroy = () => { socket.destroyed = true; socket.emit('close') }
    p2p.swarm.connections.add(socket)
    p2p.swarm.emit('connection', socket, { publicKey: peers[i].publicKey })
    const framed = p2p.framedSockets.get(socket)
    await framed.onMessage({ type: 'direct_invite', senderKey: peerIds[i], conversationId: ids[i], bootstrapReply: true })
    await framed.onMessage({ id: 'shared-' + i, conversationId: ids[i], contentType: 'image/jpeg', mediaId })
  }
  assert.ok(requests.some(request => request.conversationId === ids[1] && request.senderId === peerIds[1]))
  // The first scope is still offline. Its late chunks and an outsider's
  // chunks cannot enter the second conversation's active reassembler.
  p2p.emit('media_chunk', hash, 0, 1, bytes, peerIds[0])
  p2p.emit('media_chunk', hash, 0, 1, bytes, 'ff'.repeat(32))
  assert.equal(mediaTransfer.activeTransfers.size, 0)
  assert.equal(events.filter(event => event.type === 'media.transfer_complete').length, 0)
  p2p.emit('media_chunk', hash, 0, 1, bytes, peerIds[1])
  assert.equal(chatStore.hasAuthorizedMediaReference(ids[1], mediaId), true)
  assert.equal(chatStore.hasAuthorizedMediaReference(ids[0], mediaId), false)
  assert.equal((await chatStore.getMessages(ids[0]))[0].mediaLocalPath, null)
  assert.equal(events.find(event => event.type === 'media.transfer_complete').payload.conversationId, ids[1])
  // Completion did not discard the first conversation. It can reconnect and
  // prove its own possession without authorizing from the existing disk cache.
  firstOnline = true
  await ipc.routeMessage('media.retry', { conversationId: ids[0], messageId: 'shared-0' })
  p2p.emit('media_chunk', hash, 0, 1, bytes, peerIds[1])
  assert.equal(chatStore.hasAuthorizedMediaReference(ids[0], mediaId), false)
  p2p.emit('media_chunk', hash, 0, 1, bytes, peerIds[0])
  assert.equal(chatStore.hasAuthorizedMediaReference(ids[0], mediaId), true)
  assert.equal(events.filter(event => event.type === 'media.transfer_complete').length, 2)
})
