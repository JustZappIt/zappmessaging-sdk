'use strict'

const { test } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const EventEmitter = require('node:events')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const { P2PManager } = require('../lib/p2p-manager')
const { IPCHandler } = require('../lib/ipc-handler')
const { Identity } = require('../lib/identity')
const { ChatStore } = require('../lib/chat-store')
const { ContactStore } = require('../lib/contact-store')
const { HypercoreManager } = require('../lib/hypercore-manager')
const { MediaStore } = require('../lib/media-store')
const { MediaTransfer } = require('../lib/media-transfer')
const { createPeerReceiver, acceptAuthorizedMediaChunk } = require('../lib/peer-receiver')
const { BlindMirror } = require('../lib/blind-mirror')
const { NotificationWork } = require('../lib/notification-work')
const { DHTHealthMonitor } = require('../lib/dht-health')
const { encryptInvite, drainInvites, throughHttps } = require('../lib/invite-mailbox')
const { MailboxStore } = require('../../server/invite-mailbox')
const { entropyToMnemonic } = require('../lib/mnemonic')
globalThis.Bare = { argv: [], IPC: { on () {}, write () {} } }
const pair = n => crypto.keyPair(b4a.alloc(32, n))
const hex = keyPair => b4a.toString(keyPair.publicKey, 'hex')
const phrase = n => entropyToMnemonic(b4a.alloc(32, n))
const tick = () => new Promise(resolve => setImmediate(resolve))
function gate () {
  let resolve, reject
  const promise = new Promise((yes, no) => { resolve = yes; reject = no })
  return { promise, resolve, reject }
}
function temporary (t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'zapp-recovery-'))
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }))
  return dir
}
class Swarm extends EventEmitter {
  constructor () {
    super()
    this.connections = new Set()
    this.dht = { ready: async () => {}, ping: async () => { throw new Error('unavailable') } }
  }
  join () { return { flushed: async () => {}, destroy: async () => {} } }
  async destroy () {
    for (const socket of this.connections) socket.destroy()
    this.destroyed = true
  }
}
function connect (p2p, peer) {
  const socket = new EventEmitter()
  socket.remotePublicKey = peer.publicKey
  socket.writable = true
  socket.frames = []
  socket.write = data => { socket.frames.push(b4a.from(data)); return true }
  socket.destroy = () => { socket.destroyed = true; socket.writable = false; socket.emit('close') }
  p2p.swarm.connections.add(socket)
  p2p.swarm.emit('connection', socket, { publicKey: peer.publicKey })
  return { socket, framed: p2p.framedSockets.get(socket) }
}
async function harness (t, opts = {}) {
  const dir = temporary(t)
  const identity = new Identity()
  identity.storagePath = path.join(dir, 'identity.json')
  await identity.restoreFromMnemonic(phrase(1), 'First')
  const chatStore = new ChatStore()
  chatStore.storagePath = path.join(dir, 'chats')
  fs.mkdirSync(chatStore.storagePath)
  const contactStore = new ContactStore()
  contactStore.storagePath = path.join(dir, 'contacts.json')
  const mediaStore = new MediaStore()
  mediaStore.mediaDir = path.join(dir, 'media')
  fs.mkdirSync(mediaStore.mediaDir)
  const mediaTransfer = new MediaTransfer(mediaStore)
  const requests = new Map()
  const hypercoreManager = new HypercoreManager({ dataDir: dir })
  hypercoreManager.setKeyContext({
    getIdentityKeyPair: () => identity.keyPair,
    getConversation: id => chatStore.conversations.get(id)
  })
  await hypercoreManager.initialize()
  const p2p = new P2PManager({
    createSwarm: () => new Swarm(), blindPeerKeys: [], hypercoreManager,
    isParticipant: (id, peer) => chatStore.isPeerAuthorized(id, peer),
    getConversation: id => chatStore.conversations.get(id),
    ...opts
  })
  const events = []
  const ipc = new IPCHandler({ identity, chatStore, contactStore, p2pManager: p2p,
    hypercoreManager, mediaStore, mediaTransfer })
  process.stdin.removeAllListeners('data')
  process.stdin.pause()
  ipc.pushEvent = (type, payload) => events.push({ type, payload })
  p2p.on('status', status => ipc.pushEvent('connection.status', status))
  p2p.on('peer_online', (id, peer) => ipc.pushEvent('connection.peer_status', { id, peer }))
  p2p.on('media_chunk', (hash, index, count, data, peer) =>
    acceptAuthorizedMediaChunk(chatStore, mediaTransfer, requests, hash, index, count, data, peer))
  mediaTransfer.on('complete', hash => ipc.pushEvent('media.transfer_complete', { hash }))
  const receiver = createPeerReceiver(() => ({
    chatStore, p2pManager: p2p, ipcHandler: ipc, identity,
    processReceipt: async (id, receipt) => {
      if (receipt.kind === 'read') chatStore.markReadUpTo(id, receipt.upTo)
      else if (receipt.to === identity.publicKeyHex) chatStore.markDeliveredUpTo(id, receipt.upTo)
    },
    onMessage: (id, stored) => { if (stored) ipc.pushEvent('message.received', { id, message: stored }) }
  }))
  p2p.setPeerRecordSink(receiver)
  p2p._deliverInvite = (invite, peer) => ipc.deliverMailboxInvite(invite, peer)
  hypercoreManager.setRemoteMessageSink((id, peer, record) => receiver(id, peer, record, { replicated: true }))
  hypercoreManager.setRemoteDrainCompleteSink(async () => {})
  await p2p.start(identity.keyPair)
  t.after(async () => { mediaTransfer.resetIdentity(); await p2p.stop(); await hypercoreManager.close() })
  return { dir, identity, chatStore, contactStore, mediaTransfer, requests, hypercoreManager, p2p, ipc, events }
}
const directInvite = (sender, recipient, extra = {}) => ({
  type: 'direct_invite', senderKey: hex(sender),
  conversationId: ChatStore.directChatId(hex(sender), hex(recipient)),
  senderDisplayName: 'Sender', bootstrapReply: true, ...extra
})

test('real stop/restart retains production invite, message, receipt, media and connection handlers', async t => {
  const h = await harness(t)
  const peer = pair(9)
  const listenerCounts = Object.keys(h.p2p._events).map(name => [name, h.p2p.listenerCount(name)])
  let stale
  for (let n = 2; n <= 4; n++) {
    const oldSwarm = h.p2p.swarm
    const oldCore = h.hypercoreManager.store
    await h.ipc.handleMigration('restore_from_seed_phrase', { seedPhrase: phrase(n) })
    assert.equal(oldSwarm.destroyed, true)
    assert.notEqual(h.hypercoreManager.store, oldCore)
    assert.deepEqual(Object.keys(h.p2p._events).map(name => [name, h.p2p.listenerCount(name)]), listenerCounts)
    assert.equal(h.hypercoreManager.localCores.size, 0)
    assert.equal(h.hypercoreManager.remoteCores.size, 0)
    assert.equal(h.hypercoreManager._processedCursors.size, 0)
    assert.equal(h.chatStore.conversations.size, 0)
    const { framed } = connect(h.p2p, peer)
    const invite = directInvite(peer, h.identity.keyPair)
    await framed.onMessage(invite)
    assert.ok(await h.chatStore.getConversation(invite.conversationId))
    connect(h.p2p, peer)
    const record = { id: 'incoming-' + n, conversationId: invite.conversationId, content: 'hello', timestamp: Date.now() }
    await framed.onMessage(record)
    await framed.onMessage(record)
    assert.equal((await h.chatStore.getMessages(invite.conversationId)).filter(m => m.id === record.id).length, 1)
    const outgoing = await h.ipc.handleMessage('send', { conversationId: invite.conversationId, content: 'reply' })
    await framed.onMessage({ type: '__receipt', kind: 'delivered', conversationId: invite.conversationId,
      upTo: outgoing.message.id, to: h.identity.publicKeyHex })
    await framed.onMessage({ type: '__receipt', kind: 'read', conversationId: invite.conversationId, upTo: outgoing.message.id })
    assert.equal(h.chatStore.getMessageStatus(invite.conversationId, outgoing.message.id), 'read')
    const data = b4a.from('verified media ' + n)
    const hash = crypto.data(data)
    h.requests.set(b4a.toString(hash, 'hex'), { conversationId: invite.conversationId })
    framed.onChunk(hash, 0, 1, data)
    assert.ok(h.events.some(event => event.type === 'media.transfer_complete' && event.payload.hash === b4a.toString(hash, 'hex')))
    if (stale) {
      const before = h.events.length
      await stale.onMessage({ ...record, id: 'stale' })
      stale.onChunk(hash, 0, 1, data)
      assert.equal(h.events.length, before, 'old socket callbacks are inert')
    }
    stale = framed
    assert.ok(h.p2p._heartbeatInterval)
    assert.ok(h.p2p.healthMonitor.monitoringInterval)
  }
  assert.equal(h.events.filter(event => event.type === 'message.received').length, 3)
  assert.ok(h.events.some(event => event.type === 'connection.peer_status'))
  await h.p2p.stop()
  assert.equal(h.p2p._heartbeatInterval, null)
  assert.equal(h.p2p.healthMonitor.monitoringInterval, null)
  assert.equal(h.p2p._swarmDumpInterval, null)
})

test('same seed preserves history, cores, swarm, name and preferences; invalid restore never stops or clears', async t => {
  const h = await harness(t)
  const peer = pair(9)
  const invite = directInvite(peer, h.identity.keyPair)
  await h.ipc.deliverMailboxInvite(invite, hex(peer))
  await h.ipc.handleMessage('send', { conversationId: invite.conversationId, content: 'history' })
  const swarm = h.p2p.swarm
  const core = h.hypercoreManager.localCores.get(invite.conversationId)
  h.ipc.readReceiptsEnabled = false
  for (let i = 0; i < 2; i++) await h.ipc.handleMigration('restore_from_seed_phrase', { seedPhrase: phrase(1), displayName: 'Ignored' })
  assert.equal(h.identity.displayName, 'First')
  assert.equal(h.ipc.readReceiptsEnabled, false)
  assert.equal(h.p2p.swarm, swarm)
  assert.equal(h.hypercoreManager.localCores.get(invite.conversationId), core)
  assert.equal((await h.chatStore.getMessages(invite.conversationId)).length, 1)
  await assert.rejects(h.ipc.handleMigration('restore_from_seed_phrase', { seedPhrase: 'invalid' }))
  h.identity.keyPair = null
  await assert.rejects(h.ipc.handleMigration('restore_from_seed_phrase', { seedPhrase: 'invalid' }))
  assert.equal(h.p2p.swarm, swarm)
  assert.equal((await h.chatStore.getMessages(invite.conversationId)).length, 1)
})

test('identity replacement waits for old production receive writes and rejects overlapping IPC', async t => {
  const h = await harness(t)
  const peer = pair(9)
  const { framed } = connect(h.p2p, peer)
  const invite = directInvite(peer, h.identity.keyPair)
  await framed.onMessage(invite)
  const paused = gate()
  const entered = gate()
  const addMessage = h.chatStore.addMessage.bind(h.chatStore)
  const oldKey = h.identity.publicKeyHex
  let writeIdentity
  h.chatStore.addMessage = async (...args) => {
    entered.resolve()
    await paused.promise
    writeIdentity = h.identity.publicKeyHex
    return addMessage(...args)
  }
  const receiving = framed.onMessage({ id: 'old-pending', conversationId: invite.conversationId, content: 'old', timestamp: Date.now() })
  await entered.promise
  let changed = false
  const changing = h.ipc.handleIdentity('create', { displayName: 'New' }).then(() => { changed = true })
  await tick()
  assert.equal(changed, false)
  await assert.rejects(h.ipc.routeMessage('message.send', { conversationId: invite.conversationId, content: 'crossed' }), /Identity change/)
  paused.resolve()
  await Promise.all([receiving, changing])
  assert.equal(writeIdentity, oldKey)
  assert.notEqual(h.identity.publicKeyHex, oldKey)
  assert.equal(h.chatStore.conversations.size, 0)
  assert.equal((await h.chatStore.getMessages(invite.conversationId)).length, 0)
  assert.equal(h.hypercoreManager.localCores.size, 0)
})

test('direct delivery and IPC success occur while notification is pending; late failure cannot downgrade a receipt', async t => {
  const h = await harness(t)
  const peer = pair(9)
  const { framed, socket } = connect(h.p2p, peer)
  const invite = directInvite(peer, h.identity.keyPair)
  await framed.onMessage(invite)
  const pending = gate()
  let notifications = 0
  const failures = []
  h.p2p.blindMirror = { sendNotification: () => { notifications++; return pending.promise } }
  h.p2p.on('push_notification_failed', detail => failures.push(detail))
  socket.frames.length = 0
  const result = await h.ipc.handleMessage('send', { conversationId: invite.conversationId, content: 'prompt' })
  assert.equal(result.durability.appended, true)
  assert.equal(result.durability.relay, 'pending')
  assert.equal(result.sent, true)
  assert.equal(notifications, 1)
  assert.equal(h.hypercoreManager.localCores.get(invite.conversationId).length, 1)
  const frames = socket.frames.filter(frame => frame[4] === 1).map(frame => JSON.parse(frame.subarray(5)))
  assert.equal(frames.filter(frame => frame.id === result.message.id).length, 1)
  await framed.onMessage({ type: '__receipt', kind: 'read', conversationId: invite.conversationId, upTo: result.message.id })
  pending.reject(new Error('notification unavailable'))
  await tick()
  assert.equal(failures.length, 1)
  assert.equal(h.chatStore.getMessageStatus(invite.conversationId, result.message.id), 'read')
  h.ipc._recordOutgoingStatus(invite.conversationId, result.message, { relay: 'pending' })
  assert.equal(result.message.status, 'read')
  assert.equal((await h.chatStore.getMessages(invite.conversationId)).length, 1)
})

test('notification work bounds concurrency, retries and non-cancellable timed-out RPCs', async () => {
  const queue = new NotificationWork({ concurrency: 2, maxQueued: 3, timeoutMs: 10 })
  const pending = gate()
  let calls = 0
  const jobs = Array.from({ length: 12 }, () => queue.enqueue(() => { calls++; return pending.promise }, [0, 1, 1]))
  const outcomes = await Promise.allSettled(jobs)
  assert.equal(calls, 2)
  assert.equal(outcomes.filter(result => result.status === 'rejected').length, 12)
  assert.equal(queue.attempts.size, 2)
  pending.resolve()
  await tick()
  let failures = 0
  await assert.rejects(queue.enqueue(async () => { failures++; throw new Error('offline') }, [0, 1, 1]))
  assert.equal(failures, 3)
  queue.cancel()
  assert.equal(queue.waiters.size, 0)
})

test('suspension and close cancel notification retries; old identity notification failures stay silent', async t => {
  const mirror = new BlindMirror({}, {}, { notificationRetryDelays: [0, 1000], notificationWork: { timeoutMs: 1000 } })
  let calls = 0
  mirror._peering = { sendNotification: async () => { calls++; throw new Error('offline') }, suspend: async () => {}, close: async () => {} }
  const result = mirror.sendNotification({}, 0)
  await tick()
  await mirror.suspend()
  await assert.rejects(result, /cancelled/)
  assert.equal(calls, 1)
  await mirror.close()
  assert.equal(mirror._notifications.waiters.size, 0)
  const h = await harness(t)
  const peer = pair(9)
  const invite = directInvite(peer, h.identity.keyPair)
  await h.ipc.deliverMailboxInvite(invite, hex(peer))
  const pending = gate()
  h.p2p.blindMirror = { sendNotification: () => pending.promise }
  let late = 0
  h.p2p.on('push_notification_failed', () => late++)
  await h.ipc.handleMessage('send', { conversationId: invite.conversationId, content: 'old' })
  await h.ipc.handleMigration('restore_from_seed_phrase', { seedPhrase: phrase(2) })
  pending.reject(new Error('late old failure'))
  await tick()
  assert.equal(late, 0)
})

test('mailbox fallback invitation survives an empty recovered primary and duplicate transports, including restart/updated invite', async t => {
  const h = await harness(t)
  const sender = pair(9)
  const recipient = h.identity.keyPair
  const stores = ['primary', 'fallback'].map(name => new MailboxStore({ directory: path.join(h.dir, name) }))
  t.after(() => stores.forEach(store => store.close()))
  let primaryDown = true
  const accesses = []
  const request = (index, method, body) => {
    accesses.push([index, method])
    if (index === 0 && primaryDown) throw new Error('primary down')
    if (method === 'put') return stores[index].put(body.recipient, body.envelope, null)
    if (method === 'list') return stores[index].list(hex(recipient))
    if (method === 'ack') return stores[index].ack(hex(recipient), body.ids)
  }
  const transports = {
    throughHttps: async (url, keys, operation) => operation((method, body) => request(Number(url), method, body)),
    throughDht: async (dht, keys, key, address, operation) => operation((method, body) => request(Number(key), method, body))
  }
  const mailboxes = [{ url: '0', key: '0' }, { key: '1' }]
  const sending = new P2PManager({ mailboxStores: mailboxes, mailboxTransports: transports })
  sending.keyPair = sender
  sending.swarm = { dht: {} }
  const invite = directInvite(sender, recipient)
  assert.equal(await sending._putInviteMailbox(hex(recipient), invite), true)
  assert.equal(stores[1].list(hex(recipient)).entries.length, 1)
  primaryDown = false
  h.p2p._mailboxStores = mailboxes
  h.p2p._mailboxTransports = transports
  assert.equal(await h.p2p._drainInviteMailboxes(), 1)
  assert.ok(h.chatStore.conversations.has(invite.conversationId))
  assert.equal(stores[1].list(hex(recipient)).entries.length, 0)
  assert.equal(h.events.filter(event => event.type === 'conversation.invite_received').length, 1)
  // Redelivery after a lost ack (including after reloading persisted markers).
  for (const store of stores) store.put(hex(recipient), encryptInvite(invite, sender, hex(recipient)), null)
  h.chatStore.loadConversations()
  await h.p2p._drainInviteMailboxes()
  assert.equal(h.events.filter(event => event.type === 'conversation.invite_received').length, 1)
  for (const store of stores) assert.equal(store.list(hex(recipient)).entries.length, 0)
  const updated = { ...invite, senderDisplayName: 'Updated' }
  stores[1].put(hex(recipient), encryptInvite(updated, sender, hex(recipient)), null)
  await h.p2p._drainInviteMailboxes()
  assert.equal(h.chatStore.conversations.get(invite.conversationId).displayName, 'Updated')
  assert.equal(h.events.filter(event => event.type === 'conversation.invite_received').length, 2)
  assert.ok(accesses.some(([index, method]) => index === 1 && method === 'ack'))
})

test('slow mailbox does not block other stores; transient entries retry and malformed entries are acknowledged at source', async () => {
  const sender = pair(8), recipient = pair(7)
  const invite = directInvite(sender, recipient)
  const slow = gate(), progressed = gate()
  const acks = []
  let attempts = 0
  const entries = [{ id: 'a'.repeat(32), envelope: encryptInvite(invite, sender, hex(recipient)) }, { id: 'b'.repeat(32), envelope: 'junk' }]
  const manager = new P2PManager({
    mailboxStores: [{ url: 'slow' }, { url: 'fast' }],
    mailboxTransports: { throughHttps: async (url, keys, operation) => {
      if (url === 'slow') await slow.promise
      return operation(async (method, body) => {
        if (method === 'list') return { entries: url === 'fast' ? entries : [], more: false }
        acks.push(body.ids)
        progressed.resolve()
      })
    } },
    deliverInvite: async () => { attempts++; if (attempts === 1) throw new Error('temporary disk error'); return true }
  })
  manager.keyPair = recipient
  const draining = manager._drainInviteMailboxes()
  await progressed.promise
  assert.deepEqual(acks, [['b'.repeat(32)]])
  slow.resolve()
  await draining
  await manager._drainInviteMailboxes()
  assert.deepEqual(acks[1], ['a'.repeat(32), 'b'.repeat(32)])
  assert.equal(attempts, 2)
})

test('HTTPS timeout and pagination are bounded', async () => {
  await assert.rejects(throughHttps('https://mailbox.example', pair(1), request => request('list', {}), {
    timeout: 5, postJson: () => new Promise(() => {})
  }), /timeout/)
  let pages = 0
  await drainInvites(async method => method === 'list'
    ? (++pages, { entries: [{ id: String(pages), envelope: 'invalid' }], more: true }) : {}, pair(1))
  assert.equal(pages, 8)
})

test('global capacity reclaims inactive recipients, preserves live entries and survives restart/ack', t => {
  const directory = temporary(t)
  const sender = pair(1), first = pair(2), live = pair(3), next = pair(4)
  const now = Date.now
  let clock = now()
  Date.now = () => clock
  t.after(() => { Date.now = now })
  let store = new MailboxStore({ directory, maxTotal: 2, ttlMs: 100 })
  t.after(() => store.close())
  const put = recipient => store.put(hex(recipient), encryptInvite({}, sender, hex(recipient)), null)
  put(first)
  clock += 50
  put(live)
  clock += 51
  put(next) // No access to first's mailbox is needed.
  assert.equal(store._total, 2)
  assert.equal(fs.existsSync(path.join(directory, hex(first) + '.json')), false)
  assert.equal(store.list(hex(live)).entries.length, 1)
  store.close()
  store = new MailboxStore({ directory, maxTotal: 2, ttlMs: 100 })
  assert.equal(store._total, 2)
  const ids = store.list(hex(live)).entries.map(entry => entry.id)
  assert.equal(store.ack(hex(live), ids), 1)
  assert.equal(store.ack(hex(live), ids), 0)
  assert.equal(store._total, 1)
  clock += 101
  store.close()
  store = new MailboxStore({ directory, maxTotal: 2, ttlMs: 100 })
  assert.equal(store._total, 0)
  assert.equal(fs.readdirSync(directory).length, 0)
})

test('maintenance batches are bounded, reclaim without requests, contain delete failures and stop on close', async t => {
  const directory = temporary(t)
  const store = new MailboxStore({ directory, ttlMs: 10, cleanupIntervalMs: 5, cleanupBatchSize: 1 })
  t.after(() => store.close())
  const sender = pair(1)
  for (const recipient of [pair(2), pair(3), pair(4)]) store.put(hex(recipient), encryptInvite({}, sender, hex(recipient)), null)
  let reads = 0
  const read = store._read.bind(store)
  store._read = key => { reads++; return read(key) }
  store.reclaimExpired()
  assert.equal(reads, 1)
  const deadline = Date.now() + 1000
  while (store._total && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 5))
  assert.equal(store._total, 0)
  store.close()
  assert.equal(store._maintenanceTimer, null)
  // A disk failure cannot free capacity while its invitations still exist.
  const recipient = pair(5)
  store.put(hex(recipient), encryptInvite({}, sender, hex(recipient)), null)
  const unlink = fs.unlinkSync
  fs.unlinkSync = () => { const error = new Error('disk denied'); error.code = 'EACCES'; throw error }
  try {
    await new Promise(resolve => setTimeout(resolve, 15))
    store.reclaimExpired()
    assert.equal(store._total, 1)
    assert.equal(fs.existsSync(path.join(directory, hex(recipient) + '.json')), true)
  } finally { fs.unlinkSync = unlink }
  store.reclaimExpired()
  assert.equal(store._total, 0)
})

test('stopping a pending health check cancels its timer and prevents late events or intervals', async () => {
  const pending = gate()
  const monitor = new DHTHealthMonitor({ checkTimeout: 10000 })
  monitor.setSwarm({ dht: { ready: () => pending.promise } })
  let events = 0
  monitor.on('health_check', () => events++)
  const starting = monitor.startMonitoring()
  monitor.stopMonitoring()
  await starting
  pending.resolve()
  await tick()
  assert.equal(events, 0)
  assert.equal(monitor.monitoringInterval, null)
  assert.equal(monitor._checks.size, 0)
})

test('same-seed restore restarts a stopped manager and rejoins preserved history without reappending', async t => {
  const h = await harness(t)
  const peer = pair(9)
  const invite = directInvite(peer, h.identity.keyPair)
  await h.ipc.deliverMailboxInvite(invite, hex(peer))
  await h.ipc.handleMessage('send', { conversationId: invite.conversationId, content: 'retained' })
  const core = h.hypercoreManager.localCores.get(invite.conversationId)
  const length = core.length
  await h.p2p.stop()
  await h.ipc.handleMigration('restore_from_seed_phrase', { seedPhrase: phrase(1) })
  assert.ok(h.p2p.swarm)
  assert.ok(h.p2p.conversations.has(invite.conversationId))
  assert.equal(h.hypercoreManager.localCores.get(invite.conversationId), core)
  assert.equal(core.length, length)
  assert.equal(h.p2p.pendingMessages.get(invite.conversationId).length, 1)
  assert.equal((await h.chatStore.getMessages(invite.conversationId)).length, 1)
})

test('stop during startup cannot resurrect listeners or timers and concurrent restarts use one swarm', async () => {
  const pending = gate()
  let created = 0
  const manager = new P2PManager({ blindPeerKeys: [], createSwarm: () => {
    created++
    const swarm = new Swarm()
    if (created === 1) swarm.dht.ready = () => pending.promise
    return swarm
  } })
  const starting = manager.start(pair(1))
  const stopping = manager.stop()
  pending.resolve()
  await Promise.all([starting, stopping])
  assert.equal(manager.swarm, null)
  assert.equal(manager._heartbeatInterval, null)
  assert.equal(manager._swarmDumpInterval, null)
  assert.equal(manager.healthMonitor.monitoringInterval, null)
  await Promise.all([manager.start(pair(2)), manager.start(pair(2))])
  assert.equal(created, 2)
  assert.ok(manager._heartbeatInterval)
  await manager.dispose()
  assert.equal(Object.keys(manager._events).length, 0)
})

test('old background receipt appends finish before stop completes', async () => {
  const pending = gate()
  const manager = new P2PManager({ hypercoreManager: { appendMessage: () => pending.promise } })
  manager.sendReadReceipt('chat', 'message')
  let stopped = false
  const stopping = manager.stop().then(() => { stopped = true })
  await tick()
  assert.equal(stopped, false)
  pending.resolve()
  await stopping
  assert.equal(stopped, true)
})

test('identity reset cancels partial and queued media without later events', async () => {
  const media = new MediaTransfer({ getMedia: () => b4a.alloc(1024 * 1024, 7) })
  let sent = 0, complete = 0
  media.on('sent', () => sent++)
  media.on('complete', () => complete++)
  const hash = crypto.data(b4a.from('partial'))
  media.onChunkReceived(hash, 0, 2, b4a.from('part'))
  let reset = false
  const sending = media.sendMedia({ writeChunk () {
    if (!reset) { reset = true; media.resetIdentity() }
  } }, hex(pair(1)))
  await assert.rejects(sending, /identity changed/)
  assert.equal(media.activeTransfers.size, 0)
  assert.equal(media._activeBytes, 0)
  assert.equal(sent, 0)
  assert.equal(complete, 0)
})

test('mailbox read corruption and acknowledgement failures never discount or overwrite accepted mail', t => {
  const directory = temporary(t)
  const store = new MailboxStore({ directory })
  t.after(() => store.close())
  const sender = pair(1), recipient = pair(2)
  store.put(hex(recipient), encryptInvite({}, sender, hex(recipient)), null)
  const entry = store.list(hex(recipient)).entries[0]
  const unlink = fs.unlinkSync
  fs.unlinkSync = () => { throw new Error('disk failure') }
  try {
    assert.throws(() => store.ack(hex(recipient), [entry.id]), /disk failure/)
    assert.equal(store._total, 1)
  } finally { fs.unlinkSync = unlink }
  const target = path.join(directory, hex(recipient) + '.json')
  fs.writeFileSync(target, '{ corrupt')
  assert.throws(() => store.put(hex(recipient), encryptInvite({}, sender, hex(recipient)), null))
  assert.equal(fs.readFileSync(target, 'utf8'), '{ corrupt')
  assert.equal(store._total, 1)
  store.close()
  assert.throws(() => new MailboxStore({ directory }), 'startup fails closed on unreadable accounting')
})

test('permanent group mailbox rejection writes no dedup metadata; transient join/core failure remains retryable', async t => {
  const h = await harness(t)
  const owner = pair(9), outsider = pair(8)
  const groupId = 'dd'.repeat(32)
  const conv = await h.chatStore.createConversation('group', [hex(owner)], { groupId, creatorKey: hex(owner), displayName: 'Original' })
  const forged = { type: 'group_invite', senderKey: hex(outsider), creatorKey: hex(outsider), groupId,
    groupName: 'Forged', participants: [h.identity.publicKeyHex, hex(owner), hex(outsider)] }
  const before = JSON.stringify(conv)
  assert.equal(await h.ipc.deliverMailboxInvite(forged, hex(outsider)), true)
  assert.equal(JSON.stringify(conv), before, 'permanent rejection cannot mutate even dedup metadata')
  assert.equal(h.events.filter(event => event.type === 'conversation.invite_received').length, 0)
  const valid = { type: 'group_invite', senderKey: hex(owner), creatorKey: hex(owner), groupId,
    participants: [h.identity.publicKeyHex, hex(owner)], localCoreKey: 'cc'.repeat(32) }
  const join = h.p2p.joinGroupConversation.bind(h.p2p)
  const open = h.p2p.openRemoteCore.bind(h.p2p)
  h.p2p.joinGroupConversation = async () => false
  assert.equal(await h.ipc.deliverMailboxInvite(valid, hex(owner)), false)
  assert.equal(conv.appliedControls, undefined)
  h.p2p.joinGroupConversation = join
  h.p2p.openRemoteCore = async () => false
  assert.equal(await h.ipc.deliverMailboxInvite(valid, hex(owner)), false)
  assert.equal(conv.appliedControls, undefined)
  h.p2p.openRemoteCore = async () => true
  assert.equal(await h.ipc.deliverMailboxInvite(valid, hex(owner)), true)
  assert.equal(h.events.filter(event => event.type === 'conversation.invite_received').length, 1)
  assert.equal(await h.ipc.deliverMailboxInvite(valid, hex(owner)), true)
  assert.equal(h.events.filter(event => event.type === 'conversation.invite_received').length, 1)
  h.p2p.openRemoteCore = open
})

test('media connection preparation and durable send proceed while discovery, invitation and notification stall', async t => {
  const h = await harness(t)
  const peer = pair(16)
  const conv = await h.chatStore.createConversationWithId(ChatStore.directChatId(h.identity.publicKeyHex, hex(peer)), 'direct', [hex(peer)])
  const discovery = gate(), invitation = gate(), notification = gate()
  h.p2p.swarm.join = () => ({ flushed: () => discovery.promise, destroy: async () => {} })
  h.ipc._sendDirectInvite = () => invitation.promise
  h.p2p.blindMirror = { sendNotification: () => notification.promise }
  const deadline = new Promise((_, reject) => {
    const timer = setTimeout(() => reject(new Error('media preflight blocked')), 1000)
    t.after(() => clearTimeout(timer))
  })
  await Promise.race([h.ipc.handleConnection('connect', { conversationId: conv.id, forMedia: true }), deadline])
  const connected = connect(h.p2p, peer)
  const saved = h.ipc.mediaStore.saveMedia(b4a.alloc(1024 * 1024, 7), 'jpg')
  const accepted = await Promise.race([h.ipc.handleMedia('send_message', {
    conversationId: conv.id, contentType: 'image/jpeg', mediaId: saved.hashHex,
    mediaLocalPath: saved.filePath, mediaSize: saved.fileSize, clientMessageId: 'stable-client-row'
  }), deadline])
  assert.equal(accepted.message.id, 'stable-client-row')
  assert.equal(accepted.durability.appended, true)
  assert.equal(accepted.message.mediaTransferState, 'queued')
  await new Promise(resolve => setTimeout(resolve, 20))
  assert.equal(connected.socket.frames.filter(frame => frame[4] === 2).reduce((n, frame) => n + frame.length - 45, 0), saved.fileSize)
  // Retrying transfer reuses the persisted message, never appends another row.
  await h.ipc.handleMedia('retry', { conversationId: conv.id, messageId: accepted.message.id })
  assert.equal((await h.chatStore.getMessages(conv.id)).length, 1)
  assert.equal((await h.hypercoreManager.getOrCreateLocalCore(conv.id)).length, 1)
  discovery.resolve(); invitation.resolve(); notification.resolve()
  h.ipc._mediaBootstrap.cancel()
})

async function mediaFixture (t) {
  const h = await harness(t), peer = pair(19)
  const conv = await h.chatStore.createConversationWithId(
    ChatStore.directChatId(h.identity.publicKeyHex, hex(peer)), 'direct', [hex(peer)])
  const saved = h.ipc.mediaStore.saveMedia(b4a.alloc(1024, 7), 'jpg')
  const payload = { conversationId: conv.id, contentType: 'image/jpeg', mediaId: saved.hashHex,
    mediaLocalPath: saved.filePath, mediaSize: saved.fileSize, clientMessageId: 'retry-client-id' }
  const core = await h.hypercoreManager.getOrCreateLocalCore(conv.id)
  const send = () => h.ipc.routeMessage('media.send_message', payload)
  const retry = () => h.ipc.routeMessage('media.retry', { conversationId: conv.id, messageId: payload.clientMessageId })
  return { ...h, conv, payload, core, send, retry }
}

test('stable media IDs deduplicate sequential and concurrent IPC retries in the durable log', async t => {
  const h = await mediaFixture(t)
  let notifications = 0
  h.p2p.blindMirror = { sendNotification: async () => { notifications++ } }
  await Promise.all([h.send(), h.send(), h.send()])
  await h.send()
  await Promise.all([h.retry(), h.retry()])
  assert.equal((await h.chatStore.getMessages(h.conv.id)).length, 1)
  assert.equal(h.core.length, 1)
  assert.equal(notifications, 1)
  assert.equal(h.p2p.pendingMessages.get(h.conv.id).length, 1)
})

test('media retry repairs a failed append before reporting queued while offline', async t => {
  const h = await mediaFixture(t)
  const append = h.core.append.bind(h.core)
  h.core.append = async () => { throw new Error('disk unavailable') }
  await assert.rejects(h.send(), /disk unavailable/)
  assert.equal((await h.chatStore.getMessages(h.conv.id)).length, 1)
  assert.equal(h.core.length, 0)
  await assert.rejects(h.retry(), /disk unavailable/)
  assert.equal(h.p2p.pendingMessages.has(h.conv.id), false)
  h.core.append = append
  assert.deepEqual(await h.retry(), { queued: true })
  assert.equal(h.core.length, 1)
  assert.equal((await h.core.get(0)).id, h.payload.clientMessageId)
  assert.equal(h.p2p.pendingMessages.get(h.conv.id)[0].message.id, h.payload.clientMessageId)
  assert.ok(h.events.some(({ type, payload }) => type === 'media.transfer_state' && payload.state === 'waiting_peer'))
  await h.retry()
  assert.equal(h.core.length, 1)
})

test('media retry reconciles an append that committed before its caller saw an error', async t => {
  const h = await mediaFixture(t)
  const append = h.core.append.bind(h.core)
  h.core.append = async message => { await append(message); throw new Error('ambiguous append result') }
  await assert.rejects(h.send(), /ambiguous append result/)
  assert.equal(h.core.length, 1)
  h.core.append = append
  await h.retry()
  await h.send()
  assert.equal(h.core.length, 1)
})

test('media deduplication survives reopening the durable store without in-memory results', async t => {
  const h = await mediaFixture(t)
  await h.send()
  await h.hypercoreManager.close()
  await h.hypercoreManager.initialize()
  h.chatStore._historyCache.clear()
  await h.send()
  await h.retry()
  const reopened = await h.hypercoreManager.getOrCreateLocalCore(h.conv.id)
  assert.notEqual(reopened, h.core)
  assert.equal(reopened.length, 1)
})

test('large live catch-up is throttled without silently discarding records', async t => {
  const h = await harness(t), peer = pair(17)
  const conv = await h.chatStore.createConversationWithId(ChatStore.directChatId(h.identity.publicKeyHex, hex(peer)), 'direct', [hex(peer)])
  await h.p2p.joinConversation(conv.id, hex(peer))
  h.p2p._peerRateLimit = 10
  h.p2p._peerRateWindowMs = 5
  const connected = connect(h.p2p, peer)
  const frames = []
  const { FramedSocket } = require('../lib/socket-framing')
  const raw = new EventEmitter(); raw.write = frame => { frames.push(frame); return true }
  const writer = new FramedSocket(raw)
  for (let i = 0; i < 150; i++) writer.writeJSON({ id: 'catchup-' + i, conversationId: conv.id, content: 'record' })
  connected.socket.emit('data', b4a.concat(frames))
  for (let i = 0; i < 200 && (await h.chatStore.getMessages(conv.id, 5000)).length < 150; i++) {
    await new Promise(resolve => setTimeout(resolve, 5))
  }
  assert.equal((await h.chatStore.getMessages(conv.id, 5000)).length, 150)
})
