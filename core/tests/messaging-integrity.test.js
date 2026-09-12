const { test } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const os = require('node:os')
const EventEmitter = require('node:events')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const { ChatStore } = require('../lib/chat-store')
const { IPCHandler } = require('../lib/ipc-handler')
const { P2PManager } = require('../lib/p2p-manager')
const { HypercoreManager } = require('../lib/hypercore-manager')
const { MediaStore } = require('../lib/media-store')
const { MediaTransfer } = require('../lib/media-transfer')
const { deriveGroupChatTopic } = require('../lib/rooms')
const { createPeerReceiver, serveAuthorizedMedia } = require('../lib/peer-receiver')
const { normalizePeerRecord } = require('../lib/peer-record')
globalThis.Bare = { argv: [], IPC: { on () {} } }
const pair = n => crypto.keyPair(b4a.alloc(32, n))
const hex = n => b4a.toString(pair(n).publicKey, 'hex')
const OWNER = hex(1), MEMBER = hex(2), ME = hex(3), OUTSIDER = hex(4)
const GROUP = 'ab'.repeat(32)
const TOPIC = b4a.toString(deriveGroupChatTopic(GROUP), 'hex')
const CORE = 'cc'.repeat(32)

async function harness(t, self = 3, id = 'local-group') {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sdk-integrity-'))
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }))
  const store = new ChatStore()
  store.storagePath = dir
  store.conversations.clear()
  store.leftConversations.clear()
  const members = [OWNER, MEMBER, ME].filter(k => k !== hex(self))
  const conv = await store.createConversationWithId(id, 'group', members, { groupId: GROUP, creatorKey: OWNER, displayName: 'Original' })
  const p2p = new P2PManager({
    resolveGroupTopic: topic => store.conversationForGroupTopic(topic),
    mayServeMedia: (hash, peer) => store.canServeMedia(b4a.toString(hash, 'hex'), peer),
    isParticipant: (id, peer) => store.isPeerAuthorized(id, peer),
    getConversation: id => !store.hasLeftConversation(id) && store.conversations.get(id)
  })
  p2p.keyPair = pair(self)
  p2p.swarm = { join: () => ({ flushed: async () => {}, destroy: async () => {} }) }
  await p2p.joinGroupConversation(id, GROUP, [OWNER, MEMBER, ME])
  const events = []
  const ipc = new IPCHandler({ identity: { publicKeyHex: hex(self) }, chatStore: store, p2pManager: p2p, contactStore: { getContact: async () => null } })
  process.stdin.removeAllListeners('data')
  process.stdin.pause()
  ipc.pushEvent = (type, payload) => events.push({ type, payload })
  const receiver = createPeerReceiver(() => ({
    chatStore: store, p2pManager: p2p, ipcHandler: ipc, identity: { publicKeyHex: hex(self) },
    processReceipt: async (id, receipt) => store.markDeliveredUpTo(id, receipt.upTo),
    onMessage: (id, stored) => { if (stored) events.push({ type: 'message', stored }) }
  }))
  p2p.setPeerRecordSink(receiver)
  return { store, conv, p2p, ipc, receiver, events }
}
function connect(p2p, peer) {
  const socket = new EventEmitter()
  socket.remotePublicKey = b4a.from(peer, 'hex')
  socket.writable = true
  socket.frames = []
  socket.write = data => { socket.frames.push(b4a.from(data)); return true }
  socket.destroy = () => { socket.destroyed = true }
  p2p.handleConnection(socket, { publicKey: socket.remotePublicKey })
  socket.frames.length = 0
  return { socket, framed: p2p.framedSockets.get(socket) }
}
function decodeFrames(socket) {
  return socket.frames.filter(frame => frame[4] === 1).map(frame => JSON.parse(frame.subarray(5).toString()))
}
function drainHarness(receiver) {
  const manager = new HypercoreManager()
  manager.saveCoreKeyIndex = () => {}
  manager._drainRetryBaseMs = 60000
  manager.setRemoteMessageSink((id, peer, record) => receiver(id, peer, record, { replicated: true }))
  manager.setRemoteDrainCompleteSink(async () => {})
  return manager
}
function core(records) {
  return { length: records.length, fork: 0, get: async i => records[i] }
}
const message = (id, extra = {}) => ({ id, content: 'hello', contentType: 'text/plain', timestamp: Date.now(), ...extra })
const invite = sender => ({ type: 'group_invite', groupId: GROUP, groupName: 'Forged', creatorKey: sender, senderKey: sender, participants: [OWNER, MEMBER, ME, OUTSIDER], localCoreKey: CORE })

test('forged repeat invite has no stored, runtime, core or UI side effects; stale caches cannot leak text, controls or media', async t => {
  const { store, conv, p2p, ipc, events } = await harness(t)
  let opened = 0
  p2p.openRemoteCore = async () => { opened++; return true }
  const before = JSON.stringify(conv)
  const runtimeBefore = [...p2p.groupConversations.get(conv.id).participantKeys]
  assert.equal(await ipc._handleGroupInvite(invite(MEMBER), MEMBER), true)
  assert.equal(JSON.stringify(conv), before)
  assert.deepEqual(p2p.groupConversations.get(conv.id).participantKeys, runtimeBefore)
  assert.equal(opened, 0)
  assert.equal(events.length, 0)
  const outsider = connect(p2p, OUTSIDER)
  const authorized = connect(p2p, MEMBER)
  const group = p2p.groupConversations.get(conv.id)
  group.participantKeys.push(OUTSIDER)
  group.connections.set(OUTSIDER, [outsider.socket])
  p2p._trySendToConversation(conv.id, message('text'))
  p2p._trySendToConversation(conv.id, { type: 'group_renamed', newName: 'safe' })
  assert.equal(decodeFrames(outsider.socket).length, 0)
  assert.equal(decodeFrames(authorized.socket).length, 2)
  assert.ok(!p2p.getConversationFramedSockets(conv.id).includes(outsider.framed))
  p2p.pendingMessages.set(conv.id, [{ message: message('queued') }])
  p2p._flushPendingMessages(conv.id, [outsider.socket])
  assert.equal(outsider.socket.frames.length, 0)
  await p2p.joinGroupConversation(conv.id, GROUP, [ME, OUTSIDER])
  assert.deepEqual(group.participantKeys.sort(), [ME, OWNER, MEMBER].sort())
  assert.ok(!store.isPeerAuthorized(conv.id, OUTSIDER))
})

test('owner repeat invites add members to authoritative storage and remain repeatable', async t => {
  const { store, conv, p2p, ipc } = await harness(t)
  p2p.openRemoteCore = async () => true
  await ipc._handleGroupInvite(invite(OWNER), OWNER)
  await ipc._handleGroupInvite(invite(OWNER), OWNER)
  assert.equal(store.conversations.size, 1)
  assert.equal(conv.creatorKey, OWNER)
  assert.ok(store.isPeerAuthorized(conv.id, OUTSIDER))
  assert.equal(conv.participantIds.filter(k => k === OUTSIDER).length, 1)
  const peer = connect(p2p, OUTSIDER)
  p2p._trySendToConversation(conv.id, message('allowed'))
  assert.equal(decodeFrames(peer.socket).length, 1)
})

test('known hash grants no access; active persisted references authorize media across multiple conversations', async t => {
  const { store, conv } = await harness(t)
  const mediaStore = new MediaStore()
  const saved = mediaStore.saveMedia(b4a.from('private media bytes'), 'jpg')
  const transfer = new MediaTransfer(mediaStore)
  const chunks = []
  const socket = { writeChunk: (...args) => chunks.push(args) }
  const hash = b4a.from(saved.hashHex, 'hex')
  assert.equal(await serveAuthorizedMedia(store, transfer, hash, MEMBER, socket), false)
  await store.addMessage(conv.id, { ...message('media'), senderId: ME, isFromMe: true, mediaId: saved.hashHex })
  assert.equal(await serveAuthorizedMedia(store, transfer, hash, OUTSIDER, socket), false)
  assert.equal(await serveAuthorizedMedia(store, transfer, hash, MEMBER, socket), true)
  assert.equal(chunks.length, 1)
  const second = await store.createConversationWithId('second', 'direct', [OUTSIDER])
  await store.addMessage(second.id, { ...message('shared'), senderId: ME, isFromMe: true, mediaId: saved.hashHex })
  store.markConversationAsLeft(conv.id)
  store._mediaIndex.clear()
  assert.equal(await serveAuthorizedMedia(store, transfer, hash, MEMBER, socket), false)
  assert.equal(await serveAuthorizedMedia(store, transfer, hash, OUTSIDER, socket), true)
  await store.deleteConversation(second.id)
  assert.equal(await serveAuthorizedMedia(store, transfer, hash, OUTSIDER, socket), false)
})

test('malformed replicated records never persist or block the following valid record; identity and local fields are pinned', async t => {
  const { store, conv, receiver, events } = await harness(t)
  const records = [
    { content: {} }, [], 'bad', 42,
    message('bad-content', { content: {} }),
    message('../bad-id'), message('bad-media', { mediaId: '../secret' }),
    message('bad-reply', { replyToContent: {} }),
    message('too-large', { content: 'x'.repeat(256 * 1024 + 1) }),
    { type: 'group_renamed', newName: {} }, { type: '__future_control' },
    message('valid', { senderId: OUTSIDER, isFromMe: true, status: 'read', mediaLocalPath: '/private', mediaTransferState: 'complete' })
  ]
  const manager = drainHarness(receiver)
  await manager._drainRemoteCore(conv.id, OWNER, CORE, core(records))
  assert.equal(manager._getProcessedCursor(conv.id, CORE), records.length)
  const rows = await store.getMessages(conv.id)
  assert.equal(rows.length, 1)
  assert.equal(rows[0].senderId, OWNER)
  assert.equal(rows[0].isFromMe, false)
  assert.equal(rows[0].mediaLocalPath, null)
  assert.equal(rows[0].status, null)
  assert.equal(events.filter(e => e.type === 'message').length, 1)
})

for (const failure of ['message', 'index']) {
  test('transient ' + failure + ' persistence failure holds cursor and successful retry dedups committed rows', async t => {
    const { store, conv, receiver } = await harness(t)
    const manager = drainHarness(receiver)
    const blocks = core([message('first'), message('second')])
    const target = path.join(store.storagePath, failure === 'message' ? conv.id + '.json.tmp' : 'index.json.tmp')
    fs.mkdirSync(target)
    await manager._drainRemoteCore(conv.id, OWNER, CORE, blocks)
    assert.equal(manager._getProcessedCursor(conv.id, CORE), 0)
    fs.rmdirSync(target)
    await manager._drainRemoteCore(conv.id, OWNER, CORE, blocks)
    assert.equal(manager._getProcessedCursor(conv.id, CORE), 2)
    assert.deepEqual((await store.getMessages(conv.id)).map(m => m.id), ['first', 'second'])
    assert.equal(JSON.parse(fs.readFileSync(path.join(store.storagePath, 'index.json')))[0].lastMessage, 'hello')
  })
}

test('poisoned history is backed up and repaired without losing valid rows; unreadable files remain retryable', async t => {
  const { store, conv, receiver } = await harness(t)
  const file = path.join(store.storagePath, conv.id + '.json')
  const original = [message('old', { senderId: OWNER }), message('poison', { senderId: OWNER, content: {} }), message('old', { senderId: OWNER })]
  fs.writeFileSync(file, JSON.stringify(original))
  await receiver(conv.id, OWNER, message('new'), { replicated: true })
  assert.deepEqual((await store.getMessages(conv.id)).map(m => m.id), ['old', 'new'])
  assert.deepEqual(JSON.parse(fs.readFileSync(file + '.pre-validation.bak')), original)
  fs.writeFileSync(file, '{broken')
  await assert.rejects(receiver(conv.id, OWNER, message('later'), { replicated: true }), /unreadable/)
  assert.equal(fs.readFileSync(file, 'utf8'), '{broken')
})

test('different local group IDs exchange core keys and route live receipts by topic, never to a shared DM', async t => {
  const a = await harness(t, 1, 'group-on-owner')
  const b = await harness(t, 2, 'group-on-member')
  a.p2p.hypercoreManager = { getAllLocalCoreKeys: () => ({ [a.conv.id]: CORE }) }
  const announcements = []
  a.p2p._sendCoreKeys({ writeJSON: value => announcements.push(value) }, MEMBER)
  assert.deepEqual(Object.keys(announcements[0].cores), [TOPIC])
  assert.ok(!JSON.stringify(announcements).includes(GROUP))
  const opened = []
  b.p2p.openRemoteCore = async (...args) => opened.push(args)
  const socket = connect(b.p2p, OWNER)
  await socket.framed.onMessage(announcements[0])
  assert.deepEqual(opened, [[b.conv.id, OWNER, CORE]])
  await b.store.addMessage(b.conv.id, { ...message('sent'), senderId: MEMBER, isFromMe: true })
  const dm = await b.store.createConversationWithId('shared-dm', 'direct', [OWNER])
  await b.store.addMessage(dm.id, { ...message('sent'), senderId: MEMBER, isFromMe: true })
  b.p2p.peerToConversation.set(OWNER, { conversationId: dm.id })
  const receipt = a.p2p._outgoingRecord(a.conv.id, { type: '__receipt', kind: 'delivered', upTo: 'sent', to: MEMBER })
  await socket.framed.onMessage(receipt)
  assert.equal((await b.store.getMessages(b.conv.id))[0].status, 'delivered')
  assert.equal((await b.store.getMessages(dm.id))[0].status, 'queued')
  await socket.framed.onMessage({ ...receipt, groupTopicHex: 'dd'.repeat(32) })
  assert.equal((await b.store.getMessages(dm.id))[0].status, 'queued')
})

test('replicated group controls authorize, mutate and dedup across transports without chat rows', async t => {
  const { store, conv, p2p, receiver, events } = await harness(t)
  const peer = connect(p2p, OWNER)
  const rename = { type: 'group_renamed', id: 'rename-1', groupTopicHex: TOPIC, conversationId: 'owner-local', newName: 'Renamed' }
  await peer.framed.onMessage(rename)
  const manager = drainHarness(receiver)
  const addition = { type: 'group_member_added', newMemberKey: OUTSIDER, updatedParticipants: [ME, OWNER, MEMBER, OUTSIDER] }
  await manager._drainRemoteCore(conv.id, OWNER, CORE, core([rename, addition, { type: '__unknown' }]))
  assert.equal(conv.displayName, 'Renamed')
  assert.ok(store.isPeerAuthorized(conv.id, OUTSIDER))
  assert.equal(events.filter(e => e.type === 'conversation.group_renamed').length, 1)
  assert.equal((await store.getMessages(conv.id)).length, 0)
  assert.equal(manager._getProcessedCursor(conv.id, CORE), 3)
  await receiver(conv.id, MEMBER, { type: 'group_member_added', newMemberKey: hex(8), updatedParticipants: [ME, hex(8)] }, { replicated: true })
  assert.ok(!store.isPeerAuthorized(conv.id, hex(8)))
  await receiver(conv.id, MEMBER, { type: 'group_leave', leaverKey: OWNER }, { replicated: true })
  assert.ok(!store.isPeerAuthorized(conv.id, MEMBER))
  assert.ok(store.isPeerAuthorized(conv.id, OWNER))
  await receiver(conv.id, OUTSIDER, { type: 'group_deleted' }, { replicated: true })
  assert.ok(await store.getConversation(conv.id))
  await receiver(conv.id, OWNER, { type: 'group_deleted' }, { replicated: true })
  assert.equal(await store.getConversation(conv.id), null)
})

test('concurrent live and replicated messages dedup at disk; failed live writes do not poison transport dedup', async t => {
  const { store, conv, receiver, p2p, events } = await harness(t)
  const peer = connect(p2p, OWNER)
  const record = message('both', { groupTopicHex: TOPIC })
  await Promise.all([peer.framed.onMessage(record), receiver(conv.id, OWNER, record, { replicated: true })])
  assert.equal((await store.getMessages(conv.id)).length, 1)
  assert.equal(events.filter(e => e.type === 'message').length, 1)
  const target = path.join(store.storagePath, conv.id + '.json.tmp')
  fs.mkdirSync(target)
  await peer.framed.onMessage(message('retry-live', { groupTopicHex: TOPIC }))
  fs.rmdirSync(target)
  await peer.framed.onMessage(message('retry-live', { groupTopicHex: TOPIC }))
  assert.equal((await store.getMessages(conv.id)).length, 2)
})

test('supported application content types stay opaque strings and malformed controls are rejected before mutation', () => {
  for (const type of ['text/plain', 'image/jpeg', 'image/gif', 'video/mp4', 'application/x-zapp-payment-request', 'application/json']) {
    assert.equal(normalizePeerRecord(message('typed', { contentType: type, content: '{"amount":"1"}' }), OWNER).contentType, type)
  }
  assert.throws(() => normalizePeerRecord({ type: 'group_invite', ...invite(OWNER), groupName: {} }, OWNER), { code: 'INVALID_PEER_RECORD' })
})

test('store rejects malformed local input before any disk write, including missing-id retry poison', async t => {
  const { store, conv } = await harness(t)
  const before = fs.readFileSync(path.join(store.storagePath, conv.id + '.json'), 'utf8')
  for (let i = 0; i < 2; i++) {
    await assert.rejects(store.addMessage(conv.id, { senderId: OWNER, content: {} }), { code: 'INVALID_PEER_RECORD' })
  }
  assert.equal(fs.readFileSync(path.join(store.storagePath, conv.id + '.json'), 'utf8'), before)
})

test('replicated control storage failure is retryable and membership cache rolls back', async t => {
  const { store, conv, receiver, p2p } = await harness(t)
  const manager = drainHarness(receiver)
  const records = core([
    { type: 'group_member_added', id: 'add-retry', newMemberKey: OUTSIDER, updatedParticipants: [ME, OWNER, MEMBER, OUTSIDER] },
    { type: 'group_renamed', id: 'rename-retry', newName: 'Committed' },
    message('after-control')
  ])
  const target = path.join(store.storagePath, 'index.json.tmp')
  fs.mkdirSync(target)
  await manager._drainRemoteCore(conv.id, OWNER, CORE, records)
  assert.equal(manager._getProcessedCursor(conv.id, CORE), 0)
  assert.ok(!store.isPeerAuthorized(conv.id, OUTSIDER))
  assert.ok(!p2p.groupConversations.get(conv.id).participantKeys.includes(OUTSIDER))
  fs.rmdirSync(target)
  await manager._drainRemoteCore(conv.id, OWNER, CORE, records)
  assert.equal(manager._getProcessedCursor(conv.id, CORE), 3)
  assert.equal(conv.displayName, 'Committed')
  assert.ok(store.isPeerAuthorized(conv.id, OUTSIDER))
  assert.equal((await store.getMessages(conv.id)).length, 1)
})

test('topic routing and replicated controls work before runtime group joins', async t => {
  const { store, conv, p2p, receiver } = await harness(t)
  p2p.groupConversations.clear()
  p2p.groupTopicToConversation.clear()
  assert.equal(p2p.resolveWireConversation(TOPIC), conv.id)
  await receiver(conv.id, OWNER, { type: 'group_renamed', conversationId: 'old-local-id', newName: 'Restored' }, { replicated: true })
  assert.equal(conv.displayName, 'Restored')
  assert.equal((await store.getMessages(conv.id)).length, 0)
})

test('queued media chunk writes recheck membership after revocation', async t => {
  const { store, conv, p2p } = await harness(t)
  const mediaStore = new MediaStore()
  const saved = mediaStore.saveMedia(b4a.from('revocable bytes'), 'jpg')
  const hash = b4a.from(saved.hashHex, 'hex')
  await store.addMessage(conv.id, { ...message('media-revoke'), senderId: ME, isFromMe: true, mediaId: saved.hashHex })
  const peer = connect(p2p, MEMBER)
  peer.framed.writeChunk(hash, 0, 1, b4a.from('bytes'))
  assert.equal(peer.socket.frames.length, 1)
  await store.updateConversation(conv.id, { participantIds: [OWNER] })
  peer.framed.writeChunk(hash, 0, 1, b4a.from('bytes'))
  assert.equal(peer.socket.frames.length, 1)
})

test('failed cursor persistence rolls back in-memory cursor and retries committed messages safely', async t => {
  const { store, conv, receiver } = await harness(t)
  const { getDataDir } = require('../lib/storage')
  const manager = drainHarness(receiver)
  manager.saveCoreKeyIndex = HypercoreManager.prototype.saveCoreKeyIndex
  const target = path.join(getDataDir(), 'corekeys.json.tmp')
  fs.mkdirSync(target)
  t.after(() => fs.rmSync(target, { recursive: true, force: true }))
  const records = core([message('cursor-retry')])
  await manager._drainRemoteCore(conv.id, OWNER, CORE, records)
  assert.equal(manager._getProcessedCursor(conv.id, CORE), 0)
  assert.equal((await store.getMessages(conv.id)).length, 1)
  fs.rmdirSync(target)
  await manager._drainRemoteCore(conv.id, OWNER, CORE, records)
  assert.equal(manager._getProcessedCursor(conv.id, CORE), 1)
  assert.equal((await store.getMessages(conv.id)).length, 1)
})

test('legacy replicated group receipt uses its registered core scope despite sender-local conversation id', async t => {
  const { store, conv, receiver } = await harness(t, 2, 'recipient-local')
  await store.addMessage(conv.id, { ...message('legacy-receipt'), senderId: MEMBER, isFromMe: true })
  const manager = drainHarness(receiver)
  await manager._drainRemoteCore(conv.id, OWNER, CORE, core([
    { type: '__receipt', kind: 'delivered', to: MEMBER, upTo: 'legacy-receipt', conversationId: 'sender-local' }
  ]))
  assert.equal(manager._getProcessedCursor(conv.id, CORE), 1)
  assert.equal((await store.getMessages(conv.id))[0].status, 'delivered')
  assert.equal((await store.getMessages(conv.id)).length, 1)
})

test('repeat group invite dedups between live and replication with different routing envelopes', async t => {
  const { store, conv, p2p, receiver, events } = await harness(t)
  p2p.openRemoteCore = async () => true
  const record = invite(OWNER)
  const peer = connect(p2p, OWNER)
  await peer.framed.onMessage(record)
  await receiver(conv.id, OWNER, { ...record, conversationId: 'sender-local', groupTopicHex: TOPIC }, { replicated: true })
  assert.ok(store.isPeerAuthorized(conv.id, OUTSIDER))
  assert.equal(events.filter(e => e.type === 'conversation.invite_received').length, 1)
  assert.equal((await store.getMessages(conv.id)).length, 0)
})

for (const type of ['group_leave', 'group_deleted']) {
  test('replicated ' + type + ' closes its own production-managed core without awaiting its own drain', { timeout: 2000 }, async t => {
    const { conv, ipc, receiver, store } = await harness(t)
    const manager = drainHarness(receiver)
    ipc.hypercoreManager = manager
    const blocks = new EventEmitter()
    blocks.length = 1
    blocks.fork = 0
    blocks.get = async () => ({ type, id: type + '-self-close' })
    blocks.close = async () => { blocks.closed = true }
    manager.remoteCores.set(conv.id, new Map([[OWNER, blocks]]))
    manager._coreKeyIndex.set(conv.id, new Map([[OWNER, CORE]]))
    await manager._drainRemoteCore(conv.id, OWNER, CORE, blocks)
    assert.equal(blocks.closed, true)
    assert.equal(manager.remoteCores.has(conv.id), false)
    assert.equal(manager._getProcessedCursor(conv.id, CORE), 0)
    if (type === 'group_leave') assert.ok(!store.isPeerAuthorized(conv.id, OWNER))
    else assert.equal(await store.getConversation(conv.id), null)
  })
}

test('legacy controls allow legitimate repeated operations after intervening state changes', async t => {
  const { conv, receiver, store, p2p } = await harness(t)
  p2p.openRemoteCore = async () => true
  for (const newName of ['A', 'B', 'A']) {
    await receiver(conv.id, OWNER, { type: 'group_renamed', newName }, { replicated: true })
    assert.equal(conv.displayName, newName)
  }
  const addition = { type: 'group_member_added', newMemberKey: OUTSIDER, updatedParticipants: [ME, OWNER, MEMBER, OUTSIDER] }
  await receiver(conv.id, OWNER, addition, { replicated: true })
  await receiver(conv.id, OUTSIDER, { type: 'group_leave' }, { replicated: true })
  assert.ok(!store.isPeerAuthorized(conv.id, OUTSIDER))
  await receiver(conv.id, OWNER, addition, { replicated: true })
  assert.ok(store.isPeerAuthorized(conv.id, OUTSIDER))
})

test('deep peer JSON serialization failures are permanent and do not wedge the replication cursor', async t => {
  const { conv, receiver, store } = await harness(t)
  const nested = JSON.parse('{"ignored":' + '['.repeat(20000) + '0' + ']'.repeat(20000) + '}')
  const manager = drainHarness(receiver)
  await manager._drainRemoteCore(conv.id, OWNER, CORE, core([
    message('deep', nested), message('after-deep')
  ]))
  assert.equal(manager._getProcessedCursor(conv.id, CORE), 2)
  assert.deepEqual((await store.getMessages(conv.id)).map(row => row.id), ['after-deep'])
})

test('unauthorized explicit-ID controls do not mutate even the applied-control journal', async t => {
  const { conv, receiver, store, events } = await harness(t)
  const file = path.join(store.storagePath, 'index.json')
  const before = fs.readFileSync(file, 'utf8')
  for (const record of [
    { ...invite(MEMBER), id: 'forged-with-id' },
    { type: 'group_member_added', id: 'unauthorized-add', newMemberKey: OUTSIDER, updatedParticipants: [ME, OUTSIDER] },
    { type: 'group_deleted', id: 'unauthorized-delete' }
  ]) await receiver(conv.id, MEMBER, record, { replicated: true })
  await receiver('attacker-local-id', MEMBER, {
    ...invite(MEMBER), id: 'forged-with-foreign-route', conversationId: 'attacker-local-id'
  })
  assert.equal(fs.readFileSync(file, 'utf8'), before)
  assert.equal(conv.appliedControls, undefined)
  assert.equal(events.length, 0)
})
