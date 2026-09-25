/**
 * Removing a member and moving the group to a new secret, through the IPC
 * handler and the peer receiver. Stubs record what each step asked of the
 * transport and the cores, in order.
 */

globalThis.Bare = { argv: [], IPC: { on: () => {} } }

const { test } = require('node:test')
const assert = require('node:assert')
const fs = require('fs')
const os = require('os')
const path = require('path')
const EventEmitter = require('node:events')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const { IPCHandler } = require('../lib/ipc-handler')
const { GroupLinkStore } = require('../lib/group-link-store')
const { createPeerReceiver } = require('../lib/peer-receiver')
const { normalizePeerRecord } = require('../lib/peer-record')
const { deriveGroupChatTopic } = require('../lib/rooms')
const { MediaRequests } = require('../lib/media-requests')
const mediaBlobs = require('../lib/media-blobs')

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'ipc-removal-'))
process.once('exit', () => fs.rmSync(tmpRoot, { recursive: true, force: true }))
let counter = 0

const hex = (buf) => b4a.toString(buf, 'hex')
const kp = (seed) => crypto.keyPair(b4a.alloc(32, seed))
const OWNER = hex(kp(1).publicKey)
const ANA = hex(kp(2).publicKey)
const BEN = hex(kp(3).publicKey)
const CAL = hex(kp(4).publicKey)
const G0 = 'a0'.repeat(32)
const topic = (groupId) => hex(deriveGroupChatTopic(groupId))

function harness ({ me = OWNER, conversation }) {
  const log = []
  const chatStore = {
    conversations: new Map([[conversation.id, conversation]]),
    leftConversations: new Set(),
    async getConversation (id) { return this.conversations.get(id) || null },
    hasLeftConversation (id) { return this.leftConversations.has(id) },
    isPeerAuthorized (id, peer) {
      const conv = this.conversations.get(id)
      return !!conv && (conv.participantIds || []).includes(peer)
    },
    async createConversation () { throw new Error('a rekey must not create a conversation') },
    async updateConversation (id, updates) {
      Object.assign(this.conversations.get(id), updates)
      log.push(['update', Object.keys(updates).sort().join(',')])
      return this.conversations.get(id)
    }
  }
  let localCore = 'core-e0'
  const p2pManager = new EventEmitter()
  Object.assign(p2pManager, {
    groupTopicToConversation: new Map(),
    groupConversations: new Map(),
    sendToConversationDurably: async (id, record) => { log.push(['durable', record.type, record.removedKey]); return { sent: true } },
    sendToConversation: (id, record) => { log.push(['send', record.type]); return true },
    sendInvite: async (key, invite) => { log.push(['invite', key, invite]); return true },
    removeGroupParticipant: (id, key) => log.push(['routing-removed', key]),
    switchGroupTopic: async (id) => { localCore = 'core-e' + chatStore.conversations.get(id).groupEpoch; log.push(['switch', chatStore.conversations.get(id).groupId]); return true },
    joinGroupConversation: async () => { log.push(['join']); return true },
    openRemoteCore: async (id, peer, key) => { log.push(['open', peer, key]); return true },
    leaveConversation: async (id) => { log.push(['leave', id]) },
    setAuxiliaryDrain: (fn) => { p2pManager.auxiliaryDrain = fn }
  })
  const hypercoreManager = {
    getLocalCoreKey: () => 'cc'.repeat(31) + localCore.slice(-1).padStart(2, '0'),
    getOrCreateLocalCore: async () => { log.push(['local-core', localCore]) },
    rekeyConversation: (id, previous) => { log.push(['rekey-cores', previous]); return { prevCoreKey: 'dd'.repeat(32), prevLength: 7 } },
    appendMessage: async (id, record) => { log.push(['append', record.type, record.prevLength]) },
    removeRemoteCore: async (id, key) => { log.push(['cores-removed', key]) },
    removeConversation: async (id) => { log.push(['conversation-cores-removed', id]) }
  }
  const handler = new IPCHandler({
    identity: { keyPair: kp(me === OWNER ? 1 : 2), publicKeyHex: me, displayName: 'Me' },
    chatStore,
    contactStore: { async getContact () { return null } },
    p2pManager,
    hypercoreManager,
    blindMirror: { removeRemoteCore: (id, key) => log.push(['mirror-removed', key]), removeConversation: () => log.push(['mirror-conversation-removed']) },
    groupLinkStore: new GroupLinkStore({ filePath: path.join(tmpRoot, 'store-' + (++counter) + '.json') })
  })
  const events = []
  handler.pushEvent = (type, payload) => events.push({ type, payload })
  process.stdin.removeAllListeners('data')
  process.stdin.pause()
  return { handler, chatStore, log, events, conv: conversation }
}

function ownerGroup () {
  return { id: 'g1', type: 'group', groupId: G0, creatorKey: OWNER, displayName: 'Hiking Crew', participantIds: [ANA, BEN, CAL] }
}

const invites = (log) => log.filter(e => e[0] === 'invite')

test('the owner removes a member: record first, then a new secret for everyone else', async () => {
  const h = harness({ conversation: ownerGroup() })
  h.handler.groupLinks.store.setMemberCaps('g1', ANA, ['group_admin_v1'])
  h.handler.groupLinks.enableLink('g1', { approval: 'owner' })
  const linkBefore = h.handler.groupLinks.getLink('g1').link

  const result = await h.handler.routeMessage('conversation.remove_member', { conversationId: 'g1', publicKey: CAL })
  assert.deepStrictEqual(result.participants, [ANA, BEN])
  assert.strictEqual(result.olderMemberCount, 1, 'Ben has not announced support')

  const steps = h.log.map(e => e[0])
  assert.ok(steps.indexOf('durable') < steps.indexOf('rekey-cores'), 'the removal lands in the old core before it retires')
  assert.deepStrictEqual(h.log.find(e => e[0] === 'durable'), ['durable', 'group_member_removed', CAL])
  assert.ok(h.log.some(e => e[0] === 'cores-removed' && e[1] === CAL))
  assert.ok(h.log.some(e => e[0] === 'mirror-removed' && e[1] === CAL))
  assert.ok(steps.indexOf('local-core') < steps.indexOf('rekey-cores'), 'the old core is open so its end can be marked')
  assert.deepStrictEqual(h.log.find(e => e[0] === 'append'), ['append', '__epoch', 7])

  assert.notStrictEqual(h.conv.groupId, G0)
  assert.strictEqual(h.conv.groupEpoch, 1)
  assert.deepStrictEqual(h.conv.pastGroupIds, [{ groupId: G0, epoch: 0 }])

  const sent = invites(h.log)
  assert.strictEqual(sent.length, 1)
  assert.strictEqual(sent[0][1], ANA)
  const invite = sent[0][2]
  assert.strictEqual(invite.rekeyOf, topic(G0))
  assert.strictEqual(invite.groupEpoch, 1)
  assert.strictEqual(invite.groupId, h.conv.groupId)
  assert.deepStrictEqual(invite.participants, [OWNER, ANA, BEN])
  assert.ok(!sent.some(e => e[1] === CAL), 'the removed member gets nothing')
  assert.deepStrictEqual(h.handler.groupLinks.store.pendingRekey('g1'), { [BEN]: G0 })
  assert.ok(h.handler.groupLinks.store.removedKeys('g1').includes(CAL))
  assert.notStrictEqual(h.handler.groupLinks.getLink('g1').link, linkBefore, 'the link was reset')
  assert.ok(h.events.some(e => e.type === 'conversation.rekeyed'))
  assert.strictEqual(h.handler._enrichConversation(h.conv).pastGroupIds, undefined, 'earlier secrets never reach native')
})

test('a waiting member gets the new secret once their app announces support', async () => {
  const h = harness({ conversation: ownerGroup() })
  await h.handler.routeMessage('conversation.remove_member', { conversationId: 'g1', publicKey: CAL })
  assert.strictEqual(invites(h.log).length, 0)
  assert.deepStrictEqual(await h.handler.routeMessage('conversation.removal_status', { conversationId: 'g1' }), { olderMemberCount: 2 })

  assert.strictEqual(await h.handler.onMemberCaps('g1', BEN, ['group_admin_v1']), true)
  const sent = invites(h.log)
  assert.strictEqual(sent.length, 1)
  assert.strictEqual(sent[0][1], BEN)
  assert.strictEqual(sent[0][2].rekeyOf, topic(G0), 'addressed to the secret Ben is still on')
  assert.deepStrictEqual(h.handler.groupLinks.store.pendingRekey('g1'), { [ANA]: G0 }, 'only Ana still waits')
  assert.deepStrictEqual(await h.handler.routeMessage('conversation.removal_status', { conversationId: 'g1' }), { olderMemberCount: 1 })
})

test('a deferred secret that no transport took stays waiting and is retried at start', async () => {
  const h = harness({ conversation: ownerGroup() })
  await h.handler.routeMessage('conversation.remove_member', { conversationId: 'g1', publicKey: CAL })
  const realSend = h.handler.p2pManager.sendInvite
  h.handler.p2pManager.sendInvite = async () => false
  assert.strictEqual(await h.handler.onMemberCaps('g1', BEN, ['group_admin_v1']), false)
  assert.deepStrictEqual(Object.keys(h.handler.groupLinks.store.pendingRekey('g1')).sort(), [ANA, BEN].sort())
  h.handler.p2pManager.sendInvite = realSend
  await h.handler.retryDeferredRekeys()
  assert.deepStrictEqual(Object.keys(h.handler.groupLinks.store.pendingRekey('g1')), [ANA], 'Ana has not announced support yet')
  assert.strictEqual(invites(h.log).filter(e => e[1] === BEN).length, 1)
})

test('a new secret no transport took is kept on disk and sent at the next start', async () => {
  const h = harness({ conversation: ownerGroup() })
  h.handler.groupLinks.store.setMemberCaps('g1', ANA, ['group_admin_v1'])
  const realSend = h.handler.p2pManager.sendInvite
  h.handler.p2pManager.sendInvite = async () => false
  const result = await h.handler.routeMessage('conversation.remove_member', { conversationId: 'g1', publicKey: CAL })
  assert.strictEqual(result.olderMemberCount, 1, 'only Ben is on an older app')
  const onDisk = new GroupLinkStore({ filePath: h.handler.groupLinks.store.filePath })
  assert.deepStrictEqual(onDisk.pendingRekey('g1'), { [ANA]: G0, [BEN]: G0 }, 'Ana waits too until her invite is taken')

  h.handler.p2pManager.sendInvite = realSend
  await h.handler.retryDeferredRekeys()
  const sent = invites(h.log)
  assert.strictEqual(sent.length, 1)
  assert.strictEqual(sent[0][1], ANA)
  assert.strictEqual(sent[0][2].rekeyOf, topic(G0))
  assert.strictEqual(sent[0][2].groupId, h.conv.groupId)
  assert.deepStrictEqual(h.handler.groupLinks.store.pendingRekey('g1'), { [BEN]: G0 })
})

test('members are recorded as waiting before the group changes secret', async () => {
  const h = harness({ conversation: ownerGroup() })
  h.handler.groupLinks.store.setMemberCaps('g1', ANA, ['group_admin_v1'])
  let waitingAtSwitch = null
  const realSwitch = h.handler.p2pManager.switchGroupTopic
  h.handler.p2pManager.switchGroupTopic = async (id) => {
    waitingAtSwitch = new GroupLinkStore({ filePath: h.handler.groupLinks.store.filePath }).pendingRekey('g1')
    return realSwitch(id)
  }
  await h.handler.routeMessage('conversation.remove_member', { conversationId: 'g1', publicKey: CAL })
  assert.deepStrictEqual(waitingAtSwitch, { [ANA]: G0, [BEN]: G0 })
  assert.deepStrictEqual(h.handler.groupLinks.store.pendingRekey('g1'), { [BEN]: G0 }, 'Ana\'s invite was taken')
})

test('a secret still waiting from before the app stopped mid-rekey is not sent', async () => {
  const h = harness({ conversation: ownerGroup() })
  h.handler.groupLinks.store.setMemberCaps('g1', ANA, ['group_admin_v1'])
  h.handler.groupLinks.store.deferRekey('g1', ANA, G0)
  await h.handler.retryDeferredRekeys()
  assert.strictEqual(invites(h.log).length, 0, 'the group never left G0')
  assert.deepStrictEqual(h.handler.groupLinks.store.pendingRekey('g1'), {})
})

test('a joiner who withdrew after being let in is taken out, with a new secret, but not marked removed', async () => {
  const h = harness({ conversation: ownerGroup() })
  h.handler.groupLinks.enableLink('g1', { approval: 'auto' })
  const linkBefore = h.handler.groupLinks.getLink('g1').link
  await h.handler._withdrawLinkMember('g1', CAL)
  assert.deepStrictEqual(h.conv.participantIds, [ANA, BEN])
  assert.deepStrictEqual(h.log.find(e => e[0] === 'durable'), ['durable', 'group_member_removed', CAL])
  assert.notStrictEqual(h.conv.groupId, G0, 'they were sent the secret, so it changes')
  assert.ok(!h.handler.groupLinks.store.removedKeys('g1').includes(CAL))
  assert.strictEqual(h.handler.groupLinks.getLink('g1').link, linkBefore, 'the link stays')
  assert.deepStrictEqual(h.events.find(e => e.type === 'conversation.member_removed').payload, { conversationId: 'g1', removedKey: CAL })

  // Nothing to do for someone who is not a member.
  const before = h.log.length
  await h.handler._withdrawLinkMember('g1', CAL)
  assert.strictEqual(h.log.length, before)
})

test('a new secret that failed is retried on mailbox drains, spaced out, without queueing copies', async () => {
  const h = harness({ conversation: ownerGroup() })
  h.handler.groupLinks.store.setMemberCaps('g1', ANA, ['group_admin_v1'])
  const p2p = h.handler.p2pManager
  const realSend = p2p.sendInvite
  p2p.sendInvite = async () => false
  await h.handler.routeMessage('conversation.remove_member', { conversationId: 'g1', publicKey: CAL })
  p2p.sendInvite = realSend

  const resent = []
  let online = false
  p2p.resendInvite = async (key, invite) => { resent.push(key); return online }
  await p2p.auxiliaryDrain()
  assert.deepStrictEqual(resent, [ANA], 'Ben has not announced support, so he is not tried')
  await p2p.auxiliaryDrain()
  assert.deepStrictEqual(resent, [ANA], 'not again before its wait is over')
  assert.strictEqual(h.handler._deliveryRetry.get('rekey:g1:' + ANA).delay, 60 * 1000)

  h.handler._deliveryRetry.get('rekey:g1:' + ANA).dueAt = 0
  await p2p.auxiliaryDrain()
  assert.strictEqual(h.handler._deliveryRetry.get('rekey:g1:' + ANA).delay, 2 * 60 * 1000, 'waits twice as long')

  online = true
  h.handler._deliveryRetry.get('rekey:g1:' + ANA).dueAt = 0
  await p2p.auxiliaryDrain()
  assert.deepStrictEqual(h.handler.groupLinks.store.pendingRekey('g1'), { [BEN]: G0 })
  assert.strictEqual(h.handler._deliveryRetry.size, 0)
  assert.strictEqual(invites(h.log).length, 0, 'retries never go through the queueing sendInvite')
})

test('only the owner removes, and only members', async () => {
  const memberView = harness({ me: ANA, conversation: { ...ownerGroup(), participantIds: [OWNER, BEN] } })
  await assert.rejects(memberView.handler.routeMessage('conversation.remove_member', { conversationId: 'g1', publicKey: BEN }), e => e.code === 'NOT_GROUP_OWNER')
  const h = harness({ conversation: ownerGroup() })
  await assert.rejects(h.handler.routeMessage('conversation.remove_member', { conversationId: 'g1', publicKey: hex(kp(9).publicKey) }), e => e.code === 'NOT_A_MEMBER')
  await assert.rejects(h.handler.routeMessage('conversation.remove_member', { conversationId: 'g1', publicKey: OWNER }), e => e.code === 'INVALID_KEY')
})

function rekeyInvite (overrides = {}) {
  return {
    type: 'group_invite', groupId: 'b1'.repeat(32), groupName: 'Hiking Crew', creatorKey: OWNER, senderKey: OWNER,
    participants: [OWNER, ANA, BEN], localCoreKey: 'ee'.repeat(32), rekeyOf: topic(G0), groupEpoch: 1, ...overrides
  }
}

test('a member moves the same conversation to the new secret', async () => {
  const h = harness({ me: ANA, conversation: { id: 'local-7', type: 'group', groupId: G0, creatorKey: OWNER, displayName: 'Old name', participantIds: [OWNER, BEN, CAL] } })
  assert.strictEqual(await h.handler._handleGroupInvite(rekeyInvite(), OWNER), true)
  assert.strictEqual(h.conv.id, 'local-7')
  assert.strictEqual(h.conv.groupId, 'b1'.repeat(32))
  assert.strictEqual(h.conv.groupEpoch, 1)
  assert.deepStrictEqual(h.conv.participantIds, [OWNER, BEN])
  assert.deepStrictEqual(h.conv.pastGroupIds, [{ groupId: G0, epoch: 0 }])
  assert.strictEqual(h.conv.displayName, 'Hiking Crew')
  const steps = h.log.map(e => e[0])
  assert.ok(steps.indexOf('cores-removed') < steps.indexOf('rekey-cores'), 'the departed member\'s core is dropped, not retired')
  assert.ok(h.log.some(e => e[0] === 'cores-removed' && e[1] === CAL))
  assert.ok(h.log.some(e => e[0] === 'switch'))
  assert.deepStrictEqual(h.log.find(e => e[0] === 'open'), ['open', OWNER, 'ee'.repeat(32)])
  assert.ok(h.events.some(e => e.type === 'conversation.rekeyed'))

  // Delivered again: nothing moves twice.
  const before = h.log.length
  await h.handler._handleGroupInvite(rekeyInvite(), OWNER)
  assert.ok(!h.log.slice(before).some(e => e[0] === 'rekey-cores'))
  // An older epoch is ignored.
  await h.handler._handleGroupInvite(rekeyInvite({ groupId: 'c1'.repeat(32), rekeyOf: topic('b1'.repeat(32)), groupEpoch: 1 }), OWNER)
  assert.strictEqual(h.conv.groupId, 'b1'.repeat(32))
})

test('a retried rekey naming a secret the member already moved past still lands', async () => {
  const G1 = 'b1'.repeat(32)
  const h = harness({ me: ANA, conversation: { id: 'local-7', type: 'group', groupId: G1, groupEpoch: 1, pastGroupIds: [{ groupId: G0, epoch: 0 }], creatorKey: OWNER, participantIds: [OWNER, BEN] } })
  await h.handler._handleGroupInvite(rekeyInvite({ groupId: 'c1'.repeat(32), rekeyOf: topic(G0), groupEpoch: 2 }), OWNER)
  assert.strictEqual(h.conv.groupId, 'c1'.repeat(32))
  assert.strictEqual(h.conv.groupEpoch, 2)
  // An old one naming the same past secret does not move it back.
  await h.handler._handleGroupInvite(rekeyInvite({ groupId: G1, rekeyOf: topic(G0), groupEpoch: 1 }), OWNER)
  assert.strictEqual(h.conv.groupId, 'c1'.repeat(32))
})

test('a rekey from anyone but the creator is ignored', async () => {
  const h = harness({ me: ANA, conversation: { id: 'local-7', type: 'group', groupId: G0, creatorKey: OWNER, participantIds: [OWNER, BEN] } })
  await h.handler._handleGroupInvite(rekeyInvite({ creatorKey: BEN, senderKey: BEN, participants: [BEN, ANA] }), BEN)
  assert.strictEqual(h.conv.groupId, G0)
})

test('the removed member stops sending and receiving, and keeps their history', async () => {
  const h = harness({ me: ANA, conversation: { id: 'local-7', type: 'group', groupId: G0, creatorKey: OWNER, participantIds: [OWNER, BEN] } })
  await h.handler._handleGroupMemberRemoved({ type: 'group_member_removed', removedKey: ANA, groupTopicHex: topic(G0) }, OWNER)
  assert.ok(h.conv.removedAt > 0)
  assert.ok(h.log.some(e => e[0] === 'leave'))
  assert.ok(h.log.some(e => e[0] === 'conversation-cores-removed'))
  assert.ok(h.events.some(e => e.type === 'conversation.removed_from_group'))
  await assert.rejects(h.handler.routeMessage('message.send', { conversationId: 'local-7', content: 'hi' }), e => e.code === 'REMOVED_FROM_GROUP')
  assert.deepStrictEqual(await h.handler.routeMessage('connection.connect', { conversationId: 'local-7' }), { success: false })
})

test('other members drop the removed member, and only on the owner\'s word', async () => {
  const h = harness({ me: ANA, conversation: { id: 'local-7', type: 'group', groupId: G0, creatorKey: OWNER, participantIds: [OWNER, BEN, CAL] } })
  await h.handler._handleGroupMemberRemoved({ type: 'group_member_removed', removedKey: CAL, groupTopicHex: topic(G0) }, BEN)
  assert.deepStrictEqual(h.conv.participantIds, [OWNER, BEN, CAL], 'a member cannot remove anyone')
  await h.handler._handleGroupMemberRemoved({ type: 'group_member_removed', removedKey: CAL, groupTopicHex: topic(G0) }, OWNER)
  assert.deepStrictEqual(h.conv.participantIds, [OWNER, BEN])
  assert.ok(h.log.some(e => e[0] === 'cores-removed' && e[1] === CAL))
  assert.deepStrictEqual(h.events.find(e => e.type === 'conversation.member_removed').payload, { conversationId: 'local-7', removedKey: CAL })
})

test('members announce support once per group, and owners do not', () => {
  const member = harness({ me: ANA, conversation: { id: 'local-7', type: 'group', groupId: G0, creatorKey: OWNER, participantIds: [OWNER] } })
  member.handler.announceGroupCaps('local-7')
  member.handler.announceGroupCaps('local-7')
  assert.strictEqual(member.log.filter(e => e[0] === 'send' && e[1] === '__caps').length, 1)
  const owner = harness({ conversation: ownerGroup() })
  owner.handler.announceGroupCaps('g1')
  assert.strictEqual(owner.log.filter(e => e[0] === 'send').length, 0)
})

test('the receiver drops rows for a removed group, accepts earlier topics, and routes capabilities', async () => {
  const conv = { id: 'g1', type: 'group', groupId: 'b1'.repeat(32), pastGroupIds: [{ groupId: G0, epoch: 0 }], creatorKey: OWNER, participantIds: [OWNER, BEN] }
  const stored = []
  const caps = []
  const chatStore = {
    conversations: new Map([['g1', conv]]),
    async getConversation (id) { return this.conversations.get(id) || null },
    hasLeftConversation () { return false },
    isPeerAuthorized (id, peer) { return conv.participantIds.includes(peer) },
    async addMessage (id, record) { stored.push(record.id); return record }
  }
  const receive = createPeerReceiver(() => ({
    chatStore,
    identity: { publicKeyHex: ANA },
    ipcHandler: { onMemberCaps: (...args) => caps.push(args), handlePeerControl: async () => {} },
    processReceipt: async () => {},
    onMessage: () => {}
  }))
  const message = (id, groupId) => ({ id, content: 'x', timestamp: Date.now(), groupTopicHex: topic(groupId) })
  await receive('g1', BEN, message('old-topic', G0), { replicated: true })
  await receive('g1', BEN, message('current-topic', 'b1'.repeat(32)), { replicated: true })
  await receive('g1', BEN, message('stranger-topic', 'c1'.repeat(32)), { replicated: true })
  assert.deepStrictEqual(stored, ['old-topic', 'current-topic'])

  await receive('g1', BEN, { type: '__caps', v: 1, features: ['group_admin_v1'] }, { replicated: true })
  assert.deepStrictEqual(caps, [['g1', BEN, ['group_admin_v1']]])

  conv.removedAt = Date.now()
  await receive('g1', BEN, message('after-removal', 'b1'.repeat(32)), { replicated: true })
  assert.ok(!stored.includes('after-removal'))
})

test('new record fields are validated', () => {
  const base = { type: 'group_invite', groupId: 'b1'.repeat(32), creatorKey: OWNER, senderKey: OWNER, participants: [OWNER, ANA] }
  assert.strictEqual(normalizePeerRecord({ ...base, rekeyOf: topic(G0), groupEpoch: 2 }, OWNER).groupEpoch, 2)
  assert.throws(() => normalizePeerRecord({ ...base, rekeyOf: topic(G0) }, OWNER), e => e.code === 'INVALID_PEER_RECORD')
  assert.throws(() => normalizePeerRecord({ ...base, rekeyOf: 'nope', groupEpoch: 1 }, OWNER), e => e.code === 'INVALID_PEER_RECORD')
  assert.strictEqual(normalizePeerRecord({ type: 'group_member_removed', removedKey: CAL }, OWNER).removedKey, CAL)
  assert.throws(() => normalizePeerRecord({ type: 'group_member_removed' }, OWNER), e => e.code === 'INVALID_PEER_RECORD')
  assert.deepStrictEqual(normalizePeerRecord({ type: '__caps', features: ['group_admin_v1', 'group_admin_v1'] }, OWNER).features, ['group_admin_v1'])
  assert.throws(() => normalizePeerRecord({ type: '__caps', features: ['bad feature'] }, OWNER), e => e.code === 'INVALID_PEER_RECORD')
  assert.throws(() => normalizePeerRecord({ type: '__caps', features: new Array(17).fill('x') }, OWNER), e => e.code === 'INVALID_PEER_RECORD')
})

test('an image from before a new secret is fetched with the earlier key', async () => {
  const bytesFor = { current: b4a.from('noise'), [G0]: b4a.from('the real image') }
  const opened = []
  const originalDownload = mediaBlobs.download
  mediaBlobs.download = (core) => ({ bytes: Promise.resolve(core.bytes), cancel () {} })
  try {
    const hash = (buf) => crypto.data(buf)
    const mediaId = hex(hash(bytesFor[G0]))
    const requests = new MediaRequests(() => ({
      chatStore: { conversations: new Map([['g1', { type: 'group', pastGroupIds: [{ groupId: G0, epoch: 0 }] }]]) },
      mediaStore: { hash },
      p2pManager: {
        openRemoteMediaCore: async (conversationId, sender, coreKey, groupIdOverride) => {
          opened.push(groupIdOverride)
          return { bytes: bytesFor[groupIdOverride || 'current'], close: async () => {}, clear: async () => {} }
        }
      }
    }))
    const bytes = await requests._fetch(mediaId, { conversationId: 'g1', senderId: BEN, descriptor: { coreKey: 'ab'.repeat(32), offset: 0, n: 1 } }, { cancelled: false })
    assert.strictEqual(b4a.toString(bytes), 'the real image')
    assert.deepStrictEqual(opened, [null, G0])
  } finally {
    mediaBlobs.download = originalDownload
  }
})
