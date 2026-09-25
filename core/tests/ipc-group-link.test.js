/**
 * Group invite links through the IPC handler: request routing, the owner's
 * admission path, and the invite fields that must survive normalization.
 * A stub Bare global is installed before any core module loads, as in
 * ipc-invite-auth.test.js.
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
const { normalizePeerRecord } = require('../lib/peer-record')
const gl = require('../lib/group-link')

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'ipc-group-link-'))
process.once('exit', () => fs.rmSync(tmpRoot, { recursive: true, force: true }))
let counter = 0

function keyPairFrom (seedByte) {
  return crypto.keyPair(b4a.alloc(32, seedByte))
}
const hex = (buf) => b4a.toString(buf, 'hex')
const MY_KP = keyPairFrom(1)
const MY = hex(MY_KP.publicKey)
const PEER = hex(keyPairFrom(2).publicKey)
const NEWCOMER = hex(keyPairFrom(3).publicKey)
const GROUP_ID = 'ab'.repeat(32)

function makeHarness ({ conversations = {}, keyPair = MY_KP, storePath = null } = {}) {
  const calls = { invites: [], broadcasts: [], events: [], updated: [], joins: [] }
  const chatStore = {
    conversations: new Map(Object.entries(conversations)),
    leftConversations: new Set(),
    async getConversation (id) { return this.conversations.get(id) || null },
    hasLeftConversation (id) { return this.leftConversations.has(id) },
    async createConversation (type, participantIds, options = {}) {
      for (const conv of this.conversations.values()) {
        if (conv.type === type && options.groupId && conv.groupId === options.groupId) return conv
      }
      const conv = { id: 'local-' + this.conversations.size, type, participantIds, ...options }
      this.conversations.set(conv.id, conv)
      return conv
    },
    async updateConversation (id, updates) {
      const conv = this.conversations.get(id)
      Object.assign(conv, updates)
      calls.updated.push({ id, updates })
      return conv
    }
  }
  const p2pManager = new EventEmitter()
  p2pManager.groupTopicToConversation = new Map()
  p2pManager.groupConversations = new Map()
  p2pManager.joinGroupConversation = async (id) => { calls.joins.push(id); return true }
  p2pManager.openRemoteCore = async () => true
  p2pManager.sendInvite = async (key, invite) => { calls.invites.push({ key, invite }); return true }
  p2pManager.sendToConversation = (id, record) => { calls.broadcasts.push({ id, record }); return true }

  const handler = new IPCHandler({
    identity: { keyPair, publicKeyHex: hex(keyPair.publicKey), displayName: 'Me' },
    chatStore,
    contactStore: { async getContact () { return null }, async clearAll () {} },
    p2pManager,
    hypercoreManager: { getLocalCoreKey: () => 'cc'.repeat(32) },
    groupLinkStore: new GroupLinkStore({ filePath: storePath || path.join(tmpRoot, 'links-' + (++counter) + '.json') })
  })
  handler.pushEvent = (type, payload) => calls.events.push({ type, payload })
  process.stdin.removeAllListeners('data')
  process.stdin.pause()
  return { handler, chatStore, calls }
}

function ownedGroup (participants = [PEER]) {
  return { g1: { id: 'g1', type: 'group', groupId: GROUP_ID, creatorKey: MY, displayName: 'Hiking Crew', participantIds: participants } }
}

test('protocol init advertises group links', async () => {
  const { handler } = makeHarness()
  const result = await handler.routeMessage('protocol.init', { protocolVersion: '1.0' })
  assert.ok(result.features.includes('group_links'))
})

test('the owner manages the link over IPC, and others are refused', async () => {
  const { handler } = makeHarness({ conversations: { ...ownedGroup(), other: { id: 'other', type: 'group', groupId: 'cd'.repeat(32), creatorKey: PEER, participantIds: [PEER] } } })
  const enabled = await handler.routeMessage('group_link.enable', { conversationId: 'g1', approval: 'owner' })
  assert.strictEqual(enabled.state, 'active')
  assert.strictEqual(enabled.approval, 'owner')
  assert.strictEqual(gl.parseLink(enabled.link).nameHint, 'Hiking Crew')

  const reset = await handler.routeMessage('group_link.reset', { conversationId: 'g1' })
  assert.notStrictEqual(reset.link, enabled.link)
  assert.strictEqual((await handler.routeMessage('group_link.disable', { conversationId: 'g1' })).state, 'off')
  assert.deepStrictEqual(await handler.routeMessage('group_link.requests', { conversationId: 'g1' }), { requests: [] })

  await assert.rejects(handler.routeMessage('group_link.enable', { conversationId: 'other' }), e => e.code === 'NOT_GROUP_OWNER')
  await assert.rejects(handler.routeMessage('group_link.get', {}), e => e.code === 'INVALID_CONVERSATION')
  await assert.rejects(handler.routeMessage('group_link.enable', { conversationId: 'g1', approval: 'sometimes' }), e => e.code === 'INVALID_OPTIONS')
})

test('inspect returns display fields only', async () => {
  const { handler } = makeHarness({ conversations: ownedGroup() })
  const { link } = await handler.routeMessage('group_link.enable', { conversationId: 'g1' })
  const inspected = await handler.routeMessage('group_link.inspect', { link })
  assert.deepStrictEqual(Object.keys(inspected).sort(), ['expiresAt', 'linkId', 'nameHint', 'status', 'version'])
  assert.strictEqual((await handler.routeMessage('group_link.inspect', { link: 'https://join.justzappit.xyz/g/v1#nope' })).status, 'malformed')
})

test('admission through a link adds the member with a signed viaLink and tells everyone', async () => {
  const { handler, chatStore, calls } = makeHarness({ conversations: ownedGroup() })
  const viaLink = { linkId: '11'.repeat(16), admitSig: '22'.repeat(64) }
  assert.strictEqual(await handler._admitViaLink('g1', NEWCOMER, 'Ana', viaLink), true)

  assert.deepStrictEqual(chatStore.conversations.get('g1').participantIds, [PEER, NEWCOMER])
  assert.strictEqual(calls.invites.length, 1)
  assert.strictEqual(calls.invites[0].key, NEWCOMER)
  assert.deepStrictEqual(calls.invites[0].invite.viaLink, viaLink)
  assert.deepStrictEqual(calls.invites[0].invite.participants, [MY, PEER, NEWCOMER])
  assert.strictEqual(calls.broadcasts[0].record.type, 'group_member_added')
  assert.strictEqual(calls.broadcasts[0].record.newMemberName, 'Ana')
  assert.deepStrictEqual(calls.events.find(e => e.type === 'conversation.member_added').payload,
    { conversationId: 'g1', newMemberKey: NEWCOMER, newMemberName: 'Ana' })

  // A member who asks again only gets the invite again.
  assert.strictEqual(await handler._admitViaLink('g1', NEWCOMER, 'Ana', viaLink, { resendOnly: true }), true)
  assert.strictEqual(calls.invites.length, 2)
  assert.strictEqual(calls.broadcasts.length, 1)
})

test('add_member over IPC behaves as before', async () => {
  const { handler, calls } = makeHarness({ conversations: ownedGroup() })
  assert.deepStrictEqual(await handler.routeMessage('conversation.add_member', { conversationId: 'g1', publicKey: PEER }),
    { success: true, alreadyMember: true })
  assert.strictEqual(calls.invites.length, 0)
  const added = await handler.routeMessage('conversation.add_member', { conversationId: 'g1', publicKey: NEWCOMER, displayName: 'Ana' })
  assert.deepStrictEqual(added, { success: true, participants: [PEER, NEWCOMER] })
  assert.strictEqual(calls.invites[0].invite.viaLink, undefined)
  assert.strictEqual(calls.events.filter(e => e.type === 'conversation.member_added').length, 0)
})

test('a group invite keeps viaLink through normalization and hands it to the link service', async () => {
  const { handler } = makeHarness()
  let seen = null
  handler.groupLinks.expectsAdmission = () => true
  handler.groupLinks.onGroupInviteApplied = (invite, conversation) => { seen = { invite, conversation } }
  const viaLink = { linkId: 'AA'.repeat(16), admitSig: 'BB'.repeat(64) }
  const finished = await handler._handleGroupInvite({
    type: 'group_invite', groupId: GROUP_ID, groupName: 'Hiking Crew', creatorKey: PEER, senderKey: PEER, participants: [PEER, MY], viaLink
  }, PEER)
  assert.strictEqual(finished, true)
  assert.deepStrictEqual(seen.invite.viaLink, { linkId: 'aa'.repeat(16), admitSig: 'bb'.repeat(64) })
  assert.strictEqual(seen.conversation.groupId, GROUP_ID)
})

test('a malformed viaLink makes the record invalid', () => {
  const base = { type: 'group_invite', groupId: GROUP_ID, creatorKey: PEER, senderKey: PEER, participants: [PEER, MY] }
  for (const viaLink of ['x', { linkId: 'zz', admitSig: 'bb'.repeat(64) }, { linkId: 'aa'.repeat(16), admitSig: 'b' }]) {
    assert.throws(() => normalizePeerRecord({ ...base, viaLink }, PEER), e => e.code === 'INVALID_PEER_RECORD')
  }
})

test('join results from the mailbox reach the link service and are always finished', async () => {
  const { handler } = makeHarness()
  const seen = []
  handler.groupLinks.handleJoinResult = (result, sender) => { seen.push({ result, sender }); return true }
  const result = { type: 'group_join_result', v: 1, linkId: '11'.repeat(16), joinerKey: MY, status: 'full', sig: '00'.repeat(64) }
  assert.strictEqual(await handler.deliverMailboxInvite(result, PEER), true)
  assert.deepStrictEqual(seen, [{ result, sender: PEER }])
})

test('blocked keys are validated and stored', async () => {
  const { handler } = makeHarness()
  await handler.routeMessage('contacts.set_blocked_keys', { keys: [PEER.toUpperCase()] })
  assert.ok(handler.groupLinks.store.isBlocked(PEER))
  await assert.rejects(handler.routeMessage('contacts.set_blocked_keys', { keys: ['nope'] }), e => e.code === 'INVALID_KEYS')
})

test('joining needs a chat identity', async () => {
  const { handler } = makeHarness({ conversations: ownedGroup() })
  const { link } = await handler.routeMessage('group_link.enable', { conversationId: 'g1' })
  handler.groupLinks.identity = { keyPair: null, publicKeyHex: null }
  await assert.rejects(handler.routeMessage('group_link.join', { link }), e => e.code === 'NO_IDENTITY')
})

test('wiping the account clears link state in memory', async () => {
  const { handler } = makeHarness({ conversations: ownedGroup() })
  handler.chatStore.clearAll = async () => {}
  await handler.routeMessage('group_link.enable', { conversationId: 'g1' })
  await handler._wipeAccountData()
  assert.deepStrictEqual(handler.groupLinks.store.ownerConversationIds(), [])
})

// ── Cancellation and delivery, owner and joiner each through a real handler ──

const JOINER_KP = keyPairFrom(3)

/** An owner with a link, and a joiner whose request is waiting on it. */
async function ownerAndJoiner ({ approval = 'auto' } = {}) {
  const owner = makeHarness({ conversations: ownedGroup() })
  const { link } = await owner.handler.routeMessage('group_link.enable', { conversationId: 'g1', approval })
  const joiner = makeHarness({ keyPair: JOINER_KP })
  const { status, linkId } = await joiner.handler.routeMessage('group_link.join', { link })
  assert.strictEqual(status, 'requested')
  return { owner, joiner, linkId }
}

/** What the owner sends when it admits the joiner through the link. */
async function admissionInvite (owner, linkId) {
  const viaLink = owner.handler.groupLinks.viaLinkFor('g1', linkId, NEWCOMER)
  assert.strictEqual(await owner.handler._admitViaLink('g1', NEWCOMER, 'Ana', viaLink), true)
  return owner.calls.invites.filter(c => c.key === NEWCOMER).pop().invite
}

test('a link admission joins the group while the request is waiting', async () => {
  const { owner, joiner, linkId } = await ownerAndJoiner()
  const invite = await admissionInvite(owner, linkId)
  assert.strictEqual(await joiner.handler._handleGroupInvite(invite, MY), true)
  const conv = [...joiner.chatStore.conversations.values()].find(c => c.groupId === GROUP_ID)
  assert.ok(conv)
  assert.deepStrictEqual(joiner.calls.joins, [conv.id])
  assert.strictEqual(joiner.handler.groupLinks.store.joinerRecord(linkId).status, 'joined')

  // A second copy of the same admission changes nothing.
  assert.strictEqual(await joiner.handler._handleGroupInvite(invite, MY), true)
  assert.strictEqual(joiner.chatStore.conversations.size, 1)
})

test('after cancelling, a later admission creates and joins nothing', async () => {
  const { owner, joiner, linkId } = await ownerAndJoiner()
  assert.deepStrictEqual(await joiner.handler.routeMessage('group_link.cancel', { linkId }), { cancelled: true })
  assert.strictEqual(joiner.handler.groupLinks.store.joinerRecord(linkId).status, 'cancelled')
  assert.deepStrictEqual(joiner.calls.events.find(e => e.type === 'group_link.join_updated').payload,
    { linkId, status: 'cancelled', conversationId: null })

  const invite = await admissionInvite(owner, linkId)
  assert.strictEqual(await joiner.handler._handleGroupInvite(invite, MY), true, 'finished with, so the mailbox copy goes')
  assert.strictEqual(joiner.chatStore.conversations.size, 0)
  assert.deepStrictEqual(joiner.calls.joins, [])
  assert.strictEqual(joiner.handler.groupLinks.store.joinerRecord(linkId).status, 'cancelled')

  // Still refused once the cancelled request has been forgotten.
  joiner.handler.groupLinks.store.deleteJoinerRecord(linkId)
  await joiner.handler._handleGroupInvite(invite, MY)
  assert.strictEqual(joiner.chatStore.conversations.size, 0)
})

test('a request that ended is not revived by a late admission', async () => {
  const { owner, joiner, linkId } = await ownerAndJoiner()
  const record = joiner.handler.groupLinks.store.joinerRecord(linkId)
  joiner.handler.groupLinks._finish(record, 'expired')
  await joiner.handler._handleGroupInvite(await admissionInvite(owner, linkId), MY)
  assert.strictEqual(joiner.chatStore.conversations.size, 0)
})

test('an approved admission no transport took is kept on disk and sent after a restart', async () => {
  const { owner, joiner, linkId } = await ownerAndJoiner({ approval: 'owner' })
  const entry = owner.handler.groupLinks.store.ownerEntry('g1')
  entry.requests.push({ linkId, joinerKey: NEWCOMER, joinerName: 'Ana', requestedAt: Date.now(), receivedAt: Date.now() })
  owner.handler.p2pManager.sendInvite = async () => false

  assert.deepStrictEqual(await owner.handler.routeMessage('group_link.approve', { conversationId: 'g1', joinerKey: NEWCOMER }), { status: 'admitted' })
  assert.deepStrictEqual(owner.chatStore.conversations.get('g1').participantIds, [PEER, NEWCOMER])
  assert.deepStrictEqual(await owner.handler.routeMessage('group_link.requests', { conversationId: 'g1' }), { requests: [] })
  const storePath = owner.handler.groupLinks.store.filePath
  assert.ok(new GroupLinkStore({ filePath: storePath }).pendingAdmission('g1', NEWCOMER), 'saved before the app could close')

  // The app closes and starts again.
  const restarted = makeHarness({ conversations: { g1: owner.chatStore.conversations.get('g1') }, storePath })
  await restarted.handler.retryPendingAdmissions()
  const sent = restarted.calls.invites.filter(c => c.key === NEWCOMER)
  assert.strictEqual(sent.length, 1)
  assert.strictEqual(restarted.handler.groupLinks.store.pendingAdmission('g1', NEWCOMER), null)
  await restarted.handler.retryPendingAdmissions()
  assert.strictEqual(restarted.calls.invites.length, 1, 'sent once')

  assert.strictEqual(await joiner.handler._handleGroupInvite(sent[0].invite, MY), true)
  assert.strictEqual(joiner.handler.groupLinks.store.joinerRecord(linkId).status, 'joined')
})

test('an undelivered admission is signed again when the group moves to a new secret', async () => {
  const { owner, joiner, linkId } = await ownerAndJoiner()
  owner.handler.p2pManager.sendInvite = async () => false
  await admissionInvite(owner, linkId).catch(() => {})
  assert.ok(owner.handler.groupLinks.store.pendingAdmission('g1', NEWCOMER))
  owner.chatStore.conversations.get('g1').groupId = 'ef'.repeat(32)

  owner.handler.p2pManager.sendInvite = async (key, invite) => { owner.calls.invites.push({ key, invite }); return true }
  await owner.handler.retryPendingAdmissions()
  const { invite } = owner.calls.invites.pop()
  assert.strictEqual(invite.groupId, 'ef'.repeat(32))
  await joiner.handler._handleGroupInvite(invite, MY)
  assert.strictEqual(joiner.handler.groupLinks.store.joinerRecord(linkId).status, 'joined')
})

test('a queued admission is dropped once the joiner is no longer a member', async () => {
  const { owner, linkId } = await ownerAndJoiner()
  owner.handler.p2pManager.sendInvite = async () => false
  await admissionInvite(owner, linkId).catch(() => {})
  owner.chatStore.conversations.get('g1').participantIds = [PEER]
  owner.handler.p2pManager.sendInvite = async () => { throw new Error('must not send') }
  await owner.handler.retryPendingAdmissions()
  assert.strictEqual(owner.handler.groupLinks.store.pendingAdmission('g1', NEWCOMER), null)
})

test('cancelling over IPC hands the owner a withdrawal for that request', async () => {
  const owner = makeHarness({ conversations: ownedGroup() })
  const { link } = await owner.handler.routeMessage('group_link.enable', { conversationId: 'g1' })
  const joiner = makeHarness({ keyPair: JOINER_KP })
  const put = []
  let online = false
  joiner.handler.p2pManager.putMailboxAs = async (sender, recipient, record) => { put.push({ sender, recipient, record }); return online }
  const { linkId } = await joiner.handler.routeMessage('group_link.join', { link })
  const request = joiner.handler.groupLinks.store.joinerRecord(linkId).request

  assert.deepStrictEqual(await joiner.handler.routeMessage('group_link.cancel', { linkId }), { cancelled: true })
  const withdrawal = put.pop()
  const rendezvous = gl.parseLink(link).rendezvousPublicKey
  assert.strictEqual(withdrawal.recipient, hex(rendezvous))
  assert.notStrictEqual(hex(withdrawal.sender.publicKey), NEWCOMER, 'under a throwaway key, like the request')
  assert.strictEqual(withdrawal.record.type, 'group_join_withdraw')
  assert.strictEqual(withdrawal.record.nonce, request.nonce)
  assert.ok(gl.verifyJoinWithdrawal(withdrawal.record, rendezvous))
  assert.ok(joiner.handler.groupLinks.store.joinerRecord(linkId).withdrawal, 'kept: no transport took it')

  online = true
  await joiner.handler.groupLinks.maintainJoinRequests()
  assert.strictEqual(joiner.handler.groupLinks.store.joinerRecord(linkId).withdrawal, null)
})

test('a failed admission is retried on mailbox drains, not only at the next start', async () => {
  const { owner, joiner, linkId } = await ownerAndJoiner()
  const restarted = makeHarness({ conversations: { g1: owner.chatStore.conversations.get('g1') }, storePath: owner.handler.groupLinks.store.filePath })
  restarted.handler.p2pManager.sendInvite = async () => false
  await admissionInviteFrom(restarted.handler, linkId)
  assert.ok(restarted.handler.groupLinks.store.pendingAdmission('g1', NEWCOMER))

  // The next drain takes it through the mailbox.
  const resent = []
  restarted.handler.p2pManager.resendInvite = async (key, invite) => { resent.push({ key, invite }); return true }
  await restarted.handler.retryPendingDeliveries()
  assert.strictEqual(resent.length, 1)
  assert.strictEqual(restarted.handler.groupLinks.store.pendingAdmission('g1', NEWCOMER), null)
  await joiner.handler._handleGroupInvite(resent[0].invite, MY)
  assert.strictEqual(joiner.handler.groupLinks.store.joinerRecord(linkId).status, 'joined')
})

async function admissionInviteFrom (handler, linkId) {
  const viaLink = handler.groupLinks.viaLinkFor('g1', linkId, NEWCOMER)
  return handler._admitViaLink('g1', NEWCOMER, 'Ana', viaLink)
}
