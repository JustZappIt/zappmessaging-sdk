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

function makeHarness ({ conversations = {} } = {}) {
  const calls = { invites: [], broadcasts: [], events: [], updated: [] }
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
  p2pManager.joinGroupConversation = async () => true
  p2pManager.openRemoteCore = async () => true
  p2pManager.sendInvite = async (key, invite) => { calls.invites.push({ key, invite }); return true }
  p2pManager.sendToConversation = (id, record) => { calls.broadcasts.push({ id, record }); return true }

  const handler = new IPCHandler({
    identity: { keyPair: MY_KP, publicKeyHex: MY, displayName: 'Me' },
    chatStore,
    contactStore: { async getContact () { return null }, async clearAll () {} },
    p2pManager,
    hypercoreManager: { getLocalCoreKey: () => 'cc'.repeat(32) },
    groupLinkStore: new GroupLinkStore({ filePath: path.join(tmpRoot, 'links-' + (++counter) + '.json') })
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
