/**
 * Authorization tests for the invite / group-control handlers in ipc-handler.
 *
 * IPCHandler binds its transport at construction: Bare.IPC when the Bare global
 * exists, else the bare-ipc module. A stub Bare global is installed BEFORE any
 * core module loads so construction wires against a no-op transport (and
 * config/storage see an empty argv). node --test runs each test file in its own
 * process, so this global does not leak into other suites.
 */

globalThis.Bare = { argv: [], IPC: { on: () => {} } }

const { test } = require('node:test')
const assert = require('node:assert')
const EventEmitter = require('node:events')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const { IPCHandler } = require('../lib/ipc-handler')
const { ChatStore } = require('../lib/chat-store')

function keyHex (seedByte) {
  return b4a.toString(crypto.keyPair(b4a.alloc(32, seedByte)).publicKey, 'hex')
}

const MY = keyHex(1)
const PEER = keyHex(2)
const OTHER = keyHex(3)
const CORE_KEY = 'cc'.repeat(32)

// Minimal chat-store + p2p stubs recording what each handler tries to mutate,
// so a rejected control message is observable as "nothing happened".
function makeHarness ({ conversations = {}, groupTopics = {}, leftConversations = [] } = {}) {
  const calls = {
    created: [], updated: [], openedCores: [], deleted: [],
    runtimeRevoked: [], coreRevoked: [], mirrorRevoked: [], conversationsRemoved: [],
    clearedLeft: []
  }

  const chatStore = {
    conversations: new Map(Object.entries(conversations)),
    leftConversations: new Set(leftConversations),
    async getConversation (id) { return this.conversations.get(id) || null },
    hasLeftConversation (id) { return this.leftConversations.has(id) },
    clearLeftStatus (id) {
      this.leftConversations.delete(id)
      calls.clearedLeft.push(id)
    },
    resolveDirectChatId (a, b) {
      const id = ChatStore.directChatId(a, b)
      return { id, conversation: this.conversations.get(id) || null, migrated: false }
    },
    async createConversationWithId (id, type, participantIds, options = {}) {
      const conv = { id, type, participantIds, ...options }
      this.conversations.set(id, conv)
      calls.created.push(conv)
      return conv
    },
    async createConversation (type, participantIds, options = {}) {
      for (const conv of this.conversations.values()) {
        if (conv.type === type && options.groupId && conv.groupId === options.groupId) return conv
      }
      return this.createConversationWithId('local-' + this.conversations.size, type, participantIds, options)
    },
    async updateConversation (id, updates) {
      const conv = this.conversations.get(id)
      if (!conv) return null
      Object.assign(conv, updates)
      calls.updated.push({ id, updates })
      return conv
    },
    async deleteConversation (id) {
      calls.deleted.push(id)
      this.conversations.delete(id)
      return true
    }
  }

  const p2pManager = new EventEmitter()
  p2pManager.groupTopicToConversation = new Map(Object.entries(groupTopics))
  p2pManager.groupConversations = new Map()
  p2pManager.joinConversation = async () => true
  p2pManager.joinGroupConversation = async () => true
  // Both report success: the invite handler now acts on these results, because
  // a mailbox-delivered invite is deleted on the strength of them.
  p2pManager.openRemoteCore = async (...args) => { calls.openedCores.push(args); return true }
  p2pManager.leaveConversation = async () => {}
  p2pManager.sendInvite = async () => true
  p2pManager.sendToConversation = () => true
  p2pManager.removeGroupParticipant = (...args) => { calls.runtimeRevoked.push(args) }

  const hypercoreManager = {
    getLocalCoreKey: () => null,
    removeRemoteCore: async (...args) => { calls.coreRevoked.push(args) },
    removeConversation: async (...args) => { calls.conversationsRemoved.push(args) }
  }
  const blindMirror = {
    removeRemoteCore: (...args) => { calls.mirrorRevoked.push(args) },
    removeConversation: (...args) => { calls.conversationsRemoved.push(args) }
  }

  const handler = new IPCHandler({
    identity: { publicKeyHex: MY, displayName: 'Me' },
    chatStore,
    contactStore: { async getContact () { return null } },
    p2pManager,
    hypercoreManager,
    blindMirror
  })
  handler.pushEvent = () => {}

  // The stdin IPC fallback attaches a data listener and resume()s stdin, which
  // would hold the test process open. Detach it.
  process.stdin.removeAllListeners('data')
  process.stdin.pause()

  return { handler, chatStore, p2pManager, calls }
}

// --- direct chat lifecycle ---

test('direct status reports an explicitly left conversation without reopening it', async () => {
  const conversationId = ChatStore.directChatId(MY, PEER)
  const { handler, chatStore, calls } = makeHarness({ leftConversations: [conversationId] })

  const status = await handler.handleConversation('direct_status', { participants: [PEER] })

  assert.deepStrictEqual(status, { conversationId, isLeft: true })
  assert.ok(chatStore.leftConversations.has(conversationId), 'Status lookup must not clear the tombstone')
  assert.deepStrictEqual(calls.clearedLeft, [])
})

test('explicit direct create clears a tombstone before recreating the conversation', async () => {
  const conversationId = ChatStore.directChatId(MY, PEER)
  const { handler, chatStore, calls } = makeHarness({ leftConversations: [conversationId] })

  const result = await handler.handleConversation('create', {
    type: 'direct',
    participants: [PEER],
    displayName: 'Peer'
  })

  assert.strictEqual(result.conversation.id, conversationId)
  assert.ok(chatStore.conversations.has(conversationId), 'Rejoin must recreate the deterministic conversation')
  assert.ok(!chatStore.leftConversations.has(conversationId), 'Rejoin must clear the tombstone')
  assert.deepStrictEqual(calls.clearedLeft, [conversationId])
})

// --- direct invites ---

test('direct invite with a senderKey != authenticated peer is dropped entirely', async () => {
  const { handler, calls } = makeHarness()
  await handler._handleDirectInvite(
    { type: 'direct_invite', senderKey: OTHER, localCoreKey: CORE_KEY },
    PEER // authenticated Noise peer
  )
  assert.strictEqual(calls.created.length, 0, 'No conversation may be created for a spoofed sender')
  assert.strictEqual(calls.openedCores.length, 0, 'No remote core may be opened')
})

test('direct invite with an arbitrary conversationId falls back to the deterministic id', async () => {
  const { handler, calls } = makeHarness()
  await handler._handleDirectInvite(
    { type: 'direct_invite', senderKey: PEER, conversationId: 'dm_' + 'ff'.repeat(16), localCoreKey: CORE_KEY },
    PEER
  )
  const expected = ChatStore.directChatId(MY, PEER)
  assert.strictEqual(calls.created.length, 1)
  assert.strictEqual(calls.created[0].id, expected, 'Wire id must be ignored; deterministic id used')
  assert.strictEqual(calls.openedCores.length, 1)
  assert.strictEqual(calls.openedCores[0][0], expected)
  assert.strictEqual(calls.openedCores[0][1], PEER)
})

test('legitimate direct invite creates the conversation and opens the sender core', async () => {
  const { handler, calls } = makeHarness()
  const convId = ChatStore.directChatId(MY, PEER)
  await handler._handleDirectInvite(
    { type: 'direct_invite', senderKey: PEER, conversationId: convId, senderDisplayName: 'Peer', localCoreKey: CORE_KEY },
    PEER
  )
  assert.strictEqual(calls.created.length, 1)
  assert.strictEqual(calls.created[0].id, convId)
  assert.deepStrictEqual(calls.created[0].participantIds, [PEER])
  assert.deepStrictEqual(calls.openedCores[0], [convId, PEER, CORE_KEY])
})

// --- group invites ---

// Enumerated rather than sampled, because each of these has to answer the
// mailbox as well as reject: an entry the handler never reports finished with
// holds the first page and starves every invite behind it until its TTL. One
// branch returned undefined and did exactly that.
const GROUP = 'ab'.repeat(32)

for (const [what, invite] of [
  ['an unsigned sender', { groupId: GROUP, participants: [MY, PEER], creatorKey: PEER }],
  ['a sender other than the authenticated peer', { groupId: GROUP, participants: [MY, PEER, OTHER], senderKey: OTHER, creatorKey: OTHER }],
  ['a malformed groupId', { groupId: 'not-a-key', participants: [MY, PEER], senderKey: PEER, creatorKey: PEER }],
  ['no participant list at all', { groupId: GROUP, senderKey: PEER, creatorKey: PEER }],
  ['a participant list that is not an array', { groupId: GROUP, participants: MY + ',' + PEER, senderKey: PEER, creatorKey: PEER }],
  ['a malformed participant key', { groupId: GROUP, participants: [MY, 'zz'], senderKey: PEER, creatorKey: PEER }],
  ['a participant that is not a string', { groupId: GROUP, participants: [MY, 42], senderKey: PEER, creatorKey: PEER }],
  ['a sender outside the group', { groupId: GROUP, participants: [MY, OTHER], senderKey: PEER, creatorKey: PEER }],
  ['no creatorKey', { groupId: GROUP, participants: [MY, PEER], senderKey: PEER }],
  ['a forged creatorKey', { groupId: GROUP, participants: [MY, PEER, OTHER], senderKey: PEER, creatorKey: OTHER }],
  ['a participant list that omits us', { groupId: GROUP, participants: [PEER, OTHER], senderKey: PEER, creatorKey: PEER }]
]) {
  test('a group invite with ' + what + ' is rejected as terminal', async () => {
    const { handler, calls } = makeHarness()

    const finished = await handler._handleGroupInvite(
      { type: 'group_invite', localCoreKey: CORE_KEY, ...invite }, PEER
    )

    assert.strictEqual(finished, true, 'a rejection the mailbox cannot delete starves it')
    assert.strictEqual(calls.created.length, 0, 'no conversation may be created')
    assert.strictEqual(calls.openedCores.length, 0, 'no remote core may be opened')
  })
}

test('legitimate group invite is accepted and the sender core is opened', async () => {
  const { handler, calls } = makeHarness()
  await handler._handleGroupInvite(
    {
      type: 'group_invite', groupId: 'ab'.repeat(32), groupName: 'Trip', creatorKey: PEER,
      participants: [MY, PEER, OTHER], senderKey: PEER, localCoreKey: CORE_KEY
    },
    PEER
  )
  assert.strictEqual(calls.created.length, 1)
  assert.strictEqual(calls.created[0].type, 'group')
  assert.strictEqual(calls.openedCores.length, 1)
  assert.strictEqual(calls.openedCores[0][1], PEER)
})

// --- group control messages ---

function groupFixture () {
  return makeHarness({
    conversations: {
      g1: { id: 'g1', type: 'group', groupId: 'ab'.repeat(32), participantIds: [PEER, OTHER], creatorKey: OTHER, displayName: 'Trip' },
      dm1: { id: 'dm1', type: 'direct', participantIds: [PEER], displayName: 'Peer' }
    },
    groupTopics: { ['aa'.repeat(32)]: 'g1' }
  })
}

test('group_member_added from a non-participant is rejected', async () => {
  const { handler, calls } = groupFixture()
  const STRANGER = keyHex(9)
  await handler._handleGroupMemberAdded(
    { groupTopicHex: 'aa'.repeat(32), newMemberKey: STRANGER, updatedParticipants: [MY, STRANGER] },
    STRANGER
  )
  assert.strictEqual(calls.updated.length, 0, 'Membership must not change on a non-participant announcement')
})

test('group_member_added from a non-owner participant is rejected', async () => {
  const { handler, calls } = groupFixture()
  const NEW = keyHex(8)
  await handler._handleGroupMemberAdded(
    { groupTopicHex: 'aa'.repeat(32), newMemberKey: NEW, updatedParticipants: [MY, PEER, NEW] },
    PEER
  )
  assert.strictEqual(calls.updated.length, 0)
})

test('a non-owner cannot add a member through local IPC', async () => {
  const { handler } = groupFixture()
  await assert.rejects(
    handler.handleConversation('add_member', { conversationId: 'g1', publicKey: keyHex(8) }),
    /Only the group owner can add members/
  )
})

test('group_member_added merges additively — it can never evict existing members', async () => {
  const { handler, chatStore, calls } = groupFixture()
  const NEW = keyHex(8)
  await handler._handleGroupMemberAdded(
    // A replace-style payload that omits PEER entirely
    { groupTopicHex: 'aa'.repeat(32), newMemberKey: NEW, updatedParticipants: [MY, NEW] },
    OTHER
  )
  assert.strictEqual(calls.updated.length, 1)
  const participants = chatStore.conversations.get('g1').participantIds
  assert.ok(participants.includes(PEER), 'Existing member must survive an add announcement')
  assert.ok(participants.includes(NEW), 'New member must be added')
  assert.ok(!participants.includes(MY), 'Own key is never stored in participantIds')
})

test('group_leave removes and fully revokes only the authenticated sender', async () => {
  const { handler, chatStore, calls } = groupFixture()
  chatStore.conversations.get('g1').participantIds = [PEER, OTHER]
  await handler._handleGroupLeave({ groupTopicHex: 'aa'.repeat(32), leaverKey: OTHER }, PEER)
  const participants = chatStore.conversations.get('g1').participantIds
  assert.ok(!participants.includes(PEER), 'Authenticated sender is removed')
  assert.ok(participants.includes(OTHER), 'Wire-named victim must NOT be removed')
  assert.deepStrictEqual(calls.runtimeRevoked, [['g1', PEER]])
  assert.deepStrictEqual(calls.coreRevoked, [['g1', PEER, {}]])
  assert.deepStrictEqual(calls.mirrorRevoked, [['g1', PEER]])
})

test('group_deleted is honored only from the group owner', async () => {
  const { handler, calls, chatStore } = groupFixture()
  await handler._handleGroupDeleted({ groupTopicHex: 'aa'.repeat(32) }, PEER)
  assert.strictEqual(calls.deleted.length, 0, 'Non-owner delete must be rejected')
  assert.ok(chatStore.conversations.has('g1'))

  await handler._handleGroupDeleted({ groupTopicHex: 'aa'.repeat(32) }, OTHER)
  assert.deepStrictEqual(calls.deleted, ['g1'], 'Owner delete goes through')
})

test('group_renamed cannot target arbitrary conversations via a wire conversationId', async () => {
  const { handler, chatStore } = groupFixture()
  // Attempt to rename a DIRECT conversation through the group_renamed path
  await handler._handleGroupRenamed({ conversationId: 'dm1', newName: 'pwned' }, PEER)
  assert.strictEqual(chatStore.conversations.get('dm1').displayName, 'Peer', 'A DM must not be renamable via group_renamed')

  // Non-participant rename of the group is rejected
  const STRANGER = keyHex(9)
  await handler._handleGroupRenamed({ groupTopicHex: 'aa'.repeat(32), newName: 'pwned' }, STRANGER)
  assert.strictEqual(chatStore.conversations.get('g1').displayName, 'Trip', 'Non-participant rename rejected')

  // A participant rename via the topic is accepted
  await handler._handleGroupRenamed({ groupTopicHex: 'aa'.repeat(32), newName: 'Roadtrip' }, PEER)
  assert.strictEqual(chatStore.conversations.get('g1').displayName, 'Roadtrip', 'Participant rename via topic accepted')
})

// --- mailbox delivery contract ---
//
// deliverMailboxInvite's answer decides whether the server-side copy is deleted,
// and that copy is the only one. True means finished with; false means retry.

test('a rejected invite is reported finished, so it cannot block the mailbox', async () => {
  const { handler } = makeHarness()

  // senderKey does not match the authenticated peer: terminal, not retryable.
  const spoofed = await handler._handleDirectInvite(
    { type: 'direct_invite', senderKey: OTHER, localCoreKey: CORE_KEY }, PEER
  )

  assert.strictEqual(spoofed, true,
    'a validation rejection must not be retried forever, or one sender can starve the mailbox')
})

test('an unsupported mailbox invite type is reported finished', async () => {
  const { handler } = makeHarness()
  assert.strictEqual(await handler.deliverMailboxInvite({ type: 'nonsense' }, PEER), true)
})

test('a failed topic join leaves the invite for the next drain', async () => {
  const { handler, p2pManager } = makeHarness()
  p2pManager.joinConversation = async () => false

  const applied = await handler.deliverMailboxInvite(
    { type: 'direct_invite', senderKey: PEER, localCoreKey: CORE_KEY }, PEER
  )

  assert.strictEqual(applied, false, 'the core key must not be deleted after a failed join')
})

test('a failed remote-core open leaves the invite for the next drain', async () => {
  const { handler, p2pManager } = makeHarness()
  p2pManager.openRemoteCore = async () => false

  const applied = await handler.deliverMailboxInvite(
    { type: 'direct_invite', senderKey: PEER, localCoreKey: CORE_KEY }, PEER
  )

  assert.strictEqual(applied, false,
    'the invite carries the only copy of that core key')
})

test('a fully applied invite is reported finished', async () => {
  const { handler, calls } = makeHarness()

  const applied = await handler.deliverMailboxInvite(
    { type: 'direct_invite', senderKey: PEER, localCoreKey: CORE_KEY }, PEER
  )

  assert.strictEqual(applied, true)
  assert.strictEqual(calls.openedCores.length, 1, 'and the core was actually opened')
})

// The reciprocal reply is deliberately non-fatal: their core is already open, and
// our own key gets another chance on the next send into this conversation.
test('a failed reciprocal reply does not block the invite', async () => {
  const { handler, p2pManager } = makeHarness()
  p2pManager.sendInvite = async () => false

  const applied = await handler.deliverMailboxInvite(
    { type: 'direct_invite', senderKey: PEER, localCoreKey: CORE_KEY }, PEER
  )

  assert.strictEqual(applied, true)
})
