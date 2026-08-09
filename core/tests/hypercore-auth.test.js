/**
 * Unit tests for HypercoreManager key derivation + remote-core authorization.
 * No Corestore needed — these exercise the pre-open gates.
 */

const { test } = require('node:test')
const assert = require('node:assert')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const { HypercoreManager } = require('../lib/hypercore-manager')
const { ChatStore } = require('../lib/chat-store')

function makeIdentity (seedByte) {
  return crypto.keyPair(b4a.alloc(32, seedByte))
}

const alice = makeIdentity(1)
const bob = makeIdentity(2)
const aliceHex = b4a.toString(alice.publicKey, 'hex')
const bobHex = b4a.toString(bob.publicKey, 'hex')
const malloryHex = b4a.toString(makeIdentity(3).publicKey, 'hex')

function managerFor (keyPair, conversations) {
  const m = new HypercoreManager()
  m.setKeyContext({
    getIdentityKeyPair: () => keyPair,
    getConversation: (id) => conversations[id] || null
  })
  return m
}

test('deriveEncryptionKey fails closed without a key context', () => {
  const m = new HypercoreManager()
  assert.throws(() => m.deriveEncryptionKey('dm_' + 'ab'.repeat(16)), /key context/)
})

test('deriveEncryptionKey fails closed for an unknown conversation', () => {
  const m = managerFor(alice, {})
  assert.throws(() => m.deriveEncryptionKey('dm_' + 'ab'.repeat(16)), /unknown conversation/)
})

test('both direct-chat participants derive the identical core key', () => {
  const convId = ChatStore.directChatId(aliceHex, bobHex)
  const aliceMgr = managerFor(alice, {
    [convId]: { id: convId, type: 'direct', participantIds: [bobHex] }
  })
  const bobMgr = managerFor(bob, {
    [convId]: { id: convId, type: 'direct', participantIds: [aliceHex] }
  })
  const kA = aliceMgr.deriveEncryptionKey(convId)
  const kB = bobMgr.deriveEncryptionKey(convId)
  assert.ok(b4a.equals(kA, kB), 'Alice and Bob must derive the same key')
  assert.ok(!b4a.equals(kA, crypto.data(b4a.from('zapp-enc:' + convId))),
    'Key must not be the v1 public conversationId hash')
})

test('group members derive the same key despite different local conversation ids', () => {
  // Each member stores the group under its own randomly-generated local id;
  // only the invite-distributed groupId is shared. The key must follow groupId.
  const groupId = b4a.toString(crypto.randomBytes(32), 'hex')
  const aliceMgr = managerFor(alice, {
    convA: { id: 'convA', type: 'group', groupId, participantIds: [bobHex], creatorKey: aliceHex }
  })
  const bobMgr = managerFor(bob, {
    convB: { id: 'convB', type: 'group', groupId, participantIds: [aliceHex], creatorKey: aliceHex }
  })
  assert.ok(b4a.equals(aliceMgr.deriveEncryptionKey('convA'), bobMgr.deriveEncryptionKey('convB')),
    'Group key must interoperate across members')
})

test('direct derive fails closed when the conversation has no remote participant', () => {
  const m = managerFor(alice, {
    broken: { id: 'broken', type: 'direct', participantIds: [] }
  })
  assert.throws(() => m.deriveEncryptionKey('broken'), /no remote participant/)
})

test('_assertRemoteCoreAuthorized rejects unknown conversations and non-participants', () => {
  const convId = ChatStore.directChatId(aliceHex, bobHex)
  const m = managerFor(alice, {
    [convId]: { id: convId, type: 'direct', participantIds: [bobHex] },
    grp: { id: 'grp', type: 'group', groupId: 'aa'.repeat(32), participantIds: [bobHex], creatorKey: aliceHex }
  })

  assert.throws(() => m._assertRemoteCoreAuthorized('nope', bobHex), /unknown conversation/)
  assert.throws(() => m._assertRemoteCoreAuthorized(convId, malloryHex), /not a participant/)
  assert.doesNotThrow(() => m._assertRemoteCoreAuthorized(convId, bobHex))
  assert.doesNotThrow(() => m._assertRemoteCoreAuthorized('grp', bobHex))
  assert.doesNotThrow(() => m._assertRemoteCoreAuthorized('grp', aliceHex), 'Creator counts as authorized')
  assert.throws(() => m._assertRemoteCoreAuthorized('grp', malloryHex), /not a participant/)
})

test('openRemoteCore is gated by the authorization check before touching the store', async () => {
  const m = managerFor(alice, {})
  m._ready = true // bypass initialize(); the gate must fire before any store use
  await assert.rejects(
    m.openRemoteCore('dm_' + 'ab'.repeat(16), bobHex, 'cc'.repeat(32)),
    /unknown conversation/
  )
})

test('removeRemoteCore closes and forgets a departed peer writer', async () => {
  const m = managerFor(alice, {})
  let closes = 0
  let saves = 0
  const core = { close: async () => { closes++ } }
  m.remoteCores.set('grp', new Map([[bobHex, core]]))
  m._coreKeyIndex.set('grp', new Map([[bobHex, 'cc'.repeat(32)]]))
  m._remoteReferrerIndex.set('grp', new Map([[bobHex, aliceHex]]))
  m.saveCoreKeyIndex = () => { saves++ }

  assert.strictEqual(await m.removeRemoteCore('grp', bobHex), true)
  assert.strictEqual(closes, 1)
  assert.strictEqual(m.remoteCores.has('grp'), false)
  assert.strictEqual(m._coreKeyIndex.has('grp'), false)
  assert.strictEqual(m._remoteReferrerIndex.has('grp'), false)
  assert.strictEqual(saves, 1)
})
