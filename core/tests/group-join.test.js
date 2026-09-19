/**
 * The group invite link protocol end to end: an owner and joiners exchange
 * real encrypted mailbox envelopes through an in-memory mailbox, using the
 * same encryptInvite and drainInvites code the app uses.
 */

const { test } = require('node:test')
const assert = require('node:assert')
const fs = require('fs')
const os = require('os')
const path = require('path')
const b4a = require('b4a')
const sodium = require('sodium-universal')
const { encryptInvite, drainInvites } = require('../lib/invite-mailbox')
const { GroupLinkStore } = require('../lib/group-link-store')
const { GroupLinkService, RATE_LIMIT, RESEND_INTERVAL_MS, JOINER_TTL_MS } = require('../lib/group-link-service')
const gl = require('../lib/group-link')

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'group-join-'))
process.once('exit', () => fs.rmSync(tmpRoot, { recursive: true, force: true }))
let fileCounter = 0

function keyPair () {
  const publicKey = b4a.alloc(32)
  const secretKey = b4a.alloc(64)
  sodium.crypto_sign_keypair(publicKey, secretKey)
  return { publicKey, secretKey }
}
const hex = (buf) => b4a.toString(buf, 'hex')

/** The blind peer mailbox, reduced to its contract: put, list, ack. */
class Mailbox {
  constructor () {
    this.boxes = new Map()
    this.nextId = 0
    this.online = true
  }

  put (senderKeyPair, recipientHex, record) {
    if (!this.online) return false
    const envelope = encryptInvite(record, senderKeyPair, recipientHex)
    const box = this.boxes.get(recipientHex) || []
    box.push({ id: String(++this.nextId), envelope })
    this.boxes.set(recipientHex, box)
    return true
  }

  async drain (keyPair, deliver) {
    if (!this.online) return false
    const owner = hex(keyPair.publicKey)
    await drainInvites(async (method, body) => {
      const box = this.boxes.get(owner) || []
      if (method === 'list') return { entries: box.map(e => ({ ...e })), more: false }
      this.boxes.set(owner, box.filter(e => !body.ids.includes(e.id)))
      return {}
    }, keyPair, deliver)
    return true
  }

  count (recipientHex) {
    return (this.boxes.get(recipientHex) || []).length
  }
}

function chatStoreStub () {
  return {
    conversations: new Map(),
    left: new Set(),
    hasLeftConversation (id) { return this.left.has(id) }
  }
}

/**
 * One device. Owners admit by adding the joiner and handing the invite to
 * that joiner's node, which is what the live socket or mailbox would do.
 */
class Node {
  constructor (world, name) {
    this.world = world
    this.name = name
    this.keyPair = keyPair()
    this.identity = { keyPair: this.keyPair, publicKeyHex: hex(this.keyPair.publicKey), displayName: name }
    this.chatStore = chatStoreStub()
    this.events = []
    this.admitCalls = []
    this.store = new GroupLinkStore({ filePath: path.join(tmpRoot, 'links-' + (++fileCounter) + '.json') })
    this.service = new GroupLinkService({
      store: this.store,
      chatStore: this.chatStore,
      identity: this.identity,
      transport: {
        put: async (sender, recipient, record) => world.mailbox.put(sender, recipient, record),
        drain: async (kp, deliver) => world.mailbox.drain(kp, deliver),
        sendInvite: async () => true,
        drainOwn: async () => this.drainOwn()
      },
      admit: async (...args) => this.admit(...args),
      emit: (type, payload) => this.events.push({ type, payload }),
      now: () => world.clock,
      timers: { setInterval: () => null, clearInterval: () => {} }
    })
    world.nodes.set(this.identity.publicKeyHex, this)
  }

  get key () { return this.identity.publicKeyHex }

  createGroup (name, memberCount = 0) {
    const id = 'group-' + this.name
    const participantIds = Array.from({ length: memberCount }, () => hex(keyPair().publicKey))
    this.chatStore.conversations.set(id, {
      id, type: 'group', groupId: hex(keyPair().publicKey), creatorKey: this.key, displayName: name, participantIds
    })
    return id
  }

  async admit (conversationId, joinerKey, joinerName, viaLink, options = {}) {
    this.admitCalls.push({ conversationId, joinerKey, joinerName, resendOnly: !!options.resendOnly })
    const conv = this.chatStore.conversations.get(conversationId)
    if (!options.resendOnly) conv.participantIds = [...conv.participantIds, joinerKey]
    const joiner = this.world.nodes.get(joinerKey)
    if (joiner) {
      joiner.receiveGroupInvite({
        type: 'group_invite',
        groupId: conv.groupId,
        creatorKey: this.key,
        groupName: conv.displayName,
        participants: [this.key, ...conv.participantIds],
        viaLink
      })
    }
    return true
  }

  receiveGroupInvite (invite) {
    let conv = [...this.chatStore.conversations.values()].find(c => c.groupId === invite.groupId)
    if (!conv) {
      conv = { id: 'joined-' + invite.groupId.substring(0, 8), type: 'group', groupId: invite.groupId, creatorKey: invite.creatorKey, participantIds: invite.participants.filter(k => k !== this.key) }
      this.chatStore.conversations.set(conv.id, conv)
    }
    this.lastInvite = invite
    this.service.onGroupInviteApplied(invite, conv)
  }

  /** What p2p-manager does with this identity's own mailbox. */
  async drainOwn () {
    await this.world.mailbox.drain(this.keyPair, async ({ invite, senderKeyHex }) => {
      if (invite && invite.type === 'group_join_result') return this.service.handleJoinResult(invite, senderKeyHex)
      return true
    })
  }

  eventsOf (type) {
    return this.events.filter(e => e.type === type).map(e => e.payload)
  }
}

function world () {
  const w = { mailbox: new Mailbox(), nodes: new Map(), clock: 1_800_000_000_000 }
  return w
}

async function setup ({ linkOptions = {}, members = 0 } = {}) {
  const w = world()
  const owner = new Node(w, 'Owner')
  const joiner = new Node(w, 'Ana')
  const groupId = owner.createGroup('Hiking Crew', members)
  const info = owner.service.enableLink(groupId, linkOptions)
  return { w, owner, joiner, groupId, info }
}

test('a joiner is admitted automatically when the owner next drains', async () => {
  const { owner, joiner, groupId, info } = await setup()
  assert.strictEqual(info.state, 'active')
  assert.match(info.link, /^https:\/\/join\.justzappit\.xyz\/g\/v1#/)
  assert.strictEqual(gl.parseLink(info.link).nameHint, 'Hiking Crew')

  const result = await joiner.service.join(info.link)
  assert.strictEqual(result.status, 'requested')
  assert.strictEqual(joiner.store.joinerRecord(result.linkId).status, 'waiting')
  assert.strictEqual(owner.admitCalls.length, 0, 'nothing happens until the owner is online')

  await owner.service.drainOwnerMailboxes()
  assert.strictEqual(owner.admitCalls.length, 1)
  assert.strictEqual(owner.admitCalls[0].joinerKey, joiner.key)
  assert.strictEqual(owner.admitCalls[0].joinerName, 'Ana')
  assert.deepStrictEqual(joiner.eventsOf('group_link.join_updated').map(e => e.status), ['joined'])
  assert.strictEqual(owner.eventsOf('group_link.member_joined').length, 1)
  assert.strictEqual(owner.service.getLink(groupId).joins, 1)
  assert.ok(joiner.store.conversationJoinedVia(result.linkId))
})

test('the joiner keeps no link secret after asking', async () => {
  const { joiner, info } = await setup()
  const secretHex = hex(gl.parseLink(info.link).secret)
  await joiner.service.join(info.link)
  const onDisk = fs.readFileSync(joiner.store.filePath, 'utf8')
  assert.ok(!onDisk.includes(secretHex))
  assert.ok(!onDisk.includes(info.link.split('#')[1]))
})

test('the envelope never names the joiner, and only the owner can open the link mailbox', async () => {
  const { w, joiner, info } = await setup()
  await joiner.service.join(info.link)
  const rendezvousHex = hex(gl.parseLink(info.link).rendezvousPublicKey)
  const [entry] = w.mailbox.boxes.get(rendezvousHex)
  const envelope = JSON.parse(entry.envelope)
  assert.notStrictEqual(envelope.sender, joiner.key)
  assert.ok(!entry.envelope.includes(joiner.key))
  // Another link holder cannot read it: it is encrypted to the rendezvous key.
  const snoop = keyPair()
  await assert.rejects(drainInvites(async (method) => method === 'list' ? { entries: [entry] } : {}, snoop, async () => true)
    .then(accepted => { if (accepted.every(a => a.invalid)) throw new Error('unreadable') }))
})

test('requests wait while the owner is offline, and resends are spaced a day apart', async () => {
  const { w, owner, joiner, info } = await setup()
  const rendezvousHex = hex(gl.parseLink(info.link).rendezvousPublicKey)
  await joiner.service.join(info.link)
  assert.strictEqual(w.mailbox.count(rendezvousHex), 1)

  await joiner.service.maintainJoinRequests()
  assert.strictEqual(w.mailbox.count(rendezvousHex), 1, 'no resend within a day')
  w.clock += RESEND_INTERVAL_MS
  await joiner.service.maintainJoinRequests()
  assert.strictEqual(w.mailbox.count(rendezvousHex), 2)
  w.clock += RESEND_INTERVAL_MS
  await joiner.service.maintainJoinRequests()
  w.clock += RESEND_INTERVAL_MS
  await joiner.service.maintainJoinRequests()
  assert.strictEqual(w.mailbox.count(rendezvousHex), 3, 'at most three sends')

  await owner.service.drainOwnerMailboxes()
  assert.strictEqual(owner.admitCalls.filter(c => !c.resendOnly).length, 1, 'admitted once')
  assert.strictEqual(owner.admitCalls.filter(c => c.resendOnly).length, 2, 'duplicates only resend the invite')
  assert.strictEqual(w.mailbox.count(rendezvousHex), 0)
})

test('a request sent while the network is down goes out on the next drain', async () => {
  const { w, owner, joiner, info } = await setup()
  w.mailbox.online = false
  assert.strictEqual((await joiner.service.join(info.link)).status, 'requested')
  w.mailbox.online = true
  await joiner.service.maintainJoinRequests()
  await owner.service.drainOwnerMailboxes()
  assert.strictEqual(owner.admitCalls.length, 1)
})

test('approval mode holds requests for the owner, who can approve', async () => {
  const { owner, joiner, groupId, info } = await setup({ linkOptions: { approval: 'owner' } })
  const { linkId } = await joiner.service.join(info.link)
  await owner.service.drainOwnerMailboxes()
  assert.strictEqual(owner.admitCalls.length, 0)
  const [request] = owner.service.listRequests(groupId)
  assert.strictEqual(request.joinerKey, joiner.key)
  assert.strictEqual(request.joinerName, 'Ana')
  assert.strictEqual(owner.eventsOf('group_link.request_received').length, 1)

  await joiner.drainOwn()
  assert.strictEqual(joiner.store.joinerRecord(linkId).status, 'pending_approval')

  assert.deepStrictEqual(await owner.service.approve(groupId, joiner.key), { status: 'admitted' })
  assert.strictEqual(joiner.store.joinerRecord(linkId).status, 'joined')
  assert.strictEqual(owner.service.listRequests(groupId).length, 0)
})

test('a declined joiner is told, and the same link then ignores them', async () => {
  const { w, owner, joiner, groupId, info } = await setup({ linkOptions: { approval: 'owner' } })
  const { linkId } = await joiner.service.join(info.link)
  await owner.service.drainOwnerMailboxes()
  await owner.service.decline(groupId, joiner.key)
  await joiner.drainOwn()
  assert.strictEqual(joiner.store.joinerRecord(linkId).status, 'declined')

  // Asking again is dropped without an answer or a new request.
  joiner.service.cancel(linkId)
  await joiner.service.join(info.link)
  await owner.service.drainOwnerMailboxes()
  assert.strictEqual(owner.service.listRequests(groupId).length, 0)
  assert.strictEqual(w.mailbox.count(joiner.key), 0)

  // A reset link starts with a clean slate.
  const fresh = owner.service.resetLink(groupId)
  await joiner.service.join(fresh.link)
  await owner.service.drainOwnerMailboxes()
  assert.strictEqual(owner.service.listRequests(groupId).length, 1)
})

test('a reset link answers inactive, and so does a link that is off', async () => {
  const { owner, joiner, groupId, info } = await setup()
  owner.service.resetLink(groupId)
  const { linkId } = await joiner.service.join(info.link)
  await owner.service.drainOwnerMailboxes()
  await joiner.drainOwn()
  assert.strictEqual(joiner.store.joinerRecord(linkId).status, 'inactive')
  assert.strictEqual(owner.admitCalls.length, 0)

  const other = new Node(joiner.world, 'Ben')
  const current = owner.service.getLink(groupId)
  owner.service.disableLink(groupId)
  const second = await other.service.join(current.link)
  await owner.service.drainOwnerMailboxes()
  await other.drainOwn()
  assert.strictEqual(other.store.joinerRecord(second.linkId).status, 'inactive')
})

test('the owner enforces expiry even when the link hint is stale', async () => {
  const { w, owner, joiner, groupId } = await setup()
  const info = owner.service.updateLink(groupId, { expiresAt: w.clock + 60 * 60 * 1000 })
  const { linkId } = await joiner.service.join(info.link)
  w.clock += 2 * 60 * 60 * 1000
  await owner.service.drainOwnerMailboxes()
  await joiner.drainOwn()
  assert.strictEqual(joiner.store.joinerRecord(linkId).status, 'expired')
  assert.strictEqual(joiner.service.inspect(info.link).status, 'expired')
})

test('join limits and the group limit answer full', async () => {
  const { owner, joiner, info } = await setup({ linkOptions: { maxJoins: 1 } })
  await joiner.service.join(info.link)
  await owner.service.drainOwnerMailboxes()
  const second = new Node(joiner.world, 'Ben')
  const { linkId } = await second.service.join(info.link)
  await owner.service.drainOwnerMailboxes()
  await second.drainOwn()
  assert.strictEqual(second.store.joinerRecord(linkId).status, 'full')

  const big = await setup({ members: 99 })
  const r = await big.joiner.service.join(big.info.link)
  await big.owner.service.drainOwnerMailboxes()
  await big.joiner.drainOwn()
  assert.strictEqual(big.joiner.store.joinerRecord(r.linkId).status, 'full')
})

test('blocked keys and removed members are dropped silently in auto mode', async () => {
  const { w, owner, joiner, groupId, info } = await setup()
  owner.service.setBlockedKeys([joiner.key])
  await joiner.service.join(info.link)
  await owner.service.drainOwnerMailboxes()
  assert.strictEqual(owner.admitCalls.length, 0)
  assert.strictEqual(w.mailbox.count(joiner.key), 0, 'no answer reveals the block')

  const removed = new Node(w, 'Cal')
  owner.service.recordRemoval(groupId, removed.key)
  await removed.service.join(info.link)
  await owner.service.drainOwnerMailboxes()
  assert.strictEqual(owner.admitCalls.length, 0)

  // In approval mode the owner sees them, marked.
  owner.service.updateLink(groupId, { approval: 'owner' })
  const again = new Node(w, 'Cal2')
  owner.service.recordRemoval(groupId, again.key)
  await again.service.join(info.link)
  await owner.service.drainOwnerMailboxes()
  const request = owner.service.listRequests(groupId).find(r => r.joinerKey === again.key)
  assert.strictEqual(request.previouslyRemoved, true)
})

test('a request naming someone else is dropped', async () => {
  const { w, owner, joiner, info } = await setup()
  const victim = keyPair()
  const parsed = gl.parseLink(info.link)
  const forged = gl.createJoinRequest({ secret: parsed.secret, rendezvousPublicKey: parsed.rendezvousPublicKey, joinerKeyPair: joiner.keyPair, joinerName: 'Victim', requestedAt: w.clock })
  forged.joinerKey = hex(victim.publicKey)
  w.mailbox.put(keyPair(), hex(parsed.rendezvousPublicKey), forged)
  await owner.service.drainOwnerMailboxes()
  assert.strictEqual(owner.admitCalls.length, 0)
  assert.strictEqual(w.mailbox.count(hex(parsed.rendezvousPublicKey)), 0, 'dropped, not retried')
})

test('too many joins in an hour switch the link to owner approval', async () => {
  const { w, owner, groupId, info } = await setup()
  for (let i = 0; i < RATE_LIMIT; i++) await new Node(w, 'J' + i).service.join(info.link)
  await owner.service.drainOwnerMailboxes()
  assert.strictEqual(owner.admitCalls.length, RATE_LIMIT)
  const late = new Node(w, 'Late')
  await late.service.join(info.link)
  await owner.service.drainOwnerMailboxes()
  assert.strictEqual(owner.admitCalls.length, RATE_LIMIT)
  assert.deepStrictEqual(owner.eventsOf('group_link.approval_switched'), [{ conversationId: groupId, reason: 'rate' }])
  const link = owner.service.getLink(groupId)
  assert.strictEqual(link.approval, 'owner')
  assert.strictEqual(link.approvalReason, 'rate')
  assert.strictEqual(owner.service.listRequests(groupId).length, 1)
})

test('answers and admissions that do not verify are ignored', async () => {
  const { w, owner, joiner, info } = await setup()
  const { linkId } = await joiner.service.join(info.link)
  const parsed = gl.parseLink(info.link)

  // A result signed by someone else, even if sent from the right key.
  const impostor = keyPair()
  const fake = gl.createJoinResult(impostor, { linkId, joinerKey: joiner.key, status: 'declined' })
  assert.strictEqual(joiner.service.handleJoinResult(fake, hex(parsed.rendezvousPublicKey)), true)
  assert.strictEqual(joiner.store.joinerRecord(linkId).status, 'waiting')

  // A real result must also arrive from the link's key.
  const ownerEntry = owner.store.ownerEntry([...owner.chatStore.conversations.keys()][0])
  const rendezvous = gl.rendezvousKeyPair(b4a.from(ownerEntry.current.rendezvousSeed, 'hex'))
  const real = gl.createJoinResult(rendezvous, { linkId, joinerKey: joiner.key, status: 'declined' })
  joiner.service.handleJoinResult(real, hex(impostor.publicKey))
  assert.strictEqual(joiner.store.joinerRecord(linkId).status, 'waiting')

  // An invite claiming to come through the link, signed by the wrong key.
  const conv = { id: 'x', type: 'group' }
  const forgedInvite = { groupId: '44'.repeat(32), creatorKey: hex(impostor.publicKey), viaLink: { linkId, admitSig: gl.signAdmit(impostor, { linkId, joinerKey: joiner.key, groupId: '44'.repeat(32), creatorKey: hex(impostor.publicKey) }) } }
  assert.strictEqual(joiner.service.onGroupInviteApplied(forgedInvite, conv), false)
  assert.strictEqual(joiner.store.joinerRecord(linkId).status, 'waiting')
  assert.strictEqual(w.mailbox.count(joiner.key), 0)
})

test('opening your own link, or a link you already joined, needs nothing', async () => {
  const { owner, joiner, groupId, info } = await setup()
  const own = await owner.service.join(info.link)
  assert.deepStrictEqual(own, { status: 'already_member', linkId: own.linkId, conversationId: groupId })

  await joiner.service.join(info.link)
  await owner.service.drainOwnerMailboxes()
  const again = await joiner.service.join(info.link)
  assert.strictEqual(again.status, 'already_member')
})

test('waiting requests expire after 30 days', async () => {
  const { w, joiner, info } = await setup()
  const { linkId } = await joiner.service.join(info.link)
  w.clock += JOINER_TTL_MS
  await joiner.service.maintainJoinRequests()
  assert.strictEqual(joiner.store.joinerRecord(linkId).status, 'expired')
  assert.deepStrictEqual(joiner.eventsOf('group_link.join_updated').map(e => e.status), ['expired'])
})

test('only the owner manages the link, and leaving the group answers queued requests', async () => {
  const { w, owner, joiner, groupId, info } = await setup({ linkOptions: { approval: 'owner' } })
  const member = new Node(w, 'Member')
  member.chatStore.conversations.set(groupId, { id: groupId, type: 'group', creatorKey: owner.key, participantIds: [owner.key] })
  assert.throws(() => member.service.enableLink(groupId), e => e.code === 'NOT_GROUP_OWNER')

  const { linkId } = await joiner.service.join(info.link)
  await owner.service.drainOwnerMailboxes()
  await owner.service.onGroupGone(groupId)
  await joiner.drainOwn()
  assert.strictEqual(joiner.store.joinerRecord(linkId).status, 'inactive')
  owner.chatStore.conversations.delete(groupId)
  assert.throws(() => owner.service.getLink(groupId), e => e.code === 'CONVERSATION_NOT_FOUND')
})

test('the name can be left out of the link', async () => {
  const { owner, groupId } = await setup({ linkOptions: { includeName: false } })
  assert.strictEqual(gl.parseLink(owner.service.getLink(groupId).link).nameHint, null)
  const withName = owner.service.updateLink(groupId, { includeName: true })
  assert.strictEqual(gl.parseLink(withName.link).nameHint, 'Hiking Crew')
})

test('state survives a restart', async () => {
  const { owner, joiner, groupId, info } = await setup({ linkOptions: { approval: 'owner' } })
  await joiner.service.join(info.link)
  await owner.service.drainOwnerMailboxes()
  const reloaded = new GroupLinkStore({ filePath: owner.store.filePath })
  assert.strictEqual(reloaded.ownerEntry(groupId).requests.length, 1)
  assert.strictEqual(reloaded.ownerEntry(groupId).current.linkId, gl.inspectLink(info.link).linkId)
  const joinerReloaded = new GroupLinkStore({ filePath: joiner.store.filePath })
  assert.strictEqual(joinerReloaded.joinerRecords().length, 1)
})
