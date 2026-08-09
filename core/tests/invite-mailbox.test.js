const { test } = require('node:test')
const assert = require('node:assert')
const b4a = require('b4a')
const sodium = require('sodium-universal')
const {
  authBytes,
  createAuthRequest,
  encryptInvite,
  decryptInvite,
  putInvite,
  drainInvites,
  throughHttps,
  MAX_ENVELOPE_BYTES,
  MAX_DRAIN_PAGES
} = require('../lib/invite-mailbox')

function identity () {
  const publicKey = b4a.alloc(sodium.crypto_sign_PUBLICKEYBYTES)
  const secretKey = b4a.alloc(sodium.crypto_sign_SECRETKEYBYTES)
  sodium.crypto_sign_keypair(publicKey, secretKey)
  return { publicKey, secretKey }
}

function verifyAuth (request) {
  return sodium.crypto_sign_verify_detached(
    b4a.from(request.signature, 'hex'),
    authBytes(request.action, request.identity, request.timestamp, request.nonce, request.ids),
    b4a.from(request.identity, 'hex')
  )
}

const BASE_URL = 'https://mailbox.example/zapp-invite'

test('invite mailbox envelope is encrypted to its recipient and signed by its sender', () => {
  const sender = identity()
  const recipient = identity()
  const invite = { type: 'direct_invite', senderKey: b4a.toString(sender.publicKey, 'hex') }
  const envelope = encryptInvite(invite, sender, b4a.toString(recipient.publicKey, 'hex'))

  const decoded = decryptInvite(envelope, recipient)
  assert.deepStrictEqual(decoded.invite, invite)
  assert.strictEqual(decoded.senderKeyHex, b4a.toString(sender.publicKey, 'hex'))

  const tampered = JSON.parse(envelope)
  tampered.ciphertext = (tampered.ciphertext[0] === '0' ? '1' : '0') + tampered.ciphertext.slice(1)
  assert.throws(() => decryptInvite(JSON.stringify(tampered), recipient), /verification failed/)
})

test('a real bootstrap invite leaves the envelope cap mostly unused', () => {
  const sender = identity()
  const recipient = identity()
  const recipientHex = b4a.toString(recipient.publicKey, 'hex')
  const envelope = encryptInvite({
    type: 'direct_invite',
    conversationId: 'a'.repeat(64),
    senderKey: b4a.toString(sender.publicKey, 'hex'),
    senderDisplayName: 'a reasonably long display name',
    localCoreKey: 'b'.repeat(64)
  }, sender, recipientHex)

  // Headroom is what an attacker gets to spend filling someone else's mailbox,
  // so the cap sits close to a real invite rather than being generous.
  assert.ok(b4a.byteLength(envelope) < MAX_ENVELOPE_BYTES / 2,
    'a real invite must fit well inside the cap')
  assert.throws(
    () => encryptInvite({ padding: 'x'.repeat(MAX_ENVELOPE_BYTES) }, sender, recipientHex),
    /envelope too large/
  )
})

test('HTTPS put deposits an envelope and needs no request signature', async () => {
  const sender = identity()
  const recipient = identity()
  const recipientHex = b4a.toString(recipient.publicKey, 'hex')
  const calls = []

  await throughHttps(BASE_URL, sender,
    request => putInvite(request, sender, recipientHex, { type: 'direct_invite' }),
    { postJson: async (url, body) => { calls.push({ url, body }); return {} } }
  )

  assert.strictEqual(calls.length, 1)
  assert.strictEqual(calls[0].url, BASE_URL + '/put')
  assert.strictEqual(calls[0].body.recipient, recipientHex)
  assert.strictEqual(decryptInvite(calls[0].body.envelope, recipient).invite.type, 'direct_invite')
  assert.strictEqual(calls[0].body.signature, undefined, 'put carries no request signature')
})

test('HTTPS list and ack are signed by the receiving identity', async () => {
  const sender = identity()
  const recipient = identity()
  const envelope = encryptInvite(
    { type: 'direct_invite', senderKey: b4a.toString(sender.publicKey, 'hex') },
    sender,
    b4a.toString(recipient.publicKey, 'hex')
  )
  const calls = []

  const entries = await throughHttps(BASE_URL, recipient,
    request => drainInvites(request, recipient),
    {
      postJson: async (url, body) => {
        calls.push({ url, body })
        return url.endsWith('/list')
          ? { entries: [{ id: 'ab'.repeat(16), envelope }], more: false }
          : { ok: true }
      }
    }
  )

  assert.strictEqual(entries.length, 1)
  assert.strictEqual(entries[0].invite.type, 'direct_invite')
  assert.deepStrictEqual(calls.map(call => call.url), [BASE_URL + '/list', BASE_URL + '/ack'])
  assert.ok(verifyAuth(calls[0].body))
  assert.ok(verifyAuth(calls[1].body))
  assert.deepStrictEqual(calls[1].body.ids, ['ab'.repeat(16)])
})

test('mailbox authentication signature does not survive request mutation', () => {
  const receiver = identity()
  const request = createAuthRequest('ack', receiver, ['cd'.repeat(16)])
  assert.ok(verifyAuth(request))
  request.ids = ['ef'.repeat(16)]
  assert.strictEqual(verifyAuth(request), false)
})

// A mailbox anyone can deposit into can be flooded. The server pages listings so
// it stays drainable; the client has to keep asking, and has to stop.
test('a paged mailbox is drained across rounds and acknowledged each round', async () => {
  const recipient = identity()
  const sender = identity()
  const envelope = encryptInvite({ type: 'direct_invite' }, sender,
    b4a.toString(recipient.publicKey, 'hex'))
  const pages = [
    { ids: ['11'.repeat(16), '22'.repeat(16)], more: true },
    { ids: ['33'.repeat(16)], more: false }
  ]
  const acked = []
  let listCalls = 0

  const entries = await throughHttps(BASE_URL, recipient,
    request => drainInvites(request, recipient),
    {
      postJson: async (url, body) => {
        if (!url.endsWith('/list')) { acked.push(body.ids); return { ok: true } }
        const page = pages[listCalls++]
        return { entries: page.ids.map(id => ({ id, envelope })), more: page.more }
      }
    }
  )

  assert.strictEqual(entries.length, 3, 'every page is collected')
  assert.deepStrictEqual(acked, [
    ['11'.repeat(16), '22'.repeat(16)],
    ['33'.repeat(16)]
  ], 'each page is acknowledged before the next is requested')
})

test('draining stops even when the server always claims more', async () => {
  const recipient = identity()
  const sender = identity()
  const envelope = encryptInvite({ type: 'direct_invite' }, sender,
    b4a.toString(recipient.publicKey, 'hex'))
  let listCalls = 0

  await throughHttps(BASE_URL, recipient,
    request => drainInvites(request, recipient),
    {
      postJson: async (url) => {
        if (!url.endsWith('/list')) return { ok: true }
        listCalls++
        return { entries: [{ id: '44'.repeat(16), envelope }], more: true }
      }
    }
  )

  assert.strictEqual(listCalls, MAX_DRAIN_PAGES, 'the drain loop is bounded')
})

test('undecryptable entries are acknowledged so they cannot block later invites', async () => {
  const recipient = identity()
  const acked = []

  const entries = await throughHttps(BASE_URL, recipient,
    request => drainInvites(request, recipient),
    {
      postJson: async (url, body) => {
        if (!url.endsWith('/list')) { acked.push(...body.ids); return { ok: true } }
        return { entries: [{ id: '55'.repeat(16), envelope: 'not an envelope' }], more: false }
      }
    }
  )

  assert.strictEqual(entries[0].invalid, true)
  assert.deepStrictEqual(acked, ['55'.repeat(16)],
    'junk is cleared rather than left to accumulate')
})

test('HTTPS transport refuses to run without a host to carry it', async () => {
  await assert.rejects(
    () => throughHttps(BASE_URL, identity(), async () => {}, {}),
    /requires a host transport/
  )
})

test('HTTPS transport refuses a non-HTTPS mailbox URL', async () => {
  await assert.rejects(
    () => throughHttps('http://mailbox.example', identity(), async () => {}, {
      postJson: async () => ({})
    }),
    /must use HTTPS/
  )
})

// A client can reach a blind peer that has not been upgraded yet. That server
// replies to list with a bare array, and reading the paged shape off it would
// silently report an empty mailbox instead of delivering the invite.
test('a listing from a server without paging is still drained', async () => {
  const recipient = identity()
  const sender = identity()
  const envelope = encryptInvite({ type: 'direct_invite' }, sender,
    b4a.toString(recipient.publicKey, 'hex'))
  const acked = []

  const entries = await throughHttps(BASE_URL, recipient,
    request => drainInvites(request, recipient),
    {
      postJson: async (url, body) => {
        if (!url.endsWith('/list')) { acked.push(...body.ids); return { ok: true } }
        return [{ id: '66'.repeat(16), envelope }]
      }
    }
  )

  assert.strictEqual(entries.length, 1, 'the legacy array shape is understood')
  assert.strictEqual(entries[0].invite.type, 'direct_invite')
  assert.deepStrictEqual(acked, ['66'.repeat(16)])
})

// Acknowledging is a delete, and the mailbox holds the only copy of a bootstrap
// invite. If the ack lands but its response is lost, the invite must already
// have been applied, or it is gone with no way to ask for it again.
test('an invite is delivered before it is acknowledged', async () => {
  const recipient = identity()
  const sender = identity()
  const envelope = encryptInvite({ type: 'direct_invite' }, sender,
    b4a.toString(recipient.publicKey, 'hex'))
  const order = []

  await assert.rejects(() => throughHttps(BASE_URL, recipient,
    request => drainInvites(request, recipient, async () => { order.push('deliver'); return true }),
    {
      postJson: async (url) => {
        if (url.endsWith('/list')) return { entries: [{ id: '77'.repeat(16), envelope }], more: false }
        order.push('ack')
        throw new Error('response lost')
      }
    }
  ))

  assert.deepStrictEqual(order, ['deliver', 'ack'], 'delivery precedes the delete')
})

test('an entry that cannot be applied is left in the mailbox', async () => {
  const recipient = identity()
  const sender = identity()
  const envelope = encryptInvite({ type: 'direct_invite' }, sender,
    b4a.toString(recipient.publicKey, 'hex'))
  const acked = []

  await throughHttps(BASE_URL, recipient,
    request => drainInvites(request, recipient, async () => false),
    {
      postJson: async (url, body) => {
        if (!url.endsWith('/list')) { acked.push(...body.ids); return { ok: true } }
        return { entries: [{ id: '88'.repeat(16), envelope }], more: false }
      }
    }
  )

  assert.deepStrictEqual(acked, [], 'nothing is deleted, so a later drain retries it')
})

test('junk is acknowledged even though it is never delivered', async () => {
  const recipient = identity()
  const acked = []
  let delivers = 0

  await throughHttps(BASE_URL, recipient,
    request => drainInvites(request, recipient, async () => { delivers++; return true }),
    {
      postJson: async (url, body) => {
        if (!url.endsWith('/list')) { acked.push(...body.ids); return { ok: true } }
        return { entries: [{ id: '99'.repeat(16), envelope: 'junk' }], more: false }
      }
    }
  )

  assert.strictEqual(delivers, 0, 'undecryptable entries are not handed to the app')
  assert.deepStrictEqual(acked, ['99'.repeat(16)], 'but they are still cleared')
})

// A group invite carries every participant's key, so the cap has to hold a
// realistic group. Sizing it for DMs pushed larger groups onto the DHT path,
// which is the path that does not work for a firewalled peer.
test('the envelope cap holds a realistically large group invite', () => {
  const me = identity()
  const you = identity()
  const key = () => b4a.toString(require('crypto').randomBytes(32), 'hex')
  const invite = participants => ({
    type: 'group_invite',
    conversationId: 'a'.repeat(64),
    groupId: 'b'.repeat(64),
    groupName: 'Team',
    senderKey: b4a.toString(me.publicKey, 'hex'),
    senderDisplayName: 'a reasonably long display name',
    localCoreKey: 'c'.repeat(64),
    participants: Array.from({ length: participants }, key)
  })

  assert.doesNotThrow(() => encryptInvite(invite(100), me, b4a.toString(you.publicKey, 'hex')),
    'a 100-member group must still fit')
})
