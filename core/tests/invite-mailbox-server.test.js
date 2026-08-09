const { test } = require('node:test')
const assert = require('node:assert')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const b4a = require('b4a')
const sodium = require('sodium-universal')
const { MailboxStore, MAX_LIST_BYTES } = require('../../server/invite-mailbox')
const { encryptInvite, MAX_ENVELOPE_BYTES } = require('../lib/invite-mailbox')

let directoryCount = 0

function store (opts = {}) {
  const directory = path.join(
    fs.mkdtempSync(path.join(os.tmpdir(), 'zapp-mailbox-')),
    'mailboxes-' + (++directoryCount)
  )
  return new MailboxStore({ directory, ...opts })
}

function identity () {
  const publicKey = b4a.alloc(sodium.crypto_sign_PUBLICKEYBYTES)
  const secretKey = b4a.alloc(sodium.crypto_sign_SECRETKEYBYTES)
  sodium.crypto_sign_keypair(publicKey, secretKey)
  return { publicKey, secretKey, hex: b4a.toString(publicKey, 'hex') }
}

function envelopeFor (sender, recipient, invite = { type: 'direct_invite' }) {
  return encryptInvite(invite, sender, recipient.hex)
}

test('a stored envelope round-trips and reports its verified sender', () => {
  const mailbox = store()
  const sender = identity()
  const recipient = identity()

  assert.strictEqual(mailbox.put(recipient.hex, envelopeFor(sender, recipient), null), sender.hex)
  const { entries, more } = mailbox.list(recipient.hex)
  assert.strictEqual(entries.length, 1)
  assert.strictEqual(more, false)
  assert.strictEqual(mailbox.ack(recipient.hex, [entries[0].id]), 1)
  assert.deepStrictEqual(mailbox.list(recipient.hex).entries, [])
})

// The HTTP transport cannot authenticate a depositor, so the envelope signature
// is the only thing standing between the mailbox and arbitrary junk.
test('an envelope with a broken signature is refused', () => {
  const mailbox = store()
  const sender = identity()
  const recipient = identity()
  const tampered = JSON.parse(envelopeFor(sender, recipient))
  tampered.ciphertext = (tampered.ciphertext[0] === '0' ? '1' : '0') + tampered.ciphertext.slice(1)

  assert.throws(() => mailbox.put(recipient.hex, JSON.stringify(tampered), null),
    /signature verification failed/)
  assert.deepStrictEqual(mailbox.list(recipient.hex).entries, [])
})

test('an envelope addressed elsewhere cannot be filed under another recipient', () => {
  const mailbox = store()
  const sender = identity()
  const recipient = identity()
  const bystander = identity()

  assert.throws(() => mailbox.put(bystander.hex, envelopeFor(sender, recipient), null),
    /signature verification failed/)
})

test('a connection identity may not deposit under a different sender key', () => {
  const mailbox = store()
  const sender = identity()
  const recipient = identity()
  const impostor = identity()

  assert.throws(() => mailbox.put(recipient.hex, envelopeFor(sender, recipient), impostor.hex),
    /malformed/)
  assert.doesNotThrow(() => mailbox.put(recipient.hex, envelopeFor(sender, recipient), sender.hex))
})

test('one sender cannot consume a recipient\'s whole quota', () => {
  const mailbox = store({ maxPerSender: 2, maxPerRecipient: 10 })
  const sender = identity()
  const recipient = identity()

  mailbox.put(recipient.hex, envelopeFor(sender, recipient), null)
  mailbox.put(recipient.hex, envelopeFor(sender, recipient), null)
  assert.throws(() => mailbox.put(recipient.hex, envelopeFor(sender, recipient), null),
    /sender quota exceeded/)

  // A different sender still gets through, so one flooder cannot lock out
  // everyone else who wants to reach this recipient.
  assert.doesNotThrow(() => mailbox.put(recipient.hex, envelopeFor(identity(), recipient), null))
})

test('recipient and global ceilings are enforced', () => {
  const mailbox = store({ maxPerRecipient: 1, maxPerSender: 5 })
  const recipient = identity()
  mailbox.put(recipient.hex, envelopeFor(identity(), recipient), null)
  assert.throws(() => mailbox.put(recipient.hex, envelopeFor(identity(), recipient), null),
    /recipient is full/)

  const capped = store({ maxTotal: 1 })
  const first = identity()
  capped.put(first.hex, envelopeFor(identity(), first), null)
  const second = identity()
  assert.throws(() => capped.put(second.hex, envelopeFor(identity(), second), null),
    /mailbox is full/)
})

// A listing the client refuses to download is a listing it can never
// acknowledge, which would leave a flooded mailbox permanently unreadable.
test('a flooded mailbox is listed in bounded pages rather than one huge reply', () => {
  // The byte budget is what normally stops a flood this large; lift it here so
  // this test exercises paging rather than re-testing the budget.
  const mailbox = store({ maxPerRecipient: 200, maxPerSender: 200, maxBytesPerRecipient: 8 * 1024 * 1024 })
  const recipient = identity()
  // Ciphertext is hex-encoded on the wire, so plaintext costs roughly double.
  // Aim just under the cap: this is the worst case an attacker can deposit.
  const padding = 'x'.repeat(MAX_ENVELOPE_BYTES / 2 - 300)
  const biggest = envelopeFor(identity(), recipient, { padding })
  assert.ok(b4a.byteLength(biggest) > MAX_ENVELOPE_BYTES * 0.8,
    'the fixture must actually approach the cap')
  for (let i = 0; i < 120; i++) {
    mailbox.put(recipient.hex, envelopeFor(identity(), recipient, { padding }), null)
  }

  const page = mailbox.list(recipient.hex)
  const pageBytes = JSON.stringify(page.entries).length
  assert.ok(pageBytes <= MAX_LIST_BYTES, 'a page stays inside the budget, got ' + pageBytes)
  assert.strictEqual(page.more, true, 'the client is told to come back')

  // Acknowledging a page makes room, so repeated drains converge.
  const before = page.entries.length
  mailbox.ack(recipient.hex, page.entries.map(entry => entry.id))
  assert.ok(mailbox.list(recipient.hex).entries.length > 0)
  assert.ok(before > 0, 'progress is possible on every round')
})

test('entries past their TTL disappear', () => {
  const mailbox = store({ ttlMs: 1 })
  const recipient = identity()
  mailbox.put(recipient.hex, envelopeFor(identity(), recipient), null)

  const stale = path.join(mailbox.directory, recipient.hex + '.json')
  const entries = JSON.parse(fs.readFileSync(stale, 'utf8'))
  entries[0].createdAt = Date.now() - 60_000
  fs.writeFileSync(stale, JSON.stringify(entries))

  assert.deepStrictEqual(mailbox.list(recipient.hex).entries, [])
})

// Each recipient is its own file, so a deposit never rewrites unrelated
// mailboxes. That is what keeps a large mailbox from turning every put into a
// multi-megabyte synchronous write.
test('each recipient is stored independently', () => {
  const mailbox = store()
  const first = identity()
  const second = identity()
  mailbox.put(first.hex, envelopeFor(identity(), first), null)
  mailbox.put(second.hex, envelopeFor(identity(), second), null)

  assert.deepStrictEqual(
    fs.readdirSync(mailbox.directory).sort(),
    [first.hex + '.json', second.hex + '.json'].sort()
  )
  mailbox.ack(first.hex, mailbox.list(first.hex).entries.map(entry => entry.id))
  assert.strictEqual(mailbox.list(second.hex).entries.length, 1, 'the other mailbox is untouched')
})

test('a single-file mailbox from an earlier build is imported once', () => {
  const parent = fs.mkdtempSync(path.join(os.tmpdir(), 'zapp-mailbox-legacy-'))
  const directory = path.join(parent, 'invite-mailbox')
  const recipient = identity()
  const legacyPath = directory + '.json'
  fs.writeFileSync(legacyPath, JSON.stringify({
    [recipient.hex]: [{
      id: 'ab'.repeat(16),
      sender: identity().hex,
      envelope: envelopeFor(identity(), recipient),
      createdAt: Date.now()
    }]
  }))

  const mailbox = new MailboxStore({ directory })
  assert.strictEqual(mailbox.list(recipient.hex).entries.length, 1, 'legacy invites survive')
  assert.strictEqual(fs.existsSync(legacyPath), false, 'and the old file is retired')
})

test('an absolute storage directory is required', () => {
  assert.throws(() => new MailboxStore({ directory: 'relative/path' }), /must be absolute/)
})

// A recipient key becomes a filename, so the format is enforced at the one place
// that builds the path rather than trusted from each caller.
test('a recipient that is not a hex key can never reach the filesystem', () => {
  const mailbox = store()
  assert.throws(() => mailbox._pathFor('../../etc/passwd'), /32-byte hex key/)
  assert.deepStrictEqual(mailbox.list('../../etc/passwd').entries, [],
    'reads of a bogus recipient are simply empty')
  assert.throws(() => mailbox.put('../../etc/passwd', 'x', null))

  const inside = path.resolve(mailbox.directory)
  const legitimate = mailbox._pathFor('a'.repeat(64))
  assert.ok(legitimate.startsWith(inside + path.sep), 'paths stay inside the mailbox directory')
})

// An envelope is bound to the mailbox it is filed under, so a recipient of the
// wrong shape must fail rather than be silently coerced.
test('an envelope for a malformed recipient is rejected', () => {
  const mailbox = store()
  const sender = identity()
  const recipient = identity()
  assert.throws(() => mailbox.put('abc', envelopeFor(sender, recipient), null))
})

test('the public HTTP endpoint is rate limited in aggregate', async () => {
  const { attachInviteMailbox } = require('../../server/invite-mailbox')
  const EventEmitter = require('node:events')
  const swarm = new EventEmitter()
  const mailbox = attachInviteMailbox({ swarm }, {
    directory: path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'zapp-rl-')), 'mailboxes'),
    maxHttpRequestsPerMinute: 2
  })
  await mailbox.listenHttp(0, '127.0.0.1')
  const port = mailbox.httpAddress().port

  const post = async () => {
    const res = await fetch('http://127.0.0.1:' + port + '/put', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: '{}'
    })
    return res.status
  }

  assert.strictEqual(await post(), 400, 'a bad body is rejected on its merits')
  assert.strictEqual(await post(), 400)
  assert.strictEqual(await post(), 429, 'the third request in the window is refused')
  await mailbox.close()
})

// _total is only adjusted by a write, so an in-memory-only TTL filter left
// expired entries counted against the global ceiling. Once it filled, every
// deposit for every recipient failed until the process restarted.
test('expired entries release the global quota', () => {
  const mailbox = store({ maxTotal: 1 })
  const first = identity()
  mailbox.put(first.hex, envelopeFor(identity(), first), null)

  const stored = path.join(mailbox.directory, first.hex + '.json')
  const entries = JSON.parse(fs.readFileSync(stored, 'utf8'))
  entries[0].createdAt = Date.now() - 60 * 24 * 60 * 60 * 1000
  fs.writeFileSync(stored, JSON.stringify(entries))

  assert.deepStrictEqual(mailbox.list(first.hex).entries, [])
  assert.strictEqual(mailbox._total, 0, 'the counter follows the prune')

  const second = identity()
  assert.doesNotThrow(() => mailbox.put(second.hex, envelopeFor(identity(), second), null),
    'an unrelated recipient is not blocked by someone else\'s expired mail')
})

test('the TTL prune is persisted, not just filtered in memory', () => {
  const mailbox = store()
  const recipient = identity()
  mailbox.put(recipient.hex, envelopeFor(identity(), recipient), null)

  const stored = path.join(mailbox.directory, recipient.hex + '.json')
  const entries = JSON.parse(fs.readFileSync(stored, 'utf8'))
  entries[0].createdAt = Date.now() - 60 * 24 * 60 * 60 * 1000
  fs.writeFileSync(stored, JSON.stringify(entries))

  mailbox.list(recipient.hex)
  assert.strictEqual(fs.existsSync(stored), false, 'the emptied file is removed')
})

// Bounding entries alone let a raised envelope cap multiply what a flood costs.
// Bytes are the quantity that matters, so one large invite costs what several
// small ones would.
test('a recipient has a byte budget, not just an entry count', () => {
  const mailbox = store({ maxBytesPerRecipient: 8 * 1024, maxPerRecipient: 100, maxPerSender: 100 })
  const recipient = identity()
  const padding = 'x'.repeat(1500)

  let stored = 0
  for (;;) {
    try {
      mailbox.put(recipient.hex, envelopeFor(identity(), recipient, { padding }), null)
      stored++
    } catch (error) {
      assert.match(error.message, /byte quota exceeded/)
      break
    }
    assert.ok(stored < 100, 'the byte budget must bite before the entry count')
  }

  const bytes = mailbox.list(recipient.hex).entries
    .reduce((total, entry) => total + entry.envelope.length, 0)
  assert.ok(bytes <= 8 * 1024, 'stored bytes stay inside the budget, got ' + bytes)
  assert.ok(stored > 0, 'and legitimate invites still fit')
})
