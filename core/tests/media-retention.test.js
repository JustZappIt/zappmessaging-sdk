/**
 * Relay-side image retention against a real blind-peer server, driven by the
 * real blind-peering client the phones ship, over in-memory Noise streams.
 * Only the DHT is faked.
 *
 * blind-peer resolves from server/, the package that is deployed; this file
 * fails rather than skips without it, since `npm test` is the only gate.
 */

const { test } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const EventEmitter = require('node:events')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const Corestore = require('corestore')
const BlindPeering = require('blind-peering')
const NoiseSecretStream = require('@hyperswarm/secret-stream')
const { Duplex } = require('streamx')
const { attachMediaRetention, DEFAULTS, MESSAGE_CORE_PRIORITY } = require('../../server/media-retention')

const SERVER_DIR = path.join(__dirname, '../../server')
let BlindPeer
try {
  BlindPeer = require(require.resolve('blind-peer', { paths: [SERVER_DIR] }))
} catch (_) {
  throw new Error('media-retention tests need the server dependencies: cd server && npm ci')
}

const DAY = 24 * 60 * 60 * 1000
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))

function temporary (t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'zapp-retention-'))
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }))
  return dir
}

async function until (condition, ms = 10000) {
  const deadline = Date.now() + ms
  while (!(await condition())) {
    if (Date.now() > deadline) throw new Error('timed out waiting')
    await sleep(20)
  }
}

function duplexPair () {
  const a = new Duplex({ write (data, cb) { b.push(data); cb() }, final (cb) { b.push(null); cb() } })
  const b = new Duplex({ write (data, cb) { a.push(data); cb() }, final (cb) { a.push(null); cb() } })
  return [a, b]
}

class FakeSwarm extends EventEmitter {
  constructor () {
    super()
    this.keyPair = crypto.keyPair()
    this.dht = { destroyed: false }
  }

  join () { return { flushed: async () => {}, destroy: async () => {} } }
  listen () {}
  async destroy () {}
}

// A DHT whose connect() hands the relay the server half of a Noise pair.
function fakeDht (blindPeer, keyPair) {
  const dht = new EventEmitter()
  dht.destroyed = false
  dht.connect = (remotePublicKey) => {
    const [a, b] = duplexPair()
    const client = new NoiseSecretStream(true, a, { keyPair })
    const server = new NoiseSecretStream(false, b, { keyPair: blindPeer.swarm.keyPair })
    setImmediate(() => blindPeer._onconnection(server))
    return client
  }
  return dht
}

async function relay (t) {
  const blindPeer = new BlindPeer(temporary(t), { swarm: new FakeSwarm() })
  blindPeer.on('flush-error', error => { throw error })
  await blindPeer.ready()
  t.after(() => blindPeer.close())
  return blindPeer
}

function client (t, blindPeer, store) {
  const keyPair = crypto.keyPair()
  const peering = new BlindPeering(fakeDht(blindPeer, keyPair), store, { keys: [blindPeer.swarm.keyPair.publicKey], pick: 1 })
  peering.keyPair = keyPair
  t.after(() => peering.close())
  return peering
}

const record = (blindPeer, core) => blindPeer.db.getCoreRecord(core.key)

async function mirrored (blindPeer, core) {
  await until(async () => {
    await blindPeer.flush()
    const r = await record(blindPeer, core)
    return r && r.length === core.length && r.bytesAllocated > 0
  })
  return record(blindPeer, core)
}

// Backdate a record's newest-block time the way a week of silence would.
async function age (blindPeer, core, ms) {
  const tx = blindPeer.db.db.transaction()
  const r = await tx.get('@blind-peer/cores', { key: core.key })
  r.updated -= ms
  await tx.insert('@blind-peer/cores', r)
  await tx.flush()
}

// The relay's records are shared by everyone who registers a core, so the
// same three cores stand in for every client generation:
//   stale      an image core nobody has added to for a week
//   fresh      an image core added to today
//   messages   a message log; its owner registers it the way every release
//              has, announce requested, and here at the priority an old
//              build sends
// With retention attached, it is attached before any core exists, the way
// the launcher attaches it before listen(); runPass() then drives a sweep.
async function fixture (t, { retention = true } = {}) {
  const blindPeer = await relay(t)
  const stateFile = path.join(temporary(t), 'state.json')
  const lines = []
  const clock = { now: Date.now() }
  const attach = () => attachMediaRetention(blindPeer, { stateFile, now: () => clock.now, log: line => lines.push(line) })
  const handle = retention ? await attach() : null
  if (handle) t.after(() => handle.close())
  const runPass = async () => {
    const before = passes(lines).length
    clock.now += DEFAULTS.minIntervalMs
    blindPeer.emit('core-activity')
    await until(() => passes(lines).length === before + 1)
    return passes(lines)[before]
  }
  const store = new Corestore(temporary(t))
  t.after(() => store.close())
  const peering = client(t, blindPeer, store)
  const encryptionKey = b4a.alloc(32, 7)
  const cores = {
    stale: store.get({ name: 'zapp-media-stale', encryptionKey, valueEncoding: 'binary' }),
    fresh: store.get({ name: 'zapp-media-fresh', encryptionKey, valueEncoding: 'binary' }),
    messages: store.get({ name: 'zapp-local-stale', encryptionKey, valueEncoding: 'json' })
  }
  await cores.stale.append([crypto.randomBytes(1000), crypto.randomBytes(1000)])
  await cores.fresh.append(crypto.randomBytes(1000))
  await cores.messages.append({ id: 'm1', content: 'hello' })
  await peering.addCore(cores.stale, { priority: 0, announce: false })
  await peering.addCore(cores.fresh, { priority: 0, announce: false })
  await peering.addCore(cores.messages, { priority: 0, announce: true })
  for (const core of Object.values(cores)) await mirrored(blindPeer, core)
  await age(blindPeer, cores.stale, 8 * DAY)
  await age(blindPeer, cores.messages, 8 * DAY)
  return { blindPeer, store, peering, cores, stateFile, lines, clock, attach, runPass }
}

function passes (lines) {
  return lines.filter(line => line.startsWith('media retention pass'))
}

test('a media core silent for a week is cleared; fresh media and message history are not', async t => {
  const { blindPeer, cores, runPass } = await fixture(t)
  const pass = await runPass()

  const stale = await record(blindPeer, cores.stale)
  assert.equal(stale.blocksCleared, cores.stale.length)
  assert.equal(stale.bytesAllocated, 0)
  assert.equal(stale.priority, 0)
  const onRelay = blindPeer.store.get({ key: cores.stale.key })
  assert.equal(await onRelay.has(0, cores.stale.length), false, 'the blocks are gone from the relay')
  await onRelay.close()

  const fresh = await record(blindPeer, cores.fresh)
  assert.equal(fresh.blocksCleared, 0)
  assert.ok(fresh.bytesAllocated > 0)
  const messages = await record(blindPeer, cores.messages)
  assert.equal(messages.priority, MESSAGE_CORE_PRIORITY, 'an old build\'s message core was promoted when its owner registered it')
  assert.equal(messages.blocksCleared, 0)
  assert.ok(messages.bytesAllocated > 0)
  assert.match(pass, /cleared 1 core\(s\), \d+ byte\(s\)/)
  assert.equal(blindPeer.digest.bytesAllocated, fresh.bytesAllocated + messages.bytesAllocated)
})

test('records that predate the module are protected once, and only once', async t => {
  const { blindPeer, store, peering, cores, stateFile, lines, attach, runPass } = await fixture(t, { retention: false })
  for (const core of Object.values(cores)) assert.equal((await record(blindPeer, core)).priority, 0)

  const first = await attach()
  assert.match(lines[0], /protected 3 existing core\(s\)/)
  await until(() => passes(lines).length === 1)
  assert.match(passes(lines)[0], /cleared 0 core\(s\)/)
  for (const core of Object.values(cores)) {
    const r = await record(blindPeer, core)
    assert.equal(r.priority, MESSAGE_CORE_PRIORITY)
    assert.equal(r.blocksCleared, 0)
    assert.ok(r.bytesAllocated > 0)
  }
  assert.ok(JSON.parse(fs.readFileSync(stateFile, 'utf8')).promotedAt)
  await first.close()

  // An image registered after the marker is not swept up by a later restart.
  const image = store.get({ name: 'zapp-media-later', encryptionKey: b4a.alloc(32, 7), valueEncoding: 'binary' })
  await image.append(crypto.randomBytes(1000))
  await peering.addCore(image, { priority: 0, announce: false })
  await mirrored(blindPeer, image)
  await age(blindPeer, image, 8 * DAY)
  const again = await attach()
  t.after(() => again.close())
  await until(() => passes(lines).length === 2)
  assert.equal(lines.filter(line => line.includes('protected')).length, 1, 'the promotion ran once')
  assert.match(passes(lines)[1], /cleared 1 core\(s\)/)
  assert.equal((await record(blindPeer, image)).bytesAllocated, 0)
  assert.ok((await record(blindPeer, cores.stale)).bytesAllocated > 0, 'protected history stays')
})

test('re-registering a cleared core pulls nothing old, while a block appended later is mirrored', async t => {
  const { blindPeer, store, peering, cores, runPass } = await fixture(t)
  await runPass()
  assert.equal((await record(blindPeer, cores.stale)).bytesAllocated, 0)

  await peering.close()
  const reconnected = client(t, blindPeer, store)
  await reconnected.addCore(cores.stale, { priority: 0, announce: false })
  await sleep(300)
  await blindPeer.flush()
  const afterReregister = await record(blindPeer, cores.stale)
  assert.equal(afterReregister.bytesAllocated, 0, 'nothing is re-downloaded')
  assert.equal(afterReregister.blocksCleared, cores.stale.length)

  await cores.stale.append(crypto.randomBytes(500))
  const pulled = await mirrored(blindPeer, cores.stale)
  assert.equal(pulled.length, 3)
  assert.equal(pulled.blocksCleared, 2)
  assert.ok(pulled.bytesAllocated >= 500 && pulled.bytesAllocated < 1000, 'only the new block is stored')
})

test('a pass runs on traffic at most once per interval, and never after close', async t => {
  const blindPeer = await relay(t)
  let now = Date.now()
  const lines = []
  const retention = await attachMediaRetention(blindPeer, { stateFile: path.join(temporary(t), 'state.json'), now: () => now, log: line => lines.push(line) })
  await until(() => passes(lines).length === 1)

  for (let i = 0; i < 20; i++) blindPeer.emit('core-activity')
  await sleep(50)
  assert.equal(passes(lines).length, 1)

  now += DEFAULTS.minIntervalMs
  blindPeer.emit('core-activity')
  await until(() => passes(lines).length === 2)
  blindPeer.emit('core-activity')
  await sleep(50)
  assert.equal(passes(lines).length, 2)

  await retention.close()
  now += DEFAULTS.minIntervalMs
  blindPeer.emit('core-activity')
  await sleep(50)
  assert.equal(passes(lines).length, 2)
  for (const event of ['core-activity', 'add-cores-downgrade-announce', 'downgrade-announce']) {
    assert.equal(blindPeer.listenerCount(event), 0, event)
  }
})

test('attaching before the relay is ready is refused', async () => {
  await assert.rejects(attachMediaRetention({ opened: false }), /ready blind peer/)
})
