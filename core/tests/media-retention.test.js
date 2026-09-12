/**
 * Relay-side image retention against a real blind-peer server, driven by the
 * real blind-peering client the phones ship, over in-memory Noise streams.
 * Only the DHT is faked.
 *
 * blind-peer resolves from server/, the package that is deployed; run
 * `cd server && npm ci` once to enable this file locally.
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
const { attachMediaRetention, DEFAULTS } = require('../../server/media-retention')

const SERVER_DIR = path.join(__dirname, '../../server')
let BlindPeer = null
try {
  BlindPeer = require(require.resolve('blind-peer', { paths: [SERVER_DIR] }))
} catch (_) {}
const skip = BlindPeer ? false : 'server dependencies are not installed (cd server && npm ci)'

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

async function fixture (t) {
  const blindPeer = await relay(t)
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
  await peering.addCore(cores.messages, { priority: 1, announce: false })
  for (const core of Object.values(cores)) await mirrored(blindPeer, core)
  await age(blindPeer, cores.stale, 8 * DAY)
  return { blindPeer, store, peering, cores }
}

test('a media core silent for a week is cleared; fresh media and message history are not', { skip }, async t => {
  const { blindPeer, cores } = await fixture(t)
  const passes = []
  const retention = attachMediaRetention(blindPeer, { log: line => passes.push(line) })
  t.after(() => retention.close())
  await until(() => passes.length === 1)

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
  assert.equal(messages.priority, 1)
  assert.equal(messages.blocksCleared, 0)
  assert.ok(messages.bytesAllocated > 0)
  assert.match(passes[0], /cleared 1 core\(s\), \d+ byte\(s\)/)
  assert.equal(blindPeer.digest.bytesAllocated, fresh.bytesAllocated + messages.bytesAllocated)
})

test('re-registering a cleared core pulls nothing old, while a block appended later is mirrored', { skip }, async t => {
  const { blindPeer, store, peering, cores } = await fixture(t)
  const retention = attachMediaRetention(blindPeer, {})
  t.after(() => retention.close())
  await until(async () => (await record(blindPeer, cores.stale)).bytesAllocated === 0)

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

test('a pass runs on traffic at most once per interval, and never after close', { skip }, async t => {
  const blindPeer = await relay(t)
  let now = Date.now()
  const passes = []
  const retention = attachMediaRetention(blindPeer, { now: () => now, log: line => passes.push(line) })
  await until(() => passes.length === 1)

  for (let i = 0; i < 20; i++) blindPeer.emit('core-activity')
  await sleep(50)
  assert.equal(passes.length, 1)

  now += DEFAULTS.minIntervalMs
  blindPeer.emit('core-activity')
  await until(() => passes.length === 2)
  blindPeer.emit('core-activity')
  await sleep(50)
  assert.equal(passes.length, 2)

  await retention.close()
  now += DEFAULTS.minIntervalMs
  blindPeer.emit('core-activity')
  await sleep(50)
  assert.equal(passes.length, 2)
  assert.equal(blindPeer.listenerCount('core-activity'), 0)
})

test('attaching before the relay is ready is refused', () => {
  assert.throws(() => attachMediaRetention({ opened: false }), /ready blind peer/)
})
