/**
 * Remote-core cursor tests. Uses fake cores so failure, retry and lifecycle
 * windows are deterministic without a Corestore or network.
 */

const { test } = require('node:test')
const assert = require('node:assert')
const EventEmitter = require('node:events')
const path = require('node:path')
const fs = require('node:fs')
const { HypercoreManager } = require('../lib/hypercore-manager')
const { getDataDir } = require('../lib/storage')

class FakeCore extends EventEmitter {
  constructor (blocks = [], { fork = 0 } = {}) {
    super()
    this.blocks = blocks
    this.fork = fork
    this.getFailures = new Map()
    this.closed = false
    this.setMaxListeners(0)
  }

  get length () { return this.blocks.length }

  failNextGet (index, err) {
    const failures = this.getFailures.get(index) || []
    failures.push(err)
    this.getFailures.set(index, failures)
  }

  async get (index) {
    const failures = this.getFailures.get(index)
    if (failures && failures.length > 0) throw failures.shift()
    return this.blocks[index]
  }

  append (block) {
    this.blocks.push(block)
    this.emit('append')
  }

  async close () { this.closed = true }
}

class BlockingCore extends FakeCore {
  async get () {
    return new Promise((resolve, reject) => { this.pendingGet = { resolve, reject } })
  }

  async close () {
    this.closed = true
    if (this.pendingGet) {
      const err = new Error('cannot get on a closed session')
      err.code = 'SESSION_CLOSED'
      this.pendingGet.reject(err)
      this.pendingGet = null
    }
  }
}

function managerWithSink (onIngest, onComplete) {
  const manager = new HypercoreManager()
  manager.saveCoreKeyIndex = () => {}
  manager._drainRetryBaseMs = 60000
  manager._drainRetryMaxMs = 60000
  const ingested = []
  const completions = []
  manager.setRemoteMessageSink(async (conversationId, peerKeyHex, message, index) => {
    ingested.push({ conversationId, peerKeyHex, message, index })
    if (onIngest) return onIngest({ conversationId, peerKeyHex, message, index })
    return null
  })
  manager.setRemoteDrainCompleteSink(async (conversationId, peerKeyHex, result) => {
    completions.push({ conversationId, peerKeyHex, result })
    if (onComplete) await onComplete({ conversationId, peerKeyHex, result })
  })
  return { manager, ingested, completions }
}

async function waitFor (predicate, timeoutMs = 500) {
  const deadline = Date.now() + timeoutMs
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error('condition timed out')
    await new Promise(resolve => setTimeout(resolve, 5))
  }
}

const CONV = 'dm_' + 'ab'.repeat(16)
const PEER = 'bb'.repeat(32)
const CORE_A = 'ca'.repeat(32)
const CORE_B = 'cb'.repeat(32)

test('drain ingests every block in order and commits the batch cursor', async () => {
  const { manager, ingested } = managerWithSink()
  const core = new FakeCore([{ id: 'a' }, { id: 'b' }, { id: 'c' }])

  await manager._drainRemoteCore(CONV, PEER, CORE_A, core)

  assert.deepStrictEqual(ingested.map(x => x.message.id), ['a', 'b', 'c'])
  assert.deepStrictEqual(ingested.map(x => x.index), [0, 1, 2])
  assert.strictEqual(manager._getProcessedCursor(CONV, CORE_A, 0), 3)
})

test('a persisted cursor resumes at the first unhandled block', async () => {
  const { manager, ingested } = managerWithSink()
  manager._setProcessedCursor(CONV, CORE_A, 2, 0)
  const core = new FakeCore([{ id: 'a' }, { id: 'b' }, { id: 'c' }])

  await manager._drainRemoteCore(CONV, PEER, CORE_A, core)

  assert.deepStrictEqual(ingested.map(x => x.message.id), ['c'])
  assert.strictEqual(manager._getProcessedCursor(CONV, CORE_A, 0), 3)
})

test('a block stranded on disk before force-stop is recovered on open', async () => {
  const { manager, ingested } = managerWithSink()
  const core = new FakeCore([{ id: 'stranded' }])

  await manager._drainRemoteCore(CONV, PEER, CORE_A, core)

  assert.deepStrictEqual(ingested.map(x => x.message.id), ['stranded'])
  assert.strictEqual(manager._getProcessedCursor(CONV, CORE_A, 0), 1)
})

test('a sink failure holds the whole batch for idempotent replay', async () => {
  let fail = true
  const { manager, ingested } = managerWithSink(async ({ index }) => {
    if (index === 1 && fail) {
      fail = false
      throw new Error('disk full')
    }
  })
  const core = new FakeCore([{ id: 'a' }, { id: 'b' }, { id: 'c' }])

  await manager._drainRemoteCore(CONV, PEER, CORE_A, core)
  assert.deepStrictEqual(ingested.map(x => x.message.id), ['a', 'b'])
  assert.strictEqual(manager._getProcessedCursor(CONV, CORE_A, 0), 0)

  await manager._drainRemoteCore(CONV, PEER, CORE_A, core)
  assert.deepStrictEqual(ingested.map(x => x.message.id), ['a', 'b', 'a', 'b', 'c'])
  assert.strictEqual(manager._getProcessedCursor(CONV, CORE_A, 0), 3)
})

test('a transient core.get failure holds the cursor and retries while idle', async () => {
  const { manager, ingested } = managerWithSink()
  manager._drainRetryBaseMs = 5
  manager._drainRetryMaxMs = 5
  const core = new FakeCore([{ id: 'a' }, { id: 'b' }])
  const err = new Error('temporary storage I/O failure')
  err.code = 'STORAGE_IO'
  core.failNextGet(0, err)

  await manager._drainRemoteCore(CONV, PEER, CORE_A, core)
  assert.strictEqual(manager._getProcessedCursor(CONV, CORE_A, 0), 0)
  await waitFor(() => manager._getProcessedCursor(CONV, CORE_A, 0) === 2)

  assert.deepStrictEqual(ingested.map(x => x.message.id), ['a', 'b'])
})

test('only an explicit decoding error is stepped over', async () => {
  const { manager, ingested } = managerWithSink()
  const core = new FakeCore([{ id: 'a' }, { id: 'bad' }, { id: 'c' }])
  const err = new Error('invalid JSON block')
  err.code = 'DECODING_ERROR'
  core.failNextGet(1, err)

  await manager._drainRemoteCore(CONV, PEER, CORE_A, core)

  assert.deepStrictEqual(ingested.map(x => x.message.id), ['a', 'c'])
  assert.strictEqual(manager._getProcessedCursor(CONV, CORE_A, 0), 3)
})

test('appends after the initial drain are ingested incrementally', async () => {
  const { manager, ingested } = managerWithSink()
  const core = new FakeCore([{ id: 'a' }])
  manager._watchRemoteCore(CONV, PEER, CORE_A, core)

  await manager._drainRemoteCore(CONV, PEER, CORE_A, core)
  core.append({ id: 'b' })
  await manager._drainChains.get(core)
  core.append({ id: 'c' })
  await manager._drainChains.get(core)

  assert.deepStrictEqual(ingested.map(x => x.message.id), ['a', 'b', 'c'])
  assert.strictEqual(manager._getProcessedCursor(CONV, CORE_A, 0), 3)
})

test('concurrent drains of one core never double-walk a block', async () => {
  const { manager, ingested } = managerWithSink(async () => {
    await new Promise(resolve => setTimeout(resolve, 5))
  })
  const core = new FakeCore([{ id: 'a' }, { id: 'b' }])

  await Promise.all([
    manager._drainRemoteCore(CONV, PEER, CORE_A, core),
    manager._drainRemoteCore(CONV, PEER, CORE_A, core),
    manager._drainRemoteCore(CONV, PEER, CORE_A, core)
  ])

  assert.deepStrictEqual(ingested.map(x => x.message.id), ['a', 'b'])
  assert.strictEqual(manager._getProcessedCursor(CONV, CORE_A, 0), 2)
})

test('a missing ingestion sink fails closed without advancing', async () => {
  const manager = new HypercoreManager()
  manager.saveCoreKeyIndex = () => {}
  manager._drainRetryBaseMs = 60000
  const core = new FakeCore([{ id: 'a' }])

  await manager._drainRemoteCore(CONV, PEER, CORE_A, core)

  assert.strictEqual(manager._getProcessedCursor(CONV, CORE_A, 0), 0)
})

test('cursor identity includes physical core key and fork', () => {
  const { manager } = managerWithSink()
  manager._setProcessedCursor(CONV, CORE_A, 7, 2)

  assert.strictEqual(manager._getProcessedCursor(CONV, CORE_A, 2), 7)
  assert.strictEqual(manager._getProcessedCursor(CONV, CORE_A, 3), 0)
  assert.strictEqual(manager._getProcessedCursor(CONV, CORE_B, 2), 0)
})

test('closing a core during a blocked read cannot acknowledge the block', async () => {
  const { manager } = managerWithSink()
  const core = new BlockingCore([{ id: 'a' }])
  manager.remoteCores.set(CONV, new Map([[PEER, core]]))
  manager._coreKeyIndex.set(CONV, new Map([[PEER, CORE_A]]))
  manager._watchRemoteCore(CONV, PEER, CORE_A, core)

  const drain = manager._drainRemoteCore(CONV, PEER, CORE_A, core)
  await waitFor(() => !!core.pendingGet)
  await manager.removeRemoteCore(CONV, PEER)
  await drain

  assert.strictEqual(manager._getProcessedCursor(CONV, CORE_A, 0), 0)
  assert.strictEqual(manager._drainStates.has(core), false)
})

test('one durable completion watermark covers a whole catch-up batch', async () => {
  let releaseCompletion
  const completionGate = new Promise(resolve => { releaseCompletion = resolve })
  const { manager, completions } = managerWithSink(
    async ({ message }) => ({ deliveryReceipt: { messageId: message.id, senderId: PEER } }),
    async () => completionGate
  )
  const core = new FakeCore([{ id: 'a' }, { id: 'b' }, { id: 'c' }])

  const drain = manager._drainRemoteCore(CONV, PEER, CORE_A, core)
  await waitFor(() => completions.length === 1)
  assert.strictEqual(manager._getProcessedCursor(CONV, CORE_A, 0), 0,
    'cursor waits for the receipt append')
  assert.strictEqual(completions[0].result.messageId, 'c')

  releaseCompletion()
  await drain
  assert.strictEqual(manager._getProcessedCursor(CONV, CORE_A, 0), 3)
})

test('a delivery-watermark failure holds the cursor and retries the batch', async () => {
  let completionAttempts = 0
  const { manager, ingested } = managerWithSink(
    async ({ message }) => ({ deliveryReceipt: { messageId: message.id, senderId: PEER } }),
    async () => {
      completionAttempts++
      if (completionAttempts === 1) throw new Error('receipt append failed')
    }
  )
  manager._drainRetryBaseMs = 5
  manager._drainRetryMaxMs = 5
  const core = new FakeCore([{ id: 'a' }, { id: 'b' }])

  await manager._drainRemoteCore(CONV, PEER, CORE_A, core)
  assert.strictEqual(manager._getProcessedCursor(CONV, CORE_A, 0), 0)
  await waitFor(() => manager._getProcessedCursor(CONV, CORE_A, 0) === 2)

  assert.strictEqual(completionAttempts, 2)
  assert.deepStrictEqual(ingested.map(x => x.message.id), ['a', 'b', 'a', 'b'])
})

test('cursors round-trip by physical core key and fork', async () => {
  const indexPath = path.join(getDataDir(), 'corekeys.json')
  const backup = fs.existsSync(indexPath) ? fs.readFileSync(indexPath) : null
  try {
    const writer = new HypercoreManager()
    writer._setProcessedCursor(CONV, CORE_A, 5, 2)
    writer._setProcessedCursor(CONV, CORE_B, 3, 0)

    const onDisk = JSON.parse(fs.readFileSync(indexPath, 'utf8'))
    assert.strictEqual(onDisk.version, 3)
    assert.deepStrictEqual(onDisk.cursors[CONV][CORE_A], { nextIndex: 5, fork: 2 })

    const reader = new HypercoreManager()
    await reader.loadCoreKeyIndex()
    assert.strictEqual(reader._getProcessedCursor(CONV, CORE_A, 2), 5)
    assert.strictEqual(reader._getProcessedCursor(CONV, CORE_B, 0), 3)
  } finally {
    if (backup) fs.writeFileSync(indexPath, backup)
    else fs.rmSync(indexPath, { force: true })
  }
})

test('pre-release peer-key cursor records migrate through the remote index', async () => {
  const indexPath = path.join(getDataDir(), 'corekeys.json')
  const backup = fs.existsSync(indexPath) ? fs.readFileSync(indexPath) : null
  try {
    fs.writeFileSync(indexPath, JSON.stringify({
      version: 3,
      remotes: { [CONV]: { [PEER]: CORE_A } },
      cursors: { [CONV]: { [PEER]: 4 } }
    }))

    const reader = new HypercoreManager()
    await reader.loadCoreKeyIndex()
    assert.strictEqual(reader._getProcessedCursor(CONV, CORE_A, 0), 4)
  } finally {
    if (backup) fs.writeFileSync(indexPath, backup)
    else fs.rmSync(indexPath, { force: true })
  }
})
