/**
 * A group getting a new secret, with real Corestores replicating to each
 * other: nothing written around the switch is lost, old cores are read to
 * their end with the old key, and nobody without the new secret can read what
 * comes after.
 */

const { test } = require('node:test')
const assert = require('node:assert')
const fs = require('fs')
const os = require('os')
const path = require('path')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const { HypercoreManager } = require('../lib/hypercore-manager')

const G0 = '10'.repeat(32)
const G1 = '11'.repeat(32)
const W_KP = crypto.keyPair(b4a.alloc(32, 7))
const R_KP = crypto.keyPair(b4a.alloc(32, 8))
const W = b4a.toString(W_KP.publicKey, 'hex')
const R = b4a.toString(R_KP.publicKey, 'hex')

function tmp (label) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'rekey-' + label + '-'))
  process.once('exit', () => fs.rmSync(dir, { recursive: true, force: true }))
  return dir
}

async function node (dir, conv, keyPair) {
  const manager = new HypercoreManager({ dataDir: dir })
  manager.setKeyContext({ getIdentityKeyPair: () => keyPair, getConversation: id => (id === conv.id ? conv : null) })
  await manager.initialize()
  const ingested = []
  manager.setRemoteMessageSink(async (conversationId, peer, message) => { ingested.push(message) })
  manager.setRemoteDrainCompleteSink(async () => {})
  return { manager, conv, ingested }
}

function connect (a, b) {
  const s1 = a.manager.store.replicate(true)
  const s2 = b.manager.store.replicate(false)
  s1.pipe(s2).pipe(s1)
  return () => { s1.destroy(); s2.destroy() }
}

async function until (predicate, what, ms = 5000) {
  const start = Date.now()
  while (!predicate()) {
    if (Date.now() - start > ms) throw new Error('timed out waiting for ' + what)
    await new Promise(resolve => setTimeout(resolve, 10))
  }
}

function moveToNewSecret (n, groupId, epoch) {
  const previous = n.conv.groupId
  n.conv.pastGroupIds = [{ groupId: previous, epoch: n.conv.groupEpoch || 0 }, ...(n.conv.pastGroupIds || [])]
  n.conv.groupId = groupId
  n.conv.groupEpoch = epoch
  return previous
}

async function writerAndReader () {
  const writer = await node(tmp('w'), { id: 'gw', type: 'group', groupId: G0, creatorKey: W, participantIds: [R] }, W_KP)
  const reader = await node(tmp('r'), { id: 'gr', type: 'group', groupId: G0, creatorKey: W, participantIds: [W] }, R_KP)
  return { writer, reader }
}

test('messages written around the switch all arrive, and markers never become rows', async () => {
  const { writer, reader } = await writerAndReader()
  const disconnect = connect(writer, reader)
  try {
    const oldCore = await writer.manager.getOrCreateLocalCore('gw')
    await writer.manager.appendMessage('gw', { id: 'm1', content: 'before' })
    await reader.manager.openRemoteCore('gr', W, b4a.toString(oldCore.key, 'hex'))
    await until(() => reader.ingested.some(m => m.id === 'm1'), 'm1')

    // The reader moves first. The writer has not heard yet and writes once more
    // to its old core: that message must still arrive.
    reader.manager.rekeyConversation('gr', moveToNewSecret(reader, G1, 1))
    assert.strictEqual(reader.manager.retiringCores('gr').length, 1)
    await writer.manager.appendMessage('gw', { id: 'm2', content: 'late on the old core' })

    // Now the writer moves: new core, marker first.
    const previous = writer.manager.rekeyConversation('gw', moveToNewSecret(writer, G1, 1))
    assert.strictEqual(previous.prevLength, 2)
    const newCore = await writer.manager.getOrCreateLocalCore('gw')
    assert.notStrictEqual(b4a.toString(newCore.key, 'hex'), b4a.toString(oldCore.key, 'hex'))
    await writer.manager.appendMessage('gw', { type: '__epoch', epoch: 1, prevCoreKey: previous.prevCoreKey, prevLength: previous.prevLength })
    await writer.manager.appendMessage('gw', { id: 'm3', content: 'after' })

    await reader.manager.openRemoteCore('gr', W, b4a.toString(newCore.key, 'hex'))
    await until(() => ['m1', 'm2', 'm3'].every(id => reader.ingested.some(m => m.id === id)), 'm1 m2 m3')
    await until(() => reader.manager.retiringCores('gr').length === 0, 'the old core to be read to its end')

    assert.ok(!reader.ingested.some(m => m.type === '__epoch'), 'a marker is bookkeeping, never a row')
    // The finished old core cannot come back and replace the current one.
    assert.strictEqual(await reader.manager.openRemoteCore('gr', W, previous.prevCoreKey), null)
    assert.strictEqual(reader.manager.remoteCores.get('gr').get(W).key.toString('hex'), b4a.toString(newCore.key, 'hex'))
  } finally {
    disconnect()
    await writer.manager.close()
    await reader.manager.close()
  }
})

test('someone holding only the old secret cannot read the new core', async () => {
  const { writer, reader } = await writerAndReader()
  const disconnect = connect(writer, reader)
  try {
    const previous = writer.manager.rekeyConversation('gw', moveToNewSecret(writer, G1, 1))
    assert.strictEqual(previous.prevCoreKey, null, 'no old core was open')
    const newCore = await writer.manager.getOrCreateLocalCore('gw')
    await writer.manager.appendMessage('gw', { id: 'secret', content: 'only for members' })
    // The reader stands in for a removed member: it never got G1.
    await reader.manager.openRemoteCore('gr', W, b4a.toString(newCore.key, 'hex'))
    await until(() => reader.manager.remoteCores.get('gr').get(W).length >= 1, 'the block to replicate')
    await new Promise(resolve => setTimeout(resolve, 100))
    assert.ok(!reader.ingested.some(m => m && m.id === 'secret'))
  } finally {
    disconnect()
    await writer.manager.close()
    await reader.manager.close()
  }
})

test('a retiring core survives a restart and is still read with the old key', async () => {
  const { writer, reader } = await writerAndReader()
  let disconnect = connect(writer, reader)
  const oldCore = await writer.manager.getOrCreateLocalCore('gw')
  await writer.manager.appendMessage('gw', { id: 'a', content: 'one' })
  await reader.manager.openRemoteCore('gr', W, b4a.toString(oldCore.key, 'hex'))
  await until(() => reader.ingested.some(m => m.id === 'a'), 'a')
  reader.manager.rekeyConversation('gr', moveToNewSecret(reader, G1, 1))
  const readerDir = reader.manager._dataDir
  disconnect()
  await reader.manager.close()

  const index = JSON.parse(fs.readFileSync(path.join(readerDir, 'corekeys.json'), 'utf8'))
  assert.strictEqual(index.retiring.gr[b4a.toString(oldCore.key, 'hex')].groupId, G0)

  await writer.manager.appendMessage('gw', { id: 'b', content: 'two' })
  const restarted = await node(readerDir, reader.conv, R_KP)
  disconnect = connect(writer, restarted)
  try {
    await restarted.manager.loadCoreKeyIndex()
    assert.strictEqual(restarted.manager.retiringCores('gr').length, 1)
    await until(() => restarted.ingested.some(m => m.id === 'b'), 'b through the reopened core')
    assert.ok(!restarted.ingested.some(m => m.id === 'a'), 'the cursor survived, so nothing repeats')
  } finally {
    disconnect()
    await writer.manager.close()
    await restarted.manager.close()
  }
})

test('removing a member also drops their cores from earlier secrets', async () => {
  const { writer, reader } = await writerAndReader()
  const disconnect = connect(writer, reader)
  try {
    const oldCore = await writer.manager.getOrCreateLocalCore('gw')
    await reader.manager.openRemoteCore('gr', W, b4a.toString(oldCore.key, 'hex'))
    reader.manager.rekeyConversation('gr', moveToNewSecret(reader, G1, 1))
    assert.strictEqual(reader.manager.retiringCores('gr').length, 1)
    assert.strictEqual(await reader.manager.removeRemoteCore('gr', W), true)
    assert.strictEqual(reader.manager.retiringCores('gr').length, 0)
  } finally {
    disconnect()
    await writer.manager.close()
    await reader.manager.close()
  }
})

test('an image written under the old secret is read with it', async () => {
  const { writer, reader } = await writerAndReader()
  const disconnect = connect(writer, reader)
  try {
    const media = await writer.manager.getOrCreateLocalMediaCore('gw')
    await media.append(b4a.from('image bytes'))
    const mediaKey = b4a.toString(media.key, 'hex')
    moveToNewSecret(reader, G1, 1)

    let core = await reader.manager.openRemoteMediaCore('gr', W, mediaKey)
    await until(() => core.length >= 1, 'media replication')
    const wrong = await core.get(0)
    await core.close()
    assert.notStrictEqual(b4a.toString(wrong), 'image bytes')

    core = await reader.manager.openRemoteMediaCore('gr', W, mediaKey, G0)
    assert.strictEqual(b4a.toString(await core.get(0)), 'image bytes')
    await core.close()
  } finally {
    disconnect()
    await writer.manager.close()
    await reader.manager.close()
  }
})
