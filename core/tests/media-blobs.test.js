/**
 * Media bytes in a conversation's media core: put/fetch over a real Corestore,
 * and the sender-offline round trip through a blind third store that stands
 * in for the relay. Production HypercoreManager methods open every core.
 */

const { test } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const Corestore = require('corestore')
const { HypercoreManager } = require('../lib/hypercore-manager')
const { ChatStore } = require('../lib/chat-store')
const { MediaStore } = require('../lib/media-store')
const mediaBlobs = require('../lib/media-blobs')
const config = require('../lib/config')

const { MEDIA_BLOCK_SIZE } = mediaBlobs
const alice = crypto.keyPair(b4a.alloc(32, 1))
const bob = crypto.keyPair(b4a.alloc(32, 2))
const hex = keyPair => b4a.toString(keyPair.publicKey, 'hex')
const CONV = ChatStore.directChatId(hex(alice), hex(bob))
const mediaId = bytes => b4a.toString(new MediaStore().hash(bytes), 'hex')

function temporary (t, prefix) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix))
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }))
  return dir
}

async function manager (t, keyPair, peer) {
  const m = new HypercoreManager({ dataDir: temporary(t, 'zapp-media-') })
  m.setKeyContext({
    getIdentityKeyPair: () => keyPair,
    getConversation: id => id === CONV ? { id, type: 'direct', participantIds: [hex(peer)] } : null
  })
  await m.initialize()
  t.after(() => m.close())
  return m
}

function link (a, b) {
  const s1 = a.replicate(true)
  const s2 = b.replicate(false)
  s1.pipe(s2).pipe(s1)
  return () => { s1.destroy(); s2.destroy() }
}

test('put then fetch round-trips images of every block-boundary size', async t => {
  const store = new Corestore(temporary(t, 'zapp-blobs-'))
  t.after(() => store.close())
  const core = store.get({ name: 'media', encryptionKey: b4a.alloc(32, 7), valueEncoding: 'binary' })
  await core.append(b4a.from('an earlier image'))

  const sizes = [1, MEDIA_BLOCK_SIZE - 1, MEDIA_BLOCK_SIZE, MEDIA_BLOCK_SIZE + 1, 3 * MEDIA_BLOCK_SIZE + 5, config.MEDIA_MAX_BYTES]
  const ranges = []
  for (const size of sizes) {
    const bytes = crypto.randomBytes(size)
    const { offset, n } = await mediaBlobs.put(core, bytes)
    assert.equal(n, Math.ceil(size / MEDIA_BLOCK_SIZE))
    ranges.push({ bytes, range: { offset, n, byteLength: size } })
  }
  assert.equal(ranges[0].range.offset, 1, 'offset follows whatever was already in the core')
  assert.equal(core.length, 1 + ranges.reduce((sum, r) => sum + r.range.n, 0))

  for (const { bytes, range } of ranges) {
    const fetched = await mediaBlobs.download(core, range).bytes
    assert.ok(b4a.equals(fetched, bytes))
  }
})

test('put rejects empty and oversized media', async t => {
  const store = new Corestore(temporary(t, 'zapp-blobs-'))
  t.after(() => store.close())
  const core = store.get({ name: 'media', valueEncoding: 'binary' })
  await assert.rejects(mediaBlobs.put(core, b4a.alloc(0)), { code: 'MEDIA_RANGE_INVALID' })
  await assert.rejects(mediaBlobs.put(core, b4a.alloc(config.MEDIA_MAX_BYTES + 1)), { code: 'MEDIA_RANGE_INVALID' })
  await assert.rejects(mediaBlobs.put(core, 'not a buffer'), { code: 'MEDIA_RANGE_INVALID' })
  assert.equal(core.length, 0)
})

test('download rejects ranges that cannot describe an image before touching the network', async t => {
  const store = new Corestore(temporary(t, 'zapp-blobs-'))
  t.after(() => store.close())
  const core = store.get({ name: 'media', valueEncoding: 'binary' })
  const { offset, n } = await mediaBlobs.put(core, b4a.alloc(10, 1))

  for (const range of [
    { offset, n: 0 },
    { offset: -1, n },
    { offset: 1.5, n },
    { offset, n: mediaBlobs.MAX_MEDIA_BLOCKS + 1 },
    { offset, n, byteLength: 0 },
    { offset, n, byteLength: MEDIA_BLOCK_SIZE + 1 },
    { offset, n: 2, byteLength: 10 }
  ]) {
    assert.equal(mediaBlobs.isValidRange(range), false)
    await assert.rejects(mediaBlobs.download(core, range).bytes, { code: 'MEDIA_RANGE_INVALID' })
  }
  await assert.rejects(mediaBlobs.download(core, { offset, n, byteLength: 11 }).bytes, { code: 'MEDIA_RANGE_INVALID' },
    'bytes on disk must add up to the descriptor')
})

test('download times out when no peer supplies the blocks, and cancels on cancel() or session close', async t => {
  const store = new Corestore(temporary(t, 'zapp-blobs-'))
  t.after(() => store.close())
  const core = store.get({ key: crypto.keyPair().publicKey, valueEncoding: 'binary' })
  await core.ready()

  // The stall timer is unref'd so a pending fetch never pins the runtime;
  // here nothing else holds the loop open while it waits.
  const keepAlive = setInterval(() => {}, 1000)
  t.after(() => clearInterval(keepAlive))
  const started = Date.now()
  await assert.rejects(mediaBlobs.download(core, { offset: 0, n: 2 }, { timeoutMs: 50 }).bytes, { code: 'MEDIA_FETCH_TIMEOUT' })
  assert.ok(Date.now() - started < 5000)

  const cancelled = mediaBlobs.download(core, { offset: 0, n: 2 }, { timeoutMs: 60000 })
  cancelled.cancel()
  await assert.rejects(cancelled.bytes, { code: 'MEDIA_FETCH_CANCELLED' })
  assert.equal(core.closed, false, 'cancelling leaves the session to its owner')

  const orphaned = mediaBlobs.download(core, { offset: 0, n: 2 }, { timeoutMs: 60000 })
  await core.close()
  await assert.rejects(orphaned.bytes, { code: 'MEDIA_FETCH_CANCELLED' })
})

test('an image reaches a receiver through a blind mirror after the sender has gone', async t => {
  const sender = await manager(t, alice, bob)
  const senderCore = await sender.getOrCreateLocalMediaCore(CONV)
  await senderCore.append(b4a.from('an earlier image'))
  const image = crypto.randomBytes(1_300_000)
  const { offset, n } = await mediaBlobs.put(senderCore, image)
  const descriptor = { coreKey: b4a.toString(senderCore.key, 'hex'), offset, n, byteLength: image.length }
  assert.equal(offset, 1)
  assert.equal(n, 5)

  // The relay knows the core key and nothing else.
  const mirror = new Corestore(temporary(t, 'zapp-mirror-'))
  t.after(() => mirror.close())
  const mirrorCore = mirror.get({ key: senderCore.key })
  const unlinkSender = link(sender.store, mirror)
  await mirrorCore.download({ start: 0, end: senderCore.length }).done()
  assert.equal(mirrorCore.length, senderCore.length)
  const raw = await mirrorCore.get(offset, { raw: true })
  assert.ok(!b4a.equals(raw.subarray(0, 16), image.subarray(0, 16)), 'the relay holds ciphertext')
  unlinkSender()
  await sender.close()

  const receiver = await manager(t, bob, alice)
  const receiverCore = await receiver.openRemoteMediaCore(CONV, hex(alice), descriptor.coreKey)
  assert.equal(receiverCore.writable, false)
  const unlinkReceiver = link(mirror, receiver.store)
  const blocks = []
  const fetched = await mediaBlobs.download(receiverCore, descriptor, { onBlock: index => blocks.push(index) }).bytes
  assert.ok(b4a.equals(fetched, image))
  assert.equal(mediaId(fetched), mediaId(image))
  assert.deepEqual(blocks.sort((a, b) => a - b), [1, 2, 3, 4, 5], 'one progress callback per block in the range')
  assert.equal(await receiverCore.has(0), false, 'blocks outside the range are never fetched')

  await receiverCore.clear(offset, offset + n)
  assert.equal(await receiverCore.has(offset, offset + n), false, 'ciphertext is dropped once decoded')
  unlinkReceiver()
  await receiverCore.close()
})

test('openRemoteMediaCore refuses non-participants, malformed keys, our own writers and replicated message logs', async t => {
  const m = await manager(t, alice, bob)
  const own = await m.getOrCreateLocalMediaCore(CONV)
  const ownKey = b4a.toString(own.key, 'hex')
  await assert.rejects(m.openRemoteMediaCore(CONV, 'cc'.repeat(32), ownKey), /not a participant/)
  await assert.rejects(m.openRemoteMediaCore(CONV, hex(bob), 'not-a-key'), /invalid key/)
  await assert.rejects(m.openRemoteMediaCore(CONV, hex(bob), ownKey), /local writer/)
  assert.equal(own.closed, false, 'rejecting a session must not close the writer it belongs to')

  const ownLog = await m.getOrCreateLocalCore(CONV)
  await assert.rejects(m.openRemoteMediaCore(CONV, hex(bob), b4a.toString(ownLog.key, 'hex')), /message core/)
  const bobLog = crypto.keyPair()
  m.setRemoteMessageSink(async () => {})
  await m.openRemoteCore(CONV, hex(bob), b4a.toString(bobLog.publicKey, 'hex'))
  await assert.rejects(m.openRemoteMediaCore(CONV, hex(bob), b4a.toString(bobLog.publicKey, 'hex').toUpperCase()), /message core/,
    'a descriptor cannot point a fetch, and its clear, at a peer log we replicate')
})
