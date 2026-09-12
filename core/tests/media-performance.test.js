const { test } = require('node:test')
const assert = require('node:assert/strict')
const EventEmitter = require('node:events')
const { Writable } = require('streamx')
const crypto = require('hypercore-crypto')
const { FramedSocket } = require('../lib/socket-framing')
const { MediaTransfer, CHUNK_SIZE } = require('../lib/media-transfer')
const { MediaDownloads } = require('../lib/media-downloads')
const { mediaTiming, setMediaTimingSink } = require('../lib/media-timing')
const tick = () => new Promise(resolve => setTimeout(resolve, 2))
const data = Buffer.alloc(1024 * 1024, 7)
const hash = crypto.data(data).toString('hex')
function harness () {
  let reads = 0
  const transfer = new MediaTransfer({ getMedia: () => { reads++; return data }, hasMedia: () => true })
  return { transfer, reads: () => reads }
}
function socket (peerId, blocked = false) {
  const raw = new EventEmitter()
  raw.frames = []
  raw.write = frame => { raw.frames.push(frame); return !blocked }
  raw.destroy = () => { raw.destroyed = true; raw.emit('close') }
  const framed = new FramedSocket(raw)
  framed.peerId = peerId
  return framed
}
test('production scheduler deduplicates proactive/request and multiple sockets by recipient/hash', async () => {
  const h = harness(), a = socket('peer'), b = socket('peer')
  await Promise.all([h.transfer.sendMediaToAll([a, b], hash), h.transfer.handleRequest(Buffer.from(hash, 'hex'), b)])
  assert.equal(a.socket.frames.concat(b.socket.frames).reduce((n, frame) => n + frame.length - 45, 0), data.length)
  assert.equal(h.reads(), 1)
  assert.equal(h.transfer._pendingSends.size, 0)
  assert.equal(h.transfer._sendData.size, 0)
})
test('installed streamx implementation bounds buffering and signals drain', async () => {
  const h = harness()
  let bytes = 0, maxBuffered = 0, inFlight = 0
  const raw = new Writable({ highWaterMark: 16384, write (frame, cb) {
    bytes += frame.length - 45
    inFlight += frame.length
    maxBuffered = Math.max(maxBuffered, raw._writableState.buffered)
    setTimeout(() => { inFlight -= frame.length; cb() }, 1)
  } })
  const write = raw.write.bind(raw)
  raw.write = frame => {
    const ready = write(frame)
    maxBuffered = Math.max(maxBuffered, raw._writableState.buffered + inFlight)
    return ready
  }
  const framed = new FramedSocket(raw)
  await h.transfer.sendMedia(framed, hash)
  assert.equal(bytes, data.length)
  assert.ok(maxBuffered <= CHUNK_SIZE + 45)
  raw.destroy()
})
test('slow peer does not block another recipient, and closure permits retry on another socket', async () => {
  const h = harness(), slow = socket('slow', true), fast = socket('fast')
  const first = h.transfer.sendMedia(slow, hash)
  const rejection = assert.rejects(first, /closed|cancelled/)
  await h.transfer.sendMedia(fast, hash)
  assert.equal(slow.socket.frames.length, 1)
  assert.equal(fast.socket.frames.length, 16)
  slow.socket.destroy()
  await rejection
  const retry = socket('slow')
  await h.transfer.sendMedia(retry, hash)
  assert.equal(retry.socket.frames.length, 16)
})
test('cancel and identity reset stop blocked uploads and queued work with no completion', async () => {
  for (const reset of [false, true]) {
    const h = harness(), a = socket('peer', true)
    let sent = 0
    h.transfer.on('sent', () => sent++)
    const first = h.transfer.sendMedia(a, hash)
    const second = h.transfer.sendMedia(a, 'bb'.repeat(32))
    const settled = Promise.allSettled([first, second])
    await tick()
    if (reset) h.transfer.resetIdentity()
    else { h.transfer.cancelTransfer(hash); h.transfer.cancelTransfer('bb'.repeat(32)) }
    assert.ok((await settled).every(result => result.status === 'rejected'))
    assert.equal(sent, 0)
    assert.equal(h.transfer._sendQueue.length, 0)
    assert.equal(h.transfer._pendingSends.size, 0)
    assert.equal(a.socket.listenerCount('drain'), 0)
  }
})
test('false write is not retransmitted; missing drain and synchronous failures reject', async () => {
  const h = harness(), a = socket('peer', true)
  h.transfer._transferTimeoutMs = 10
  await assert.rejects(h.transfer.sendMedia(a, hash), /stalled/)
  assert.equal(a.socket.frames.length, 1)
  a.socket.write = () => { throw new Error('write failure') }
  await assert.rejects(h.transfer.sendMedia(a, hash), /write failure/)
})
test('framing awaits metadata registration before chunks in the same socket read', async () => {
  const raw = new EventEmitter(), frames = []
  raw.write = frame => { frames.push(frame); return true }
  const sender = new FramedSocket(raw)
  sender.writeJSON({ mediaId: hash })
  sender.writeChunk(Buffer.from(hash, 'hex'), 0, 1, Buffer.from('chunk'))
  const receiverRaw = new EventEmitter()
  receiverRaw.write = () => true
  const receiver = new FramedSocket(receiverRaw)
  let expected = false, accepted = 0
  receiver.onMessage = async () => { await tick(); expected = true }
  receiver.onChunk = () => { assert.equal(expected, true); accepted++ }
  receiverRaw.emit('data', Buffer.concat(frames))
  assert.equal(accepted, 0)
  await new Promise(resolve => setTimeout(resolve, 10))
  assert.equal(accepted, 1)
})
test('no-first-byte requests retry to a bounded failure and reconnect can recover', async () => {
  let attempts = 0
  const states = []
  let finished
  const failure = new Promise(resolve => { finished = resolve })
  const downloads = new MediaDownloads({ request: () => { attempts++; return false }, cancel () {},
    state: (hash, state) => { states.push(state); if (state === 'failed') finished() },
    timeoutMs: 5, retryMs: 1, maxAttempts: 3 })
  downloads.enqueue(hash, 'conversation', 'peer')
  const keepAlive = setInterval(() => {}, 100)
  try { await failure } finally { clearInterval(keepAlive) }
  assert.equal(attempts, 3)
  assert.equal(downloads.get(hash).failed, true)
  assert.equal(states.at(-1), 'failed')
  downloads.enqueue(hash, 'conversation', 'peer', true)
  assert.equal(attempts, 4)
  assert.equal(downloads.get(hash, 'conversation').failed, false)
  downloads.delete(hash)
  assert.equal(downloads.active, 0)
})
test('download queue is bounded and cancelled timers do not cross identity reset', async () => {
  let attempts = 0
  const downloads = new MediaDownloads({ request: () => { attempts++; return true }, cancel () {}, state () {},
    maxPending: 2, concurrency: 1, timeoutMs: 5 })
  assert.equal(downloads.enqueue('a', 'conv', 'peer'), true)
  assert.equal(downloads.enqueue('b', 'conv', 'peer'), true)
  assert.equal(downloads.enqueue('c', 'conv', 'peer'), false)
  downloads.clear()
  await new Promise(resolve => setTimeout(resolve, 15))
  assert.equal(attempts, 1)
  assert.equal(downloads.size, 0)
})

test('an offline same-hash scope does not block an online conversation or lose its own retry', () => {
  const requested = [], cancelled = []
  let firstOnline = false
  const downloads = new MediaDownloads({
    request: conversation => { requested.push(conversation); return conversation === 'second' || firstOnline },
    cancel: hash => cancelled.push(hash), state () {}
  })
  downloads.enqueue(hash, 'first', 'peer-a')
  downloads.enqueue(hash, 'second', 'peer-b')
  assert.deepEqual(requested, ['first', 'second'])
  assert.equal(downloads.size, 2)
  assert.equal(downloads.active, 1)
  assert.equal(downloads.get(hash).conversationId, 'second')
  assert.equal(downloads.get(hash, 'first').senderId, 'peer-a')
  downloads.complete(hash)
  assert.equal(downloads.size, 1)
  assert.equal(downloads.get(hash, 'second'), undefined)
  firstOnline = true
  downloads.enqueue(hash, 'first', 'peer-a', true)
  assert.deepEqual(requested, ['first', 'second', 'first'])
  assert.equal(downloads.get(hash).conversationId, 'first')
  downloads.complete(hash)
  assert.equal(downloads.size, 0)
  assert.equal(downloads.active, 0)
  assert.deepEqual(cancelled, [])
})

test('same-hash downloads serialize authorization scopes and preserve queued scopes on completion', () => {
  const requested = []
  const downloads = new MediaDownloads({ request: conversation => { requested.push(conversation); return true }, cancel () {}, state () {} })
  downloads.enqueue(hash, 'first', 'peer-a')
  downloads.enqueue(hash, 'second', 'peer-b', true)
  assert.deepEqual(requested, ['first'])
  assert.equal(downloads.get(hash).conversationId, 'first')
  assert.equal(downloads.active, 1)
  downloads.complete(hash)
  assert.deepEqual(requested, ['first', 'second'])
  assert.equal(downloads.get(hash).conversationId, 'second')
  assert.equal(downloads.active, 1)
  downloads.delete(hash)
  assert.equal(downloads.active, 0)
  assert.equal(downloads.size, 0)
})

test('a stalled same-hash scope yields to another conversation without waiting through all retries', () => {
  const requested = [], cancelled = []
  const downloads = new MediaDownloads({ request: conversation => { requested.push(conversation); return true },
    cancel: hash => cancelled.push(hash), state () {} })
  downloads.enqueue(hash, 'first', 'peer-a')
  downloads.enqueue(hash, 'second', 'peer-b')
  downloads.retry(hash)
  assert.deepEqual(requested, ['first', 'second'])
  assert.deepEqual(cancelled, [hash])
  assert.equal(downloads.get(hash).conversationId, 'second')
  downloads.clear()
})

test('pending limits count conversation scopes even when every scope names the same hash', () => {
  const downloads = new MediaDownloads({ request: () => false, cancel () {}, state () {}, maxPending: 2 })
  assert.equal(downloads.enqueue(hash, 'first', 'peer-a'), true)
  assert.equal(downloads.enqueue(hash, 'second', 'peer-b'), true)
  assert.equal(downloads.enqueue(hash, 'third', 'peer-c'), false)
  assert.equal(downloads.size, 2)
  downloads.clear()
})
test('diagnostics are disabled by default and accept only numeric metrics and temporary IDs', () => {
  const events = []
  mediaTiming()('message_append', 4)
  setMediaTimingSink(event => events.push(event))
  const timing = mediaTiming()
  timing('message_append', 123)
  timing('/private/path message contents', 500)
  setMediaTimingSink(null)
  assert.equal(events.length, 1)
  assert.match(events[0].id, /^[a-f0-9]{16}$/)
  assert.deepEqual(Object.keys(events[0]).sort(), ['bytes', 'durationMs', 'elapsedMs', 'id', 'stage'])
})

test('a receive-side retry cannot cancel an unrelated authorized upload of the same hash', async () => {
  const h = harness(), framed = socket('peer', true)
  const sending = h.transfer.sendMedia(framed, hash)
  const rejection = assert.rejects(sending)
  await tick()
  h.transfer.cancelDownload(hash)
  assert.equal(h.transfer._activeSendCount, 1)
  h.transfer.resetIdentity()
  await rejection
})

test('history cache invalidates external edits and does not expose mutable committed rows', async t => {
  const fs = require('node:fs'), path = require('node:path'), os = require('node:os')
  const { ChatStore } = require('../lib/chat-store')
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'media-cache-'))
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }))
  const store = new ChatStore(); store.storagePath = dir; store.conversations.clear()
  await store.createConversationWithId('conv', 'direct', ['peer'])
  await store.addMessage('conv', { id: 'shared', senderId: 'me', isFromMe: true, mediaId: hash })
  assert.equal(store.canServeMedia(hash, 'peer'), true)
  const messages = await store.getMessages('conv')
  messages[0].mediaAuthorized = false
  assert.equal(store.canServeMedia(hash, 'peer'), true)
  fs.writeFileSync(path.join(dir, 'conv.json'), JSON.stringify(messages))
  assert.equal(store.canServeMedia(hash, 'peer'), false)
  await store.clearAll()
  assert.equal(store._historyCache.size, 0)
})

test('fast-recipient acceptance cannot hide a pending or failed slow-recipient upload', async () => {
  const h = harness(), slow = socket('slow', true), fast = socket('fast'), states = []
  h.transfer.on('upload_state', (hash, state) => states.push(state))
  const slowSend = h.transfer.sendMedia(slow, hash)
  const rejected = assert.rejects(slowSend)
  await h.transfer.sendMedia(fast, hash)
  assert.equal(states.at(-1), 'sending')
  slow.socket.destroy()
  await rejected
  assert.equal(states.at(-1), 'failed')
})

test('late initial request consumes completed proactive work; explicit retries still upload', async () => {
  const h = harness(), a = socket('peer'), b = socket('peer')
  await h.transfer.sendMediaToAll([a], hash)
  await h.transfer.handleRequest(Buffer.from(hash, 'hex'), b, 1)
  assert.equal(h.reads(), 1)
  assert.equal(b.socket.frames.length, 0)
  await h.transfer.handleRequest(Buffer.from(hash, 'hex'), b, 2)
  assert.equal(h.reads(), 2)
  assert.equal(b.socket.frames.length, 16)
  // A legacy requester has no attempt semantics and must never be suppressed.
  await h.transfer.sendMediaToAll([a], hash)
  await h.transfer.handleRequest(Buffer.from(hash, 'hex'), b)
  assert.equal(h.reads(), 4)
})
test('completed proactive credits clear on close, cancellation and identity change', async () => {
  for (const action of ['close', 'cancel', 'identity']) {
    const h = harness(), a = socket('peer'), b = socket('peer')
    await h.transfer.sendMediaToAll([a], hash)
    assert.equal(h.transfer._completedProactive.size, 1)
    if (action === 'close') a.socket.destroy()
    else if (action === 'cancel') h.transfer.cancelTransfer(hash)
    else await h.transfer.resetIdentity()
    assert.equal(h.transfer._completedProactive.size, 0)
    await h.transfer.handleRequest(Buffer.from(hash, 'hex'), b, 1)
    assert.equal(b.socket.frames.length, 16)
  }
})
test('request attempt framing preserves legacy requests and carries retry evidence', () => {
  const sender = socket('sender'), receiver = socket('receiver')
  const attempts = []
  receiver.onRequest = (receivedHash, attempt) => {
    assert.equal(receivedHash.toString('hex'), hash)
    attempts.push(attempt)
  }
  for (const attempt of [0, 1, 2]) sender.writeRequest(Buffer.from(hash, 'hex'), attempt)
  receiver.socket.emit('data', Buffer.concat(sender.socket.frames))
  assert.deepEqual(attempts, [0, 1, 2])
})

test('new author connection wakes a no-peer request without waiting for its first-byte timeout', () => {
  let attempts = 0
  const downloads = new MediaDownloads({ request: () => ++attempts > 1, cancel () {}, state () {} })
  downloads.enqueue(hash, 'conversation', 'peer')
  assert.equal(attempts, 1)
  assert.equal(downloads.get(hash).waitingPeer, true)
  downloads.enqueue(hash, 'conversation', 'peer', true)
  assert.equal(attempts, 2)
  assert.equal(downloads.get(hash).waitingPeer, false)
  assert.equal(downloads.active, 1)
  // Repeated connection events must not interrupt a request already sent.
  downloads.enqueue(hash, 'conversation', 'peer', true)
  assert.equal(attempts, 2)
  downloads.clear()
})
