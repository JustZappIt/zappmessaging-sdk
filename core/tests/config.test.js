/**
 * config.js reads Bare.argv once at load, so every case here runs in a fresh
 * module instance with its own argv.
 */

const { test } = require('node:test')
const assert = require('node:assert')

function loadConfig (argv) {
  globalThis.Bare = { argv }
  const configPath = require.resolve('../lib/config')
  delete require.cache[configPath]
  try {
    return require(configPath)
  } finally {
    delete require.cache[configPath]
    delete globalThis.Bare
  }
}

test('numeric flags override their defaults when they are whole numbers in range', () => {
  const config = loadConfig(['--ipc-max-buf=64', '--max-messages-per-conv=10', '--dht-check-interval=5000'])
  assert.strictEqual(config.IPC_MAX_RECV_BUF_SIZE, 64)
  assert.strictEqual(config.MAX_MESSAGES_PER_CONVERSATION, 10)
  assert.strictEqual(config.DHT_CHECK_INTERVAL_MS, 5000)
})

test('malformed or out-of-range numeric flags keep the default', () => {
  const defaults = loadConfig([])
  const config = loadConfig([
    '--ipc-max-buf=0',
    '--max-messages-per-conv=-5',
    '--dht-check-interval=12abc',
    '--dht-check-timeout=1.5',
    '--heartbeat-interval=',
    '--socket-frame-max=99999999999999999999'
  ])
  assert.strictEqual(config.IPC_MAX_RECV_BUF_SIZE, defaults.IPC_MAX_RECV_BUF_SIZE)
  assert.strictEqual(config.MAX_MESSAGES_PER_CONVERSATION, defaults.MAX_MESSAGES_PER_CONVERSATION)
  assert.strictEqual(config.DHT_CHECK_INTERVAL_MS, defaults.DHT_CHECK_INTERVAL_MS)
  assert.strictEqual(config.DHT_CHECK_TIMEOUT_MS, defaults.DHT_CHECK_TIMEOUT_MS)
  assert.strictEqual(config.HEARTBEAT_INTERVAL_MS, defaults.HEARTBEAT_INTERVAL_MS)
  assert.strictEqual(config.SOCKET_FRAME_MAX_LEN, defaults.SOCKET_FRAME_MAX_LEN)
})

test('configured limits reach the modules that enforce them', () => {
  globalThis.Bare = { argv: ['--ipc-max-buf=64', '--socket-frame-max=128', '--dht-check-timeout=250'], IPC: { on () {} } }
  for (const name of ['../lib/config', '../lib/ipc-handler', '../lib/socket-framing', '../lib/dht-health']) {
    delete require.cache[require.resolve(name)]
  }
  try {
    const { IPCHandler } = require('../lib/ipc-handler')
    const { FramedSocket } = require('../lib/socket-framing')
    const { DHTHealthMonitor } = require('../lib/dht-health')
    const EventEmitter = require('node:events')

    class Handler extends IPCHandler { setupMessageHandler () {} }
    const handler = new Handler({ identity: {}, chatStore: {}, contactStore: {}, p2pManager: new EventEmitter() })
    assert.strictEqual(handler._maxRecvBufSize, 64)

    const socket = new EventEmitter()
    const framed = new FramedSocket(socket)
    const errors = []
    framed.onError = error => errors.push(error.message)
    const frame = Buffer.alloc(4)
    frame.writeUInt32BE(129, 0)
    socket.emit('data', frame)
    assert.deepStrictEqual(errors, ['Frame too large: 129'])

    assert.strictEqual(new DHTHealthMonitor().checkTimeout, 250)
  } finally {
    delete globalThis.Bare
    for (const name of ['../lib/config', '../lib/ipc-handler', '../lib/socket-framing', '../lib/dht-health']) {
      delete require.cache[require.resolve(name)]
    }
  }
})
