const { test } = require('node:test')
const assert = require('node:assert')
const EventEmitter = require('node:events')
const { attachBlindRelay } = require('../../server/blind-relay')

test('blind-peer server attaches the blind-relay protocol to every connection', async () => {
  const swarm = new EventEmitter()
  const relayId = Buffer.alloc(32, 7)
  const rawStream = {}
  let rawStreamOptions = null
  swarm.dht = {
    createRawStream: (options) => {
      rawStreamOptions = options
      return rawStream
    }
  }

  const accepted = []
  let relayClosed = false
  let createStream = null
  const relay = attachBlindRelay(
    { swarm },
    {
      createServer: (options) => {
        createStream = options.createStream
        return {
          accept: (connection, options) => {
            accepted.push({ connection, options })
            return new EventEmitter()
          },
          close: async () => { relayClosed = true }
        }
      }
    }
  )

  const connection = { remotePublicKey: relayId }
  swarm.emit('connection', connection)
  assert.deepStrictEqual(accepted, [{ connection, options: { id: relayId } }])

  const streamOptions = { firewall: () => false }
  assert.strictEqual(createStream(streamOptions), rawStream)
  assert.strictEqual(rawStreamOptions, streamOptions)

  await relay.close()
  assert.strictEqual(relayClosed, true)

  swarm.emit('connection', {})
  assert.strictEqual(accepted.length, 1, 'listener must be removed on close')
})
