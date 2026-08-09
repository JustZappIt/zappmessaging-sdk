/**
 * Regression coverage for native-to-worklet NDJSON byte framing.
 */

globalThis.Bare = { argv: [], IPC: { on: () => {} } }

const { test } = require('node:test')
const assert = require('node:assert')
const { IPCHandler } = require('../lib/ipc-handler')

function framingHarness (maxBytes = 64) {
  const handler = Object.create(IPCHandler.prototype)
  handler._recvBuf = Buffer.alloc(0)
  handler._skippingOversizedFrame = false
  handler._maxRecvBufSize = maxBytes

  const lines = []
  const errors = []
  handler._processLine = line => lines.push(line)
  handler.pushEvent = (type, payload) => errors.push({ type, payload })
  return { handler, lines, errors }
}

test('worklet IPC preserves UTF-8 split across input chunks', () => {
  const { handler, lines } = framingHarness(1024)
  const expected = JSON.stringify({ type: 'message.send', payload: { content: 'hello 😀' } })
  const frame = Buffer.from(expected + '\n')
  const emoji = Buffer.from('😀')
  const emojiStart = frame.indexOf(emoji)
  assert.notStrictEqual(emojiStart, -1)

  handler._handleIPCData(frame.subarray(0, emojiStart + 2))
  handler._handleIPCData(frame.subarray(emojiStart + 2))

  assert.deepStrictEqual(lines, [expected])
  assert.strictEqual(JSON.parse(lines[0]).payload.content, 'hello 😀')
})

test('worklet IPC skips a complete oversized frame and preserves the next frame', () => {
  const { handler, lines, errors } = framingHarness(64)
  const oversized = JSON.stringify({ type: 'oversized.event', payload: { pad: 'x'.repeat(100) } })
  const valid = JSON.stringify({ type: 'valid.event', payload: {} })

  handler._handleIPCData(Buffer.from(oversized + '\n' + valid + '\n'))

  assert.deepStrictEqual(lines, [valid])
  assert.strictEqual(errors.length, 1)
  assert.strictEqual(errors[0].payload.code, 'BUFFER_OVERFLOW')
})

test('worklet IPC skips a chunked oversized frame and preserves trailing frames', () => {
  const { handler, lines, errors } = framingHarness(64)
  const oversized = Buffer.from(JSON.stringify({
    type: 'oversized.event',
    payload: { pad: 'x'.repeat(150) }
  }))
  const valid = JSON.stringify({ type: 'valid.event', payload: {} })

  handler._handleIPCData(oversized.subarray(0, 65))
  handler._handleIPCData(Buffer.concat([
    oversized.subarray(65),
    Buffer.from('\n' + valid + '\n')
  ]))

  assert.deepStrictEqual(lines, [valid])
  assert.strictEqual(errors.length, 1)
  assert.strictEqual(errors[0].payload.code, 'BUFFER_OVERFLOW')
})
