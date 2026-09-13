/**
 * Unit tests for socket-framing.js
 */

const { test } = require('node:test')
const assert = require('node:assert')
const EventEmitter = require('events')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const { FramedSocket, MSG_TYPE_JSON, MSG_TYPE_CHUNK, MSG_TYPE_REQUEST } = require('../lib/socket-framing')

// Mock socket for testing
class MockSocket extends EventEmitter {
  constructor() {
    super()
    this.written = []
  }

  write(data) {
    this.written.push(data)
  }

  simulateData(data) {
    this.emit('data', data)
  }
}

test('FramedSocket wraps raw socket', () => {
  const mockSocket = new MockSocket()
  const framed = new FramedSocket(mockSocket)
  
  assert.ok(framed, 'FramedSocket should be created')
  assert.strictEqual(framed.socket, mockSocket, 'Should wrap the raw socket')
})

test('writeJSON sends framed JSON message', () => {
  const mockSocket = new MockSocket()
  const framed = new FramedSocket(mockSocket)
  
  const message = { type: 'test', content: 'hello' }
  framed.writeJSON(message)
  
  assert.strictEqual(mockSocket.written.length, 1, 'Should write one frame')
  
  const frame = mockSocket.written[0]
  const length = frame.readUInt32BE(0)
  const type = frame[4]
  
  assert.strictEqual(type, MSG_TYPE_JSON, 'Frame type should be JSON')
  assert.ok(length > 0, 'Frame should have length')
})

test('onMessage receives JSON messages', () => {
  return new Promise((resolve) => {
    const mockSocket = new MockSocket()
    const framed = new FramedSocket(mockSocket)
    
    const testMessage = { type: 'test', content: 'hello' }
    
    framed.onMessage = (message) => {
      assert.deepEqual(message, testMessage, 'Should receive the sent message')
      resolve()
    }
    
    // Send a framed JSON message
    const json = b4a.from(JSON.stringify(testMessage))
    const frame = b4a.alloc(4 + 1 + json.length)
    frame.writeUInt32BE(1 + json.length, 0)
    frame[4] = MSG_TYPE_JSON
    json.copy(frame, 5)
    
    mockSocket.simulateData(frame)
  })
})

test('writeChunk sends framed media chunk', () => {
  const mockSocket = new MockSocket()
  const framed = new FramedSocket(mockSocket)
  
  const hash = crypto.randomBytes(32)
  const chunkData = b4a.from('test chunk data')
  
  framed.writeChunk(hash, 0, 5, chunkData)
  
  assert.strictEqual(mockSocket.written.length, 1, 'Should write one frame')
  
  const frame = mockSocket.written[0]
  const type = frame[4]
  
  assert.strictEqual(type, MSG_TYPE_CHUNK, 'Frame type should be CHUNK')
})

test('onChunk receives media chunks', () => {
  return new Promise((resolve) => {
    const mockSocket = new MockSocket()
    const framed = new FramedSocket(mockSocket)
    
    const testHash = crypto.randomBytes(32)
    const testData = b4a.from('chunk data')
    
    framed.onChunk = (hash, chunkIndex, totalChunks, data) => {
      assert.ok(b4a.equals(hash, testHash), 'Hash should match')
      assert.strictEqual(chunkIndex, 2, 'Chunk index should match')
      assert.strictEqual(totalChunks, 10, 'Total chunks should match')
      assert.ok(b4a.equals(data, testData), 'Data should match')
      resolve()
    }
    
    // Send a framed chunk
    const payloadLen = 1 + 32 + 4 + 4 + testData.length
    const frame = b4a.alloc(4 + payloadLen)
    frame.writeUInt32BE(payloadLen, 0)
    frame[4] = MSG_TYPE_CHUNK
    testHash.copy(frame, 5)
    frame.writeUInt32BE(2, 37) // chunkIndex
    frame.writeUInt32BE(10, 41) // totalChunks
    testData.copy(frame, 45)
    
    mockSocket.simulateData(frame)
  })
})

test('writeRequest sends framed media request', () => {
  const mockSocket = new MockSocket()
  const framed = new FramedSocket(mockSocket)
  
  const hash = crypto.randomBytes(32)
  
  framed.writeRequest(hash)
  
  assert.strictEqual(mockSocket.written.length, 1, 'Should write one frame')
  
  const frame = mockSocket.written[0]
  const type = frame[4]
  
  assert.strictEqual(type, MSG_TYPE_REQUEST, 'Frame type should be REQUEST')
})

test('onRequest receives media requests', () => {
  return new Promise((resolve) => {
    const mockSocket = new MockSocket()
    const framed = new FramedSocket(mockSocket)
    
    const testHash = crypto.randomBytes(32)
    
    framed.onRequest = (hash) => {
      assert.ok(b4a.equals(hash, testHash), 'Hash should match')
      resolve()
    }
    
    // Send a framed request
    const frame = b4a.alloc(4 + 1 + 32)
    frame.writeUInt32BE(33, 0)
    frame[4] = MSG_TYPE_REQUEST
    testHash.copy(frame, 5)
    
    mockSocket.simulateData(frame)
  })
})

test('handles multiple messages in single data event', () => {
  return new Promise((resolve) => {
    const mockSocket = new MockSocket()
    const framed = new FramedSocket(mockSocket)
    
    const messages = []
    framed.onMessage = (message) => {
      messages.push(message)
      if (messages.length === 2) {
        assert.deepEqual(messages[0], { first: true })
        assert.deepEqual(messages[1], { second: true })
        resolve()
      }
    }
    
    // Create two frames
    const msg1 = b4a.from(JSON.stringify({ first: true }))
    const frame1 = b4a.alloc(4 + 1 + msg1.length)
    frame1.writeUInt32BE(1 + msg1.length, 0)
    frame1[4] = MSG_TYPE_JSON
    msg1.copy(frame1, 5)
    
    const msg2 = b4a.from(JSON.stringify({ second: true }))
    const frame2 = b4a.alloc(4 + 1 + msg2.length)
    frame2.writeUInt32BE(1 + msg2.length, 0)
    frame2[4] = MSG_TYPE_JSON
    msg2.copy(frame2, 5)
    
    // Send both frames in one data event
    mockSocket.simulateData(b4a.concat([frame1, frame2]))
  })
})

test('handles partial frames across multiple data events', () => {
  return new Promise((resolve) => {
    const mockSocket = new MockSocket()
    const framed = new FramedSocket(mockSocket)
    
    framed.onMessage = (message) => {
      assert.deepEqual(message, { split: true })
      resolve()
    }
    
    const msg = b4a.from(JSON.stringify({ split: true }))
    const frame = b4a.alloc(4 + 1 + msg.length)
    frame.writeUInt32BE(1 + msg.length, 0)
    frame[4] = MSG_TYPE_JSON
    msg.copy(frame, 5)
    
    // Split frame into two parts
    const part1 = frame.subarray(0, 10)
    const part2 = frame.subarray(10)
    
    mockSocket.simulateData(part1)
    mockSocket.simulateData(part2)
  })
})

test('legacy mode handles raw JSON', () => {
  return new Promise((resolve) => {
    const mockSocket = new MockSocket()
    const framed = new FramedSocket(mockSocket)
    
    framed.onMessage = (message) => {
      assert.deepEqual(message, { legacy: true })
      resolve()
    }
    
    // Send raw JSON (starts with '{')
    const rawJson = b4a.from(JSON.stringify({ legacy: true }))
    mockSocket.simulateData(rawJson)
  })
})

test('destroy cleans up resources', () => {
  const mockSocket = new MockSocket()
  const framed = new FramedSocket(mockSocket)
  
  framed.destroy()
  
  assert.strictEqual(framed._destroyed, true, 'Should be marked as destroyed')
  assert.strictEqual(framed.writeJSON({ test: true }), false, 'Should not write after destroy')
})

test('rejects frames that are too large', () => {
  return new Promise((resolve) => {
    const mockSocket = new MockSocket()
    const framed = new FramedSocket(mockSocket)
    
    framed.onError = (error) => {
      assert.ok(error.message.includes('too large'), 'Should error on oversized frame')
      resolve()
    }
    
    // Create frame with absurdly large length
    const frame = b4a.alloc(4)
    frame.writeUInt32BE(100 * 1024 * 1024, 0) // 100MB
    
    mockSocket.simulateData(frame)
  })
})


test('legacy mode decodes a UTF-8 code point split across reads', () => {
  const mockSocket = new MockSocket()
  const framed = new FramedSocket(mockSocket)
  const received = []
  framed.onMessage = message => received.push(message)

  const bytes = b4a.from(JSON.stringify({ content: 'hello 😀' }) + '\n')
  const cut = bytes.indexOf(b4a.from('😀')) + 2
  mockSocket.simulateData(bytes.subarray(0, cut))
  assert.deepStrictEqual(received, [], 'nothing is emitted before the line completes')
  mockSocket.simulateData(bytes.subarray(cut))

  assert.deepStrictEqual(received, [{ content: 'hello 😀' }])
})

test('legacy mode handles several newline-delimited frames and a trailing unterminated one', () => {
  const mockSocket = new MockSocket()
  const framed = new FramedSocket(mockSocket)
  const received = []
  framed.onMessage = message => received.push(message)

  mockSocket.simulateData(b4a.from('{"a":1}\n\n{"b":"é"}\n{"c":'))
  assert.deepStrictEqual(received, [{ a: 1 }, { b: 'é' }])
  mockSocket.simulateData(b4a.from('3}'))
  assert.deepStrictEqual(received, [{ a: 1 }, { b: 'é' }, { c: 3 }])
})
