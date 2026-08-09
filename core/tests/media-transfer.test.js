/**
 * Unit tests for media-transfer.js
 */

const { test } = require('node:test')
const assert = require('node:assert')
const EventEmitter = require('events')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const { MediaTransfer, CHUNK_SIZE } = require('../lib/media-transfer')
const { MediaStore } = require('../lib/media-store')

// Mock FramedSocket for testing
class MockFramedSocket {
  constructor() {
    this.chunks = []
  }

  writeChunk(hashBuf, chunkIndex, totalChunks, data) {
    this.chunks.push({ hashBuf, chunkIndex, totalChunks, data })
  }
}

test('MediaTransfer initializes correctly', () => {
  const mediaStore = new MediaStore()
  const transfer = new MediaTransfer(mediaStore)
  
  assert.ok(transfer, 'Transfer should be created')
  assert.ok(transfer.mediaStore, 'Should have media store')
  assert.ok(transfer.activeTransfers instanceof Map, 'Active transfers should be a Map')
})

test('CHUNK_SIZE is 64KB', () => {
  assert.strictEqual(CHUNK_SIZE, 64 * 1024, 'Chunk size should be 64KB')
})

test('sendMedia sends file in chunks', async () => {
  const mediaStore = new MediaStore()
  const transfer = new MediaTransfer(mediaStore)
  
  // Create test media
  const testData = b4a.alloc(CHUNK_SIZE * 2 + 1000) // 2.5 chunks
  const { hashHex } = mediaStore.saveMedia(testData, 'jpg')
  
  const mockSocket = new MockFramedSocket()
  await transfer.sendMedia(mockSocket, hashHex)
  
  assert.strictEqual(mockSocket.chunks.length, 3, 'Should send 3 chunks')
  assert.strictEqual(mockSocket.chunks[0].chunkIndex, 0, 'First chunk index should be 0')
  assert.strictEqual(mockSocket.chunks[0].totalChunks, 3, 'Total chunks should be 3')
  
  // Cleanup
  mediaStore.deleteMedia(hashHex)
})

test('sendMedia throws error for non-existent media', async () => {
  const mediaStore = new MediaStore()
  const transfer = new MediaTransfer(mediaStore)
  
  const mockSocket = new MockFramedSocket()
  
  try {
    await transfer.sendMedia(mockSocket, 'nonexistent')
    assert.fail('Should throw error')
  } catch (err) {
    assert.ok(err.message.includes('not found'), 'Error should mention not found')
  }
})

test('sendMediaToAll sends to multiple sockets', async () => {
  const mediaStore = new MediaStore()
  const transfer = new MediaTransfer(mediaStore)
  
  const testData = b4a.alloc(CHUNK_SIZE + 100)
  const { hashHex } = mediaStore.saveMedia(testData, 'jpg')
  
  const socket1 = new MockFramedSocket()
  const socket2 = new MockFramedSocket()
  
  await transfer.sendMediaToAll([socket1, socket2], hashHex)
  
  assert.strictEqual(socket1.chunks.length, 2, 'Socket 1 should receive 2 chunks')
  assert.strictEqual(socket2.chunks.length, 2, 'Socket 2 should receive 2 chunks')
  
  // Cleanup
  mediaStore.deleteMedia(hashHex)
})

test('onChunkReceived assembles chunks', () => {
  return new Promise((resolve) => {
    const mediaStore = new MediaStore()
    const transfer = new MediaTransfer(mediaStore)

    const testData = b4a.from('test chunk data')
    // Must be the real content hash: complete only fires after the
    // reassembled data passes the integrity check.
    const hashBuf = mediaStore.hash(testData)

    transfer.on('complete', (hashHex, fullData) => {
      assert.ok(b4a.equals(fullData, testData), 'Assembled data should match')
      resolve()
    })
    
    // Send as single chunk
    transfer.onChunkReceived(hashBuf, 0, 1, testData)
  })
})

test('onChunkReceived handles multiple chunks', () => {
  return new Promise((resolve) => {
    const mediaStore = new MediaStore()
    const transfer = new MediaTransfer(mediaStore)
    
    const part1 = b4a.from('first')
    const part2 = b4a.from('second')
    const expected = b4a.concat([part1, part2])
    const hashBuf = mediaStore.hash(expected)

    transfer.on('complete', (hashHex, fullData) => {
      assert.ok(b4a.equals(fullData, expected), 'Assembled data should match')
      resolve()
    })
    
    transfer.onChunkReceived(hashBuf, 0, 2, part1)
    transfer.onChunkReceived(hashBuf, 1, 2, part2)
  })
})

test('onChunkReceived emits progress events', () => {
  return new Promise((resolve) => {
    const mediaStore = new MediaStore()
    const transfer = new MediaTransfer(mediaStore)

    // Real content hash so the final chunk completes cleanly instead of
    // emitting an unhandled integrity 'error' after the test resolves.
    const hashBuf = mediaStore.hash(b4a.concat([b4a.from('a'), b4a.from('b')]))
    let progressCount = 0
    
    transfer.on('progress', (hashHex, progress) => {
      progressCount++
      if (progressCount === 1) {
        assert.strictEqual(progress, 0.5, 'First progress should be 0.5')
      } else if (progressCount === 2) {
        assert.strictEqual(progress, 1.0, 'Second progress should be 1.0')
        resolve()
      }
    })
    
    transfer.onChunkReceived(hashBuf, 0, 2, b4a.from('a'))
    transfer.onChunkReceived(hashBuf, 1, 2, b4a.from('b'))
  })
})

test('onChunkReceived ignores duplicate chunks', () => {
  const mediaStore = new MediaStore()
  const transfer = new MediaTransfer(mediaStore)
  
  const hashBuf = crypto.randomBytes(32)
  const hashHex = b4a.toString(hashBuf, 'hex')
  
  transfer.onChunkReceived(hashBuf, 0, 2, b4a.from('first'))
  transfer.onChunkReceived(hashBuf, 0, 2, b4a.from('duplicate'))
  
  const activeTransfer = transfer.activeTransfers.get(hashHex)
  assert.strictEqual(activeTransfer.received, 1, 'Should only count chunk once')
})

test('handleRequest sends media if available', async () => {
  const mediaStore = new MediaStore()
  const transfer = new MediaTransfer(mediaStore)
  
  const testData = b4a.from('request test data')
  const { hashHex } = mediaStore.saveMedia(testData, 'jpg')
  const hashBuf = b4a.from(hashHex, 'hex')
  
  const mockSocket = new MockFramedSocket()
  await transfer.handleRequest(hashBuf, mockSocket)
  
  assert.ok(mockSocket.chunks.length > 0, 'Should send chunks')
  
  // Cleanup
  mediaStore.deleteMedia(hashHex)
})

test('handleRequest does nothing for non-existent media', async () => {
  const mediaStore = new MediaStore()
  const transfer = new MediaTransfer(mediaStore)
  
  const hashBuf = crypto.randomBytes(32)
  const mockSocket = new MockFramedSocket()
  
  await transfer.handleRequest(hashBuf, mockSocket)
  
  assert.strictEqual(mockSocket.chunks.length, 0, 'Should not send anything')
})

test('cancelTransfer removes active transfer', () => {
  const mediaStore = new MediaStore()
  const transfer = new MediaTransfer(mediaStore)
  
  const hashBuf = crypto.randomBytes(32)
  const hashHex = b4a.toString(hashBuf, 'hex')
  
  transfer.onChunkReceived(hashBuf, 0, 2, b4a.from('data'))
  assert.ok(transfer.activeTransfers.has(hashHex), 'Transfer should be active')
  
  transfer.cancelTransfer(hashHex)
  assert.strictEqual(transfer.activeTransfers.has(hashHex), false, 'Transfer should be cancelled')
})

test('cancelTransfer emits cancelled event', () => {
  return new Promise((resolve) => {
    const mediaStore = new MediaStore()
    const transfer = new MediaTransfer(mediaStore)
    
    const hashBuf = crypto.randomBytes(32)
    const hashHex = b4a.toString(hashBuf, 'hex')
    
    transfer.on('cancelled', (cancelledHash) => {
      assert.strictEqual(cancelledHash, hashHex, 'Hash should match')
      resolve()
    })
    
    transfer.onChunkReceived(hashBuf, 0, 2, b4a.from('data'))
    transfer.cancelTransfer(hashHex)
  })
})

test('getProgress returns correct progress', () => {
  const mediaStore = new MediaStore()
  const transfer = new MediaTransfer(mediaStore)
  
  const hashBuf = crypto.randomBytes(32)
  const hashHex = b4a.toString(hashBuf, 'hex')
  
  assert.strictEqual(transfer.getProgress(hashHex), null, 'Should return null for inactive')
  
  transfer.onChunkReceived(hashBuf, 0, 4, b4a.from('a'))
  assert.strictEqual(transfer.getProgress(hashHex), 0.25, 'Should return 0.25')
  
  transfer.onChunkReceived(hashBuf, 1, 4, b4a.from('b'))
  assert.strictEqual(transfer.getProgress(hashHex), 0.5, 'Should return 0.5')
})

test('sendMedia emits sent event', () => {
  return new Promise((resolve) => {
    const mediaStore = new MediaStore()
    const transfer = new MediaTransfer(mediaStore)
    
    const testData = b4a.from('sent event test')
    const { hashHex } = mediaStore.saveMedia(testData, 'jpg')
    
    transfer.on('sent', (sentHash, totalChunks) => {
      assert.strictEqual(sentHash, hashHex, 'Hash should match')
      assert.strictEqual(totalChunks, 1, 'Should have 1 chunk')
      mediaStore.deleteMedia(hashHex)
      resolve()
    })
    
    const mockSocket = new MockFramedSocket()
    transfer.sendMedia(mockSocket, hashHex)
  })
})

