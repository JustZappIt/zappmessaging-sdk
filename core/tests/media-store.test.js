/**
 * Unit tests for media-store.js
 */

const { test } = require('node:test')
const assert = require('node:assert')
const path = require('path')
const fs = require('fs')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const { MediaStore } = require('../lib/media-store')

test('MediaStore initializes correctly', () => {
  const store = new MediaStore()
  
  assert.ok(store, 'Store should be created')
  assert.ok(store.mediaDir.includes('media'), 'Media dir should include media')
  assert.ok(fs.existsSync(store.mediaDir), 'Media directory should exist')
})

test('hash computes Blake2b hash', () => {
  const store = new MediaStore()
  const data = b4a.from('test data')
  
  const hash = store.hash(data)
  
  assert.ok(hash, 'Hash should be computed')
  assert.strictEqual(hash.byteLength, 32, 'Hash should be 32 bytes')
})

test('hash is deterministic', () => {
  const store = new MediaStore()
  const data = b4a.from('test data')
  
  const hash1 = store.hash(data)
  const hash2 = store.hash(data)
  
  assert.ok(b4a.equals(hash1, hash2), 'Same data should produce same hash')
})

test('saveMedia saves file and returns info', () => {
  const store = new MediaStore()
  const data = b4a.from('test image data')
  
  const result = store.saveMedia(data, 'jpg')
  
  assert.ok(result.hashHex, 'Should return hash hex')
  assert.ok(result.filePath, 'Should return file path')
  assert.strictEqual(result.fileSize, data.length, 'File size should match')
  assert.ok(fs.existsSync(result.filePath), 'File should exist')
  
  // Cleanup
  fs.unlinkSync(result.filePath)
})

test('saveMedia deduplicates identical files', () => {
  const store = new MediaStore()
  const data = b4a.from('duplicate test data')
  
  const result1 = store.saveMedia(data, 'jpg')
  const result2 = store.saveMedia(data, 'jpg')
  
  assert.strictEqual(result1.hashHex, result2.hashHex, 'Hashes should match')
  assert.strictEqual(result1.filePath, result2.filePath, 'Paths should match')
  
  // Cleanup
  fs.unlinkSync(result1.filePath)
})

test('saveMediaWithHash saves with known hash', () => {
  const store = new MediaStore()
  const data = b4a.from('test data')
  const hashHex = 'abcd1234' + '0'.repeat(56) // 64 hex chars
  
  const filePath = store.saveMediaWithHash(data, hashHex, 'jpg')
  
  assert.ok(filePath, 'Should return file path')
  assert.ok(fs.existsSync(filePath), 'File should exist')
  assert.ok(filePath.includes(hashHex), 'Path should include hash')
  
  // Cleanup
  fs.unlinkSync(filePath)
})

test('saveThumbnail saves thumbnail file', () => {
  const store = new MediaStore()
  const thumbData = b4a.from('thumbnail data')
  const hashHex = 'thumb123' + '0'.repeat(56)
  
  const filePath = store.saveThumbnail(thumbData, hashHex)
  
  assert.ok(filePath, 'Should return file path')
  assert.ok(filePath.includes('_thumb.jpg'), 'Path should include _thumb')
  assert.ok(fs.existsSync(filePath), 'Thumbnail should exist')
  
  // Cleanup
  fs.unlinkSync(filePath)
})

test('getMedia retrieves saved media', () => {
  const store = new MediaStore()
  const data = b4a.from('retrieve test data')
  
  const { hashHex, filePath } = store.saveMedia(data, 'jpg')
  const retrieved = store.getMedia(hashHex)
  
  assert.ok(retrieved, 'Should retrieve media')
  assert.ok(b4a.equals(retrieved, data), 'Retrieved data should match')
  
  // Cleanup
  fs.unlinkSync(filePath)
})

test('getMedia returns null for non-existent media', () => {
  const store = new MediaStore()
  
  const result = store.getMedia('nonexistent' + '0'.repeat(54))
  assert.strictEqual(result, null, 'Should return null')
})

test('hasMedia checks if media exists', () => {
  const store = new MediaStore()
  const data = b4a.from('exists test data')
  
  const { hashHex, filePath } = store.saveMedia(data, 'jpg')
  
  assert.strictEqual(store.hasMedia(hashHex), true, 'Should return true for existing media')
  assert.strictEqual(store.hasMedia('nonexistent'), false, 'Should return false for non-existent')
  
  // Cleanup
  fs.unlinkSync(filePath)
})

test('getMediaPath returns correct path', () => {
  const store = new MediaStore()
  const data = b4a.from('path test data')
  
  const { hashHex, filePath } = store.saveMedia(data, 'png')
  const retrievedPath = store.getMediaPath(hashHex)
  
  assert.strictEqual(retrievedPath, filePath, 'Paths should match')
  
  // Cleanup
  fs.unlinkSync(filePath)
})

test('getMediaPath returns null for non-existent media', () => {
  const store = new MediaStore()
  
  const result = store.getMediaPath('nonexistent')
  assert.strictEqual(result, null, 'Should return null')
})

test('deleteMedia removes media file', () => {
  const store = new MediaStore()
  const data = b4a.from('delete test data')
  
  const { hashHex, filePath } = store.saveMedia(data, 'jpg')
  assert.ok(fs.existsSync(filePath), 'File should exist before delete')
  
  const deleted = store.deleteMedia(hashHex)
  
  assert.strictEqual(deleted, true, 'Should return true')
  assert.strictEqual(fs.existsSync(filePath), false, 'File should be deleted')
})

test('deleteMedia removes thumbnail too', () => {
  const store = new MediaStore()
  const data = b4a.from('delete thumb test')
  const thumbData = b4a.from('thumbnail')
  
  const { hashHex, filePath } = store.saveMedia(data, 'jpg')
  const thumbPath = store.saveThumbnail(thumbData, hashHex)
  
  store.deleteMedia(hashHex)
  
  assert.strictEqual(fs.existsSync(filePath), false, 'Media should be deleted')
  assert.strictEqual(fs.existsSync(thumbPath), false, 'Thumbnail should be deleted')
})

test('deleteMedia returns false for non-existent media', () => {
  const store = new MediaStore()
  
  const result = store.deleteMedia('nonexistent')
  assert.strictEqual(result, false, 'Should return false')
})

test('supports multiple file extensions', () => {
  const store = new MediaStore()
  const extensions = ['jpg', 'jpeg', 'png', 'gif']
  const savedFiles = []
  
  for (const ext of extensions) {
    const data = b4a.from(`test data for ${ext}`)
    const { hashHex, filePath } = store.saveMedia(data, ext)
    savedFiles.push({ hashHex, filePath })
    
    assert.ok(store.hasMedia(hashHex), `Should find ${ext} file`)
    assert.ok(store.getMedia(hashHex), `Should retrieve ${ext} file`)
  }
  
  // Cleanup
  for (const { filePath } of savedFiles) {
    fs.unlinkSync(filePath)
  }
})

