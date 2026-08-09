/**
 * Unit tests for storage.js
 */

const { test } = require('node:test')
const assert = require('node:assert')
const path = require('path')
const fs = require('fs')
const { getDataDir, ensureDir, readJSON, writeJSON, fileExists } = require('../lib/storage')

test('getDataDir returns a valid path', () => {
  const dataDir = getDataDir()
  assert.ok(dataDir, 'Data directory should be defined')
  assert.ok(dataDir.includes('zappmessaging'), 'Data directory should include zappmessaging')
  assert.ok(fs.existsSync(dataDir), 'Data directory should exist')
})

test('ensureDir creates directory if not exists', () => {
  const testDir = path.join(getDataDir(), 'test-dir-' + Date.now())
  
  // Directory should not exist initially
  assert.ok(!fs.existsSync(testDir), 'Test directory should not exist initially')
  
  // Create directory
  ensureDir(testDir)
  assert.ok(fs.existsSync(testDir), 'Test directory should exist after ensureDir')
  
  // Cleanup
  fs.rmdirSync(testDir)
})

test('writeJSON and readJSON work correctly', () => {
  const testFile = path.join(getDataDir(), 'test-' + Date.now() + '.json')
  const testData = {
    name: 'Test User',
    timestamp: Date.now(),
    nested: { value: 123 }
  }
  
  // Write JSON
  writeJSON(testFile, testData)
  assert.ok(fs.existsSync(testFile), 'Test file should exist after writeJSON')
  
  // Read JSON
  const readData = readJSON(testFile)
  assert.deepEqual(readData, testData, 'Read data should match written data')
  
  // Cleanup
  fs.unlinkSync(testFile)
})

test('readJSON returns null for non-existent file', () => {
  const nonExistentFile = path.join(getDataDir(), 'non-existent-' + Date.now() + '.json')
  const result = readJSON(nonExistentFile)
  assert.strictEqual(result, null, 'readJSON should return null for non-existent file')
})

test('fileExists returns correct boolean', () => {
  const testFile = path.join(getDataDir(), 'exists-test-' + Date.now() + '.json')
  
  // File should not exist
  assert.strictEqual(fileExists(testFile), false, 'fileExists should return false for non-existent file')
  
  // Create file
  writeJSON(testFile, { test: true })
  assert.strictEqual(fileExists(testFile), true, 'fileExists should return true for existing file')
  
  // Cleanup
  fs.unlinkSync(testFile)
})

test('writeJSON creates parent directories', () => {
  const nestedPath = path.join(getDataDir(), 'nested', 'deep', 'test-' + Date.now() + '.json')
  const testData = { nested: true }
  
  writeJSON(nestedPath, testData)
  assert.ok(fs.existsSync(nestedPath), 'Nested file should exist')
  
  const readData = readJSON(nestedPath)
  assert.deepEqual(readData, testData, 'Nested data should match')
  
  // Cleanup
  fs.unlinkSync(nestedPath)
  fs.rmdirSync(path.dirname(nestedPath))
  fs.rmdirSync(path.dirname(path.dirname(nestedPath)))
})

