/**
 * Unit tests for contact-store.js
 */

const { test } = require('node:test')
const assert = require('node:assert')
const path = require('path')
const fs = require('fs')
const { ContactStore } = require('../lib/contact-store')

test('ContactStore initializes correctly', () => {
  const store = new ContactStore()
  
  assert.ok(store, 'Store should be created')
  assert.ok(store.contacts instanceof Map, 'Contacts should be a Map')
  assert.ok(store.storagePath.includes('contacts.json'), 'Storage path should include contacts.json')
})

test('addContact adds new contact', async () => {
  const store = new ContactStore()
  
  const contact = await store.addContact('pubkey123', 'Alice')
  
  assert.ok(contact, 'Contact should be created')
  assert.strictEqual(contact.publicKey, 'pubkey123', 'Public key should match')
  assert.strictEqual(contact.name, 'Alice', 'Name should match')
  assert.ok(contact.addedAt, 'Should have addedAt timestamp')
  
  // Cleanup
  await store.deleteContact('pubkey123')
})

test('addContact throws error for missing fields', async () => {
  const store = new ContactStore()
  
  try {
    await store.addContact('', 'Name')
    assert.fail('Should throw error for missing public key')
  } catch (err) {
    assert.ok(err.message.includes('required'), 'Error should mention required')
  }
  
  try {
    await store.addContact('pubkey', '')
    assert.fail('Should throw error for missing name')
  } catch (err) {
    assert.ok(err.message.includes('required'), 'Error should mention required')
  }
})

test('getContact retrieves contact by public key', async () => {
  const store = new ContactStore()
  
  await store.addContact('pubkey456', 'Bob')
  const retrieved = await store.getContact('pubkey456')
  
  assert.ok(retrieved, 'Contact should be retrieved')
  assert.strictEqual(retrieved.name, 'Bob', 'Name should match')
  
  // Cleanup
  await store.deleteContact('pubkey456')
})

test('getContact returns null for non-existent contact', async () => {
  const store = new ContactStore()
  
  const result = await store.getContact('nonexistent')
  assert.strictEqual(result, null, 'Should return null')
})

test('listContacts returns all contacts sorted by name', async () => {
  const store = new ContactStore()
  
  await store.addContact('key1', 'Charlie')
  await store.addContact('key2', 'Alice')
  await store.addContact('key3', 'Bob')
  
  const list = await store.listContacts()
  
  assert.strictEqual(list.length, 3, 'Should return 3 contacts')
  assert.strictEqual(list[0].name, 'Alice', 'First should be Alice')
  assert.strictEqual(list[1].name, 'Bob', 'Second should be Bob')
  assert.strictEqual(list[2].name, 'Charlie', 'Third should be Charlie')
  
  // Cleanup
  await store.deleteContact('key1')
  await store.deleteContact('key2')
  await store.deleteContact('key3')
})

test('listContacts returns empty array when no contacts', async () => {
  const store = new ContactStore()
  
  const list = await store.listContacts()
  assert.ok(Array.isArray(list), 'Should return array')
})

test('updateContact updates contact fields', async () => {
  const store = new ContactStore()
  
  await store.addContact('key789', 'Original Name')
  const updated = await store.updateContact('key789', { name: 'Updated Name' })
  
  assert.strictEqual(updated.name, 'Updated Name', 'Name should be updated')
  
  const retrieved = await store.getContact('key789')
  assert.strictEqual(retrieved.name, 'Updated Name', 'Persisted name should be updated')
  
  // Cleanup
  await store.deleteContact('key789')
})

test('updateContact throws error for non-existent contact', async () => {
  const store = new ContactStore()
  
  try {
    await store.updateContact('nonexistent', { name: 'New Name' })
    assert.fail('Should throw error')
  } catch (err) {
    assert.ok(err.message.includes('not found'), 'Error should mention not found')
  }
})

test('deleteContact removes contact', async () => {
  const store = new ContactStore()
  
  await store.addContact('keydelete', 'To Delete')
  assert.ok(await store.getContact('keydelete'), 'Contact should exist')
  
  await store.deleteContact('keydelete')
  
  const retrieved = await store.getContact('keydelete')
  assert.strictEqual(retrieved, null, 'Contact should be deleted')
})

test('deleteContact handles non-existent contact gracefully', async () => {
  const store = new ContactStore()
  
  await assert.doesNotReject(async () => {
    await store.deleteContact('nonexistent')
  }, 'Should not throw error')
})

test('contacts persist across instances', async () => {
  const store1 = new ContactStore()
  await store1.addContact('persist123', 'Persistent Contact')
  
  // Create new instance
  const store2 = new ContactStore()
  const retrieved = await store2.getContact('persist123')
  
  assert.ok(retrieved, 'Contact should be loaded in new instance')
  assert.strictEqual(retrieved.name, 'Persistent Contact', 'Name should match')
  
  // Cleanup
  await store2.deleteContact('persist123')
})

test('addContact overwrites existing contact with same key', async () => {
  const store = new ContactStore()
  
  await store.addContact('duplicate', 'First Name')
  await store.addContact('duplicate', 'Second Name')
  
  const retrieved = await store.getContact('duplicate')
  assert.strictEqual(retrieved.name, 'Second Name', 'Should have second name')
  
  const list = await store.listContacts()
  const duplicates = list.filter(c => c.publicKey === 'duplicate')
  assert.strictEqual(duplicates.length, 1, 'Should only have one contact with this key')
  
  // Cleanup
  await store.deleteContact('duplicate')
})

