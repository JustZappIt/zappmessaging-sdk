/**
 * Contact Store - Manages contact list
 * 
 * Stores contacts in contacts.json with public key as identifier
 */

const path = require('bare-path')
const { getDataDir, readJSON, writeJSON, ensureDir } = require('./storage')

function diag (...args) { /* no-op; contact-store uses file-level try/catch instead */ }

class ContactStore {
  constructor() {
    this.storagePath = path.join(getDataDir(), 'contacts.json')
    this.contacts = new Map()
    this.loadContacts()
  }

  loadContacts() {
    try {
      const contactList = readJSON(this.storagePath)
      if (contactList && Array.isArray(contactList)) {
        for (const contact of contactList) {
          this.contacts.set(contact.publicKey, contact)
        }
      }
    } catch (error) {
      diag('Failed to load contacts:', error)
    }
  }

  saveContacts() {
    const contactList = Array.from(this.contacts.values())
    const dir = path.dirname(this.storagePath)
    ensureDir(dir)
    writeJSON(this.storagePath, contactList)
  }

  /**
   * Add a new contact
   * @param {string} publicKey - Contact's public key (hex)
   * @param {string} name - Contact's display name
   * @returns {Object} Created contact
   */
  async addContact(publicKey, name) {
    if (!publicKey || !name) {
      throw new Error('Public key and name are required')
    }

    const contact = {
      publicKey,
      name,
      addedAt: Date.now()
    }

    this.contacts.set(publicKey, contact)
    this.saveContacts()

    return contact
  }

  /**
   * Get a contact by public key
   * @param {string} publicKey - Contact's public key (hex)
   * @returns {Object|null} Contact or null
   */
  async getContact(publicKey) {
    return this.contacts.get(publicKey) || null
  }

  /**
   * List all contacts sorted by name
   * @returns {Array<Object>} Sorted contacts
   */
  async listContacts() {
    return Array.from(this.contacts.values())
      .sort((a, b) => a.name.localeCompare(b.name))
  }

  /**
   * Update a contact
   * @param {string} publicKey - Contact's public key (hex)
   * @param {Object} updates - Fields to update
   * @returns {Object} Updated contact
   */
  async updateContact(publicKey, updates) {
    const contact = this.contacts.get(publicKey)
    if (!contact) {
      throw new Error('Contact not found')
    }

    // Allowlist: only permit known safe fields to be updated
    const allowedFields = ['name', 'walletAddress', 'addressType', 'addressUpdatedAt']
    for (const key of allowedFields) {
      if (updates.hasOwnProperty(key)) {
        contact[key] = updates[key]
      }
    }
    this.contacts.set(publicKey, contact)
    this.saveContacts()

    return contact
  }

  /**
   * Clear all contacts.
   * Used when creating a new identity so no prior-user data leaks through.
   */
  async clearAll() {
    const fs = require('bare-fs')
    try {
      if (fs.existsSync(this.storagePath)) {
        fs.unlinkSync(this.storagePath)
      }
    } catch (e) { /* ignore */ }
    this.contacts.clear()
  }

  /**
   * Delete a contact
   * @param {string} publicKey - Contact's public key (hex)
   */
  async deleteContact(publicKey) {
    this.contacts.delete(publicKey)
    this.saveContacts()
  }

  /**
   * Update wallet address for a contact
   * @param {string} publicKey - Contact's public key (hex)
   * @param {string} walletAddress - ZEC wallet address
   * @returns {Object} Updated contact
   */
  async updateWalletAddress(publicKey, walletAddress) {
    const contact = this.contacts.get(publicKey)
    if (!contact) {
      throw new Error('Contact not found')
    }

    if (!this.isValidZcashAddress(walletAddress)) {
      throw new Error('Invalid Zcash address format')
    }

    const addressType = this.detectAddressType(walletAddress)
    
    contact.walletAddress = walletAddress
    contact.addressType = addressType
    contact.addressUpdatedAt = Date.now()

    this.contacts.set(publicKey, contact)
    this.saveContacts()

    return contact
  }

  /**
   * Validate Zcash address format
   * @param {string} address - Address to validate
   * @returns {boolean} True if valid
   */
  isValidZcashAddress(address) {
    if (!address || typeof address !== 'string') {
      return false
    }

    const validPrefixes = ['u1', 'zs1', 't1', 't3']
    const hasValidPrefix = validPrefixes.some(prefix => address.startsWith(prefix))
    
    return hasValidPrefix && address.length > 20
  }

  /**
   * Detect Zcash address type from prefix
   * @param {string} address - ZEC address
   * @returns {string} Address type
   */
  detectAddressType(address) {
    if (address.startsWith('u1')) return 'unified'
    if (address.startsWith('zs1')) return 'sapling'
    if (address.startsWith('t1') || address.startsWith('t3')) return 'transparent'
    return 'unified'
  }
}

module.exports = { ContactStore }
