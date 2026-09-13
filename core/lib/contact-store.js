/**
 * Contact Store - Manages contact list
 *
 * Stores contacts in contacts.json with public key as identifier. Every
 * mutation is written to disk before it becomes visible, so a rejected write
 * leaves the in-memory list matching the file.
 */

const path = require('bare-path')
const fs = require('bare-fs')
const { getDataDir, readJSON, writeJSON, ensureDir } = require('./storage')
const { createDiagnosticLogger } = require('./diagnostics')

const diag = createDiagnosticLogger('CONTACTS')

const MAX_NAME_LENGTH = 100
const UPDATABLE_FIELDS = new Set(['name', 'walletAddress'])

function isValidName (name) {
  return typeof name === 'string' && name.length > 0 && name.length <= MAX_NAME_LENGTH
}

function hasOwn (object, key) {
  return Object.prototype.hasOwnProperty.call(object, key)
}

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

  saveContacts(contacts = this.contacts) {
    const contactList = Array.from(contacts.values())
    const dir = path.dirname(this.storagePath)
    ensureDir(dir)
    writeJSON(this.storagePath, contactList)
  }

  /**
   * Persist the list with one entry replaced (or removed when `contact` is
   * null) and only then publish the change to `this.contacts`.
   * @private
   */
  _commit (publicKey, contact) {
    const next = new Map(this.contacts)
    if (contact) next.set(publicKey, contact)
    else next.delete(publicKey)
    this.saveContacts(next)
    if (contact) this.contacts.set(publicKey, contact)
    else this.contacts.delete(publicKey)
    return contact
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
    if (typeof publicKey !== 'string') throw new Error('Invalid publicKey')
    if (!isValidName(name)) throw new Error('Invalid name')

    return this._commit(publicKey, {
      publicKey,
      name,
      addedAt: Date.now()
    })
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
   * Update a contact. Accepts `name` and `walletAddress`; the address is
   * validated and its derived fields set exactly as updateWalletAddress does.
   * Anything else is rejected rather than silently dropped.
   * @param {string} publicKey - Contact's public key (hex)
   * @param {Object} updates - Fields to update
   * @returns {Object} Updated contact
   */
  async updateContact(publicKey, updates) {
    const contact = this.contacts.get(publicKey)
    if (!contact) {
      throw new Error('Contact not found')
    }
    if (!updates || typeof updates !== 'object' || Array.isArray(updates)) {
      throw new Error('Invalid contact updates')
    }
    const fields = Object.keys(updates)
    if (fields.length === 0) throw new Error('No contact fields to update')
    for (const field of fields) {
      if (!UPDATABLE_FIELDS.has(field)) throw new Error('Unsupported contact field: ' + field)
    }

    const candidate = { ...contact }
    if (hasOwn(updates, 'name')) {
      if (!isValidName(updates.name)) throw new Error('Invalid name')
      candidate.name = updates.name
    }
    if (hasOwn(updates, 'walletAddress')) {
      Object.assign(candidate, this._walletAddressFields(updates.walletAddress))
    }

    return this._commit(publicKey, candidate)
  }

  /**
   * Clear all contacts.
   * Used when creating a new identity so no prior-user data leaks through.
   * A missing file is fine; any other deletion failure is thrown and the
   * in-memory list is left intact, so the caller cannot mistake a surviving
   * file for a completed wipe.
   */
  async clearAll() {
    try {
      fs.unlinkSync(this.storagePath)
    } catch (error) {
      if (!error || error.code !== 'ENOENT') throw error
    }
    this.contacts.clear()
  }

  /**
   * Delete a contact
   * @param {string} publicKey - Contact's public key (hex)
   */
  async deleteContact(publicKey) {
    if (!this.contacts.has(publicKey)) return
    this._commit(publicKey, null)
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

    return this._commit(publicKey, { ...contact, ...this._walletAddressFields(walletAddress) })
  }

  _walletAddressFields (walletAddress) {
    if (!this.isValidZcashAddress(walletAddress)) {
      throw new Error('Invalid Zcash address format')
    }
    return {
      walletAddress,
      addressType: this.detectAddressType(walletAddress),
      addressUpdatedAt: Date.now()
    }
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
