'use strict'

/**
 * Persistent state for group invite links, in chats/group-links.json.
 *
 * Kept out of the conversation records on purpose: ipc-handler sends a whole
 * conversation record to the native side on every list, and a link secret must
 * only leave the worklet when the owner asks to copy or share it.
 *
 * The file sits in the chats folder, so ChatStore.clearAll removes it with the
 * rest of an account; clearAll here resets the in-memory copy to match.
 *
 * Shape:
 *   owner:      conversationId -> { current, retired[], requests[], admissions[] }
 *   joiner:     linkId -> pending or finished join request made by this device
 *   joinedVia:  conversationId -> linkIds this device joined it with
 *   removed:    conversationId -> keys the owner removed from that group
 *   memberCaps: conversationId -> { memberKey: [feature, ...] }
 *   pendingRekey: conversationId -> { memberKey: groupId they were last sent }
 *     members on an older app who get the group's new secret once they
 *     announce they understand it
 *   capsAnnounced: conversationId -> version this device announced there
 *   blockedKeys: keys the app has blocked, pushed down by the native side
 */

const path = require('bare-path')
const { getDataDir, ensureDir, readJSON, writeJSON } = require('./storage')
const { createDiagnosticLogger } = require('./diagnostics')

const diag = createDiagnosticLogger('GLINK')

const FILE_VERSION = 1

function emptyState () {
  return { version: FILE_VERSION, owner: {}, joiner: {}, joinedVia: {}, removed: {}, memberCaps: {}, pendingRekey: {}, capsAnnounced: {}, blockedKeys: [] }
}

function isObject (value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}

class GroupLinkStore {
  /**
   * @param {{filePath?: string}} opts filePath defaults to <data>/chats/group-links.json
   */
  constructor (opts = {}) {
    this.filePath = opts.filePath || path.join(getDataDir(), 'chats', 'group-links.json')
    this.state = emptyState()
    this.load()
  }

  load () {
    const stored = readJSON(this.filePath)
    if (!isObject(stored) || stored.version !== FILE_VERSION) {
      // Unreadable, or written by another version: start empty. The file is
      // replaced on the next change, which only matters after a downgrade.
      if (stored !== null) diag('group-links.json unreadable or from another version; starting empty')
      this.state = emptyState()
      return
    }
    const state = emptyState()
    for (const key of ['owner', 'joiner', 'joinedVia', 'removed', 'memberCaps', 'pendingRekey', 'capsAnnounced']) {
      if (isObject(stored[key])) state[key] = stored[key]
    }
    if (Array.isArray(stored.blockedKeys)) state.blockedKeys = stored.blockedKeys.filter(k => typeof k === 'string')
    this.state = state
  }

  save () {
    ensureDir(path.dirname(this.filePath))
    writeJSON(this.filePath, this.state)
  }

  /** Called when the account is wiped; ChatStore.clearAll already deleted the file. */
  async clearAll () {
    this.state = emptyState()
  }

  // ── Owner ────────────────────────────────────────────────────────────────

  ownerEntry (conversationId, create = false) {
    let entry = this.state.owner[conversationId]
    if (!entry && create) {
      entry = { current: null, retired: [], requests: [], admissions: [] }
      this.state.owner[conversationId] = entry
    }
    return entry || null
  }

  ownerConversationIds () {
    return Object.keys(this.state.owner)
  }

  // ── Joiner ───────────────────────────────────────────────────────────────

  joinerRecord (linkId) {
    return this.state.joiner[linkId] || null
  }

  setJoinerRecord (linkId, record) {
    this.state.joiner[linkId] = record
  }

  deleteJoinerRecord (linkId) {
    delete this.state.joiner[linkId]
  }

  joinerRecords () {
    return Object.values(this.state.joiner)
  }

  addJoinedVia (conversationId, linkId) {
    const list = this.state.joinedVia[conversationId] || []
    if (!list.includes(linkId)) list.push(linkId)
    this.state.joinedVia[conversationId] = list.slice(-16)
  }

  conversationJoinedVia (linkId) {
    for (const [conversationId, list] of Object.entries(this.state.joinedVia)) {
      if (Array.isArray(list) && list.includes(linkId)) return conversationId
    }
    return null
  }

  // ── Removal and capabilities ─────────────────────────────────────────────

  removedKeys (conversationId) {
    return this.state.removed[conversationId] || []
  }

  addRemovedKey (conversationId, key) {
    const list = this.removedKeys(conversationId)
    if (!list.includes(key)) list.push(key)
    this.state.removed[conversationId] = list.slice(-512)
  }

  memberCaps (conversationId) {
    return this.state.memberCaps[conversationId] || {}
  }

  setMemberCaps (conversationId, memberKey, features) {
    const caps = this.memberCaps(conversationId)
    caps[memberKey] = features
    this.state.memberCaps[conversationId] = caps
  }

  pendingRekey (conversationId) {
    return this.state.pendingRekey[conversationId] || {}
  }

  /** Remember the secret a member was last on; the first one wins across rekeys. */
  deferRekey (conversationId, memberKey, fromGroupId) {
    const pending = this.pendingRekey(conversationId)
    if (!pending[memberKey]) pending[memberKey] = fromGroupId
    this.state.pendingRekey[conversationId] = pending
  }

  clearPendingRekey (conversationId, memberKey) {
    const pending = this.state.pendingRekey[conversationId]
    if (!pending) return
    delete pending[memberKey]
    if (Object.keys(pending).length === 0) delete this.state.pendingRekey[conversationId]
  }

  capsAnnounced (conversationId) {
    return this.state.capsAnnounced[conversationId] || 0
  }

  markCapsAnnounced (conversationId, version) {
    this.state.capsAnnounced[conversationId] = version
  }

  // ── Blocked keys ─────────────────────────────────────────────────────────

  setBlockedKeys (keys) {
    this.state.blockedKeys = [...new Set(keys.map(k => k.toLowerCase()))]
  }

  isBlocked (key) {
    return this.state.blockedKeys.includes((key || '').toLowerCase())
  }

  /** Forget everything about one conversation, for example after it is deleted. */
  forgetConversation (conversationId) {
    delete this.state.joinedVia[conversationId]
    delete this.state.removed[conversationId]
    delete this.state.memberCaps[conversationId]
    delete this.state.pendingRekey[conversationId]
    delete this.state.capsAnnounced[conversationId]
  }
}

module.exports = { GroupLinkStore, FILE_VERSION }
