/**
 * Chat Store - Manages conversations and messages
 * 
 * Stores conversation metadata in chats/index.json
 * Stores messages per conversation in chats/{conversationId}.json
 */

const path = require('bare-path')
const crypto = require('bare-crypto')
const hcrypto = require('hypercore-crypto')
const b4a = require('b4a')
const { getDataDir, ensureDir, readJSON, writeJSON, fileExists } = require('./storage')
const { createDiagnosticLogger } = require('./diagnostics')
const config = require('./config')

const { deriveGroupChatTopic } = require('./rooms')
const { validateMessage, MEDIA_DESCRIPTOR_FIELDS } = require('./peer-record')
const diag = createDiagnosticLogger('CHAT')

const MAX_PLATFORM_INT = 0x7fffffff
const MAX_FUTURE_MESSAGE_SKEW_MS = 5 * 60 * 1000
const SUPPORTED_CONVERSATION_TYPES = new Set(['direct', 'group'])
// Thumbnails are UI previews, not the media payload. Keeping the encoded value
// to 1 MiB guarantees a one-thumbnail IPC response stays comfortably below the
// native 16 MiB frame ceiling, even after its JSON envelope is added.
const MAX_THUMBNAIL_DATA_BYTES = 1024 * 1024

function normalizeThumbnailData (value) {
  if (typeof value !== 'string' || value.length === 0) return null
  return b4a.byteLength(value, 'utf8') <= MAX_THUMBNAIL_DATA_BYTES ? value : null
}

function normalizePeerInteger (value) {
  if (typeof value !== 'number' || !Number.isFinite(value)) return null
  return Math.min(Math.max(Math.trunc(value), 0), MAX_PLATFORM_INT)
}

function normalizeMessageTimestamp (value, receivedAt = Date.now()) {
  if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) return receivedAt
  const timestamp = Math.trunc(value)
  return timestamp > receivedAt + MAX_FUTURE_MESSAGE_SKEW_MS ? receivedAt : timestamp
}

function assertSupportedConversationType (type) {
  if (!SUPPORTED_CONVERSATION_TYPES.has(type)) {
    throw new Error('Unsupported conversation type: ' + type)
  }
}

// ── Chronological ordering ──────────────────────────────────────────────────
// Messages do not arrive in order: blind-peer catch-up replays a peer's older
// messages after newer live ones. The persisted per-conversation array is kept
// sorted by timestamp — stable, so same-millisecond messages keep arrival
// order — because positional logic across this store treats "later in the
// array" as "newer": getMessages' newest-N window, the markReadUpTo /
// markDeliveredUpTo cutoffs, the latest-incoming watermark and the storage cap.

function messageTime (message) {
  return (message && typeof message.timestamp === 'number' && Number.isFinite(message.timestamp))
    ? message.timestamp
    : 0
}

function isChronological (messages) {
  for (let i = 1; i < messages.length; i++) {
    if (messageTime(messages[i - 1]) > messageTime(messages[i])) return false
  }
  return true
}

// First index whose message sorts strictly after `message` (upper bound), so
// an equal-timestamp insert lands after what already arrived.
function insertionIndex (messages, message) {
  let lo = 0
  let hi = messages.length
  while (lo < hi) {
    const mid = (lo + hi) >>> 1
    if (messageTime(messages[mid]) <= messageTime(message)) lo = mid + 1
    else hi = mid
  }
  return lo
}

// Normalize every file read, not only message.list. A background message can
// arrive before the app opens a room on an upgraded device, and binary insert,
// positional receipt cutoffs and newest-message lookups all require the legacy
// file to be ordered already.
function readNormalizedMessages (messagesPath, { strict = true } = {}) {
  const stored = readJSON(messagesPath)
  if (strict && stored === null && fileExists(messagesPath)) {
    throw new Error('Message store is unreadable')
  }
  if (stored !== null && !Array.isArray(stored)) throw new Error('Message store is not an array')
  const raw = stored || []
  const seen = new Set()
  const messages = []
  const receivedAt = Date.now()
  let healed = false

  for (const rawMessage of raw) {
    try {
      validateMessage(rawMessage)
    } catch (err) {
      if (err.code !== 'INVALID_PEER_RECORD') throw err
      healed = true
      continue
    }
    // Normalize a copy so a repair backup retains the original field values.
    const message = { ...rawMessage }
    if (message && message.id) {
      if (seen.has(message.id)) continue
      seen.add(message.id)
    }
    if (message) {
      for (const field of ['mediaSize', 'mediaWidth', 'mediaHeight']) {
        const normalized = normalizePeerInteger(message[field])
        if (message[field] != null && normalized !== message[field]) {
          message[field] = normalized
          healed = true
        }
      }
      if (message.thumbnailData != null) {
        const thumbnail = normalizeThumbnailData(message.thumbnailData)
        if (thumbnail !== message.thumbnailData) { message.thumbnailData = thumbnail; healed = true }
      }
      const timestamp = normalizeMessageTimestamp(message.timestamp, receivedAt)
      if (timestamp !== message.timestamp) {
        message.timestamp = timestamp
        healed = true
      }
    }
    messages.push(message)
  }

  healed = healed || messages.length !== raw.length
  if (!isChronological(messages)) {
    messages.sort((a, b) => messageTime(a) - messageTime(b))
    healed = true
  }
  if (healed) {
    // Keep the original history for recovery/inspection before removing poison.
    const backup = messagesPath + '.pre-validation.bak'
    if (!fileExists(backup)) writeJSON(backup, raw)
    writeJSON(messagesPath, messages)
  }
  return messages
}

class ChatStore {
  constructor() {
    this.storagePath = path.join(getDataDir(), 'chats')
    this.conversations = new Map()
    this.leftConversations = new Set()
    // mediaId → Set<conversationId> index for efficient updateMediaPath
    // Note: index is populated at runtime via addMessage(); pre-existing media
    // messages fall back to full conversation scan in updateMediaPath().
    this._mediaIndex = new Map()
    // conversationId → newest peer message id we've already acked with a read
    // receipt. Lets _sendReadReceiptIfNeeded skip redundant receipts.
    this.sentReadWatermarks = new Map()
    this.ensureStorageDir()
    this._loadPersistedState()
  }

  ensureStorageDir() {
    ensureDir(this.storagePath)
  }

  _loadPersistedState () {
    this.loadConversations()
    this.loadLeftConversations()
    this.loadReadReceiptWatermarks()
  }

  loadConversations() {
    try {
      const indexPath = path.join(this.storagePath, 'index.json')
      const convos = readJSON(indexPath)
      if (convos && Array.isArray(convos)) {
        for (const conv of convos) {
          // Keep unsupported legacy records on disk for a non-destructive
          // upgrade, but do not expose them as chats.
          if (!SUPPORTED_CONVERSATION_TYPES.has(conv.type)) continue
          if (!Array.isArray(conv.participantIds)) conv.participantIds = []
          this.conversations.set(conv.id, conv)
        }
      }
    } catch (error) {
      diag('Failed to load conversations:', error)
    }
  }

  loadLeftConversations() {
    try {
      const leftPath = path.join(this.storagePath, 'left.json')
      const data = readJSON(leftPath)
      if (Array.isArray(data)) {
        this.leftConversations = new Set(data)
      }
    } catch (e) { /* ok */ }
  }

  markConversationAsLeft(conversationId) {
    this.leftConversations.add(conversationId)
    const leftPath = path.join(this.storagePath, 'left.json')
    writeJSON(leftPath, Array.from(this.leftConversations))
  }

  hasLeftConversation(conversationId) {
    return this.leftConversations.has(conversationId)
  }

  clearLeftStatus(conversationId) {
    this.leftConversations.delete(conversationId)
    const leftPath = path.join(this.storagePath, 'left.json')
    writeJSON(leftPath, Array.from(this.leftConversations))
  }

  /**
   * Generate a deterministic ID for a direct chat from two pubkey hex strings.
   * Order-independent: keys are sorted, then Blake2b-hashed for a fixed-length,
   * collision-resistant ID that doesn't leak key material.
   * @param {string} keyA - First public key hex
   * @param {string} keyB - Second public key hex
   * @returns {string} Deterministic conversation ID
   */
  static directChatId(keyA, keyB) {
    const sorted = [keyA, keyB].sort()
    const input = b4a.from(sorted[0] + ':' + sorted[1], 'utf8')
    const hash = hcrypto.data(input) // 32-byte Blake2b
    return 'dm_' + b4a.toString(hash, 'hex').substring(0, 32)
  }

  /**
   * Legacy directChatId format (pre-migration).
   * Used only for backward-compatible lookups.
   * @param {string} keyA - First public key hex
   * @param {string} keyB - Second public key hex
   * @returns {string} Legacy conversation ID
   */
  static _legacyDirectChatId(keyA, keyB) {
    const sorted = [keyA, keyB].sort()
    const combined = sorted[0].substring(0, 32) + sorted[1].substring(0, 32)
    return 'dm_' + combined
  }

  /**
   * Find an existing direct chat by trying both new and legacy ID formats.
   * If a legacy conversation is found, migrate it to the new ID.
   * @param {string} keyA - First public key hex
   * @param {string} keyB - Second public key hex
   * @returns {{ id: string, conversation: Object|null, migrated: boolean }}
   */
  resolveDirectChatId(keyA, keyB) {
    const newId = ChatStore.directChatId(keyA, keyB)
    const existing = this.conversations.get(newId)
    if (existing) return { id: newId, conversation: existing, migrated: false }

    // Check legacy format
    const legacyId = ChatStore._legacyDirectChatId(keyA, keyB)
    const legacy = this.conversations.get(legacyId)
    if (legacy) {
      // Migrate: re-key the conversation from legacy ID to new ID
      this.conversations.delete(legacyId)
      legacy.id = newId
      this.conversations.set(newId, legacy)

      // Rename the messages file
      const fs = require('bare-fs')
      const oldPath = path.join(this.storagePath, `${legacyId}.json`)
      const newPath = path.join(this.storagePath, `${newId}.json`)
      try {
        if (fs.existsSync(oldPath)) {
          fs.renameSync(oldPath, newPath)
        }
      } catch (e) {
        diag('Failed to rename messages file during DM migration:', e.message)
      }

      // Also migrate left-status if present
      if (this.leftConversations.has(legacyId)) {
        this.leftConversations.delete(legacyId)
        this.leftConversations.add(newId)
        const leftPath = path.join(this.storagePath, 'left.json')
        writeJSON(leftPath, Array.from(this.leftConversations))
      }

      this.saveConversationsIndex()
      diag('Migrated DM conversation from legacy ID', legacyId.substring(0, 16), 'to', newId.substring(0, 16))
      return { id: newId, conversation: legacy, migrated: true }
    }

    return { id: newId, conversation: null, migrated: false }
  }

  saveConversationsIndex() {
    const indexPath = path.join(this.storagePath, 'index.json')
    const convos = Array.from(this.conversations.values())
    writeJSON(indexPath, convos)
  }

  /**
   * Create a new conversation
   * @param {string} type - Conversation type (direct or group)
   * @param {Array<string>} participantIds - Participant public keys
   * @param {Object} options - Additional options (groupId, creatorKey, displayName)
   * @returns {Object} Created conversation
   */
  async createConversation(type, participantIds, options = {}) {
    assertSupportedConversationType(type)
    const { groupId } = options

    // Check if conversation already exists
    for (const conv of this.conversations.values()) {
      if (conv.type === type) {
        // For groups with a groupId, dedup by groupId
        if (type === 'group' && groupId && conv.groupId === groupId) {
          return conv
        }
        // For direct chats, dedup by participants
        if (type === 'direct' &&
            conv.participantIds.length === participantIds.length &&
            conv.participantIds.every(p => participantIds.includes(p))) {
          return conv
        }
      }
    }

    const id = crypto.randomBytes(16).toString('hex')
    return this.createConversationWithId(id, type, participantIds, options)
  }

  /**
   * Create a new conversation with a specific ID
   * @param {string} conversationId - Conversation ID to use
   * @param {string} type - Conversation type (direct or group)
   * @param {Array<string>} participantIds - Participant public keys
   * @param {Object} options - Additional options (groupId, creatorKey, displayName)
   * @returns {Object} Created conversation
   */
  async createConversationWithId(conversationId, type, participantIds, options = {}) {
    assertSupportedConversationType(type)
    const { groupId, creatorKey, displayName } = options
    const safeParticipants = Array.isArray(participantIds) ? participantIds.filter(Boolean) : []
    const conversation = {
      id: conversationId,
      type,
      participantIds: safeParticipants,
      groupId: type === 'group' ? (groupId || crypto.randomBytes(32).toString('hex')) : undefined,
      creatorKey: type === 'group' ? (creatorKey || undefined) : undefined,
      displayName: displayName || safeParticipants[0]?.substring(0, 8) || 'Unknown',
      lastMessage: null,
      lastMessageTimestamp: null,
      createdAt: Date.now()
    }

    // Initialize history first; a failed index write must not leave an in-memory
    // conversation that a retry mistakes for a durable creation.
    const messagesPath = path.join(this.storagePath, `${conversationId}.json`)
    if (!fileExists(messagesPath)) writeJSON(messagesPath, [])
    this.conversations.set(conversationId, conversation)
    try { this.saveConversationsIndex() } catch (err) {
      this.conversations.delete(conversationId)
      throw err
    }

    return conversation
  }

  /**
   * List all conversations sorted by last message timestamp.
   * Excludes conversations the user has explicitly left — those are tracked in
   * left.json and should never resurface in the UI.
   * @returns {Array<Object>} Sorted conversations
   */
  async listConversations() {
    return Array.from(this.conversations.values())
      .filter(conv => !this.leftConversations.has(conv.id))
      .sort((a, b) => (b.lastMessageTimestamp || 0) - (a.lastMessageTimestamp || 0))
  }

  /**
   * Get a single conversation by ID
   * @param {string} conversationId - Conversation ID
   * @returns {Object|null} Conversation or null
   */
  async getConversation(conversationId) {
    return this.conversations.get(conversationId) || null
  }

  /**
   * Update conversation metadata
   * @param {string} conversationId - Conversation ID
   * @param {Object} updates - Fields to update
   * @returns {Object|null} Updated conversation or null
   */
  async updateConversation(conversationId, updates) {
    const conv = this.conversations.get(conversationId)
    if (!conv) return null
    const previous = { ...conv }
    // Allowlist: only permit known safe fields to be updated
    const allowedFields = ['displayName', 'lastMessage', 'lastMessageTimestamp', 'participantIds', 'groupId', 'creatorKey', 'localCoreKey', 'remoteCoreKeys']
    for (const key of allowedFields) {
      if (updates.hasOwnProperty(key)) {
        conv[key] = updates[key]
      }
    }
    try { this.saveConversationsIndex() } catch (err) {
      Object.keys(conv).forEach(key => delete conv[key])
      Object.assign(conv, previous)
      throw err
    }
    return conv
  }

  /**
   * Delete a conversation and its messages
   * @param {string} conversationId - Conversation ID
   * @returns {boolean} Success status
   */
  async deleteConversation(conversationId) {
    const conv = this.conversations.get(conversationId)
    if (!conv) return false

    this.conversations.delete(conversationId)
    try { this.saveConversationsIndex() } catch (err) {
      this.conversations.set(conversationId, conv)
      throw err
    }

    // Remove message file
    const messagesPath = path.join(this.storagePath, `${conversationId}.json`)
    try {
      const fs = require('bare-fs')
      if (fs.existsSync(messagesPath)) {
        fs.unlinkSync(messagesPath)
      }
    } catch (error) {
      diag('Failed to delete messages file:', error)
    }

    return true
  }

  /**
   * Add a message to a conversation
   * @param {string} conversationId - Conversation ID
   * @param {Object} messageData - Message data
   * @returns {Object} Created message
   */
  async addMessage(conversationId, messageData) {
    validateMessage(messageData)
    const conversation = this.conversations.get(conversationId)
    if (!conversation) {
      throw new Error('Conversation not found')
    }

    const receivedAt = Date.now()
    const message = {
      id: messageData.id || crypto.randomBytes(16).toString('hex'),
      conversationId,
      senderId: messageData.senderId,
      senderName: messageData.senderName || null,
      content: messageData.content || '',
      contentType: messageData.contentType || 'text/plain',
      // Remote peers control their wire timestamp. Keep reasonable clock skew
      // for offline chronology, but do not let a far-future value permanently
      // pin previews, read watermarks, or retention ordering.
      timestamp: normalizeMessageTimestamp(messageData.timestamp, receivedAt),
      isFromMe: messageData.isFromMe || false,
      mediaId: messageData.mediaId || null,
      // Only this local send boundary or verified incoming bytes may grant
      // sharing. A peer-authored hash reference is not proof of possession.
      mediaAuthorized: !!(messageData.mediaId && messageData.isFromMe === true),
      // These values can originate on a remote peer. Keep the persisted wire
      // record inside the common Kotlin/Swift Int range so malformed media
      // metadata cannot poison every later message.list response.
      mediaSize: normalizePeerInteger(messageData.mediaSize),
      mediaWidth: normalizePeerInteger(messageData.mediaWidth),
      mediaHeight: normalizePeerInteger(messageData.mediaHeight),
      // A remote participant controls this field. Never persist an arbitrarily
      // large inline preview: lazy hydration must always produce a bounded IPC
      // response, even for hostile or malformed messages.
      thumbnailData: normalizeThumbnailData(messageData.thumbnailData),
      // validateMessage above accepted the descriptor whole or rejected it.
      mediaCoreKey: messageData.mediaCoreKey || null,
      mediaBlockOffset: normalizePeerInteger(messageData.mediaBlockOffset),
      mediaBlockLength: normalizePeerInteger(messageData.mediaBlockLength),
      mediaLocalPath: messageData.mediaLocalPath || null,
      mediaTransferState: messageData.mediaTransferState || null,
      replyToId: messageData.replyToId || null,
      replyToSenderName: messageData.replyToSenderName || null,
      replyToContent: messageData.replyToContent || null,
      // MIME type of the quoted message, so a client can label a quoted photo,
      // file or payment request instead of echoing its raw body. Absent on
      // records from older clients, which the UI reads as a text quote.
      replyToContentType: messageData.replyToContentType || null,
      status: messageData.status || (messageData.isFromMe ? 'queued' : null)
    }

    // Load existing messages
    const messagesPath = path.join(this.storagePath, `${conversationId}.json`)
    let messages = readNormalizedMessages(messagesPath, { strict: true })

    // Dedup: same message can arrive via the live socket AND the per-peer
    // Hypercore (blind-peer replication). Returning null lets callers
    // suppress the duplicate IPC `message.received` event so the UI's
    // LazyColumn doesn't see two items with the same key.
    if (message.id && messages.some(m => m.id === message.id)) {
      // A previous attempt may have committed the row but failed its index write.
      this._saveMessagePreview(conversation, messages)
      return null
    }

    // Insert at the chronological position, not the tail: a message replicated
    // late (blind-peer catch-up) is older than what's already stored and must
    // not render at the bottom of the room.
    messages.splice(insertionIndex(messages, message), 0, message)

    // Cap messages per conversation to prevent unbounded file growth
    const cap = config.MAX_MESSAGES_PER_CONVERSATION
    if (messages.length > cap) {
      diag('Chat', conversationId.substring(0, 12), ': trimmed', messages.length - cap, 'oldest message(s) at cap', cap)
      messages = messages.slice(-cap)
      if (!messages.includes(message)) {
        // The late insert itself fell outside retention. Persist the trim but
        // report null — like the dedup path — so callers suppress the UI event.
        writeJSON(messagesPath, messages)
        return null
      }
    }

    // Save messages
    writeJSON(messagesPath, messages)

    // Update media index for efficient lookups
    if (message.mediaId) {
      if (!this._mediaIndex.has(message.mediaId)) {
        this._mediaIndex.set(message.mediaId, new Set())
      }
      this._mediaIndex.get(message.mediaId).add(conversationId)
    }

    this._saveMessagePreview(conversation, messages)
    return message
  }

  _saveMessagePreview(conversation, messages) {
    const newest = messages[messages.length - 1]
    const ct = newest.contentType || 'text/plain'
    if (ct.startsWith('image/gif')) {
      conversation.lastMessage = '[GIF]'
    } else if (ct.startsWith('image/')) {
      conversation.lastMessage = '[Photo]'
    } else if (ct.startsWith('video/')) {
      conversation.lastMessage = '[Video]'
    } else {
      conversation.lastMessage = (newest.content || '').substring(0, 100)
    }
    conversation.lastMessageTimestamp = newest.timestamp
    this.saveConversationsIndex()
  }

  conversationForGroupTopic(topic) {
    for (const conv of this.conversations.values()) {
      if (conv.type === 'group' && !this.hasLeftConversation(conv.id) &&
          b4a.toString(deriveGroupChatTopic(conv.groupId), 'hex') === topic) return conv.id
    }
    return null
  }

  isPeerAuthorized(conversationId, peer) {
    const conv = this.conversations.get(conversationId)
    if (!conv || this.hasLeftConversation(conversationId)) return false
    return (conv.participantIds || []).some(k => k.toLowerCase() === peer.toLowerCase())
  }

  canServeMedia(mediaId, peer) {
    // Scan persisted references too: the runtime media index is only a cache.
    for (const conv of this.conversations.values()) {
      if (!this.isPeerAuthorized(conv.id, peer)) continue
      const messages = readNormalizedMessages(path.join(this.storagePath, conv.id + '.json'), { strict: true })
      if (messages.some(m => m.mediaId === mediaId && m.mediaAuthorized === true)) return true
    }
    return false
  }

  hasAuthorizedMediaReference(conversationId, mediaId) {
    if (!this.conversations.has(conversationId) || this.hasLeftConversation(conversationId)) return false
    const messages = readNormalizedMessages(path.join(this.storagePath, conversationId + '.json'), { strict: true })
    return messages.some(m => m.mediaId === mediaId && m.mediaAuthorized === true)
  }

  /**
   * The media-core descriptor of an image we already sent in this
   * conversation, so a resend reuses the blocks instead of appending again.
   * @returns {{mediaCoreKey: string, mediaBlockOffset: number, mediaBlockLength: number}|null}
   */
  findOwnMediaDescriptor(conversationId, mediaId) {
    if (!this.conversations.has(conversationId)) return null
    const messages = readNormalizedMessages(path.join(this.storagePath, conversationId + '.json'), { strict: true })
    const message = messages.find(m => m.isFromMe === true && m.mediaId === mediaId && m.mediaCoreKey != null)
    if (!message) return null
    const descriptor = {}
    for (const field of MEDIA_DESCRIPTOR_FIELDS) descriptor[field] = message[field]
    return descriptor
  }

  authorizeReceivedMedia(conversationId, mediaId) {
    if (!this.conversations.has(conversationId) || this.hasLeftConversation(conversationId)) return false
    const messagesPath = path.join(this.storagePath, conversationId + '.json')
    const messages = readNormalizedMessages(messagesPath, { strict: true })
    let changed = false
    for (const message of messages) {
      if (message.mediaId === mediaId && message.mediaAuthorized !== true) {
        message.mediaAuthorized = true
        changed = true
      }
    }
    if (changed) writeJSON(messagesPath, messages)
    return changed
  }

  hasAppliedControl(conversationId, key) {
    const conv = this.conversations.get(conversationId)
    return !!(conv && (conv.appliedControls || []).includes(key))
  }

  markControlApplied(conversationId, key) {
    const conv = this.conversations.get(conversationId)
    if (!conv) return
    const previous = conv.appliedControls
    conv.appliedControls = [...new Set([...(previous || []), key])].slice(-5000)
    try { this.saveConversationsIndex() } catch (err) {
      conv.appliedControls = previous
      throw err
    }
  }

  /**
   * Update mediaLocalPath for all messages matching a mediaId
   * @param {string} mediaId - Media hash (hex)
   * @param {string} mediaLocalPath - Local file path
   */
  updateMediaPath(mediaId, mediaLocalPath) {
    const fs = require('bare-fs')
    // Use media index for targeted lookup instead of scanning all conversations
    const convIds = this._mediaIndex.get(mediaId)
    const targets = convIds && convIds.size > 0
      ? convIds
      : this.conversations.keys() // fallback: scan all if index misses (e.g. older messages)

    for (const convId of targets) {
      const messagesPath = path.join(this.storagePath, `${convId}.json`)
      try {
        if (!fs.existsSync(messagesPath)) continue
        const messages = readNormalizedMessages(messagesPath)
        let changed = false
        for (const msg of messages) {
          if (msg.mediaId === mediaId && !msg.mediaLocalPath) {
            msg.mediaLocalPath = mediaLocalPath
            changed = true
          }
        }
        if (changed) {
          writeJSON(messagesPath, messages)
        }
      } catch (err) {
        diag('Failed to update media path for', convId.substring(0, 12), err)
      }
    }
  }

  /**
   * Mark our own outgoing messages as read, up to and including upToMessageId.
   * Called on the sender's device when a read receipt arrives from the peer.
   * Read is monotonic: a message never leaves the 'read' state.
   *
   * @param {string} conversationId
   * @param {string} upToMessageId - One of our outgoing message ids (the newest
   *   the peer reports having read). Resolved against the store's chronological
   *   order, so everything we sent before it is implicitly read too.
   * @returns {Array<string>} ids of messages that transitioned to 'read'
   */
  markReadUpTo(conversationId, upToMessageId) {
    if (!upToMessageId) return []
    const messagesPath = path.join(this.storagePath, `${conversationId}.json`)
    const messages = readNormalizedMessages(messagesPath, { strict: true })

    // The receipt names one of our messages; we don't have it if it was trimmed
    // at the cap or never reached us. Without a position there's nothing to mark.
    const cutoff = messages.findIndex(m => m && m.id === upToMessageId)
    if (cutoff < 0) return []

    const changed = []
    for (let i = 0; i <= cutoff; i++) {
      const m = messages[i]
      if (m && m.isFromMe && m.status !== 'read') {
        m.status = 'read'
        changed.push(m.id)
      }
    }
    if (changed.length > 0) writeJSON(messagesPath, messages)
    return changed
  }

  markDeliveredUpTo(conversationId, upToMessageId) {
    if (!upToMessageId) return []
    const messagesPath = path.join(this.storagePath, `${conversationId}.json`)
    const messages = readNormalizedMessages(messagesPath, { strict: true })

    const cutoff = messages.findIndex(m => m && m.id === upToMessageId)
    if (cutoff < 0) return []

    const changed = []
    for (let i = 0; i <= cutoff; i++) {
      const message = messages[i]
      if (message && message.isFromMe &&
          message.status !== 'delivered' && message.status !== 'read') {
        message.status = 'delivered'
        changed.push(message.id)
      }
    }
    if (changed.length > 0) writeJSON(messagesPath, messages)
    return changed
  }

  /**
   * Mark specific outgoing messages as accepted by the blind relay. This is
   * deliberately weaker than recipient delivery: a relay acknowledgement
   * proves the encrypted block was fetched to build its doorbell proof, while
   * only a recipient receipt advances the message to 'delivered'.
   *
   * Monotonic in the same way as the watermark helpers: delivered and read
   * messages are left alone.
   * @param {string} conversationId
   * @param {Array<string>} messageIds
   * @returns {Array<string>} ids that transitioned to 'sent' (relayed)
   */
  markRelayedByIds(conversationId, messageIds) {
    if (!Array.isArray(messageIds) || messageIds.length === 0) return []
    const messagesPath = path.join(this.storagePath, `${conversationId}.json`)
    const messages = readNormalizedMessages(messagesPath, { strict: true })

    const wanted = new Set(messageIds)
    const changed = []
    for (const message of messages) {
      if (!message || !message.isFromMe || !wanted.has(message.id)) continue
      if (message.status === 'sent' || message.status === 'delivered' || message.status === 'read') {
        continue
      }
      message.status = 'sent'
      changed.push(message.id)
    }
    if (changed.length > 0) writeJSON(messagesPath, messages)
    return changed
  }

  getPendingOutgoingMessages(conversationId) {
    const messagesPath = path.join(this.storagePath, `${conversationId}.json`)
    try {
      return readNormalizedMessages(messagesPath).filter(message =>
        message && message.isFromMe &&
        (message.status === 'queued' || message.status === 'sent')
      )
    } catch (err) {
      return []
    }
  }

  /**
   * Id of the newest message in a conversation authored by the peer (not us).
   * This is the watermark a read receipt reports back to the original sender.
   * @returns {string|null}
   */
  getLatestIncomingMessageId(conversationId) {
    return this.getLatestIncomingMessage(conversationId)?.id || null
  }

  getLatestIncomingMessage(conversationId) {
    const messagesPath = path.join(this.storagePath, `${conversationId}.json`)
    let messages
    try {
      messages = readNormalizedMessages(messagesPath)
    } catch (err) {
      return null
    }
    for (let i = messages.length - 1; i >= 0; i--) {
      const message = messages[i]
      if (message && !message.isFromMe && message.id) return message
    }
    return null
  }

  getLatestIncomingMessages(conversationId) {
    const messagesPath = path.join(this.storagePath, `${conversationId}.json`)
    let messages
    try {
      messages = readNormalizedMessages(messagesPath)
    } catch (err) {
      return []
    }
    const latestBySender = new Map()
    for (let i = 0; i < messages.length; i++) {
      const m = messages[i]
      if (m && !m.isFromMe && m.id && m.senderId) latestBySender.set(m.senderId, m)
    }
    return Array.from(latestBySender.values())
  }

  getSentReadWatermark(conversationId) {
    return this.sentReadWatermarks.get(conversationId) || null
  }

  setSentReadWatermark(conversationId, messageId) {
    this.sentReadWatermarks.set(conversationId, messageId)
    const wmPath = path.join(this.storagePath, 'read-receipts.json')
    writeJSON(wmPath, Object.fromEntries(this.sentReadWatermarks))
  }

  loadReadReceiptWatermarks() {
    try {
      const wmPath = path.join(this.storagePath, 'read-receipts.json')
      const data = readJSON(wmPath)
      if (data && typeof data === 'object') {
        for (const [convId, messageId] of Object.entries(data)) {
          if (messageId) this.sentReadWatermarks.set(convId, messageId)
        }
      }
    } catch (e) { /* ok — no receipts sent yet */ }
  }

  /**
   * Clear all conversations, messages, and left-conversation records.
   * Used when creating a new identity so no prior-user data leaks through.
   * A missing directory or file is fine. Every other file is still attempted,
   * but the first real deletion failure is thrown after the in-memory state
   * has been reloaded from whatever survived on disk, so a partial wipe is
   * neither reported as complete nor hidden behind stale memory.
   */
  async clearAll() {
    const fs = require('bare-fs')
    let entries = []
    try {
      entries = fs.readdirSync(this.storagePath)
    } catch (error) {
      if (!error || error.code !== 'ENOENT') throw error
    }

    let failure = null
    for (const entry of entries) {
      try {
        fs.unlinkSync(path.join(this.storagePath, entry))
      } catch (error) {
        if (error && error.code === 'ENOENT') continue
        diag('Failed to delete chat file:', error)
        if (!failure) failure = error
      }
    }

    this.conversations.clear()
    this.leftConversations.clear()
    this._mediaIndex.clear()
    this.sentReadWatermarks.clear()
    if (failure) {
      this._loadPersistedState()
      throw failure
    }
  }

  /**
   * Get messages for a conversation
   * @param {string} conversationId - Conversation ID
   * @param {number} limit - Maximum number of messages to return
   * @returns {Array<Object>} The newest `limit` messages, oldest → newest
   */
  async getMessages(conversationId, limit = 50) {
    const messagesPath = path.join(this.storagePath, `${conversationId}.json`)

    try {
      const messages = readNormalizedMessages(messagesPath)
      return messages.slice(-limit)
    } catch (error) {
      diag('Failed to get messages:', error)
      return []
    }
  }

  /**
   * Return one message's base64 thumbnail by id, or null. Backs the lazy
   * message.get_thumbnail IPC so thumbnails are fetched per-message instead of
   * inlined into the message.list frame.
   * @param {string} conversationId
   * @param {string} messageId
   * @returns {string|null}
   */
  async getMessageThumbnail(conversationId, messageId) {
    if (!conversationId || !messageId) return null
    const messagesPath = path.join(this.storagePath, `${conversationId}.json`)
    try {
      const raw = readNormalizedMessages(messagesPath)
      for (const m of raw) {
        if (m && m.id === messageId) return m.thumbnailData || null
      }
      return null
    } catch (error) {
      diag('Failed to get message thumbnail:', error)
      return null
    }
  }
}

module.exports = { ChatStore, MAX_THUMBNAIL_DATA_BYTES }
