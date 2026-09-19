/**
 * IPC Handler - Routes messages between Swift/Kotlin UI and JavaScript core
 *
 * Uses newline-delimited JSON (NDJSON) framing over the raw BareKit IPC
 * byte stream. Each message is a single JSON object followed by '\n'.
 */

const { deriveGroupChatTopic } = require('./rooms')
const { normalizePeerRecord } = require('./peer-record')

const fs = require('bare-fs')
const bareIpc = require('bare-ipc')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const { ChatStore } = require('./chat-store')
const mnemonic = require('./mnemonic')
const config = require('./config')
const { IPCRequestError, validateDirectParticipant } = require('./direct-recipient')
const { isMediaId } = require('./media-id')
const mediaBlobs = require('./media-blobs')
const { GroupLinkStore } = require('./group-link-store')
const { GroupLinkService } = require('./group-link-service')
const { createDiagnosticLogger } = require('./diagnostics')

// Protocol version — bump when IPC message format changes
const PROTOCOL_VERSION = '1.0'
const SUPPORTED_FEATURES = ['messaging', 'groups', 'media', 'blind_peer', 'contacts', 'payments', 'read_receipts', 'group_links']
const MAX_LINK_CHARS = 16 * 1024

const diag = createDiagnosticLogger('IPC')

// Input validation helpers
function isValidPublicKey(value) {
  return typeof value === 'string' && /^[0-9a-fA-F]{64}$/.test(value)
}

function isValidString(value, maxLen) {
  return typeof value === 'string' && value.length > 0 && value.length <= maxLen
}

function isPlainObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}

class IPCHandler {
  constructor({ identity, chatStore, contactStore, p2pManager, mediaStore, mediaTransfer, blindMirror, hypercoreManager, ensureBlindMirror, groupLinkStore }) {
    this.identity = identity
    this.chatStore = chatStore
    this.contactStore = contactStore
    this.p2pManager = p2pManager
    this.mediaStore = mediaStore || null
    this.mediaTransfer = mediaTransfer || null
    this.blindMirror = blindMirror || null
    this.hypercoreManager = hypercoreManager || null
    // Callback to (re-)wire the blind-peer mirror after the swarm restarts.
    // Provided by core/index.js so the IPC handler doesn't need to know the
    // BlindMirror class itself.
    this.ensureBlindMirror = typeof ensureBlindMirror === 'function' ? ensureBlindMirror : (() => {})
    // Read receipts: send our 'read' acks and surface incoming ones. Symmetric —
    // turning this off stops both. Defaults on; native pushes the user's setting
    // via message.set_read_receipts on startup.
    this.readReceiptsEnabled = true
    this.ipc = (typeof Bare !== 'undefined' && Bare.IPC) ? Bare.IPC : bareIpc
    diag('IPC instance: ' + (this.ipc ? 'available' : 'null') + ', Bare.IPC: ' + (typeof Bare !== 'undefined' && Bare.IPC ? 'yes' : 'no') + ', bare-ipc: ' + (bareIpc ? 'yes' : 'no'))
    // IPC is a byte stream: a UTF-8 code point may straddle arbitrary reads.
    // Keep raw bytes until a complete NDJSON line is available.
    this._recvBuf = b4a.alloc(0)
    this._skippingOversizedFrame = false
    this._maxRecvBufSize = config.IPC_MAX_RECV_BUF_SIZE

    if (this.ipc) {
      this.setupMessageHandler()
    } else {
      diag('IPCHandler: IPC not available, message handler not set up')
    }

    // Listen for control messages from P2P manager
    this.p2pManager.on('direct_invite', (inviteData, senderPeerId) => {
      this._handleDirectInvite(inviteData, senderPeerId)
    })
    this.p2pManager.on('group_invite', (inviteData, senderPeerId) => {
      this._handleGroupInvite(inviteData, senderPeerId)
    })
    this.p2pManager.on('group_leave', (data, senderPeerId) => {
      this._handleGroupLeave(data, senderPeerId)
    })
    this.p2pManager.on('group_deleted', (data, senderPeerId) => {
      this._handleGroupDeleted(data, senderPeerId)
    })
    this.p2pManager.on('group_renamed', (data, senderPeerId) => {
      this._handleGroupRenamed(data, senderPeerId)
    })
    this.p2pManager.on('group_member_added', (data, senderPeerId) => {
      this._handleGroupMemberAdded(data, senderPeerId)
    })

    // Group invite links. The service holds the protocol; this handler gives
    // it the mailbox transport and the owner's add member path.
    const p2p = this.p2pManager
    this.groupLinks = new GroupLinkService({
      store: groupLinkStore || new GroupLinkStore(),
      chatStore,
      identity,
      transport: {
        put: (senderKeyPair, recipientHex, record) =>
          typeof p2p.putMailboxAs === 'function' ? p2p.putMailboxAs(senderKeyPair, recipientHex, record) : false,
        drain: (keyPair, deliver) =>
          typeof p2p.drainMailboxAs === 'function' ? p2p.drainMailboxAs(keyPair, deliver) : false,
        sendInvite: (recipientHex, record) => p2p.sendInvite(recipientHex, record),
        drainOwn: () => typeof p2p._drainInviteMailboxes === 'function' ? p2p._drainInviteMailboxes() : Promise.resolve()
      },
      admit: (conversationId, joinerKey, joinerName, viaLink, options) =>
        this._admitViaLink(conversationId, joinerKey, joinerName, viaLink, options),
      emit: (type, payload) => this.pushEvent(type, payload)
    })
    if (typeof p2p.setAuxiliaryDrain === 'function') {
      p2p.setAuxiliaryDrain(() => this.groupLinks.onMailboxDrain())
    }
  }

  _conversationForGroupTopic(topic) {
    if (!topic) return null
    // Restore can drain a core before its swarm topic has been joined.
    for (const conv of this.chatStore.conversations.values()) {
      if (conv.type === 'group' && conv.groupId &&
          b4a.toString(deriveGroupChatTopic(conv.groupId), 'hex') === topic) return conv.id
    }
    return this.p2pManager.groupTopicToConversation.get(topic) || null
  }

  async handlePeerControl(record, peer) {
    const handlers = {
      direct_invite: '_handleDirectInvite', group_invite: '_handleGroupInvite',
      group_leave: '_handleGroupLeave', group_deleted: '_handleGroupDeleted',
      group_renamed: '_handleGroupRenamed', group_member_added: '_handleGroupMemberAdded'
    }
    const method = handlers[record.type]
    if (!method) return
    if (await this[method](record, peer, { waitForDrain: false }) === false) throw new Error('Peer control persistence failed')
  }

  setupMessageHandler() {
    if (!this.ipc) {
      diag('setupMessageHandler: IPC not available, trying process.stdin fallback')
      this._setupStdinHandler()
      return
    }

    diag('Setting up IPC message handler, ipc type: ' + (this.ipc === Bare.IPC ? 'Bare.IPC' : 'bare-ipc'))
    
    this.ipc.on('data', (data) => {
      diag('IPC data received: ' + data.length + ' bytes')
      this._handleIPCData(data)
    })
    
    // Also try stdin as fallback for BareKit
    this._setupStdinHandler()
  }

  _setupStdinHandler() {
    // BareKit on Android uses stdin/stdout for IPC instead of Bare.IPC
    if (typeof process !== 'undefined' && process.stdin) {
      diag('Setting up stdin handler for BareKit IPC')
      process.stdin.on('data', (data) => {
        diag('Stdin data received: ' + data.length + ' bytes')
        this._handleIPCData(data)
      })
      process.stdin.resume()
    } else {
      diag('process.stdin not available')
    }
  }

  _handleIPCData(data) {
    if (!data || data.length === 0) return

    this._recvBuf = b4a.concat([this._recvBuf, b4a.from(data)])

    // Process all complete newline-delimited messages
    let newlineIdx
    while ((newlineIdx = b4a.indexOf(this._recvBuf, 0x0a)) !== -1) {
      const lineBytes = this._recvBuf.subarray(0, newlineIdx)
      this._recvBuf = this._recvBuf.subarray(newlineIdx + 1)

      if (this._skippingOversizedFrame) {
        this._skippingOversizedFrame = false
        continue
      }

      // Enforce the cap even when a complete oversized line arrives in one
      // read; a post-drain buffer check alone cannot see that case.
      if (lineBytes.byteLength > this._maxRecvBufSize) {
        this._reportIPCBufferOverflow(lineBytes.byteLength)
        continue
      }

      if (lineBytes.byteLength === 0) continue
      const line = b4a.toString(lineBytes, 'utf8')
      // _processLine settles every outcome itself; this catch is the backstop
      // that keeps a native-side frame from ever reaching the global rejection
      // handler.
      this._processLine(line).catch(error => {
        diag('IPC line handling failed: ' + (error && error.message ? error.message : error))
      })
    }

    // An unterminated frame crossed the cap. Discard only that frame's bytes,
    // then ignore input up to its newline while preserving later frames.
    if (this._recvBuf.byteLength > this._maxRecvBufSize) {
      if (!this._skippingOversizedFrame) this._reportIPCBufferOverflow(this._recvBuf.byteLength)
      this._skippingOversizedFrame = true
      this._recvBuf = b4a.alloc(0)
    }
  }

  _reportIPCBufferOverflow (size) {
    diag('IPC receive frame overflow — discarding ' + size + ' bytes')
    this.pushEvent('ipc.error', {
      code: 'BUFFER_OVERFLOW',
      message: 'IPC frame exceeded limit and was discarded'
    })
  }

  /**
   * Parse and dispatch one NDJSON line from the native side.
   *
   * Every outcome is controlled: a line that is not a request envelope is
   * dropped with an `ipc.error` event (or an error response when it at least
   * carries a usable id), a request that fails gets an error response, and a
   * response that cannot be written is logged. The returned promise never
   * rejects.
   */
  async _processLine(line) {
    let message
    try {
      message = JSON.parse(line)
    } catch (error) {
      diag('Failed to parse IPC message')
      this.pushEvent('ipc.error', { code: 'MALFORMED_FRAME', message: 'IPC frame is not valid JSON' })
      return
    }

    if (!isPlainObject(message) || !isValidString(message.id, 256)) {
      diag('IPC frame is not a request envelope')
      this.pushEvent('ipc.error', { code: 'INVALID_ENVELOPE', message: 'IPC frame is not a request envelope' })
      return
    }
    const { id, type, payload } = message
    if (!isValidString(type, 256) || (payload != null && !isPlainObject(payload))) {
      diag('IPC request has an invalid type or payload')
      this._respond(id, false, null, { code: 'INVALID_ENVELOPE', message: 'IPC request has an invalid type or payload' })
      return
    }

    let result
    try {
      diag('Routing message: ' + type)
      result = await this.routeMessage(type, payload || {})
      diag('Message routed successfully: ' + type)
    } catch (error) {
      diag('Error handling ' + type + ': ' + (error.message || error))
      this._respond(id, false, null, {
        code: typeof error.code === 'string' ? error.code : 'ERROR',
        message: error.message || 'Unknown error'
      })
      return
    }
    this._respond(id, true, result)
  }

  /**
   * Write a response without letting a serialization or transport failure
   * escape. A result that cannot be written is downgraded to an error response
   * so the native caller is released instead of waiting for its timeout.
   */
  _respond (id, success, data, error = null) {
    try {
      this.sendResponse(id, success, data, error)
    } catch (err) {
      diag('Failed to send IPC response: ' + (err.message || err))
      if (!success) return
      try {
        this.sendResponse(id, false, null, { code: 'RESPONSE_FAILED', message: 'Response could not be delivered' })
      } catch (_) {
        // The transport is gone; the caller's timeout is the remaining signal.
      }
    }
  }

  async routeMessage(type, payload) {
    const [category, action] = type.split('.')

    switch (category) {
      case 'protocol':
        return this.handleProtocol(action, payload)
      case 'identity':
        return this.handleIdentity(action, payload)
      case 'contacts':
        return this.handleContacts(action, payload)
      case 'conversation':
        return this.handleConversation(action, payload)
      case 'message':
        return this.handleMessage(action, payload)
      case 'connection':
        return this.handleConnection(action, payload)
      case 'migration':
        return this.handleMigration(action, payload)
      case 'media':
        return this.handleMedia(action, payload)
      case 'blind_peer':
        return this.handleBlindPeer(action, payload)
      case 'push':
        return this.handlePush(action, payload)
      case 'platform':
        return this.handlePlatform(action, payload)
      case 'group_link':
        return this.handleGroupLink(action, payload)
      default:
        throw new Error(`Unknown message type: ${type}`)
    }
  }

  /**
   * Apply an invite that arrived through the bootstrap mailbox.
   *
   * The mailbox holds the only copy of a bootstrap invite, so the caller deletes
   * it server-side only once this resolves true. Anything unrecognised is
   * reported as finished: retrying it forever would starve real invites behind
   * it.
   * @param {Object} invite
   * @param {string} senderKeyHex verified sender, from the envelope signature
   * @returns {Promise<boolean>}
   */
  async deliverMailboxInvite (invite, senderKeyHex) {
    if (invite.type === 'direct_invite') {
      return await this._handleDirectInvite(invite, senderKeyHex)
    }
    if (invite.type === 'group_invite') {
      return await this._handleGroupInvite(invite, senderKeyHex)
    }
    if (invite.type === 'group_join_result') {
      return this.groupLinks.handleJoinResult(invite, senderKeyHex)
    }
    diag('Discarding unsupported mailbox invite type: ' + invite.type)
    return true
  }

  /**
   * Group invite link requests. The link itself (a bearer secret) only leaves
   * the worklet in the owner's get, enable, update and reset results.
   */
  async handleGroupLink (action, payload) {
    const conversationId = () => {
      if (!isValidString(payload.conversationId, 256)) throw new IPCRequestError('INVALID_CONVERSATION', 'conversationId is required')
      return payload.conversationId
    }
    const link = () => {
      if (!isValidString(payload.link, MAX_LINK_CHARS)) throw new IPCRequestError('INVALID_LINK', 'link is required')
      return payload.link
    }
    const options = () => {
      const out = {}
      for (const key of ['expiresAt', 'maxJoins', 'includeName', 'approval']) {
        if (Object.prototype.hasOwnProperty.call(payload, key)) out[key] = payload[key]
      }
      return out
    }
    const joinerKey = () => {
      const key = (payload.joinerKey || '').toLowerCase().replace(/^0x/, '')
      if (!isValidPublicKey(key)) throw new IPCRequestError('INVALID_KEY', 'joinerKey is required')
      return key
    }
    const links = this.groupLinks
    switch (action) {
      case 'get': return links.getLink(conversationId())
      case 'enable': return links.enableLink(conversationId(), options())
      case 'update': return links.updateLink(conversationId(), options())
      case 'reset': return links.resetLink(conversationId())
      case 'disable': return links.disableLink(conversationId())
      case 'requests': return { requests: links.listRequests(conversationId()) }
      case 'approve': return await links.approve(conversationId(), joinerKey())
      case 'decline': return await links.decline(conversationId(), joinerKey())
      case 'inspect': return links.inspect(link())
      case 'join': {
        const name = isValidString(payload.joinerName, 100) ? payload.joinerName : null
        return await links.join(link(), name)
      }
      case 'join_status': return { requests: links.joinStatus() }
      case 'cancel': {
        if (!isValidString(payload.linkId, 64)) throw new IPCRequestError('INVALID_LINK_ID', 'linkId is required')
        return { cancelled: links.cancel(payload.linkId) }
      }
      default:
        throw new Error(`Unknown group_link action: ${action}`)
    }
  }

  /** The identity payload the native side expects. */
  _identityPayload () {
    return {
      publicKey: this.identity.publicKeyHex,
      displayName: this.identity.displayName
    }
  }

  /**
   * Replace the loaded identity: stop the transport, wipe every account-scoped
   * store, run `install`, then bring the transport up on the new keypair.
   *
   * Nothing from the previous account may survive into the next one, so a
   * wipe or install failure aborts the transition and is reported to the
   * caller. The previous identity is still the one loaded at that point; its
   * transport is restarted so the failure leaves a working (if partially
   * wiped) account that can retry, rather than a silently offline process.
   */
  async _replaceIdentity (install) {
    await this.p2pManager.stop()
    // Honor-system prefs return to their defaults for the fresh identity; native
    // re-pushes the new user's real values once its settings load.
    const previousReadReceipts = this.readReceiptsEnabled
    this.readReceiptsEnabled = true
    try {
      await this._wipeAccountData()
      await install()
    } catch (error) {
      this.readReceiptsEnabled = previousReadReceipts
      await this._restartPreviousIdentity()
      throw error
    }
    await this.p2pManager.start(this.identity.keyPair)
    await this.ensureBlindMirror()
  }

  /**
   * Both stores are attempted even when the first fails, so one bad file
   * cannot leave the other store's data behind unnoticed. Each store leaves
   * its memory consistent with disk on failure; the first error is rethrown.
   */
  async _wipeAccountData () {
    let failure = null
    for (const store of [this.chatStore, this.contactStore, this.groupLinks && this.groupLinks.store]) {
      if (!store) continue
      try {
        await store.clearAll()
      } catch (error) {
        if (!failure) failure = error
      }
    }
    if (failure) throw failure
  }

  async _restartPreviousIdentity () {
    if (!this.identity.keyPair) return
    try {
      await this.p2pManager.start(this.identity.keyPair)
      await this.ensureBlindMirror()
    } catch (error) {
      diag('Transport restart after failed identity replacement failed: ' + (error.message || error))
    }
  }

  async handlePlatform (action, payload) {
    switch (action) {
      case 'http_response':
        if (!payload || typeof payload.requestId !== 'string') {
          throw new Error('platform http_response requires requestId')
        }
        return {
          accepted: this.p2pManager.completePlatformHttpRequest(payload)
        }
      default:
        throw new Error(`Unknown platform action: ${action}`)
    }
  }

  // Protocol negotiation handler
  async handleProtocol (action, payload) {
    switch (action) {
      case 'init': {
        const clientVersion = payload.protocolVersion || '0.0'
        const clientMajor = parseInt(clientVersion.split('.')[0], 10)
        const serverMajor = parseInt(PROTOCOL_VERSION.split('.')[0], 10)
        const compatible = clientMajor === serverMajor
        diag('Protocol init: client=' + clientVersion + ' server=' + PROTOCOL_VERSION + ' compatible=' + compatible)
        return {
          protocolVersion: PROTOCOL_VERSION,
          features: SUPPORTED_FEATURES,
          compatible
        }
      }
      default:
        throw new Error('Unknown protocol action: ' + action)
    }
  }

  // Blind-peer relay handlers
  async handleBlindPeer (action, payload) {
    if (!this.blindMirror) {
      return { enabled: false, reason: 'not_initialized' }
    }
    switch (action) {
      case 'status':
        return this.blindMirror.getDebugInfo()
      case 'set_keys':
        if (!Array.isArray(payload.keys)) throw new Error('keys must be an array')
        this.blindMirror.setKeys(payload.keys)
        return { ok: true, keyCount: payload.keys.length }
      default:
        throw new Error(`Unknown blind_peer action: ${action}`)
    }
  }

  // Push-notification registration handlers
  async handlePush (action, payload) {
    switch (action) {
      case 'topics': {
        const conversations = await this.chatStore.listConversations()
        const direct = conversations
          .filter(conversation => conversation.type === 'direct')
          .map(conversation => {
            const inboundTopics = this.hypercoreManager
              ? this.hypercoreManager.getInboundPushTopics(conversation.id)
              : []
            return {
              conversationId: conversation.id,
              lifecycle: inboundTopics.length > 0 ? 'ready' : 'awaiting_remote_core',
              inboundTopics
            }
          })
        return {
          version: 1,
          hydrated: this.hypercoreManager
            ? this.hypercoreManager.isPushTopicSnapshotHydrated()
            : false,
          supportsGroups: false,
          conversations: direct
        }
      }
      case 'register_endpoint': {
        if (!payload || !payload.endpoint || !payload.p256dh || !payload.auth) {
          throw new Error('register_endpoint requires endpoint, p256dh and auth')
        }
        const ok = await this.p2pManager.registerPushEndpoint({
          endpoint: payload.endpoint,
          p256dh: payload.p256dh,
          auth: payload.auth
        })
        return { ok }
      }
      default:
        throw new Error(`Unknown push action: ${action}`)
    }
  }

  // Identity handlers
  async handleIdentity(action, payload) {
    switch (action) {
      case 'create': {
        await this._replaceIdentity(() => this.identity.create(payload.displayName))
        // Return the recovery phrase in the same response so the native side
        // doesn't need a second migration.get_seed_phrase IPC that could fail
        // independently of identity creation.
        return { ...this._identityPayload(), seedPhrase: this.identity.exportMnemonic() }
      }

      case 'get':
        if (!this.identity.keyPair) {
          return {}
        }
        return this._identityPayload()

      case 'update':
        if (!isValidString(payload.displayName, 100)) throw new Error('Invalid displayName')
        await this.identity.updateDisplayName(payload.displayName)
        return this._identityPayload()

      case 'export':
        return {
          publicKey: this.identity.publicKeyHex
        }

      case 'restore':
        // Alias for migration.restore_from_seed_phrase (backward compat with iOS app)
        return this.handleMigration('restore_from_seed_phrase', payload)

      default:
        throw new Error(`Unknown identity action: ${action}`)
    }
  }

  // Contacts handlers
  async handleContacts(action, payload) {
    switch (action) {
      case 'add':
        if (!isValidPublicKey(payload.publicKey)) throw new Error('Invalid publicKey')
        if (!isValidString(payload.name, 100)) throw new Error('Invalid name')
        await this.contactStore.addContact(payload.publicKey, payload.name)
        return { success: true }

      case 'list':
        const contacts = await this.contactStore.listContacts()
        return { contacts }

      case 'remove':
        if (!isValidPublicKey(payload.publicKey)) throw new Error('Invalid publicKey')
        await this.contactStore.deleteContact(payload.publicKey)
        return { success: true }

      case 'update': {
        if (!isValidPublicKey(payload.publicKey)) throw new Error('Invalid publicKey')
        const updates = payload.updates
        if (!updates || typeof updates !== 'object' || Array.isArray(updates)) throw new Error('Invalid updates')
        // Same name rules as contacts.add; the store validates the rest.
        if (Object.prototype.hasOwnProperty.call(updates, 'name') && !isValidString(updates.name, 100)) {
          throw new Error('Invalid name')
        }
        await this.contactStore.updateContact(payload.publicKey, updates)
        return { success: true }
      }

      case 'set_blocked_keys':
        // Blocking is app policy, kept natively; group link admission needs
        // to honor it without the app being asked each time.
        this.groupLinks.setBlockedKeys(payload.keys)
        return { success: true }

      case 'updateWalletAddress':
        if (!isValidPublicKey(payload.publicKey)) throw new Error('Invalid publicKey')
        const updatedContact = await this.contactStore.updateWalletAddress(
          payload.publicKey,
          payload.walletAddress
        )
        return { success: true, contact: updatedContact }

      default:
        throw new Error(`Unknown contacts action: ${action}`)
    }
  }

  // Conversation handlers
  async handleConversation(action, payload) {
    switch (action) {
      case 'create': {
        if (payload.type !== 'direct' && payload.type !== 'group') {
          throw new IPCRequestError(
            'UNSUPPORTED_CONVERSATION_TYPE',
            'Only direct and group conversations are supported'
          )
        }
        const options = {}
        if (payload.type === 'group') {
          options.creatorKey = this.identity.publicKeyHex
        }

        let conversation

        let directParticipant = null
        if (payload.type === 'direct') {
          // Use deterministic ID for direct chats so both sides share the same room.
          // Normalize keys: strip optional 0x prefix and lowercase so iOS/Android
          // always compute the same hash regardless of how the key was formatted.
          const theirKey = validateDirectParticipant(this.identity.publicKeyHex, payload.participants)
          directParticipant = theirKey
          const myKey = this.identity.publicKeyHex

          // Migration-aware lookup: checks new hash-based ID, then legacy truncated ID
          const resolved = this.chatStore.resolveDirectChatId(myKey, theirKey)
          // An explicit local create means the user chose to re-enter this DM. Clear the
          // tombstone before either reusing or recreating its deterministic conversation;
          // removeConversation deletes the old record, so doing this only in the reuse
          // branch leaves recreated chats permanently hidden from conversation.list.
          this.chatStore.clearLeftStatus(resolved.id)
          if (resolved.conversation) {
            await this.p2pManager.joinConversation(resolved.id, theirKey)
            await this._sendDirectInvite(resolved.conversation, theirKey)
            return { conversation: this._enrichConversation(resolved.conversation) }
          }

          // Resolve display name: payload > contact name > key prefix
          const contact = await this.contactStore.getContact(theirKey)
          const resolvedName = payload.displayName || contact?.name || theirKey.substring(0, 8)
          options.displayName = resolvedName

          conversation = await this.chatStore.createConversationWithId(
            resolved.id,
            'direct',
            [theirKey],
            options
          )
        } else {
          conversation = await this.chatStore.createConversation(
            payload.type,
            payload.participants || [],
            options
          )

          // Set display name: use payload for groups/other types
          if (payload.displayName) {
            conversation.displayName = payload.displayName
          }
          // Persist the display name
          await this.chatStore.updateConversation(conversation.id, { displayName: conversation.displayName })
        }

        // For groups: auto-connect to group topic and send invites to participants
        if (payload.type === 'group') {
          await this._setupGroupAndSendInvites(conversation)
        }

        // For direct chats: send invite to recipient via personal topic
        if (directParticipant) {
          this._sendDirectInvite(conversation, directParticipant)
        }

        return { conversation: this._enrichConversation(conversation) }
      }

      case 'direct_status': {
        const theirKey = validateDirectParticipant(this.identity.publicKeyHex, payload.participants)
        const resolved = this.chatStore.resolveDirectChatId(this.identity.publicKeyHex, theirKey)
        return {
          conversationId: resolved.id,
          isLeft: this.chatStore.hasLeftConversation(resolved.id)
        }
      }

      case 'list':
        const conversations = await this.chatStore.listConversations()
        return { conversations: conversations.map(c => this._enrichConversation(c)) }

      case 'get':
        const conv = await this.chatStore.getConversation(payload.conversationId)
        return { conversation: conv ? this._enrichConversation(conv) : null }

      case 'leave':
        return await this._leaveGroup(payload.conversationId)

      case 'delete':
        return await this._deleteGroup(payload.conversationId)

      case 'remove':
        return await this._removeConversation(payload.conversationId)

      case 'rename': {
        const conv = await this.chatStore.getConversation(payload.conversationId)
        if (!conv) throw new Error('Conversation not found')
        const newName = (payload.name || '').trim()
        if (!newName) throw new Error('Name cannot be empty')
        await this.chatStore.updateConversation(payload.conversationId, { displayName: newName })
        if (conv.type === 'group') {
          const groupEntry = this.p2pManager.groupConversations.get(payload.conversationId)
          this.p2pManager.sendToConversation(payload.conversationId, {
            type: 'group_renamed',
            conversationId: payload.conversationId,
            groupTopicHex: groupEntry?.groupTopicHex,
            newName
          })
        }
        return { success: true }
      }

      case 'add_member': {
        const conv = await this.chatStore.getConversation(payload.conversationId)
        if (!conv || conv.type !== 'group') throw new Error('Not a group conversation')
        const myKey = this.identity.publicKeyHex.toLowerCase()
        if ((conv.creatorKey || '').toLowerCase() !== myKey) {
          throw new Error('Only the group owner can add members')
        }
        const newKey = (payload.publicKey || '').toLowerCase().replace(/^0x/, '')
        if (!isValidPublicKey(newKey) || newKey === myKey) throw new Error('Invalid publicKey')
        const newName = (payload.displayName || newKey.substring(0, 8)).trim()
        if (this._normalizedParticipants(conv).includes(newKey)) return { success: true, alreadyMember: true }
        const updatedParticipants = await this._addMemberAsOwner(conv, newKey, newName)
        return { success: true, participants: updatedParticipants }
      }

      case 'get_messages': {
        const offset = payload.offset || 0
        const limit = payload.limit || 50
        const allMessages = await this.chatStore.getMessages(payload.conversationId)
        const paginatedMessages = allMessages.slice(offset, offset + limit)
        return {
          // Same thumbnail stripping as message.list — keep the frame small.
          messages: this._stripThumbnails(paginatedMessages),
          total: allMessages.length,
          hasMore: offset + limit < allMessages.length
        }
      }

      default:
        throw new Error(`Unknown conversation action: ${action}`)
    }
  }

  _normalizedParticipants (conv) {
    return (conv.participantIds || []).map(key => (key || '').toLowerCase().replace(/^0x/, '')).filter(Boolean)
  }

  /** The group invite the owner sends a member, with optional extra fields. */
  _ownerGroupInvite (conv, participantKeys, extra = {}) {
    const myKey = this.identity.publicKeyHex.toLowerCase()
    return {
      type: 'group_invite',
      groupId: conv.groupId,
      groupName: conv.displayName,
      creatorKey: conv.creatorKey,
      participants: [myKey, ...participantKeys],
      senderKey: myKey,
      localCoreKey: this.hypercoreManager ? this.hypercoreManager.getLocalCoreKey(conv.id) : null,
      ...extra
    }
  }

  /**
   * Owner only: add newKey to the group, invite them, and tell the others.
   * The caller has checked ownership and that newKey is not a member yet.
   * @returns {Promise<string[]>} the updated participant list (without us)
   */
  async _addMemberAsOwner (conv, newKey, newName, { viaLink = null } = {}) {
    const updatedParticipants = [...new Set([...this._normalizedParticipants(conv), newKey])]
    await this.chatStore.updateConversation(conv.id, { participantIds: updatedParticipants })
    const invite = this._ownerGroupInvite(conv, updatedParticipants, viaLink ? { viaLink } : {})
    await this.p2pManager.joinGroupConversation(conv.id, conv.groupId, invite.participants)
    await this.p2pManager.sendInvite(newKey, invite)
    this.p2pManager.sendToConversation(conv.id, {
      type: 'group_member_added',
      newMemberKey: newKey,
      newMemberName: newName,
      updatedParticipants: invite.participants
    })
    return updatedParticipants
  }

  /**
   * The group link service's admission hook. With resendOnly the joiner is
   * already a member and only gets their invite again.
   * @returns {Promise<boolean>}
   */
  async _admitViaLink (conversationId, joinerKey, joinerName, viaLink, { resendOnly = false } = {}) {
    const conv = await this.chatStore.getConversation(conversationId)
    if (!conv || conv.type !== 'group') return false
    const myKey = this.identity.publicKeyHex.toLowerCase()
    if ((conv.creatorKey || '').toLowerCase() !== myKey) return false
    const key = (joinerKey || '').toLowerCase()
    if (!isValidPublicKey(key) || key === myKey) return false
    const name = (typeof joinerName === 'string' && joinerName.trim()) || key.substring(0, 8)
    const participants = this._normalizedParticipants(conv)
    if (resendOnly || participants.includes(key)) {
      if (!participants.includes(key)) return false
      await this.p2pManager.sendInvite(key, this._ownerGroupInvite(conv, participants, viaLink ? { viaLink } : {}))
      return true
    }
    await this._addMemberAsOwner(conv, key, name, { viaLink })
    // The native side did not start this, so it learns of it here.
    this.pushEvent('conversation.member_added', { conversationId, newMemberKey: key, newMemberName: name })
    return true
  }

  /**
   * Replace each message's inline base64 thumbnail with a `hasThumbnail` flag.
   * Returns shallow copies so the on-disk/in-memory records keep their
   * thumbnails for message.get_thumbnail. Keeps list frames well under the
   * native 1 MB receive cap.
   */
  _stripThumbnails(messages) {
    return messages.map((msg) => {
      if (!msg || msg.thumbnailData == null) return msg
      const { thumbnailData, ...rest } = msg
      rest.hasThumbnail = true
      return rest
    })
  }

  /**
   * Add isOwner field to conversation objects sent to Swift/Kotlin
   */
  _enrichConversation(conversation) {
    const enriched = { ...conversation }
    delete enriched.appliedControls
    if (enriched.type === 'group') {
      enriched.isOwner = enriched.creatorKey === this.identity.publicKeyHex
    }
    return enriched
  }

  /**
   * After creating a group, join the group topic and send invites to all participants
   */
  async _setupGroupAndSendInvites(conversation) {
    try {
      const myKey = this.identity.publicKeyHex
      const allKeys = [myKey, ...conversation.participantIds]

      // Join group topic so we're discoverable
      await this.p2pManager.joinGroupConversation(conversation.id, conversation.groupId, allKeys)

      // Build invite payload, including our local core key so the invitee
      // can pull anything we've already sent via the blind peer.
      const inviteData = {
        type: 'group_invite',
        groupId: conversation.groupId,
        groupName: conversation.displayName,
        creatorKey: conversation.creatorKey,
        participants: allKeys,
        senderKey: myKey,
        localCoreKey: this.hypercoreManager ? this.hypercoreManager.getLocalCoreKey(conversation.id) : null
      }

      // Send invites to all other participants
      for (const participantId of conversation.participantIds) {
        await this.p2pManager.sendInvite(participantId, inviteData)
      }

      diag('Group setup complete (' + allKeys.length + ' members)')
    } catch (error) {
      diag('Failed to setup group and send invites: ' + (error.stack || error.message || error))
    }
  }

  /**
   * After creating a direct chat, join the pairwise topic and send an invite
   */
  async _sendDirectInvite(conversation, recipientKeyHex) {
    try {
      const myKey = this.identity.publicKeyHex

      // Join the pairwise topic so we're discoverable. This also ensures a
      // local Hypercore exists for the conversation.
      await this.p2pManager.joinConversation(conversation.id, recipientKeyHex)

      // Send invite via the recipient's personal topic. Including our local
      // core's public key lets the recipient pull anything we've sent while
      // they were offline, via the blind peer.
      const inviteData = {
        type: 'direct_invite',
        conversationId: conversation.id,
        senderKey: myKey,
        senderDisplayName: this.identity.displayName || myKey.substring(0, 8),
        localCoreKey: this.hypercoreManager ? this.hypercoreManager.getLocalCoreKey(conversation.id) : null
      }
      await this.p2pManager.sendInvite(recipientKeyHex, inviteData)

      diag('Direct invite sent to ' + recipientKeyHex.substring(0, 12))
    } catch (error) {
      diag('Failed to send direct invite: ' + (error.stack || error.message || error))
    }
  }

  /**
   * Handle an incoming direct chat invite from a peer
   */
  /**
   * @returns {Promise<boolean>} whether the invite is finished with. A validation
   *   rejection counts as finished; only a thrown error might succeed later, and
   *   a mailbox-delivered invite must not be deleted before then.
   */
  async _handleDirectInvite(inviteData, senderPeerId) {
    let failed = false
    try {
      try { inviteData = normalizePeerRecord({ ...inviteData, type: 'direct_invite' }, senderPeerId) } catch (err) {
        if (err.code === 'INVALID_PEER_RECORD') return true
        throw err
      }
      const { conversationId, senderDisplayName } = inviteData
      // Normalize senderKey: strip 0x prefix and lowercase for consistent hashing
      const senderKey = (inviteData.senderKey || '').toLowerCase().replace(/^0x/, '')
      const myKey = this.identity.publicKeyHex

      // The claimed sender must be the Noise-authenticated peer that delivered
      // the invite: everything below — conversation creation, participant
      // binding, remote-core opening — attributes to senderKey, so a spoofed
      // field would let a peer plant cores/conversations as someone else.
      const authenticatedPeer = (senderPeerId || '').toLowerCase()
      if (!senderKey || senderKey !== authenticatedPeer) {
        diag('Rejecting direct invite: claimed sender ' + senderKey.substring(0, 12) +
          ' != authenticated peer ' + authenticatedPeer.substring(0, 12))
        return true // rejected: terminal, nothing to retry
      }

      if (senderKey === myKey) {
        diag('Received own direct_invite, ignoring')
        return true // rejected: terminal, nothing to retry
      }

      // Migration-aware lookup: checks new hash-based ID, then legacy truncated ID
      let resolvedId
      let conversation
      if (conversationId &&
          (conversationId === ChatStore.directChatId(myKey, senderKey) ||
           conversationId === ChatStore._legacyDirectChatId(myKey, senderKey))) {
        // Wire id checks out as the deterministic id for this exact pair
        resolvedId = conversationId
        conversation = await this.chatStore.getConversation(resolvedId)
      } else if (conversationId) {
        // Arbitrary wire ids are never honored — they let a peer graft its
        // identity into any conversation whose id it can compute.
        diag('Ignoring unverifiable conversationId in direct invite from ' + senderKey.substring(0, 12))
      }
      if (!conversation) {
        // Fall back to deterministic ID with migration support
        const resolved = this.chatStore.resolveDirectChatId(myKey, senderKey)
        resolvedId = resolved.id
        conversation = resolved.conversation
      }

      // Do not recreate a conversation the user explicitly left
      if (this.chatStore.hasLeftConversation(resolvedId)) {
        diag('Ignoring direct invite for left conversation: ' + resolvedId.substring(0, 12))
        return true // rejected: terminal, nothing to retry
      }

      if (!conversation) {
        conversation = await this.chatStore.createConversationWithId(resolvedId, 'direct', [senderKey], {})
      }

      // Ensure participants are set (handles legacy conversations with empty participantIds)
      if (!conversation.participantIds || conversation.participantIds.length === 0) {
        await this.chatStore.updateConversation(conversation.id, { participantIds: [senderKey] })
        conversation.participantIds = [senderKey]
      }

      // Set display name from the invite
      const displayName = senderDisplayName || senderKey.substring(0, 8)
      await this.chatStore.updateConversation(conversation.id, { displayName })
      conversation.displayName = displayName

      // Join the pairwise topic so messages flow. Both of the steps below report
      // failure rather than throwing, so their results have to be checked: a
      // mailbox-delivered invite is deleted once this returns true, and it holds
      // the only copy of the core key.
      if (!await this.p2pManager.joinConversation(conversation.id, senderKey)) {
        diag('Invite accepted but joinConversation failed; leaving it to retry')
        return false
      }

      // If the invite included the sender's local core key, open it so we can
      // pull any messages they've already written via the blind peer.
      if (inviteData.localCoreKey) {
        if (!await this.p2pManager.openRemoteCore(conversation.id, senderKey, inviteData.localCoreKey)) {
          diag('Invite accepted but the remote core did not open; leaving it to retry')
          return false
        }
      }

      // A mailbox-delivered invite bootstraps the sender's core in one
      // direction. Return our own core key through the same mailbox so both
      // firewalled peers can replicate bidirectionally without first opening
      // a direct socket. Mark the response to prevent an invite ping-pong.
      //
      // Not treated as fatal: their core is already open, so their messages
      // reach us either way, and our own core key gets another chance the next
      // time we send into this conversation and find no live socket. Failing
      // here instead would replay the whole invite, including this reply.
      // Contained so that its own failure, thrown or reported, cannot fail the
      // invite: the work that matters is already done above.
      if (!inviteData.bootstrapReply) {
        try {
          const localCoreKey = this.hypercoreManager
            ? this.hypercoreManager.getLocalCoreKey(conversation.id)
            : null
          const replied = await this.p2pManager.sendInvite(senderKey, {
            type: 'direct_invite',
            conversationId: conversation.id,
            senderKey: myKey,
            senderDisplayName: this.identity.displayName || myKey.substring(0, 8),
            localCoreKey,
            bootstrapReply: true
          })
          if (!replied) throw new Error('no transport accepted the reply')
        } catch (error) {
          diag('Reciprocal core key not delivered yet for ' + senderKey.substring(0, 12) +
            ' (' + (error.message || error) + '); the next send will retry it')
        }
      }

      // Notify Swift/Kotlin UI so the conversation appears in the list
      this.pushEvent('conversation.invite_received', { conversation: this._enrichConversation(conversation) })

      diag('Accepted direct invite from ' + senderKey.substring(0, 12))
    } catch (error) {
      failed = true
      diag('Failed to handle direct invite: ' + (error.stack || error.message || error))
    }
    return !failed
  }

  /**
   * Handle an incoming group invite from a peer.
   * @returns {Promise<boolean>} whether the invite is finished with, as in
   *   [_handleDirectInvite].
   */
  async _handleGroupInvite(inviteData, senderPeerId) {
    let failed = false
    try {
      try { inviteData = normalizePeerRecord({ ...inviteData, type: 'group_invite' }, senderPeerId) } catch (err) {
        if (err.code === 'INVALID_PEER_RECORD') return true
        throw err
      }
      const { groupId, groupName, participants } = inviteData
      const myKey = this.identity.publicKeyHex

      // The claimed sender must be the Noise-authenticated peer, and must be a
      // member of the group it is inviting us into — the invite's localCoreKey
      // is attributed to senderKey below.
      const authenticatedPeer = (senderPeerId || '').toLowerCase()
      const claimedSender = (inviteData.senderKey || '').toLowerCase().replace(/^0x/, '')
      if (!claimedSender || claimedSender !== authenticatedPeer) {
        diag('Rejecting group invite: claimed sender ' + claimedSender.substring(0, 12) +
          ' != authenticated peer ' + authenticatedPeer.substring(0, 12))
        return true // rejected: terminal, nothing to retry
      }
      if (!isValidPublicKey((groupId || '').toLowerCase())) {
        diag('Rejecting group invite with malformed groupId')
        return true // rejected: terminal, nothing to retry
      }
      if (!Array.isArray(participants)) {
        diag('Rejecting group invite without a participant list')
        return true // rejected: terminal, nothing to retry
      }
      const normalizedParticipants = participants.map(k =>
        typeof k === 'string' ? k.toLowerCase().replace(/^0x/, '') : '')
      if (normalizedParticipants.some(k => !isValidPublicKey(k))) {
        diag('Rejecting group invite with malformed participant key')
        return true // rejected: terminal, nothing to retry
      }
      if (!normalizedParticipants.includes(claimedSender)) {
        diag('Rejecting group invite from non-member sender ' + claimedSender.substring(0, 12))
        return true // rejected: terminal, nothing to retry
      }

      // Membership is owner-managed. Requiring the Noise-authenticated inviter
      // to be the declared creator prevents an ordinary member from forging
      // creatorKey and later exercising owner-only controls on this recipient.
      const creatorKey = (inviteData.creatorKey || '').toLowerCase().replace(/^0x/, '')
      if (!isValidPublicKey(creatorKey) || creatorKey !== claimedSender) {
        diag('Rejecting group invite not authenticated by its creator')
        return true // rejected: terminal, nothing to retry
      }

      // Verify our key is in the participant list
      if (!normalizedParticipants.includes(myKey.toLowerCase())) {
        diag('Received group invite but my key not in participants, ignoring')
        return true // rejected: terminal, nothing to retry
      }

      const existing = [...this.chatStore.conversations.values()].find(conv =>
        conv.type === 'group' && conv.groupId === groupId.toLowerCase())
      if (existing && ((existing.creatorKey || '').toLowerCase() !== authenticatedPeer ||
          !this._isConversationParticipant(existing, authenticatedPeer) ||
          this.chatStore.hasLeftConversation(existing.id))) return true

      // Other participants (excluding self) become the conversation's participantIds
      const otherParticipants = [...new Set([...(existing ? existing.participantIds : []),
        ...normalizedParticipants.filter(k => k !== myKey.toLowerCase())])]

      const metadataChanged = !existing ||
        otherParticipants.some(key => !existing.participantIds.includes(key)) ||
        (groupName && groupName !== existing.displayName)

      // Create conversation (dedup by groupId prevents duplicates)
      const conversation = await this.chatStore.createConversation('group', otherParticipants, {
        groupId: groupId.toLowerCase(),
        creatorKey
      })

      // Recheck after the await: another invite may have created this group.
      if (conversation.creatorKey !== creatorKey) return true
      if (existing) {
        await this.chatStore.updateConversation(conversation.id, { participantIds: otherParticipants })
      }

      // Set group name
      if (groupName) {
        await this.chatStore.updateConversation(conversation.id, { displayName: groupName })
        conversation.displayName = groupName
      }

      // Join group topic so we can send/receive messages
      await this.p2pManager.joinGroupConversation(
        conversation.id,
        groupId.toLowerCase(),
        [myKey.toLowerCase(), ...otherParticipants]
      )

      // If the sender shared their local core key, open it so we can pull
      // group messages they sent while we were offline via the blind peer.
      if (inviteData.localCoreKey && claimedSender !== myKey.toLowerCase()) {
        await this.p2pManager.openRemoteCore(conversation.id, claimedSender, inviteData.localCoreKey)
      }

      // Notify Swift/Kotlin UI about the new group
      if (metadataChanged) this.pushEvent('conversation.invite_received', { conversation: this._enrichConversation(conversation) })

      // An admission through a group invite link completes our request.
      if (inviteData.viaLink) this.groupLinks.onGroupInviteApplied(inviteData, conversation)

      diag('Accepted group invite with ' + participants.length + ' members from ' + senderPeerId.substring(0, 12))
    } catch (error) {
      failed = true
      diag('Failed to handle group invite: ' + (error.stack || error.message || error))
    }
    return !failed
  }

  /**
   * The group is gone for us: stop its invite link and answer anything still
   * queued. Not awaited by callers; answering needs the network.
   */
  _groupLinksGone (conversationId) {
    this.groupLinks.onGroupGone(conversationId)
      .catch(err => diag('Group link cleanup failed: ' + (err.message || err)))
      .finally(() => {
        this.groupLinks.store.forgetConversation(conversationId)
        try { this.groupLinks.store.save() } catch (err) { diag('Group link store save failed: ' + (err.message || err)) }
      })
  }

  /**
   * Leave a group conversation
   */
  async _leaveGroup(conversationId) {
    const conversation = await this.chatStore.getConversation(conversationId)
    if (!conversation || conversation.type !== 'group') {
      throw new Error('Conversation not found or not a group')
    }

    // Notify other members before disconnecting
    this.p2pManager.sendToConversation(conversationId, {
      type: 'group_leave',
      leaverKey: this.identity.publicKeyHex
    })

    // Disconnect from P2P
    await this.p2pManager.leaveConversation(conversationId)

    // Delete locally
    await this.chatStore.deleteConversation(conversationId)
    if (this.hypercoreManager) await this.hypercoreManager.removeConversation(conversationId)
    if (this.blindMirror) this.blindMirror.removeConversation(conversationId)

    this._groupLinksGone(conversationId)
    diag('Left group: ' + conversationId.substring(0, 12))
    return { success: true }
  }

  /**
   * Delete a group conversation (owner only)
   */
  async _deleteGroup(conversationId) {
    const conversation = await this.chatStore.getConversation(conversationId)
    if (!conversation || conversation.type !== 'group') {
      throw new Error('Conversation not found or not a group')
    }
    if (conversation.creatorKey !== this.identity.publicKeyHex) {
      throw new Error('Only the group owner can delete the group')
    }

    // Notify all members before disconnecting
    this.p2pManager.sendToConversation(conversationId, {
      type: 'group_deleted'
    })

    // Disconnect from P2P
    await this.p2pManager.leaveConversation(conversationId)

    // Delete locally
    await this.chatStore.deleteConversation(conversationId)
    if (this.hypercoreManager) await this.hypercoreManager.removeConversation(conversationId)
    if (this.blindMirror) this.blindMirror.removeConversation(conversationId)

    this._groupLinksGone(conversationId)
    diag('Deleted group: ' + conversationId.substring(0, 12))
    return { success: true }
  }

  /**
   * Remove a conversation locally (works for both direct and group chats).
   * For direct chats: just removes locally without notifying the other party.
   * For groups: same as leave (removes locally, notifies others).
   */
  async _removeConversation(conversationId) {
    const conversation = await this.chatStore.getConversation(conversationId)
    if (!conversation) {
      throw new Error('Conversation not found')
    }

    // Mark as left BEFORE any deletion — prevents re-creation on incoming message
    this.chatStore.markConversationAsLeft(conversationId)

    // For groups, notify other members before disconnecting
    if (conversation.type === 'group') {
      this.p2pManager.sendToConversation(conversationId, {
        type: 'group_leave',
        leaverKey: this.identity.publicKeyHex
      })
    }

    // Disconnect from P2P
    await this.p2pManager.leaveConversation(conversationId)

    // Delete locally
    await this.chatStore.deleteConversation(conversationId)
    if (this.hypercoreManager) await this.hypercoreManager.removeConversation(conversationId)
    if (this.blindMirror) this.blindMirror.removeConversation(conversationId)

    if (conversation.type === 'group') this._groupLinksGone(conversationId)
    diag('Removed conversation: ' + conversationId.substring(0, 12))
    return { success: true }
  }

  /**
   * Handle a remote peer leaving a group
   */
  async _handleGroupLeave(data, senderPeerId, closeOptions = {}) {
    try {
      try { data = normalizePeerRecord({ ...data, type: 'group_leave' }, senderPeerId) } catch (err) {
        if (err.code === 'INVALID_PEER_RECORD') return true
        throw err
      }
      const { groupTopicHex } = data
      // A peer may only remove itself. The wire leaverKey field is ignored —
      // honoring it let any connected peer evict arbitrary members.
      const leaverHex = (senderPeerId || '').toLowerCase()
      if (!leaverHex) return

      // Find the conversation by groupTopicHex
      let conversationId = null
      if (groupTopicHex) {
        conversationId = this._conversationForGroupTopic(groupTopicHex)
      }
      if (!conversationId) return

      const conversation = await this.chatStore.getConversation(conversationId)
      if (!conversation) return
      if (!this._isConversationParticipant(conversation, leaverHex)) {
        diag('Rejecting group_leave from non-participant ' + leaverHex.substring(0, 12))
        return
      }

      // Remove the leaver from participantIds
      const updatedParticipants = conversation.participantIds.filter(k => (k || '').toLowerCase() !== leaverHex)
      const updates = { participantIds: updatedParticipants }
      if ((conversation.creatorKey || '').toLowerCase() === leaverHex) updates.creatorKey = null
      await this.chatStore.updateConversation(conversationId, updates)

      // Revoke every delivery path, not just the persisted membership record.
      if (typeof this.p2pManager.removeGroupParticipant === 'function') {
        this.p2pManager.removeGroupParticipant(conversationId, leaverHex)
      }
      if (this.hypercoreManager) {
        await this.hypercoreManager.removeRemoteCore(conversationId, leaverHex, closeOptions)
      }
      if (this.blindMirror && typeof this.blindMirror.removeRemoteCore === 'function') {
        this.blindMirror.removeRemoteCore(conversationId, leaverHex)
      }

      // Notify Swift/Kotlin UI
      this.pushEvent('conversation.member_left', {
        conversationId,
        leaverKey: leaverHex
      })

      diag('Peer ' + leaverHex.substring(0, 12) + ' left group ' + conversationId.substring(0, 12))
    } catch (error) {
      diag('Failed to handle group leave: ' + (error.stack || error.message || error))
      return false
    }
  }

  /**
   * Handle a remote group rename
   */
  async _handleGroupRenamed(data, senderPeerId) {
    try {
      try { data = normalizePeerRecord({ ...data, type: 'group_renamed' }, senderPeerId) } catch (err) {
        if (err.code === 'INVALID_PEER_RECORD') return true
        throw err
      }
      const { groupTopicHex, conversationId: wireConversationId, newName } = data
      // Prefer the topic-derived id; a wire-supplied conversationId is only a
      // fallback and still has to pass the checks below (group + participant),
      // or any peer could rename arbitrary conversations, DMs included.
      let conversationId = null
      if (groupTopicHex) {
        conversationId = this._conversationForGroupTopic(groupTopicHex)
      }
      if (!groupTopicHex) conversationId = wireConversationId
      if (!conversationId || !newName) return
      const conversation = await this.chatStore.getConversation(conversationId)
      if (!conversation || conversation.type !== 'group') return
      if (!this._isConversationParticipant(conversation, senderPeerId)) {
        diag('Rejecting group_renamed from non-participant ' + (senderPeerId || '').substring(0, 12))
        return
      }
      if (conversation.displayName === newName) return
      await this.chatStore.updateConversation(conversationId, { displayName: newName })
      this.pushEvent('conversation.group_renamed', { conversationId, newName })
      diag('Group renamed: ' + conversationId.substring(0, 12))
    } catch (error) {
      diag('Failed to handle group renamed: ' + (error.stack || error.message || error))
      return false
    }
  }

  /**
   * Is the authenticated peer a participant (or the creator) of this
   * conversation? Group control messages arrive over the group's DHT topic,
   * which is publicly announced — the sender's membership must be verified
   * before honoring any mutation.
   */
  _isConversationParticipant(conversation, peerKeyHex) {
    const peer = (peerKeyHex || '').toLowerCase()
    if (!peer) return false
    return (conversation.participantIds || []).some(k => (k || '').toLowerCase() === peer)
  }

  /**
   * Handle a new member being added to the group by its owner
   */
  async _handleGroupMemberAdded(data, senderPeerId) {
    try {
      try { data = normalizePeerRecord({ ...data, type: 'group_member_added' }, senderPeerId) } catch (err) {
        if (err.code === 'INVALID_PEER_RECORD') return true
        throw err
      }
      const { groupTopicHex, newMemberKey, newMemberName, updatedParticipants } = data
      const conversationId = groupTopicHex
        ? this._conversationForGroupTopic(groupTopicHex)
        : null
      if (!conversationId) return
      const conversation = await this.chatStore.getConversation(conversationId)
      if (!conversation) return
      if (!this._isConversationParticipant(conversation, senderPeerId) ||
          (conversation.creatorKey || '').toLowerCase() !== (senderPeerId || '').toLowerCase()) {
        diag('Rejecting group_member_added from non-owner ' + (senderPeerId || '').substring(0, 12))
        return
      }
      const myKey = this.identity.publicKeyHex
      const normalizedNewMember = (newMemberKey || '').toLowerCase().replace(/^0x/, '')
      if (!isValidPublicKey(normalizedNewMember) || !Array.isArray(updatedParticipants)) return
      const normalizedUpdate = updatedParticipants.map(k =>
        typeof k === 'string' ? k.toLowerCase().replace(/^0x/, '') : '')
      if (normalizedUpdate.some(k => !isValidPublicKey(k)) ||
          !normalizedUpdate.includes(normalizedNewMember)) return
      // Merge additively: an add announcement may only grow membership. A
      // wholesale replace would let one member (or a stale message) silently
      // evict others — removals go through group_leave, bound to the
      // authenticated leaver.
      const merged = new Set((conversation.participantIds || []).filter(Boolean))
      for (const key of normalizedUpdate) {
        if (key !== myKey.toLowerCase()) merged.add(key)
      }
      const others = [...merged]
      if (others.length === conversation.participantIds.length &&
          others.every(key => conversation.participantIds.includes(key))) return
      await this.chatStore.updateConversation(conversationId, { participantIds: others })
      if (updatedParticipants) {
        await this.p2pManager.joinGroupConversation(conversationId, conversation.groupId, [myKey, ...others])
      }
      this.pushEvent('conversation.member_added', {
        conversationId,
        newMemberKey: normalizedNewMember,
        newMemberName
      })
      diag('New member added to group: ' + conversationId.substring(0, 12))
    } catch (error) {
      diag('Failed to handle group member added: ' + (error.stack || error.message || error))
      return false
    }
  }

  /**
   * Handle a remote group deletion (owner deleted the group)
   */
  async _handleGroupDeleted(data, senderPeerId, closeOptions = {}) {
    try {
      try { data = normalizePeerRecord({ ...data, type: 'group_deleted' }, senderPeerId) } catch (err) {
        if (err.code === 'INVALID_PEER_RECORD') return true
        throw err
      }
      const { groupTopicHex } = data

      // Find the conversation by groupTopicHex
      let conversationId = null
      if (groupTopicHex) {
        conversationId = this._conversationForGroupTopic(groupTopicHex)
      }
      if (!conversationId) return

      const conversation = await this.chatStore.getConversation(conversationId)
      if (!conversation) return

      // Only the group owner may delete the group for everyone.
      if (!this._isConversationParticipant(conversation, senderPeerId) ||
          (conversation.creatorKey || '').toLowerCase() !== (senderPeerId || '').toLowerCase()) {
        diag('Rejecting group_deleted from non-owner ' + (senderPeerId || '').substring(0, 12))
        return
      }

      const displayName = conversation.displayName

      // Disconnect and delete locally
      await this.chatStore.deleteConversation(conversationId)
      await this.p2pManager.leaveConversation(conversationId)
      if (this.hypercoreManager) await this.hypercoreManager.removeConversation(conversationId, closeOptions)
      if (this.blindMirror) this.blindMirror.removeConversation(conversationId)

      // Notify Swift/Kotlin UI
      this.pushEvent('conversation.group_deleted', {
        conversationId,
        displayName
      })

      diag('Group deleted by owner: ' + conversationId.substring(0, 12))
    } catch (error) {
      diag('Failed to handle group deleted: ' + (error.stack || error.message || error))
      return false
    }
  }

  // Message handlers
  _recordOutgoingStatus(conversationId, message, durability) {
    const status = durability.relay === 'request_acknowledged' ? 'sent' : 'queued'
    if (status === 'sent') {
      message.status = 'sent'
      try {
        this.chatStore.markRelayedByIds(conversationId, [message.id])
      } catch (err) {
        // Relay acceptance is already true and the message is already durable.
        // A secondary status-store failure must not turn that successful send
        // into an IPC failure or skip follow-on work such as media transfer.
        diag('relay status persistence failed: ' + (err.message || err))
      }
    }
    this.pushEvent('message.status', {
      messageId: message.id,
      conversationId,
      status
    })
    return status
  }

  async handleMessage(action, payload) {
    switch (action) {
      case 'send': {
        const conversation = await this._requireSendableConversation(payload.conversationId)
        const message = await this.chatStore.addMessage(payload.conversationId, {
          content: payload.content,
          contentType: payload.contentType || 'text/plain',
          senderId: this.identity.publicKeyHex,
          senderName: this.identity.displayName || 'Me',
          isFromMe: true,
          replyToId: payload.replyToId || null,
          replyToSenderName: payload.replyToSenderName || null,
          replyToContent: payload.replyToContent || null
        })

        diag('Outgoing message stored locally conv=' + payload.conversationId.substring(0, 12) +
          ' message=' + message.id)
        const durability = await this.p2pManager.sendToConversationDurably(payload.conversationId, message, {
          notificationEligible: conversation && conversation.type === 'direct'
        })
        this._recordOutgoingStatus(payload.conversationId, message, durability)

        return { message, sent: durability.sent, durability }
      }

      case 'send_transaction':
      case 'send_payment_request':
      case 'send_wallet_address': {
        const conversation = await this._requireSendableConversation(payload.conversationId)
        const message = await this.chatStore.addMessage(payload.conversationId, {
          ...payload.message,
          senderId: this.identity.publicKeyHex,
          senderName: this.identity.displayName || 'Me',
          isFromMe: true
        })
        diag('Outgoing structured message stored locally conv=' + payload.conversationId.substring(0, 12) +
          ' message=' + message.id)
        const durability = await this.p2pManager.sendToConversationDurably(payload.conversationId, message, {
          notificationEligible: conversation && conversation.type === 'direct'
        })
        this._recordOutgoingStatus(payload.conversationId, message, durability)
        return { message, sent: durability.sent, durability }
      }

      case 'list': {
        const messages = await this.chatStore.getMessages(
          payload.conversationId,
          payload.limit || 50
        )
        this._sendDeliveryReceiptIfNeeded(payload.conversationId)
        // Resolve mediaLocalPath for media messages where the file exists on disk
        if (this.mediaStore) {
          for (const msg of messages) {
            if (msg.mediaId && !msg.mediaLocalPath) {
              const resolvedPath = this.mediaStore.getMediaPath(msg.mediaId)
              if (resolvedPath) msg.mediaLocalPath = resolvedPath
            }
          }
        }
        // Strip inline base64 thumbnails: 50 of them can push this one NDJSON
        // line past the native 1 MB receive cap, which used to discard the
        // whole frame and hang the room. The SDK hydrates each thumbnail
        // lazily via message.get_thumbnail (a small, per-message response).
        return { messages: this._stripThumbnails(messages) }
      }

      case 'get_thumbnail': {
        const thumbnailData = await this.chatStore.getMessageThumbnail(
          payload.conversationId,
          payload.messageId
        )
        return { thumbnailData: thumbnailData || null }
      }

      case 'mark_read': {
        await this._sendReadReceiptIfNeeded(payload.conversationId)
        return { success: true }
      }

      case 'set_read_receipts': {
        this.readReceiptsEnabled = payload.enabled !== false
        return { success: true, enabled: this.readReceiptsEnabled }
      }

      case 'set_presence_visible': {
        // Presence visibility lives on the p2p manager — it owns the live sockets
        // it must notify — so there's no local copy here to keep in sync.
        const visible = payload.enabled !== false
        if (this.p2pManager && typeof this.p2pManager.setPresenceVisible === 'function') {
          this.p2pManager.setPresenceVisible(visible)
        }
        return { success: true, enabled: visible }
      }

      default:
        throw new Error(`Unknown message action: ${action}`)
    }
  }

  async _requireSendableConversation(conversationId) {
    const conversation = await this.chatStore.getConversation(conversationId)
    if (!conversation) {
      throw new IPCRequestError('CONVERSATION_NOT_FOUND', 'Conversation not found')
    }
    if (conversation.type !== 'direct') return conversation

    const ownKey = this.identity.publicKeyHex.toLowerCase().replace(/^0x/, '')
    const participants = Array.isArray(conversation.participantIds) ? conversation.participantIds : []
    const hasRemoteParticipant = participants.some(value => {
      const normalized = typeof value === 'string' ? value.toLowerCase().replace(/^0x/, '') : ''
      return isValidPublicKey(normalized) && normalized !== ownKey
    })
    if (hasRemoteParticipant) return conversation

    const containsOwnKey = participants.some(value => {
      const normalized = typeof value === 'string' ? value.toLowerCase().replace(/^0x/, '') : ''
      return normalized === ownKey
    })
    if (containsOwnKey) {
      throw new IPCRequestError('OWN_PUBLIC_KEY', 'A direct conversation cannot use your own messaging key')
    }
    throw new IPCRequestError('MISSING_PARTICIPANT', 'The direct conversation has no remote participant')
  }

  _sendDeliveryReceiptIfNeeded (conversationId) {
    for (const message of this.chatStore.getLatestIncomingMessages(conversationId)) {
      this.p2pManager.sendDeliveryReceipt(conversationId, message.id, message.senderId)
    }
  }

  async _sendReadReceiptIfNeeded (conversationId) {
    if (!this.readReceiptsEnabled) return
    if (!conversationId || !this.chatStore || !this.p2pManager) return

    const conversation = await this.chatStore.getConversation(conversationId)
    if (!conversation || conversation.type !== 'direct') return

    const upTo = this.chatStore.getLatestIncomingMessageId(conversationId)
    if (!upTo) return
    if (this.chatStore.getSentReadWatermark(conversationId) === upTo) return

    // Ensure our writable core is joined and registered with the blind mirror, so
    // the receipt replicates back to the sender while they're offline. Idempotent;
    // it matters for a recipient who received the messages purely via blind-peer
    // replication and hasn't otherwise connected this conversation this session.
    const peerKey = (conversation.participantIds || []).find(k => k && k !== this.identity.publicKeyHex)
    if (peerKey) {
      try {
        await this.p2pManager.joinConversation(conversationId, peerKey)
      } catch (err) {
        diag('mark_read: joinConversation failed: ' + (err.message || err))
      }
    }

    this.chatStore.setSentReadWatermark(conversationId, upTo)
    this.p2pManager.sendReadReceipt(conversationId, upTo)
  }

  // Connection handlers
  async handleConnection(action, payload) {
    switch (action) {
      case 'connect':
        const conversation = await this.chatStore.getConversation(payload.conversationId)
        if (!conversation) return { success: false }

        if (conversation.type === 'group' && conversation.groupId) {
          // Group: join shared group topic
          const allKeys = [this.identity.publicKeyHex, ...conversation.participantIds]
          await this.p2pManager.joinGroupConversation(payload.conversationId, conversation.groupId, allKeys)
        } else if (conversation.participantIds) {
          // Direct: join pairwise topics
          for (const participantId of conversation.participantIds) {
            if (participantId !== this.identity.publicKeyHex) {
              await this.p2pManager.joinConversation(payload.conversationId, participantId)
              await this._sendDirectInvite(conversation, participantId)
            }
          }
        }
        this.p2pManager.restorePendingMessages(
          payload.conversationId,
          this.chatStore.getPendingOutgoingMessages(payload.conversationId)
        )
        this._sendDeliveryReceiptIfNeeded(payload.conversationId)
        return { success: true }

      case 'status':
        return {
          online: this.p2pManager.isOnline,
          peerCount: this.p2pManager.peerCount
        }

      case 'details': {
        const debug = this.p2pManager.getConnectionDiagnostics()
        const dht = debug.dht || {}
        const relay = this.blindMirror ? this.blindMirror.getDebugInfo() : null
        // Count total pending messages across all conversation queues
        let pendingMessageCount = 0
        if (this.p2pManager.pendingMessages) {
          for (const [, queue] of this.p2pManager.pendingMessages) {
            pendingMessageCount += queue.length
          }
        }
        return {
          online: debug.isOnline,
          peerCount: debug.peerCount,
          globalConnections: debug.globalConnections,
          pendingQueues: debug.pendingQueues,
          pendingMessageCount,
          pendingInvites: debug.pendingInvites || 0,
          dhtHealth: debug.health ? debug.health.status : 'unknown',
          dhtLastCheck: debug.health ? debug.health.lastCheckTime : null,
          consecutiveFailures: debug.health ? debug.health.consecutiveFailures : 0,
          directConversations: Object.keys(debug.directConversations || {}).length,
          groupConversations: Object.keys(debug.groupConversations || {}).length,
          dhtBootstrapped: dht.bootstrapped === true,
          dhtFirewalled: typeof dht.firewalled === 'boolean' ? dht.firewalled : null,
          dhtRandomized: typeof dht.randomized === 'boolean' ? dht.randomized : null,
          rtNodes: typeof dht.rtNodes === 'number' ? dht.rtNodes : 0,
          relayEnabled: relay ? !!relay.enabled : false,
          relaysConnected: relay ? (relay.relaysConnected || 0) : 0,
          relaysTotal: relay ? (relay.relaysTotal || 0) : 0
        }
      }

      default:
        throw new Error(`Unknown connection action: ${action}`)
    }
  }

  // Mnemonic-based seed-phrase backup + restore. Named "migration.*" for
  // historical reasons; the only durable shape now is BIP-39 entropy and the
  // 24-word mnemonic derived from it.
  async handleMigration(action, payload) {
    switch (action) {
      case 'get_seed_phrase': {
        if (!this.identity.keyPair) throw new Error('No identity')
        return { seedPhrase: this.identity.exportMnemonic() }
      }

      case 'restore_from_seed_phrase': {
        // Wiping the chat + contact stores is destructive — only do it when the
        // incoming seed produces a different identity than the one we already
        // serve. Otherwise an auto-derive retry (process-death recovery, retry
        // button, or any same-seed re-publish from the reactive Kotlin path)
        // would silently erase the user's local history.
        // Deriving up front also rejects a malformed phrase before anything
        // destructive happens.
        const incomingSeed = mnemonic.mnemonicToEd25519Seed(payload.seedPhrase)
        const incomingPubkeyHex = b4a.toString(crypto.keyPair(incomingSeed).publicKey, 'hex')

        if (this.identity.publicKeyHex === incomingPubkeyHex) {
          // Idempotent re-publish: same identity already loaded. Leave the local
          // stores, swarm state, and stored display name as-is.
          await this.ensureBlindMirror()
          return this._identityPayload()
        }

        await this._replaceIdentity(
          () => this.identity.restoreFromMnemonic(payload.seedPhrase, payload.displayName || '')
        )
        return this._identityPayload()
      }

      default:
        throw new Error(`Unknown migration action: ${action}`)
    }
  }

  // Media handlers
  async handleMedia(action, payload) {
    if (!this.mediaStore) throw new Error('Media store not initialized')

    switch (action) {
      case 'prepare_send': {
        // Swift/Kotlin wrote a compressed image to a temp path; JS reads, hashes, and stores it
        const filePath = payload.filePath
        const ext = payload.extension || 'jpg'
        const data = fs.readFileSync(filePath)
        if (data.length < 1 || data.length > config.MEDIA_MAX_BYTES) {
          throw new Error('Media must be between 1 byte and ' + config.MEDIA_MAX_BYTES + ' bytes')
        }
        const { hashHex, filePath: storedPath, fileSize } = this.mediaStore.saveMedia(data, ext)

        return {
          mediaId: hashHex,
          mediaSize: fileSize,
          mediaLocalPath: storedPath
        }
      }

      case 'send_message': {
        const conversation = await this._requireSendableConversation(payload.conversationId)
        const descriptor = await this._mediaCoreDescriptor(payload.conversationId, payload.mediaId)
        // Store the message with media metadata and send via P2P
        const message = await this.chatStore.addMessage(payload.conversationId, {
          content: payload.content || '',
          contentType: payload.contentType,
          senderId: this.identity.publicKeyHex,
          senderName: this.identity.displayName || 'Me',
          isFromMe: true,
          mediaId: payload.mediaId,
          ...descriptor,
          mediaSize: payload.mediaSize,
          mediaWidth: payload.mediaWidth,
          mediaHeight: payload.mediaHeight,
          thumbnailData: payload.thumbnailData,
          mediaLocalPath: payload.mediaLocalPath,
          mediaTransferState: 'complete',
          replyToId: payload.replyToId || null,
          replyToSenderName: payload.replyToSenderName || null,
          replyToContent: payload.replyToContent || null
        })

        // Send message JSON (with thumbnail) over P2P
        // Strip mediaLocalPath before sending - it's a local-only field
        const wireMessage = { ...message }
        delete wireMessage.mediaLocalPath
        delete wireMessage.mediaTransferState
        diag('Outgoing media message stored locally conv=' + payload.conversationId.substring(0, 12) +
          ' message=' + message.id)
        const durability = await this.p2pManager.sendToConversationDurably(payload.conversationId, wireMessage, {
          notificationEligible: conversation && conversation.type === 'direct'
        })
        this._recordOutgoingStatus(payload.conversationId, message, durability)

        // Initiate chunked media transfer to all connected peers
        if (this.mediaTransfer && payload.mediaId) {
          const framedSockets = this.p2pManager.getConversationFramedSockets(payload.conversationId)
          if (framedSockets.length > 0) {
            this.mediaTransfer.sendMediaToAll(framedSockets, payload.mediaId).catch(err => {
              diag('Failed to send media chunks: ' + err.message)
            })
          }
        }

        return { message, durability }
      }

      case 'get_path': {
        const mediaPath = this.mediaStore.getMediaPath(payload.mediaId)
        return {
          path: mediaPath,
          exists: mediaPath !== null
        }
      }

      default:
        throw new Error(`Unknown media action: ${action}`)
    }
  }

  /**
   * Append the image to the conversation's media core so the blind peer can
   * deliver it after we go offline, and describe where it landed. Null when
   * that is not possible; the record then travels without a descriptor and
   * the image reaches only peers that are online with us.
   */
  async _mediaCoreDescriptor(conversationId, mediaId) {
    if (!this.hypercoreManager || !isMediaId(mediaId)) return null
    try {
      const core = await this.p2pManager.ensureLocalMediaCore(conversationId)
      if (!core) return null
      const mediaCoreKey = b4a.toString(core.key, 'hex')
      const existing = this.chatStore.findOwnMediaDescriptor(conversationId, mediaId)
      if (existing && existing.mediaCoreKey === mediaCoreKey) return existing
      const bytes = this.mediaStore.getMedia(mediaId)
      if (!bytes) return null
      const { offset, n } = await mediaBlobs.put(core, bytes)
      diag('Media appended to core conv=' + conversationId.substring(0, 12) +
        ' media=' + mediaId.substring(0, 12) + ' offset=' + offset + ' blocks=' + n)
      return { mediaCoreKey, mediaBlockOffset: offset, mediaBlockLength: n }
    } catch (err) {
      diag('Media core append failed conv=' + conversationId.substring(0, 12) + ': ' + (err.message || err))
      return null
    }
  }

  sendResponse(id, success, data, error = null) {
    if (!this.ipc) {
      diag('sendResponse: IPC not available, trying stdout fallback')
      // Try stdout as fallback for BareKit
      if (typeof process !== 'undefined' && process.stdout) {
        try {
          const response = { id, success }
          if (data !== null) response.data = data
          if (error !== null) response.error = error
          const buffer = Buffer.from(JSON.stringify(response) + '\n')
          process.stdout.write(buffer)
          diag('Response sent via stdout')
          return
        } catch (e) {
          diag('Failed to write to stdout: ' + e.message)
        }
      }
      return
    }

    const response = { id, success }
    if (data !== null) response.data = data
    if (error !== null) response.error = error

    const buffer = Buffer.from(JSON.stringify(response) + '\n')
    this.ipc.write(buffer)
  }

  pushEvent(type, payload) {
    if (!this.ipc) {
      diag('pushEvent: IPC not available, trying stdout fallback')
      if (typeof process !== 'undefined' && process.stdout) {
        try {
          const event = { type, payload, timestamp: Date.now() }
          const buffer = Buffer.from(JSON.stringify(event) + '\n')
          process.stdout.write(buffer)
          diag('Event pushed via stdout')
          return
        } catch (e) {
          diag('Failed to push event to stdout: ' + e.message)
        }
      }
      return
    }

    const event = { type, payload, timestamp: Date.now() }
    const buffer = Buffer.from(JSON.stringify(event) + '\n')
    this.ipc.write(buffer)
  }
}

module.exports = { IPCHandler }
