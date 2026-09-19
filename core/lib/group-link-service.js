'use strict'

/**
 * Group invite links: the join protocol, owner and joiner sides.
 *
 * Owner. A link names a random rendezvous key M. Joiners deposit signed,
 * encrypted requests in the blind peer mailbox addressed to M; only the owner
 * holds M's secret half, so only the owner can list and acknowledge them. The
 * owner drains those mailboxes whenever its own mailbox is drained (start,
 * resume, heartbeat) and faster for a while after touching the link. A valid
 * request is admitted through the ordinary add member path, or held for the
 * owner to approve.
 *
 * Joiner. A request carries two signatures: one by a key derived from the
 * link secret (proof of holding the link) and one by the joiner's identity
 * (proof the key is theirs). The envelope is signed by a throwaway key so the
 * mailbox operator does not learn who is asking. After signing, the secret is
 * no longer needed and is not kept.
 *
 * Everything the service touches outside itself is injected, so the whole
 * protocol runs under node --test without a network.
 */

const b4a = require('b4a')
const sodium = require('sodium-universal')
const gl = require('./group-link')
const { createDiagnosticLogger } = require('./diagnostics')

const diag = createDiagnosticLogger('GLINK')

const DAY_MS = 24 * 60 * 60 * 1000
const MAX_MEMBERS = 100
const MAX_RETIRED = 4
const RETIRED_KEEP_MS = 30 * DAY_MS
const REQUEST_TTL_MS = 30 * DAY_MS
const JOINER_TTL_MS = 30 * DAY_MS
const FINISHED_KEEP_MS = 7 * DAY_MS
const RESEND_INTERVAL_MS = DAY_MS
const MAX_SENDS = 3
const CLOCK_SKEW_MS = 5 * 60 * 1000
const RATE_WINDOW_MS = 60 * 60 * 1000
const RATE_LIMIT = 20
const FAST_POLL_MS = 10 * 1000
const FAST_WINDOW_MS = 10 * 60 * 1000

const WAITING_STATUSES = new Set(['waiting', 'pending_approval'])
const FINAL_STATUSES = new Set(['joined', 'inactive', 'expired', 'full', 'declined', 'cancelled'])

class GroupLinkRequestError extends Error {
  constructor (code, message) {
    super(message)
    this.code = code
  }
}

function hex (buf) {
  return b4a.toString(buf, 'hex')
}

function fromHex (text) {
  return b4a.from(text, 'hex')
}

function throwawayKeyPair () {
  const publicKey = b4a.alloc(sodium.crypto_sign_PUBLICKEYBYTES)
  const secretKey = b4a.alloc(sodium.crypto_sign_SECRETKEYBYTES)
  sodium.crypto_sign_keypair(publicKey, secretKey)
  return { publicKey, secretKey }
}

/** The group name, cut to what a link may carry, on a character boundary. */
function linkNameHint (displayName) {
  if (typeof displayName !== 'string') return null
  // eslint-disable-next-line no-control-regex
  let name = displayName.replace(/[\u0000-\u001f\u007f]/g, ' ').trim()
  if (typeof name.normalize === 'function') name = name.normalize('NFC')
  while (name.length > 0 && b4a.byteLength(name) > gl.MAX_NAME_BYTES) {
    name = Array.from(name).slice(0, -1).join('').trim()
  }
  return name.length > 0 ? name : null
}

class GroupLinkService {
  /**
   * @param {object} deps
   * @param {import('./group-link-store').GroupLinkStore} deps.store
   * @param {{conversations: Map<string, object>, hasLeftConversation: Function}} deps.chatStore
   * @param {{keyPair: object|null, publicKeyHex: string|null, displayName: string}} deps.identity
   * @param {{put: Function, drain: Function, sendInvite: Function, drainOwn?: Function}} deps.transport
   *   put(senderKeyPair, recipientHex, record) -> Promise<boolean>
   *   drain(keyPair, deliver) -> Promise<boolean>
   *   sendInvite(recipientHex, record) -> Promise<boolean>, the identity invite path
   *   drainOwn() -> Promise, drains this identity's own mailbox
   * @param {Function} deps.admit async (conversationId, joinerKey, joinerName, viaLink) -> boolean
   * @param {Function} deps.emit (type, payload) -> void
   * @param {Function} [deps.now]
   * @param {{setInterval: Function, clearInterval: Function}} [deps.timers]
   */
  constructor ({ store, chatStore, identity, transport, admit, emit, now, timers }) {
    this.store = store
    this.chatStore = chatStore
    this.identity = identity
    this.transport = transport
    this.admit = admit
    this.emit = emit || (() => {})
    this.now = now || (() => Date.now())
    this.timers = timers || { setInterval, clearInterval }
    this._ownerFastUntil = 0
    this._joinerFastUntil = 0
    this._fastTimer = null
    this._ownerDrainInFlight = null
  }

  get myKey () {
    return (this.identity && this.identity.publicKeyHex) || null
  }

  close () {
    if (this._fastTimer) this.timers.clearInterval(this._fastTimer)
    this._fastTimer = null
  }

  // ── Owner: link management ───────────────────────────────────────────────

  _ownedGroup (conversationId) {
    const conv = this.chatStore.conversations.get(conversationId)
    if (!conv || conv.type !== 'group' || this.chatStore.hasLeftConversation(conversationId)) {
      throw new GroupLinkRequestError('CONVERSATION_NOT_FOUND', 'Group not found')
    }
    if (!this.myKey || (conv.creatorKey || '').toLowerCase() !== this.myKey.toLowerCase()) {
      throw new GroupLinkRequestError('NOT_GROUP_OWNER', 'Only the group owner can manage the invite link')
    }
    return conv
  }

  _newLinkRecord (options) {
    const material = gl.createLinkMaterial()
    return {
      linkId: hex(material.linkId),
      secret: hex(material.secret),
      rendezvousSeed: hex(material.rendezvousSeed),
      rendezvousPublicKey: hex(material.rendezvous.publicKey),
      proofPublicKey: hex(material.proofPublicKey),
      createdAt: this.now(),
      expiresAt: options.expiresAt ?? null,
      maxJoins: options.maxJoins ?? null,
      joins: 0,
      includeName: options.includeName !== false,
      approval: options.approval === 'owner' ? 'owner' : 'auto',
      approvalReason: null,
      state: 'active',
      declinedKeys: []
    }
  }

  _buildLink (record, conv) {
    const expiresAtSeconds = record.expiresAt == null
      ? null
      : Math.min(0xffffffff, Math.max(0, Math.floor(record.expiresAt / 1000)))
    return gl.buildLink({
      secret: fromHex(record.secret),
      rendezvousPublicKey: fromHex(record.rendezvousPublicKey),
      expiresAt: expiresAtSeconds,
      nameHint: record.includeName ? linkNameHint(conv.displayName) : null
    })
  }

  _linkInfo (conversationId, conv) {
    const entry = this.store.ownerEntry(conversationId)
    const current = entry && entry.current
    if (!current) return { conversationId, state: 'none' }
    return {
      conversationId,
      state: current.state,
      link: this._buildLink(current, conv),
      linkId: current.linkId,
      createdAt: current.createdAt,
      expiresAt: current.expiresAt,
      maxJoins: current.maxJoins,
      joins: current.joins,
      includeName: current.includeName,
      approval: current.approval,
      approvalReason: current.approvalReason,
      pendingRequests: entry.requests.length
    }
  }

  static _validateOptions (options = {}) {
    const out = {}
    if (Object.prototype.hasOwnProperty.call(options, 'expiresAt')) {
      if (options.expiresAt !== null && (!Number.isSafeInteger(options.expiresAt) || options.expiresAt <= 0)) {
        throw new GroupLinkRequestError('INVALID_OPTIONS', 'expiresAt must be a positive time in milliseconds or null')
      }
      out.expiresAt = options.expiresAt
    }
    if (Object.prototype.hasOwnProperty.call(options, 'maxJoins')) {
      if (options.maxJoins !== null && (!Number.isSafeInteger(options.maxJoins) || options.maxJoins < 1)) {
        throw new GroupLinkRequestError('INVALID_OPTIONS', 'maxJoins must be a positive number or null')
      }
      out.maxJoins = options.maxJoins
    }
    if (Object.prototype.hasOwnProperty.call(options, 'includeName')) out.includeName = options.includeName !== false
    if (Object.prototype.hasOwnProperty.call(options, 'approval')) {
      if (options.approval !== 'auto' && options.approval !== 'owner') {
        throw new GroupLinkRequestError('INVALID_OPTIONS', 'approval must be auto or owner')
      }
      out.approval = options.approval
    }
    return out
  }

  /** The owner's current link, or state none. Touching the link starts fast polling. */
  getLink (conversationId) {
    const conv = this._ownedGroup(conversationId)
    this._touchOwner()
    return this._linkInfo(conversationId, conv)
  }

  /** Turns the link on, creating one when there is none. */
  enableLink (conversationId, options = {}) {
    const conv = this._ownedGroup(conversationId)
    const opts = GroupLinkService._validateOptions(options)
    const entry = this.store.ownerEntry(conversationId, true)
    if (!entry.current) {
      entry.current = this._newLinkRecord(opts)
    } else {
      Object.assign(entry.current, opts, { state: 'active' })
      if (opts.approval === 'auto') entry.current.approvalReason = null
    }
    this.store.save()
    this._touchOwner()
    return this._linkInfo(conversationId, conv)
  }

  updateLink (conversationId, options = {}) {
    const conv = this._ownedGroup(conversationId)
    const opts = GroupLinkService._validateOptions(options)
    const entry = this.store.ownerEntry(conversationId)
    if (!entry || !entry.current) throw new GroupLinkRequestError('NO_LINK', 'The group has no invite link')
    Object.assign(entry.current, opts)
    if (opts.approval) entry.current.approvalReason = null
    this.store.save()
    this._touchOwner()
    return this._linkInfo(conversationId, conv)
  }

  /** New secret and new rendezvous key. The old link answers "inactive" for 30 days. */
  resetLink (conversationId) {
    const conv = this._ownedGroup(conversationId)
    const entry = this.store.ownerEntry(conversationId, true)
    const previous = entry.current
    if (previous) this._retire(entry, previous)
    entry.current = this._newLinkRecord(previous || {})
    if (previous) entry.current.approval = previous.approval
    this.store.save()
    this._touchOwner()
    return this._linkInfo(conversationId, conv)
  }

  disableLink (conversationId) {
    const conv = this._ownedGroup(conversationId)
    const entry = this.store.ownerEntry(conversationId)
    if (entry && entry.current) {
      entry.current.state = 'off'
      this.store.save()
    }
    return this._linkInfo(conversationId, conv)
  }

  _retire (entry, record) {
    entry.retired.unshift({
      linkId: record.linkId,
      rendezvousSeed: record.rendezvousSeed,
      rendezvousPublicKey: record.rendezvousPublicKey,
      proofPublicKey: record.proofPublicKey,
      createdAt: record.createdAt,
      retiredAt: this.now()
    })
    entry.retired = entry.retired.slice(0, MAX_RETIRED)
  }

  listRequests (conversationId) {
    this._ownedGroup(conversationId)
    const entry = this.store.ownerEntry(conversationId)
    this._touchOwner()
    return (entry ? entry.requests : []).map(r => ({
      joinerKey: r.joinerKey,
      joinerName: r.joinerName,
      requestedAt: r.requestedAt,
      receivedAt: r.receivedAt,
      previouslyRemoved: !!r.previouslyRemoved
    }))
  }

  async approve (conversationId, joinerKey) {
    const conv = this._ownedGroup(conversationId)
    const entry = this.store.ownerEntry(conversationId)
    const key = (joinerKey || '').toLowerCase()
    const request = entry && entry.requests.find(r => r.joinerKey === key)
    if (!request) throw new GroupLinkRequestError('REQUEST_NOT_FOUND', 'No such join request')
    const record = this._recordForLinkId(entry, request.linkId)

    if (this._groupIsFull(conv) || (record === entry.current && record.maxJoins != null && record.joins >= record.maxJoins)) {
      if (record) await this._sendResult(record, key, 'full')
      entry.requests = entry.requests.filter(r => r !== request)
      this.store.save()
      return { status: 'full' }
    }

    const viaLink = record ? this._viaLink(record, conv, key) : null
    if (!await this.admit(conversationId, key, request.joinerName, viaLink)) {
      throw new GroupLinkRequestError('ADMIT_FAILED', 'The member could not be added')
    }
    entry.requests = entry.requests.filter(r => r !== request)
    if (record === entry.current) {
      record.joins += 1
      entry.admissions.push(this.now())
    }
    this.store.save()
    this.emit('group_link.member_joined', { conversationId, memberKey: key, memberName: request.joinerName })
    return { status: 'admitted' }
  }

  async decline (conversationId, joinerKey) {
    this._ownedGroup(conversationId)
    const entry = this.store.ownerEntry(conversationId)
    const key = (joinerKey || '').toLowerCase()
    const request = entry && entry.requests.find(r => r.joinerKey === key)
    if (!request) throw new GroupLinkRequestError('REQUEST_NOT_FOUND', 'No such join request')
    const record = this._recordForLinkId(entry, request.linkId)
    if (record) {
      if (!Array.isArray(record.declinedKeys)) record.declinedKeys = []
      if (!record.declinedKeys.includes(key)) record.declinedKeys.push(key)
      record.declinedKeys = record.declinedKeys.slice(-512)
    }
    entry.requests = entry.requests.filter(r => r !== request)
    this.store.save()
    if (record) await this._sendResult(record, key, 'declined')
    return { status: 'declined' }
  }

  /**
   * The group is gone for this owner (left, deleted or removed locally). The
   * link stops at once; queued requests are answered "inactive".
   */
  async onGroupGone (conversationId) {
    const entry = this.store.ownerEntry(conversationId)
    if (!entry) return
    const requests = entry.requests
    entry.requests = []
    if (entry.current) {
      this._retire(entry, entry.current)
      entry.current = null
    }
    this.store.save()
    for (const request of requests) {
      const record = this._recordForLinkId(entry, request.linkId)
      if (record) await this._sendResult(record, request.joinerKey, 'inactive')
    }
  }

  _recordForLinkId (entry, linkId) {
    if (!entry) return null
    if (entry.current && entry.current.linkId === linkId) return entry.current
    return entry.retired.find(r => r.linkId === linkId) || null
  }

  _groupIsFull (conv) {
    return (conv.participantIds || []).length + 1 >= MAX_MEMBERS
  }

  _viaLink (record, conv, joinerKey) {
    return {
      linkId: record.linkId,
      admitSig: gl.signAdmit(gl.rendezvousKeyPair(fromHex(record.rendezvousSeed)), {
        linkId: record.linkId,
        joinerKey,
        groupId: conv.groupId,
        creatorKey: this.myKey.toLowerCase()
      })
    }
  }

  async _sendResult (record, joinerKey, status) {
    try {
      const rendezvous = gl.rendezvousKeyPair(fromHex(record.rendezvousSeed))
      const result = gl.createJoinResult(rendezvous, { linkId: record.linkId, joinerKey, status })
      // Signed and carried as M, so the result neither reveals nor needs the
      // owner's identity.
      return await this.transport.put(rendezvous, joinerKey, result)
    } catch (err) {
      diag('Join result not delivered: ' + (err.message || err))
      return false
    }
  }

  // ── Owner: request intake ────────────────────────────────────────────────

  /** Drains every link mailbox this device owns. Safe to call often. */
  async drainOwnerMailboxes () {
    if (this._ownerDrainInFlight) return this._ownerDrainInFlight
    this._ownerDrainInFlight = this._drainOwnerMailboxesOnce()
    try {
      return await this._ownerDrainInFlight
    } finally {
      this._ownerDrainInFlight = null
    }
  }

  async _drainOwnerMailboxesOnce () {
    let handled = 0
    for (const conversationId of this.store.ownerConversationIds()) {
      const entry = this.store.ownerEntry(conversationId)
      this._pruneOwnerEntry(entry)
      const records = [entry.current, ...entry.retired].filter(Boolean)
      for (const record of records) {
        const rendezvous = gl.rendezvousKeyPair(fromHex(record.rendezvousSeed))
        const isCurrent = record === entry.current
        try {
          await this.transport.drain(rendezvous, async (item) => {
            const finished = await this._handleRequest(conversationId, entry, record, isCurrent, item.invite)
            if (finished) handled++
            return finished
          })
        } catch (err) {
          diag('Link mailbox drain failed: ' + (err.message || err))
        }
      }
      if (!entry.current && entry.retired.length === 0 && entry.requests.length === 0) {
        delete this.store.state.owner[conversationId]
      }
    }
    this.store.save()
    return handled
  }

  _pruneOwnerEntry (entry) {
    const now = this.now()
    entry.retired = entry.retired.filter(r => now - r.retiredAt < RETIRED_KEEP_MS)
    entry.requests = entry.requests.filter(r => now - r.receivedAt < REQUEST_TTL_MS)
    entry.admissions = entry.admissions.filter(t => now - t < RATE_WINDOW_MS)
  }

  /**
   * One request from a link mailbox. Returns true when it is finished with,
   * so the mailbox copy may be deleted; false only when a later drain might
   * succeed where this one failed.
   */
  async _handleRequest (conversationId, entry, record, isCurrent, request) {
    if (!request || request.type !== 'group_join_request') return true
    const context = { rendezvousPublicKey: fromHex(record.rendezvousPublicKey), proofPublicKey: fromHex(record.proofPublicKey) }
    if (request.linkId !== record.linkId || !gl.verifyJoinRequest(request, context)) {
      diag('Discarded a join request that did not verify')
      return true
    }
    const now = this.now()
    if (request.requestedAt < record.createdAt - CLOCK_SKEW_MS || request.requestedAt > now + CLOCK_SKEW_MS) return true
    const joinerKey = request.joinerKey.toLowerCase()
    if (joinerKey === (this.myKey || '').toLowerCase()) return true

    const conv = this.chatStore.conversations.get(conversationId)
    const ownsGroup = conv && conv.type === 'group' && !this.chatStore.hasLeftConversation(conversationId) &&
      (conv.creatorKey || '').toLowerCase() === (this.myKey || '').toLowerCase()
    if (!ownsGroup || !isCurrent || record.state !== 'active') {
      await this._sendResult(record, joinerKey, 'inactive')
      return true
    }
    if (record.expiresAt != null && now >= record.expiresAt) {
      await this._sendResult(record, joinerKey, 'expired')
      return true
    }
    // A block, and a decline on this link, are never revealed.
    if (this.store.isBlocked(joinerKey) || (record.declinedKeys || []).includes(joinerKey)) return true
    const previouslyRemoved = this.store.removedKeys(conversationId).includes(joinerKey)
    if (previouslyRemoved && record.approval !== 'owner') return true

    const isMember = (conv.participantIds || []).some(k => (k || '').toLowerCase() === joinerKey)
    if (isMember) {
      // Already in: the first invite may have been lost. Send it again.
      await this.admit(conversationId, joinerKey, request.joinerName, this._viaLink(record, conv, joinerKey), { resendOnly: true })
      return true
    }
    if (this._groupIsFull(conv) || (record.maxJoins != null && record.joins >= record.maxJoins)) {
      await this._sendResult(record, joinerKey, 'full')
      return true
    }

    if (record.approval !== 'owner' && entry.admissions.filter(t => now - t < RATE_WINDOW_MS).length >= RATE_LIMIT) {
      record.approval = 'owner'
      record.approvalReason = 'rate'
      this.store.save()
      this.emit('group_link.approval_switched', { conversationId, reason: 'rate' })
    }

    if (record.approval === 'owner') {
      const existing = entry.requests.find(r => r.joinerKey === joinerKey)
      if (!existing) {
        entry.requests.push({
          linkId: record.linkId,
          joinerKey,
          joinerName: request.joinerName,
          requestedAt: request.requestedAt,
          receivedAt: now,
          previouslyRemoved
        })
        // Saved before the mailbox copy is acknowledged (and so deleted).
        this.store.save()
        this.emit('group_link.request_received', { conversationId, joinerKey, joinerName: request.joinerName, previouslyRemoved })
      }
      await this._sendResult(record, joinerKey, 'pending_approval')
      return true
    }

    let admitted = false
    try {
      admitted = await this.admit(conversationId, joinerKey, request.joinerName, this._viaLink(record, conv, joinerKey))
    } catch (err) {
      diag('Link admission failed: ' + (err.message || err))
    }
    if (!admitted) return false
    record.joins += 1
    entry.admissions.push(now)
    this.store.save()
    this.emit('group_link.member_joined', { conversationId, memberKey: joinerKey, memberName: request.joinerName })
    return true
  }

  // ── Joiner ───────────────────────────────────────────────────────────────

  /** Display fields for a preview. Contacts nobody. */
  inspect (link) {
    return gl.inspectLink(link, Math.floor(this.now() / 1000))
  }

  _ownLinkConversation (rendezvousPublicKeyHex) {
    for (const conversationId of this.store.ownerConversationIds()) {
      const entry = this.store.ownerEntry(conversationId)
      if (entry.current && entry.current.rendezvousPublicKey === rendezvousPublicKeyHex) return conversationId
    }
    return null
  }

  _liveConversation (conversationId) {
    return conversationId && this.chatStore.conversations.has(conversationId) &&
      !this.chatStore.hasLeftConversation(conversationId)
  }

  /**
   * Asks to join through a link.
   * @returns {Promise<{status: string, linkId?: string, conversationId?: string}>}
   */
  async join (link, joinerName = null) {
    if (!this.identity || !this.identity.keyPair) {
      throw new GroupLinkRequestError('NO_IDENTITY', 'A chat identity is needed to join a group')
    }
    const inspected = this.inspect(link)
    if (inspected.status !== 'ok') return { status: inspected.status, linkId: inspected.linkId }
    const parsed = gl.parseLink(link)
    const linkId = hex(gl.deriveLinkId(parsed.secret))
    const rendezvousPublicKey = hex(parsed.rendezvousPublicKey)
    this._touchJoiner()

    const ownConversation = this._ownLinkConversation(rendezvousPublicKey)
    if (ownConversation) return { status: 'already_member', linkId, conversationId: ownConversation }
    const joined = this.store.conversationJoinedVia(linkId)
    if (this._liveConversation(joined)) return { status: 'already_member', linkId, conversationId: joined }

    const existing = this.store.joinerRecord(linkId)
    if (existing && WAITING_STATUSES.has(existing.status)) {
      await this._maybeResend(existing)
      return { status: 'already_requested', linkId }
    }

    const name = joinerName || this.identity.displayName || this.myKey.substring(0, 8)
    const request = gl.createJoinRequest({
      secret: parsed.secret,
      rendezvousPublicKey: parsed.rendezvousPublicKey,
      joinerKeyPair: this.identity.keyPair,
      joinerName: name,
      requestedAt: this.now()
    })
    // The secret stops here: only the signed request is kept.
    parsed.secret.fill(0)
    const record = {
      linkId,
      rendezvousPublicKey,
      nameHint: parsed.nameHint,
      request,
      createdAt: this.now(),
      lastSentAt: 0,
      sendCount: 0,
      status: 'waiting',
      updatedAt: this.now(),
      conversationId: null
    }
    this.store.setJoinerRecord(linkId, record)
    this.store.save()
    await this._send(record)
    return { status: 'requested', linkId }
  }

  async _send (record) {
    let sent = false
    try {
      sent = await this.transport.put(throwawayKeyPair(), record.rendezvousPublicKey, record.request)
    } catch (err) {
      diag('Join request not sent yet: ' + (err.message || err))
    }
    if (sent) {
      record.sendCount += 1
      record.lastSentAt = this.now()
      this.store.save()
    }
    return sent
  }

  async _maybeResend (record) {
    if (record.status !== 'waiting') return false
    const due = record.sendCount === 0 ||
      (record.sendCount < MAX_SENDS && this.now() - record.lastSentAt >= RESEND_INTERVAL_MS)
    return due ? this._send(record) : false
  }

  joinStatus () {
    return this.store.joinerRecords().map(r => ({
      linkId: r.linkId,
      status: r.status,
      nameHint: r.nameHint,
      createdAt: r.createdAt,
      conversationId: r.conversationId || null
    }))
  }

  cancel (linkId) {
    const record = this.store.joinerRecord(linkId)
    if (!record) return false
    this.store.deleteJoinerRecord(linkId)
    this.store.save()
    return true
  }

  _finish (record, status, conversationId = null) {
    record.status = status
    record.updatedAt = this.now()
    if (conversationId) record.conversationId = conversationId
    // Nothing waiting needs the request any more.
    if (FINAL_STATUSES.has(status)) record.request = null
    this.store.save()
    this.emit('group_link.join_updated', { linkId: record.linkId, status, conversationId: record.conversationId || null })
  }

  /**
   * A signed answer that is not an admission, from the link's rendezvous key.
   * Always finished with: an answer that does not verify is discarded.
   */
  handleJoinResult (result, senderKeyHex) {
    if (!result || result.type !== 'group_join_result') return true
    const record = this.store.joinerRecord(result.linkId)
    if (!record || !WAITING_STATUSES.has(record.status)) return true
    if ((senderKeyHex || '').toLowerCase() !== record.rendezvousPublicKey) return true
    if ((result.joinerKey || '').toLowerCase() !== (this.myKey || '').toLowerCase()) return true
    if (!gl.verifyJoinResult(result, fromHex(record.rendezvousPublicKey))) return true
    if (result.status === 'pending_approval') {
      if (record.status !== 'pending_approval') this._finish(record, 'pending_approval')
      return true
    }
    this._finish(record, result.status)
    return true
  }

  /**
   * Called after the ordinary group invite path accepted an invite. When it
   * carries a valid admission for one of our requests, that request is done.
   */
  onGroupInviteApplied (invite, conversation) {
    const via = invite && invite.viaLink
    if (!via || !conversation) return false
    const record = this.store.joinerRecord(via.linkId)
    if (!record) return false
    const ok = gl.verifyAdmit(via.admitSig, fromHex(record.rendezvousPublicKey), {
      linkId: via.linkId,
      joinerKey: this.myKey,
      groupId: invite.groupId,
      creatorKey: invite.creatorKey
    })
    if (!ok) {
      diag('Ignored an admission that did not verify')
      return false
    }
    this.store.addJoinedVia(conversation.id, via.linkId)
    if (record.status !== 'joined' || record.conversationId !== conversation.id) {
      this._finish(record, 'joined', conversation.id)
    } else {
      this.store.save()
    }
    return true
  }

  /** Resends and expires waiting requests. Called on every mailbox drain. */
  async maintainJoinRequests () {
    const now = this.now()
    for (const record of this.store.joinerRecords()) {
      if (WAITING_STATUSES.has(record.status)) {
        if (now - record.createdAt >= JOINER_TTL_MS) {
          this._finish(record, 'expired')
          continue
        }
        await this._maybeResend(record)
      } else if (now - (record.updatedAt || record.createdAt) >= FINISHED_KEEP_MS) {
        this.store.deleteJoinerRecord(record.linkId)
        this.store.save()
      }
    }
  }

  // ── Removal support ──────────────────────────────────────────────────────

  /** Keys the owner removed are never admitted again automatically. */
  recordRemoval (conversationId, memberKey) {
    this.store.addRemovedKey(conversationId, memberKey.toLowerCase())
    this.store.save()
  }

  setBlockedKeys (keys) {
    if (!Array.isArray(keys) || keys.some(k => typeof k !== 'string' || !/^(0x)?[0-9a-f]{64}$/i.test(k))) {
      throw new GroupLinkRequestError('INVALID_KEYS', 'keys must be 64 character hex public keys')
    }
    this.store.setBlockedKeys(keys.map(k => k.replace(/^0x/i, '')))
    this.store.save()
  }

  // ── Scheduling ───────────────────────────────────────────────────────────

  /** Everything that should happen whenever the app drains mailboxes. */
  async onMailboxDrain () {
    await this.drainOwnerMailboxes()
    await this.maintainJoinRequests()
  }

  _touchOwner () {
    this._ownerFastUntil = this.now() + FAST_WINDOW_MS
    this._ensureFastTimer()
  }

  _touchJoiner () {
    this._joinerFastUntil = this.now() + FAST_WINDOW_MS
    this._ensureFastTimer()
  }

  _ensureFastTimer () {
    if (this._fastTimer) return
    this._fastTimer = this.timers.setInterval(() => {
      const now = this.now()
      const owner = now < this._ownerFastUntil
      const joiner = now < this._joinerFastUntil && this.store.joinerRecords().some(r => WAITING_STATUSES.has(r.status))
      if (!owner && !joiner) {
        this.close()
        return
      }
      if (owner) this.drainOwnerMailboxes().catch(() => {})
      if (joiner && this.transport.drainOwn) this.transport.drainOwn().catch(() => {})
    }, FAST_POLL_MS)
    if (this._fastTimer && typeof this._fastTimer.unref === 'function') this._fastTimer.unref()
  }
}

module.exports = {
  GroupLinkService,
  GroupLinkRequestError,
  linkNameHint,
  MAX_MEMBERS,
  RATE_LIMIT,
  RESEND_INTERVAL_MS,
  MAX_SENDS,
  JOINER_TTL_MS,
  RETIRED_KEEP_MS
}
