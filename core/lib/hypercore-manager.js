/**
 * Hypercore Manager - Manages Corestore + per-conversation Hypercores
 *
 * Each conversation gets:
 * - A local writable Hypercore (this peer's message log)
 * - Remote read-only Hypercores (other peers' logs, keyed by their public key)
 *
 * All cores are encrypted with a key derived from the conversation ID,
 * so blind peer servers can store but never read the data.
 *
 * Messages are BOTH sent via existing FramedSocket (real-time) and appended
 * to the local Hypercore (persistence). When blind-peering syncs remote cores,
 * replicated entries flow through an awaitable sink → ChatStore → UI.
 *
 * Usage:
 *   const mgr = new HypercoreManager()
 *   await mgr.initialize()
 *   await mgr.getOrCreateLocalCore(conversationId)
 *   await mgr.appendMessage(conversationId, message)
 *   await mgr.openRemoteCore(conversationId, peerKey, coreKey)
 */

const Corestore = require('corestore')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const path = require('bare-path')
const EventEmitter = require('bare-events')
const { getDataDir, ensureDir, readJSON, writeJSON, fileExists } = require('./storage')
const { getInboundPushTopics } = require('./push-topics')
const conversationKeys = require('./conversation-keys')

// Diagnostic logging
let _diagFs, _diagPath, _diagOs, _diagFile
try {
  _diagFs = require('bare-fs')
  _diagPath = require('bare-path')
  _diagOs = require('bare-os')
  const _dataDirArg = (typeof Bare !== 'undefined' ? Bare.argv : [])
    .find(a => a.startsWith('--data-dir='))
  const _baseDir = _dataDirArg
    ? _dataDirArg.substring(_dataDirArg.indexOf('=') + 1)
    : _diagPath.join(_diagOs.homedir(), 'Documents')
  _diagFile = _diagPath.join(_baseDir, 'zappmessaging', 'hypercore-diag.log')
} catch (e) { /* logging unavailable */ }

function diag (...args) {
  try {
    if (!_diagFs || !_diagFile) return
    const logDir = _diagPath.dirname(_diagFile)
    if (!_diagFs.existsSync(logDir)) _diagFs.mkdirSync(logDir, { recursive: true })
    _diagFs.appendFileSync(_diagFile, new Date().toISOString() + ' [HC] ' + args.join(' ') + '\n')
  } catch (e) { /* ignore */ }
}

const DRAIN_RETRY_BASE_MS = 250
const DRAIN_RETRY_MAX_MS = 30000

function isPermanentDecodeError (err) {
  return !!err && err.code === 'DECODING_ERROR'
}

function coreFork (core) {
  return Number.isSafeInteger(core && core.fork) && core.fork >= 0 ? core.fork : 0
}

class HypercoreManager extends EventEmitter {
  constructor () {
    super()
    this.store = null
    this.localCores = new Map()    // conversationId → Hypercore (writable)
    this.remoteCores = new Map()   // conversationId → Map<peerKeyHex, Hypercore>
    this._coreKeyIndex = new Map() // conversationId → Map<peerKeyHex, coreKeyHex>
    this._localReferrerIndex = new Map() // conversationId → recipient identity pubkey hex
    this._remoteReferrerIndex = new Map() // conversationId → Map<peerKeyHex, recipient identity pubkey hex>
    this._coreKeyIndexHydrated = false
    this._coreKeyIndexLoadPromise = null
    // Per-core processed cursor: conversationId →
    // Map<coreKeyHex, { nextIndex, fork }>. The immutable physical core key and
    // fork prevent a replacement/truncated writer from inheriting another
    // feed's cursor and silently skipping its beginning.
    this._processedCursors = new Map()
    // Awaitable ingestion callback, wired by index.js:
    //   async (conversationId, peerKeyHex, message, index) => void
    // It must fully persist the message before resolving; a throw means "not
    // ingested" and the cursor is left where it is so the block is retried.
    this._remoteMessageSink = null
    // Called once after a drain batch with the newest delivery watermark
    // returned by the message sink. It must durably queue the receipt before
    // resolving; only then can the whole batch cursor advance.
    this._remoteDrainCompleteSink = null
    // Serializes drains per core and tracks lifecycle cancellation/retry.
    this._drainChains = new Map()
    this._drainStates = new Map()
    this._drainRetryBaseMs = DRAIN_RETRY_BASE_MS
    this._drainRetryMaxMs = DRAIN_RETRY_MAX_MS
    this._ready = false
    this._closed = false
    // Key/authorization context, wired by index.js before any core opens.
    // Both callbacks are synchronous: getIdentityKeyPair() → {publicKey,
    // secretKey}|null, getConversation(id) → conversation record|null.
    this._keyContext = null
  }

  /**
   * Wire the identity + conversation lookups that key derivation and
   * remote-core authorization depend on. Must be called before any core is
   * opened; deriveEncryptionKey fails closed without it.
   */
  setKeyContext (keyContext) {
    this._keyContext = keyContext
  }

  /**
   * Wire the awaitable sink that turns replicated blocks into stored messages.
   * Set before any remote core opens so the initial drain has somewhere to go.
   */
  setRemoteMessageSink (fn) {
    this._remoteMessageSink = fn
  }

  setRemoteDrainCompleteSink (fn) {
    this._remoteDrainCompleteSink = fn
  }

  async initialize () {
    if (this._ready) return
    const dataDir = getDataDir()
    const storePath = path.join(dataDir, 'corestore')
    ensureDir(storePath)
    this.store = new Corestore(storePath)
    await this.store.ready()
    this._ready = true
    diag('Corestore ready at ' + storePath)
  }

  /**
   * Derive the encryption key for a conversation (v2 — see
   * conversation-keys.js). Direct chats use X25519 ECDH between the two
   * identity keys; groups use the invite-distributed groupId; open rooms
   * (store/city) keep a deterministic key with no confidentiality claim.
   *
   * Fails closed: throws when the key context is missing, the conversation is
   * unknown, or the secret material for its type is unavailable. Callers all
   * treat a throw as "do not open this core".
   *
   * @param {string} conversationId
   * @param {number} epoch - Key epoch for rotation
   */
  deriveEncryptionKey (conversationId, epoch = 0) {
    if (!this._keyContext) {
      throw new Error('deriveEncryptionKey: key context not configured')
    }
    const conv = this._keyContext.getConversation(conversationId)
    if (!conv) {
      throw new Error('deriveEncryptionKey: unknown conversation ' + conversationId.substring(0, 12))
    }

    if (conv.type === 'group') {
      return conversationKeys.deriveGroupKey(conv.groupId, epoch)
    }

    if (conv.type === 'store' || conv.type === 'city') {
      return conversationKeys.derivePublicRoomKey(
        conv.type,
        conv.storeId || conv.citySlug || conversationId,
        epoch
      )
    }

    // Direct chat: ECDH with the single remote participant.
    const keyPair = this._keyContext.getIdentityKeyPair()
    if (!keyPair) {
      throw new Error('deriveEncryptionKey: identity unavailable')
    }
    const myHex = b4a.toString(keyPair.publicKey, 'hex')
    const peerHex = (conv.participantIds || [])
      .map(k => (k || '').toLowerCase())
      .find(k => k && k !== myHex)
    if (!peerHex) {
      throw new Error('deriveEncryptionKey: no remote participant for ' + conversationId.substring(0, 12))
    }
    return conversationKeys.deriveDirectKey(keyPair, peerHex, conversationId, epoch)
  }

  /**
   * Throw unless peerKeyHex is a verified participant (or group creator) of
   * the conversation. Last-line defense — the p2p/ipc layers gate first.
   * @private
   */
  _assertRemoteCoreAuthorized (conversationId, peerKeyHex) {
    if (!this._keyContext) {
      throw new Error('remote core rejected: key context not configured')
    }
    const conv = this._keyContext.getConversation(conversationId)
    if (!conv) {
      throw new Error('remote core rejected: unknown conversation ' + conversationId.substring(0, 12))
    }
    const peer = (peerKeyHex || '').toLowerCase()
    const isParticipant = (conv.participantIds || []).some(k => (k || '').toLowerCase() === peer)
    const isCreator = (conv.creatorKey || '').toLowerCase() === peer
    if (!isParticipant && !isCreator) {
      throw new Error('remote core rejected: ' + peer.substring(0, 12) +
        ' is not a participant of ' + conversationId.substring(0, 12))
    }
  }

  /**
   * Get the current key epoch for a conversation.
   * Returns 0 for conversations that have never rotated.
   */
  getKeyEpoch (conversationId) {
    return this._keyEpochs ? (this._keyEpochs.get(conversationId) || 0) : 0
  }

  /**
   * Rotate the encryption key for a conversation by advancing the epoch.
   * After rotation, new cores must be created with the new key.
   * Existing cores remain readable with the old key.
   *
   * @param {string} conversationId
   * @returns {number} The new epoch
   */
  async rotateKeyEpoch (conversationId) {
    if (!this._keyEpochs) this._keyEpochs = new Map()
    const current = this._keyEpochs.get(conversationId) || 0
    const next = current + 1
    this._keyEpochs.set(conversationId, next)

    // Evict the cached local core so getOrCreateLocalCore re-opens it with the
    // new epoch key on the next call. Without eviction the stale (epoch-0) core
    // would continue to be used for all subsequent appends.
    const oldCore = this.localCores.get(conversationId)
    if (oldCore) {
      try { await oldCore.close() } catch (e) { /* ignore */ }
      this.localCores.delete(conversationId)
    }

    diag('Key epoch rotated conv=' + conversationId.substring(0, 12) + ' epoch=' + next)
    return next
  }

  /**
   * Get or create the local writable Hypercore for a conversation.
   */
  async getOrCreateLocalCore (conversationId) {
    if (this.localCores.has(conversationId)) {
      return this.localCores.get(conversationId)
    }
    if (!this._ready) throw new Error('HypercoreManager not initialized')

    const encKey = this.deriveEncryptionKey(conversationId)
    const core = this.store.get({
      name: 'zapp-local-' + conversationId,
      encryptionKey: encKey,
      valueEncoding: 'json'
    })
    await core.ready()
    this.localCores.set(conversationId, core)
    diag('Local core ready conv=' + conversationId.substring(0, 12) +
      ' key=' + b4a.toString(core.key, 'hex').substring(0, 12) +
      ' len=' + core.length)
    return core
  }

  /**
   * Open a remote peer's Hypercore (read-only) for a conversation.
   * Drains new entries through the configured awaitable sink.
   */
  async openRemoteCore (conversationId, peerKeyHex, coreKeyHex, referrerKeyHex = null) {
    if (!this._ready) throw new Error('HypercoreManager not initialized')
    this._assertRemoteCoreAuthorized(conversationId, peerKeyHex)

    const normalizedCoreKey = (coreKeyHex || '').toLowerCase()

    if (!this.remoteCores.has(conversationId)) {
      this.remoteCores.set(conversationId, new Map())
    }
    const remotes = this.remoteCores.get(conversationId)
    if (remotes.has(peerKeyHex)) {
      const indexedKey = this._coreKeyIndex.get(conversationId)?.get(peerKeyHex)
      if ((indexedKey || '').toLowerCase() === normalizedCoreKey) {
        if (referrerKeyHex) this.setRemoteCoreReferrer(conversationId, peerKeyHex, referrerKeyHex)
        return remotes.get(peerKeyHex)
      }

      // A peer can replace its writer after key rotation or conversation
      // recreation. Stop the old drain before opening the new physical core;
      // its cursor is keyed by core key and must never carry across.
      const oldCore = remotes.get(peerKeyHex)
      await this._closeRemoteCore(oldCore)
      remotes.delete(peerKeyHex)
      if (indexedKey) this._deleteProcessedCursor(conversationId, indexedKey)
    }

    if (!this._coreKeyIndex.has(conversationId)) {
      this._coreKeyIndex.set(conversationId, new Map())
    }
    this._coreKeyIndex.get(conversationId).set(peerKeyHex, normalizedCoreKey)
    if (referrerKeyHex) this._setRemoteCoreReferrerInMemory(conversationId, peerKeyHex, referrerKeyHex)

    const encKey = this.deriveEncryptionKey(conversationId)
    const keyBuf = b4a.from(normalizedCoreKey, 'hex')

    const core = this.store.get({
      key: keyBuf,
      encryptionKey: encKey,
      valueEncoding: 'json'
    })
    await core.ready()
    remotes.set(peerKeyHex, core)

    this._watchRemoteCore(conversationId, peerKeyHex, normalizedCoreKey, core)
    // Drain anything already on disk that a prior session replicated but never
    // ingested (the force-stop silent-loss case). Fire-and-forget: the drain
    // serializes with the watcher via _drainChains and persists its own cursor.
    this._drainRemoteCore(conversationId, peerKeyHex, normalizedCoreKey, core)

    diag('Remote core opened conv=' + conversationId.substring(0, 12) +
      ' peer=' + peerKeyHex.substring(0, 12) +
      ' key=' + normalizedCoreKey.substring(0, 12) +
      ' len=' + core.length +
      ' cursor=' + this._getProcessedCursor(conversationId, normalizedCoreKey, coreFork(core)))

    // Persist immediately. close() only runs on graceful shutdown, but
    // Android force-stops skip it, leaving the index empty after restart
    // and breaking offline blind-peer delivery (the device no longer
    // knows which remote cores to reopen).
    this.saveCoreKeyIndex()
    this.emit('push-topics-changed')
    return core
  }

  /** Next un-ingested block index for a physical remote core and fork. */
  _getProcessedCursor (conversationId, coreKeyHex, fork = 0) {
    const perConv = this._processedCursors.get(conversationId)
    const cursor = perConv && perConv.get((coreKeyHex || '').toLowerCase())
    if (!cursor || cursor.fork !== fork) return 0
    return cursor.nextIndex
  }

  /**
   * Advance the persisted processed cursor. Monotonic — never moves backward.
   * Persists synchronously (atomic tmp+rename) so the low-water mark survives
   * a force-stop that skips close().
   * @private
   */
  _setProcessedCursor (conversationId, coreKeyHex, nextIndex, fork = 0) {
    if (!this._processedCursors.has(conversationId)) {
      this._processedCursors.set(conversationId, new Map())
    }
    const perConv = this._processedCursors.get(conversationId)
    const key = (coreKeyHex || '').toLowerCase()
    const current = perConv.get(key)
    if (current && current.fork === fork && current.nextIndex >= nextIndex) return
    perConv.set(key, { nextIndex, fork })
    this.saveCoreKeyIndex()
  }

  _deleteProcessedCursor (conversationId, coreKeyHex) {
    const perConv = this._processedCursors.get(conversationId)
    if (!perConv) return
    perConv.delete((coreKeyHex || '').toLowerCase())
    if (perConv.size === 0) this._processedCursors.delete(conversationId)
  }

  /**
   * Watch a remote core for new entries arriving via replication. Every append
   * re-drives the same cursor-based drain, which resumes from the persisted
   * cursor rather than a per-open in-memory length.
   * @private
   */
  _watchRemoteCore (conversationId, peerKeyHex, coreKeyHex, core) {
    const state = this._ensureDrainState(conversationId, peerKeyHex, coreKeyHex, core)
    if (state.appendHandler) return
    state.appendHandler = () => {
      this._drainRemoteCore(conversationId, peerKeyHex, coreKeyHex, core)
    }
    core.on('append', state.appendHandler)
  }

  /**
   * Ingest blocks [cursor, core.length) in order, advancing the persisted
   * cursor only after the entire batch and its coalesced delivery watermark
   * are durable. Serialized per core so the open-time drain and append-driven
   * drains never interleave.
   * @private
   */
  _drainRemoteCore (conversationId, peerKeyHex, coreKeyHex, core) {
    const state = this._ensureDrainState(conversationId, peerKeyHex, coreKeyHex, core)
    if (state.cancelled || this._closed) return Promise.resolve()
    if (state.retryTimer) {
      clearTimeout(state.retryTimer)
      state.retryTimer = null
    }
    const prev = this._drainChains.get(core) || Promise.resolve()
    const attempt = prev.then(async () => {
      if (state.cancelled || this._closed) return
      await this._drainRemoteCoreInner(state)
      state.retryAttempt = 0
    })
    const next = attempt.catch((err) => {
      diag('Remote drain failed conv=' + conversationId.substring(0, 12) +
        ': ' + (err.message || err))
      if (!state.cancelled && !this._closed) this._scheduleDrainRetry(state)
    })
    this._drainChains.set(core, next)
    return next
  }

  _ensureDrainState (conversationId, peerKeyHex, coreKeyHex, core) {
    let state = this._drainStates.get(core)
    if (state) return state
    state = {
      conversationId,
      peerKeyHex,
      coreKeyHex: (coreKeyHex || '').toLowerCase(),
      core,
      appendHandler: null,
      cancelled: false,
      retryAttempt: 0,
      retryTimer: null
    }
    this._drainStates.set(core, state)
    return state
  }

  _scheduleDrainRetry (state) {
    if (state.retryTimer || state.cancelled || this._closed) return
    const delay = Math.min(
      this._drainRetryBaseMs * Math.pow(2, state.retryAttempt++),
      this._drainRetryMaxMs
    )
    state.retryTimer = setTimeout(() => {
      state.retryTimer = null
      this._drainRemoteCore(
        state.conversationId,
        state.peerKeyHex,
        state.coreKeyHex,
        state.core
      )
    }, delay)
    if (state.retryTimer && typeof state.retryTimer.unref === 'function') state.retryTimer.unref()
  }

  async _drainRemoteCoreInner (state) {
    const { conversationId, peerKeyHex, coreKeyHex, core } = state
    const fork = coreFork(core)
    const target = core.length
    let i = this._getProcessedCursor(conversationId, coreKeyHex, fork)
    let deliveryReceipt = null

    while (i < target) {
      let message
      try {
        message = await core.get(i)
      } catch (err) {
        if (state.cancelled || this._closed || !isPermanentDecodeError(err)) throw err
        // Only Hypercore's explicit DECODING_ERROR is permanently stepped over.
        // Session closure, request cancellation and storage errors are retryable
        // and must never move the persisted low-water mark.
        diag('Skipping undecodable remote entry ' + i + ' conv=' +
          conversationId.substring(0, 12) + ': ' + (err.message || err))
        i++
        continue
      }

      if (state.cancelled || this._closed) throw new Error('remote drain cancelled')
      if (message) {
        if (!this._remoteMessageSink) throw new Error('remote message sink unavailable')
        const result = await this._remoteMessageSink(conversationId, peerKeyHex, message, i)
        if (result && result.deliveryReceipt) deliveryReceipt = result.deliveryReceipt
      }
      i++
    }

    if (state.cancelled || this._closed) throw new Error('remote drain cancelled')
    if (coreFork(core) !== fork) throw new Error('remote core fork changed during drain')
    if (deliveryReceipt) {
      if (!this._remoteDrainCompleteSink) throw new Error('remote drain completion sink unavailable')
      await this._remoteDrainCompleteSink(conversationId, peerKeyHex, deliveryReceipt)
    }
    if (state.cancelled || this._closed) throw new Error('remote drain cancelled')
    if (coreFork(core) !== fork) throw new Error('remote core fork changed during drain')

    // Commit the batch only after every message/control record and the single
    // coalesced delivery watermark are durable. A crash earlier replays the
    // whole batch; ChatStore and receipt watermarks are idempotent.
    this._setProcessedCursor(conversationId, coreKeyHex, target, fork)
  }

  _cancelRemoteDrain (core) {
    const state = this._drainStates.get(core)
    if (!state) return
    state.cancelled = true
    if (state.retryTimer) clearTimeout(state.retryTimer)
    state.retryTimer = null
    if (state.appendHandler && typeof core.off === 'function') core.off('append', state.appendHandler)
  }

  async _closeRemoteCore (core) {
    if (!core) return
    this._cancelRemoteDrain(core)
    try { await core.close() } catch (_) {}
    const chain = this._drainChains.get(core)
    if (chain) await chain
    this._drainChains.delete(core)
    this._drainStates.delete(core)
  }

  /**
   * Append a message to the local Hypercore for a conversation.
   */
  async appendMessage (conversationId, message) {
    const core = await this.getOrCreateLocalCore(conversationId)
    const appendResult = await core.append(message)
    const index = appendResult.length - 1
    diag('Appended conv=' + conversationId.substring(0, 12) +
      ' index=' + index + ' newLen=' + core.length +
      ' msgId=' + (message && message.id ? message.id.substring(0, 8) : 'n/a'))
    return { core, index }
  }

  /**
   * Return direct-chat inbound notification capabilities for a conversation.
   * Topic derivation deliberately lives here in JS beside the Hypercores.
   */
  getInboundPushTopics (conversationId) {
    return getInboundPushTopics(this.remoteCores, conversationId)
  }

  /** Whether the persisted remote-writer index has been fully restored. */
  isPushTopicSnapshotHydrated () {
    return this._coreKeyIndexHydrated
  }

  /** Remove all local lifecycle state for a conversation and notify native. */
  async removeConversation (conversationId) {
    const local = this.localCores.get(conversationId)
    if (local) {
      try { await local.close() } catch (_) {}
      this.localCores.delete(conversationId)
    }
    const remotes = this.remoteCores.get(conversationId)
    if (remotes) {
      for (const core of remotes.values()) {
        await this._closeRemoteCore(core)
      }
      this.remoteCores.delete(conversationId)
    }
    this._coreKeyIndex.delete(conversationId)
    this._localReferrerIndex.delete(conversationId)
    this._remoteReferrerIndex.delete(conversationId)
    this._processedCursors.delete(conversationId)
    this.saveCoreKeyIndex()
    this.emit('push-topics-changed')
  }

  /** Close and forget one departed peer's remote writer immediately. */
  async removeRemoteCore (conversationId, peerKeyHex) {
    const peer = (peerKeyHex || '').toLowerCase()
    const remotes = this.remoteCores.get(conversationId)
    let removed = false
    if (remotes) {
      for (const [storedPeer, core] of remotes) {
        if ((storedPeer || '').toLowerCase() !== peer) continue
        await this._closeRemoteCore(core)
        remotes.delete(storedPeer)
        removed = true
      }
      if (remotes.size === 0) this.remoteCores.delete(conversationId)
    }

    const keyMap = this._coreKeyIndex.get(conversationId)
    if (keyMap) {
      for (const storedPeer of keyMap.keys()) {
        if ((storedPeer || '').toLowerCase() !== peer) continue
        this._deleteProcessedCursor(conversationId, keyMap.get(storedPeer))
        keyMap.delete(storedPeer)
      }
      if (keyMap.size === 0) this._coreKeyIndex.delete(conversationId)
    }

    const refMap = this._remoteReferrerIndex.get(conversationId)
    if (refMap) {
      for (const storedPeer of refMap.keys()) {
        if ((storedPeer || '').toLowerCase() === peer) refMap.delete(storedPeer)
      }
      if (refMap.size === 0) this._remoteReferrerIndex.delete(conversationId)
    }

    this.saveCoreKeyIndex()
    this.emit('push-topics-changed')
    return removed
  }

  /**
   * Get the local core's public key hex for a conversation.
   */
  getLocalCoreKey (conversationId) {
    const core = this.localCores.get(conversationId)
    if (!core) return null
    return b4a.toString(core.key, 'hex')
  }

  setLocalCoreReferrer (conversationId, referrerKeyHex) {
    if (!referrerKeyHex) return
    if (this._localReferrerIndex.get(conversationId) === referrerKeyHex) return
    this._localReferrerIndex.set(conversationId, referrerKeyHex)
    this.saveCoreKeyIndex()
  }

  getLocalCoreReferrer (conversationId) {
    return this._localReferrerIndex.get(conversationId) || null
  }

  setRemoteCoreReferrer (conversationId, peerKeyHex, referrerKeyHex) {
    if (!referrerKeyHex) return
    if (this.getRemoteCoreReferrer(conversationId, peerKeyHex) === referrerKeyHex) return
    this._setRemoteCoreReferrerInMemory(conversationId, peerKeyHex, referrerKeyHex)
    this.saveCoreKeyIndex()
  }

  _setRemoteCoreReferrerInMemory (conversationId, peerKeyHex, referrerKeyHex) {
    if (!this._remoteReferrerIndex.has(conversationId)) {
      this._remoteReferrerIndex.set(conversationId, new Map())
    }
    this._remoteReferrerIndex.get(conversationId).set(peerKeyHex, referrerKeyHex)
  }

  getRemoteCoreReferrer (conversationId, peerKeyHex) {
    const refs = this._remoteReferrerIndex.get(conversationId)
    return refs ? (refs.get(peerKeyHex) || null) : null
  }

  /**
   * Build a map of conversationId → local core key hex for all active conversations.
   * Used for core key exchange with peers.
   */
  getAllLocalCoreKeys () {
    const keys = {}
    for (const [convId, core] of this.localCores) {
      keys[convId] = b4a.toString(core.key, 'hex')
    }
    return keys
  }

  /**
   * Get all Hypercore instances for a conversation (for blind peer registration).
   */
  getAllCores (conversationId) {
    const cores = []
    const local = this.localCores.get(conversationId)
    if (local) cores.push(local)
    const remotes = this.remoteCores.get(conversationId)
    if (remotes) {
      for (const [, core] of remotes) {
        cores.push(core)
      }
    }
    return cores
  }

  /**
   * Persist core key index to disk so remote cores can be reopened on restart.
   */
  saveCoreKeyIndex () {
    try {
      const remotes = {}
      for (const [convId, keyMap] of this._coreKeyIndex) {
        remotes[convId] = {}
        for (const [peerKey, coreKey] of keyMap) {
          remotes[convId][peerKey] = coreKey
        }
      }
      const localReferrers = {}
      for (const [convId, referrerKey] of this._localReferrerIndex) {
        localReferrers[convId] = referrerKey
      }
      const remoteReferrers = {}
      for (const [convId, refMap] of this._remoteReferrerIndex) {
        remoteReferrers[convId] = {}
        for (const [peerKey, referrerKey] of refMap) {
          remoteReferrers[convId][peerKey] = referrerKey
        }
      }
      const cursors = {}
      for (const [convId, curMap] of this._processedCursors) {
        cursors[convId] = {}
        for (const [coreKey, cursor] of curMap) {
          cursors[convId][coreKey] = cursor
        }
      }
      const data = { version: 3, remotes, localReferrers, remoteReferrers, cursors }
      const dataDir = getDataDir()
      writeJSON(path.join(dataDir, 'corekeys.json'), data)
      diag('Saved core key index: ' + Object.keys(remotes).length + ' conversation(s)')
    } catch (err) {
      diag('Failed to save core key index: ' + (err.message || err))
    }
  }

  /**
   * Load core key index from disk and reopen remote cores.
   */
  async loadCoreKeyIndex () {
    if (this._coreKeyIndexHydrated) return true
    if (this._coreKeyIndexLoadPromise) return this._coreKeyIndexLoadPromise

    this._coreKeyIndexLoadPromise = this._loadCoreKeyIndex()
    try {
      return await this._coreKeyIndexLoadPromise
    } finally {
      this._coreKeyIndexLoadPromise = null
    }
  }

  async _loadCoreKeyIndex () {
    let hydrated = false
    try {
      const dataDir = getDataDir()
      const indexPath = path.join(dataDir, 'corekeys.json')
      const data = readJSON(indexPath)
      if (!data && fileExists(indexPath)) {
        throw new Error('core key index is unreadable')
      }

      if (data && data.localReferrers) {
        for (const [convId, referrerKey] of Object.entries(data.localReferrers)) {
          if (referrerKey) this._localReferrerIndex.set(convId, referrerKey)
        }
      }
      if (data && data.remoteReferrers) {
        for (const [convId, refs] of Object.entries(data.remoteReferrers)) {
          for (const [peerKey, referrerKey] of Object.entries(refs || {})) {
            if (referrerKey) this._setRemoteCoreReferrerInMemory(convId, peerKey, referrerKey)
          }
        }
      }
      // Restore processed cursors BEFORE reopening cores. Current records are
      // keyed by immutable core key and include the fork. Also accept the
      // branch's pre-release peer-key → number shape and translate it through
      // the persisted remote index so developer builds migrate safely.
      if (data && data.cursors) {
        for (const [convId, curs] of Object.entries(data.cursors)) {
          for (const [storedKey, value] of Object.entries(curs || {})) {
            const legacyNextIndex = typeof value === 'number' ? value : null
            const nextIndex = legacyNextIndex === null ? value && value.nextIndex : legacyNextIndex
            const fork = legacyNextIndex === null ? value && value.fork : 0
            if (!Number.isSafeInteger(nextIndex) || nextIndex <= 0) continue
            const coreKey = legacyNextIndex !== null && data.remotes && data.remotes[convId]
              ? data.remotes[convId][storedKey]
              : storedKey
            if (!coreKey || !Number.isSafeInteger(fork) || fork < 0) continue
            if (!this._processedCursors.has(convId)) this._processedCursors.set(convId, new Map())
            this._processedCursors.get(convId).set(coreKey.toLowerCase(), { nextIndex, fork })
          }
        }
      }

      const remoteData = data ? (data.remotes || data) : {}
      let count = 0
      for (const [convId, keys] of Object.entries(remoteData)) {
        if (convId === 'version' || convId === 'localReferrers' ||
            convId === 'remoteReferrers' || convId === 'cursors') continue
        for (const [peerKey, coreKey] of Object.entries(keys)) {
          try {
            await this.openRemoteCore(convId, peerKey, coreKey, this.getRemoteCoreReferrer(convId, peerKey))
            count++
          } catch (err) {
            diag('Failed to reopen remote core: ' + (err.message || err))
          }
        }
      }
      diag('Loaded core key index: ' + count + ' remote core(s)')
      hydrated = true
      return true
    } catch (err) {
      diag('Failed to load core key index: ' + (err.message || err))
      return false
    } finally {
      this._coreKeyIndexHydrated = hydrated
      // Always publish completion, including an empty index. Native retains
      // restored subscriptions until this flag becomes true.
      this.emit('push-topics-changed')
    }
  }

  async close () {
    if (this._closed) return
    this._closed = true

    this.saveCoreKeyIndex()

    for (const [, core] of this.localCores) {
      try { await core.close() } catch (e) { /* ignore */ }
    }
    for (const [, remotes] of this.remoteCores) {
      for (const [, core] of remotes) {
        await this._closeRemoteCore(core)
      }
    }
    this.localCores.clear()
    this.remoteCores.clear()
    this._coreKeyIndex.clear()
    this._localReferrerIndex.clear()
    this._remoteReferrerIndex.clear()
    this._processedCursors.clear()
    this._drainChains.clear()
    this._drainStates.clear()

    if (this.store) {
      try { await this.store.close() } catch (e) { /* ignore */ }
      this.store = null
    }
    this._ready = false
    diag('HypercoreManager closed')
  }
}

module.exports = { HypercoreManager }
