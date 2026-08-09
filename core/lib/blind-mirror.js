/**
 * Blind Mirror - Manages blind peer connections for offline message delivery
 *
 * Wraps the blind-peering client library to register conversation Autobases
 * with always-on blind peer servers. The blind peers store encrypted Hypercore
 * blocks and serve them to peers that come online later.
 *
 * The blind peer cannot read any data -- it only holds encrypted blocks.
 *
 * Usage:
 *   const mirror = new BlindMirror(swarm, store, { keys: [blindPeerPublicKey] })
 *   await mirror.ready()
 *   mirror.addAutobase(base)       // register a conversation for mirroring
 *   await mirror.suspend()         // app backgrounded
 *   await mirror.resume()          // app foregrounded
 *   await mirror.close()           // shutdown
 */

const EventEmitter = require('bare-events')
const b4a = require('b4a')
const { BLIND_PEER_KEYS, BLIND_PEER_ADDRESS, CUSTOM_BOOTSTRAP_NODES, LOG_LEVEL } = require('./config')
const { createDiagnosticLogger } = require('./diagnostics')

const diag = createDiagnosticLogger('MIRROR')

// Only logs when --log-level=debug is set. Use for high-frequency diagnostic
// output that should not appear in production logs.
function debugDiag (...args) {
  diag(...args)
}

/**
 * Blind peer server keys are loaded from config.js.
 * Pass at runtime via: --blind-peer-keys=key1,key2,...
 * Empty by default — blind mirroring is disabled until production keys are injected.
 */
const DEFAULT_BLIND_PEER_KEYS = BLIND_PEER_KEYS
const DEFAULT_NOTIFICATION_RETRY_DELAYS = [0, 1000, 3000]

function delay (ms) {
  if (ms <= 0) return Promise.resolve()
  return new Promise(resolve => setTimeout(resolve, ms))
}

class BlindMirror extends EventEmitter {
  /**
   * @param {Hyperswarm} swarm - The Hyperswarm instance from P2PManager
   * @param {Corestore} store - The Corestore instance from HypercoreManager
   * @param {Object} opts
   * @param {string[]} opts.keys - Blind peer server public keys (hex or z32)
   * @param {number} opts.mirrors - Number of blind peers per autobase (default: 2)
   */
  constructor (swarm, store, opts = {}) {
    super()
    // Keep the swarm for lifecycle diagnostics, but blind-peering 2.x accepts
    // the raw HyperDHT instance.
    this.swarm = swarm
    this.store = store
    this.keys = opts.keys || DEFAULT_BLIND_PEER_KEYS
    this.mirrors = opts.mirrors || Math.min(2, this.keys.length)
    this._usesDefaultMirrorCount = !opts.mirrors
    this._notificationRetryDelays = opts.notificationRetryDelays || DEFAULT_NOTIFICATION_RETRY_DELAYS
    this._peering = null
    this._ready = false
    this._closed = false
    // tag → lifecycle-aware registration metadata. Keeping the actual core
    // lets key changes re-register existing conversations with new blind peers.
    this._registeredBases = new Map()
  }

  /**
   * Build the key list handed to blind-peering for dialing. When BLIND_PEER_ADDRESS
   * is configured, each key is returned as a hyperdht-address buffer embedding
   * that host:port so HyperDHT pre-connects directly (skipping the findPeer walk).
   * On any error, or when no address is configured, the original keys are
   * returned unchanged — this path is strictly additive and cannot regress the
   * default deployment.
   */
  _buildDialKeys () {
    if (!BLIND_PEER_ADDRESS || !BLIND_PEER_ADDRESS.host) return this.keys
    try {
      const HyperDHTAddress = require('hyperdht-address')
      const ID = require('hypercore-id-encoding')
      const address = [{ host: BLIND_PEER_ADDRESS.host, port: BLIND_PEER_ADDRESS.port || 49737 }]
      const embedded = this.keys.map((key) => {
        // key may already be a buffer (raw or address-embedded) or a z32 string.
        const raw = b4a.isBuffer(key) ? key : ID.decode(key)
        // If it is already an address-embedded buffer, ID.decode throws above and
        // we fall through to the catch, keeping it as-is.
        return HyperDHTAddress.encode(raw, address)
      })
      diag('Dialing blind peer(s) via embedded address ' +
        BLIND_PEER_ADDRESS.host + ':' + (BLIND_PEER_ADDRESS.port || 49737))
      return embedded
    } catch (e) {
      diag('Failed to embed blind-peer address, using raw keys: ' + (e.message || e))
      return this.keys
    }
  }

  /**
   * Initialize the blind peering client.
   * Call this after swarm and store are both ready.
   */
  async ready () {
    if (this._ready || this._closed) return
    if (this.keys.length === 0) {
      diag('No blind peer keys configured -- blind mirroring disabled')
      this.emit('status', { enabled: false, reason: 'no_keys' })
      return
    }

    try {
      const BlindPeering = require('blind-peering')

      // If a dial address is configured, embed it into each blind-peer key so
      // HyperDHT pre-connects straight to the VPS and skips the ~1.9s findPeer
      // walk on cold start (it still falls back to a walk if the address is
      // stale). The embedded keys must be passed as BUFFERS — hypercore-id
      // z32 encoding only accepts bare 32-byte keys — so we hand blind-peering
      // the 40-byte hyperdht-address buffers directly. Any failure falls back
      // to the raw keys, so this is strictly opt-in and cannot regress.
      const effectiveKeys = this._buildDialKeys()

      this._peering = new BlindPeering(this.swarm.dht, this.store, {
        keys: effectiveKeys,
        pick: this.mirrors
      })

      // blind-peering 2.x accepts a raw HyperDHT and reads this optional field
      // when opening its muxed replication connection. Keep using the stable
      // messaging identity instead of the DHT's per-process default keypair.
      this._peering.keyPair = this.swarm.keyPair

      // INSTRUMENTATION: monkey-patch _getBlindPeer so we see every new
      // BlindPeerClient that BlindPeering creates, plus the lifecycle of its
      // underlying ProtomuxRPC connection (open/close/error).
      try {
        const _origGetBlindPeer = this._peering._getBlindPeer.bind(this._peering)
        const seenClients = new WeakSet()
        this._peering._getBlindPeer = (mirrorKey) => {
          const peer = _origGetBlindPeer(mirrorKey)
          if (peer && !seenClients.has(peer)) {
            seenClients.add(peer)
            const keyHex = this._b4aHex(mirrorKey).slice(0, 12)
            debugDiag('NEW BlindPeer created mirrorKey=' + keyHex + '… [identity keyPair configured]')
            const originalOnStream = peer.onstream.bind(peer)
            peer.onstream = (stream) => {
              debugDiag('BlindPeer[' + keyHex + '] STREAM opened, remote=' +
                this._b4aHex(stream.remotePublicKey).slice(0, 12) + '…')
              stream.on('error', (err) => {
                debugDiag('BlindPeer[' + keyHex + '] STREAM error: ' +
                  (err && (err.code || err.message)) + ' remote=' +
                  this._b4aHex(stream.remotePublicKey).slice(0, 12) + '…')
              })
              stream.on('close', () => {
                debugDiag('BlindPeer[' + keyHex + '] STREAM closed')
              })
              return originalOnStream(stream)
            }
          }
          return peer
        }
      } catch (e) {
        diag('Failed to wrap _getBlindPeer: ' + (e.message || e))
      }

      // Debug-only: log identity + DHT default keypair for VPS log correlation.
      // Gate behind LOG_LEVEL=debug — too sensitive and noisy for production.
      try {
        const swarmKey = this.swarm && this.swarm.keyPair
          ? this._b4aHex(this.swarm.keyPair.publicKey).slice(0, 24)
          : 'n/a'
        const dhtKey = this.swarm && this.swarm.dht && this.swarm.dht.defaultKeyPair
          ? this._b4aHex(this.swarm.dht.defaultKeyPair.publicKey).slice(0, 24)
          : 'n/a'
        debugDiag('Identity (swarm.keyPair pub hex): ' + swarmKey + '… [identity]')
        debugDiag('Ephemeral (swarm.dht.defaultKeyPair pub hex): ' + dhtKey + '… [shows up in blind-peer.log as remote pubkey]')
      } catch (e) {
        debugDiag('Identity dump failed: ' + (e.message || e))
      }

      this._ready = true
      diag('BlindMirror ready with ' + this.keys.length + ' blind peer(s)')
      this.emit('status', { enabled: true, peerCount: this.keys.length })

      // Periodic state dump so we can see whether each blind peer is actually
      // connected, how many cores are registered, and whether the connection
      // ever drops without us getting an explicit error.
      this._stateInterval = setInterval(() => this._dumpState(), 10000)
      // First dump shortly after construction so we don't wait 10s for signal.
      setTimeout(() => this._dumpState(), 2000)

      // Debug-only: run an independent findPeer() probe every 20s and log
      // responder counts. Useful for diagnosing PEER_NOT_FOUND failures and
      // double-NAT routing issues. Gated behind LOG_LEVEL=debug to avoid
      // flooding production logs.
      if (LOG_LEVEL === 'debug') {
        this._probeInterval = setInterval(() => this._probeFindPeer(), 20000)
        setTimeout(() => this._probeFindPeer(), 3000)
      }
    } catch (err) {
      diag('Failed to initialize blind-peering: ' + (err.message || err))
      this.emit('status', { enabled: false, reason: 'init_failed', error: err.message })
    }
  }

  _b4aHex (buf) {
    if (!buf) return 'null'
    try { return Buffer.from(buf).toString('hex') } catch (_) { /* ignore */ }
    try {
      let out = ''
      for (let i = 0; i < buf.length; i++) out += buf[i].toString(16).padStart(2, '0')
      return out
    } catch (_) { return '?' }
  }

  async _probeFindPeer () {
    if (this._closed || !this.swarm || !this.swarm.dht) return
    let HypercoreIdProbe
    try { HypercoreIdProbe = require('hypercore-id-encoding') } catch (_) { return }

    const dht = this.swarm.dht
    // HyperDHT.hash is a static on the class; identical to the
    // hyperdht/lib/crypto.js `hash` used by connect.js to derive the announce
    // target. Without this, our probes hit the wrong DHT keyspace (raw key
    // instead of hash(key)).
    const hashFn = (dht.constructor && dht.constructor.hash) || null

    // PROBE B: dump DHT state that determines whether dht.connect can bypass
    // findPeer. routing-table size + _socketPool.routes per target key +
    // _relayAddressesCache size + remoteAddress + firewalled flag.
    try {
      let rtNodes = 0
      try {
        if (dht.nodes && typeof dht.nodes.length === 'number') rtNodes = dht.nodes.length
        else if (dht.table && dht.table.toArray) rtNodes = dht.table.toArray().length
        else if (dht.io && dht.io.routingTable && dht.io.routingTable.toArray) {
          rtNodes = dht.io.routingTable.toArray().length
        }
      } catch (_) {}
      const routesByKey = {}
      const relayCacheByKey = {}
      try {
        const sockPool = dht.io && dht.io.serverSocket && dht.io.serverSocket._socketPool
        const socketRoutes = sockPool && sockPool.routes
        // SocketRoutes (hyperdht/lib/socket-pool.js) wraps an internal Map at
        // `_routes`; `get(publicKey)` returns a {socket,address,gc} object or
        // null. Truthy ⇒ this peer has a cached route, which lets dht.connect
        // bypass findPeer entirely.
        const innerRoutes = socketRoutes && socketRoutes._routes
        const relayCache = dht._relayAddressesCache
        for (const key of this.keys) {
          const rawKey = HypercoreIdProbe.decode(key)
          const tag = this._b4aHex(rawKey).slice(0, 12)
          if (socketRoutes && typeof socketRoutes.get === 'function') {
            const cached = socketRoutes.get(rawKey)
            routesByKey[tag] = cached ? 1 : 0
          }
          if (relayCache && typeof relayCache.get === 'function') {
            const cached = relayCache.get(this._b4aHex(rawKey))
            relayCacheByKey[tag] = cached ? cached.length : 0
          }
        }
        const totalRoutes = innerRoutes && typeof innerRoutes.size === 'number' ? innerRoutes.size : 'n/a'
        let addrStr = 'none'
        try {
          const addr = dht.remoteAddress && dht.remoteAddress()
          if (addr) addrStr = addr.host + ':' + addr.port
        } catch (_) {}
        diag('PROBE dht rtNodes=' + rtNodes + ' totalRoutes=' + totalRoutes +
          ' routesByTarget=' + JSON.stringify(routesByKey) +
          ' relayCacheByTarget=' + JSON.stringify(relayCacheByKey) +
          ' addr=' + addrStr +
          ' firewalled=' + !!dht.firewalled +
          ' online=' + !!dht.online +
          ' destroyed=' + !!dht.destroyed)
      } catch (e) {
        diag('PROBE dht state error: ' + (e.message || e))
      }
    } catch (_) {}

    // PROBE PING: directly UDP-ping each custom bootstrap node (typically the
    // blind-peer's own VPS) to confirm the host is reachable on the DHT port.
    // If this fails, the VPS firewall / Oracle Cloud security list is blocking
    // UDP inbound — in which case dht.connect would have to rely entirely on
    // holepunching, and adding the VPS as bootstrap won't fix PEER_NOT_FOUND.
    try {
      const customNodes = Array.isArray(CUSTOM_BOOTSTRAP_NODES) ? CUSTOM_BOOTSTRAP_NODES : []
      for (const node of customNodes) {
        if (!node || !node.host) continue
        const tag = node.host + ':' + node.port
        const start = Date.now()
        try {
          await dht.ping({ host: node.host, port: node.port })
          diag('PROBE ping(' + tag + ') ok ms=' + (Date.now() - start))
        } catch (e) {
          diag('PROBE ping(' + tag + ') ERROR ' +
            (e && (e.code || e.message)) + ' ms=' + (Date.now() - start))
        }
      }
    } catch (_) {}

    // PROBE A (corrected): fresh findPeer walk against the SAME target
    // hyperdht/lib/connect.js uses (hash(publicKey)). Earlier version was
    // bugged: it passed { hash: false } with the raw publicKey, querying a
    // totally different DHT-keyspace coordinate. Both phones returning 0 was
    // an artifact of that bug, not signal.
    for (const key of this.keys) {
      let rawKey
      try { rawKey = HypercoreIdProbe.decode(key) } catch (_) { continue }
      const keyHex = this._b4aHex(rawKey).slice(0, 12)

      // findPeer with default opts auto-hashes — matches connect.js path.
      await this._runOne('findPeer', keyHex, () =>
        this.swarm.dht.findPeer(rawKey, { retries: 3 })
      )

      // lookup() uses the same hashed target but a different DHT command;
      // useful to confirm whether the announce record exists at all in the
      // DHT, independent of findPeer's particular response path.
      if (hashFn) {
        const target = hashFn(rawKey)
        await this._runOne('lookup', keyHex, () =>
          this.swarm.dht.lookup(target, { retries: 3 })
        )
      }
    }
  }

  async _runOne (label, keyHex, makeQuery) {
    let responders = 0
    let firstResponderHex = ''
    const start = Date.now()
    try {
      const q = makeQuery()
      for await (const data of q) {
        responders++
        if (responders === 1 && data && data.from && data.from.id) {
          firstResponderHex = this._b4aHex(data.from.id).slice(0, 12)
        }
        if (responders >= 5) break
      }
      const ms = Date.now() - start
      diag('PROBE ' + label + '(' + keyHex + ') responders=' + responders +
        ' first=' + (firstResponderHex || 'n/a') + ' ms=' + ms)
    } catch (e) {
      const ms = Date.now() - start
      diag('PROBE ' + label + '(' + keyHex + ') ERROR ' +
        (e && (e.code || e.message)) + ' responders=' + responders + ' ms=' + ms)
    }
  }

  _dumpState () {
    if (!this._peering || this._closed) return
    try {
      const peers = []
      for (const [keyHex, peer] of this._peering.blindPeers || []) {
        peers.push({
          key: keyHex.slice(0, 12),
          connected: peer ? !!peer.connected : false,
          suspended: peer ? !!peer.suspended : false,
          destroyed: peer ? !!peer.destroyed : false,
          hasChannel: !!(peer && peer.channel),
          gc: peer ? peer.gc : 0,
          coreCount: peer && peer.cores ? peer.cores.size : 0
        })
      }
      const summary = peers.length === 0
        ? 'NO_BLIND_PEERS'
        : peers.map(p =>
          'key=' + p.key +
          ' connected=' + p.connected +
          ' channel=' + p.hasChannel +
          ' suspended=' + p.suspended +
          ' destroyed=' + p.destroyed +
          ' cores=' + p.coreCount +
          ' gc=' + p.gc
        ).join(' | ')
      diag('STATE registered=' + (this._registeredBases ? this._registeredBases.size : 0) + ' peers=' + peers.length + ' :: ' + summary)
    } catch (e) {
      diag('STATE dump error: ' + (e.message || e))
    }
  }

  /**
   * Register an Autobase with the blind peer(s) for offline replication.
   * This sends the autobase's local core, view cores, and writer cores
   * to the blind peer servers. They will replicate and serve these blocks
   * to any peer that connects later.
   *
   * Safe to call multiple times for the same base -- will skip if already registered.
   *
   * @param {string} conversationId - For tracking/logging
   * @param {Autobase} base - The conversation's Autobase instance
   */
  addAutobase (conversationId, base) {
    if (!this._peering || this._closed) {
      diag('Cannot add autobase: mirror not ready (conv=' + conversationId.substring(0, 12) + ')')
      return
    }

    const existing = this._registeredBases.get(conversationId)
    if (existing && existing.base === base) {
      diag('Autobase already registered: ' + conversationId.substring(0, 12))
      return
    }

    try {
      this._peering.addAutobaseBackground(base)
      this._registeredBases.set(conversationId, { kind: 'autobase', base })
      if (base && typeof base.on === 'function') {
        base.on('close', () => {
          if (this._registeredBases.get(conversationId)?.base === base) {
            this._registeredBases.delete(conversationId)
          }
        })
      }
      diag('Registered autobase: ' + conversationId.substring(0, 12))
    } catch (err) {
      diag('Failed to register autobase ' + conversationId.substring(0, 12) + ': ' + (err.message || err))
    }
  }

  /**
   * Register an individual Hypercore (e.g. a conversation's local writable core)
   * with the blind peer(s) so it gets replicated for offline delivery.
   *
   * @param {string} conversationId - For tracking/logging
   * @param {Hypercore} core - The Hypercore instance to mirror
   * @param {string} [referrerKeyHex] - Identity pubkey (hex) the blind peer
   *   should wake when this core gets new blocks. For our writable core that is
   *   the conversation peer (the recipient of what we send). Omit for groups.
   */
  addLocalCore (conversationId, core, referrerKeyHex = null) {
    if (!this._peering || this._closed) {
      diag('Cannot add local core: mirror not ready (conv=' + conversationId.substring(0, 12) + ')')
      return
    }
    const tag = 'local:' + conversationId
    const registered = this._registeredBases.get(tag)
    const registeredReferrer = registered && registered.referrerKeyHex
    if (registered && registered.core === core && (!referrerKeyHex || registeredReferrer === referrerKeyHex)) return
    try {
      const opts = { target: core.key, announce: true }
      if (referrerKeyHex) opts.referrer = b4a.from(referrerKeyHex, 'hex')
      this._peering.addCoreBackground(core, opts)
      this._trackCoreRegistration(tag, core, referrerKeyHex || registeredReferrer || null, opts)
      diag('Registered local core: conv=' + conversationId.substring(0, 12) +
        (referrerKeyHex ? ' referrer=' + referrerKeyHex.substring(0, 12) : ''))
    } catch (err) {
      diag('Failed to register local core ' + conversationId.substring(0, 12) + ': ' + (err.message || err))
    }
  }

  /**
   * Register a remote peer's Hypercore (read-only) so the blind peer keeps a
   * replica we can pull from later. Called when we learn a peer's core key
   * via invite or peer-to-peer key exchange.
   *
   * @param {string} conversationId
   * @param {string} peerKeyHex
   * @param {Hypercore} core
   * @param {string} [referrerKeyHex] - Identity pubkey (hex) the blind peer
   *   should wake when this core gets new blocks. A remote core is written by
   *   the peer, so its blocks are addressed to us -> our own identity. Omit for
   *   groups (multi-recipient, deferred).
   */
  addRemoteCore (conversationId, peerKeyHex, core, referrerKeyHex = null) {
    if (!this._peering || this._closed) return
    const tag = 'remote:' + conversationId + ':' + peerKeyHex
    const registered = this._registeredBases.get(tag)
    const registeredReferrer = registered && registered.referrerKeyHex
    if (registered && registered.core === core && (!referrerKeyHex || registeredReferrer === referrerKeyHex)) return
    try {
      const opts = { target: core.key, announce: false }
      if (referrerKeyHex) opts.referrer = b4a.from(referrerKeyHex, 'hex')
      this._peering.addCoreBackground(core, opts)
      this._trackCoreRegistration(tag, core, referrerKeyHex || registeredReferrer || null, opts)
      diag('Registered remote core: conv=' + conversationId.substring(0, 12) +
        ' peer=' + peerKeyHex.substring(0, 12) +
        (referrerKeyHex ? ' referrer=' + referrerKeyHex.substring(0, 12) : ''))
    } catch (err) {
      diag('Failed to register remote core: ' + (err.message || err))
    }
  }

  /**
   * Check if a conversation is registered with the blind mirror.
   * @param {string} conversationId
   * @returns {boolean}
   */
  hasConversation (conversationId) {
    return this._registeredBases.has(conversationId) ||
      this._registeredBases.has('local:' + conversationId)
  }

  _trackCoreRegistration (tag, core, referrerKeyHex, opts) {
    this._registeredBases.set(tag, { kind: 'core', core, referrerKeyHex, opts })
    if (!core || typeof core.on !== 'function') return
    core.on('close', () => {
      if (this._registeredBases.get(tag)?.core === core) {
        this._registeredBases.delete(tag)
      }
    })
  }

  /** Forget all wrapper registrations for a removed conversation. */
  removeConversation (conversationId) {
    this._registeredBases.delete(conversationId)
    this._registeredBases.delete('local:' + conversationId)
    const remotePrefix = 'remote:' + conversationId + ':'
    for (const tag of this._registeredBases.keys()) {
      if (tag.startsWith(remotePrefix)) this._registeredBases.delete(tag)
    }
  }

  /** Forget one departed peer's remote-core registration. */
  removeRemoteCore (conversationId, peerKeyHex) {
    this._registeredBases.delete('remote:' + conversationId + ':' + peerKeyHex)
  }

  /**
   * Ask a blind peer to create and forward a proof-backed notification for an
   * exact appended block. The gateway derives the FCM topic from this core's
   * random discovery key; no identity-to-token lookup is involved.
   */
  async sendNotification (core, index) {
    let lastError = null
    for (const retryDelay of this._notificationRetryDelays) {
      if (retryDelay > 0) await delay(retryDelay)
      if (!this._peering || this._closed) throw new Error('blind mirror unavailable')
      try {
        await this._peering.sendNotification(core, { index })
        return
      } catch (err) {
        lastError = err
      }
    }
    throw lastError || new Error('blind notification failed')
  }

  /**
   * Update blind peer keys at runtime (e.g. from settings).
   * @param {string[]} keys - New blind peer public keys
   */
  setKeys (keys) {
    this.keys = keys
    if (this._peering) {
      this._peering.setKeys(keys)
      if (this._usesDefaultMirrorCount) {
        this.mirrors = Math.min(2, keys.length)
        this._peering.pick = this.mirrors
      }
      // blind-peering setKeys() does not rebalance existing cores. Replay the
      // wrapper's live registrations so a fresh relay starts mirroring them.
      for (const registration of this._registeredBases.values()) {
        if (registration.kind === 'core' && !registration.core.closing) {
          this._peering.addCoreBackground(registration.core, registration.opts)
        } else if (registration.kind === 'autobase' && !registration.base.closing) {
          this._peering.addAutobaseBackground(registration.base)
        }
      }
      diag('Updated blind peer keys: ' + keys.length + ' key(s)')
    }
  }

  /**
   * Suspend blind peering (app going to background).
   * Closes connections to blind peers to save battery.
   */
  async suspend () {
    if (!this._peering) return
    try {
      await this._peering.suspend()
      diag('Suspended')
    } catch (err) {
      diag('Suspend error: ' + (err.message || err))
    }
  }

  /**
   * Resume blind peering (app coming to foreground).
   * Reconnects to blind peers and flushes any pending data.
   */
  async resume () {
    if (!this._peering) return
    try {
      await this._peering.resume()
      diag('Resumed')
    } catch (err) {
      diag('Resume error: ' + (err.message || err))
    }
  }

  /**
   * Close blind peering and clean up all connections.
   */
  async close () {
    if (this._closed) return
    this._closed = true

    if (this._stateInterval) {
      clearInterval(this._stateInterval)
      this._stateInterval = null
    }

    if (this._probeInterval) {
      clearInterval(this._probeInterval)
      this._probeInterval = null
    }

    if (this._peering) {
      try {
        await this._peering.close()
        diag('Closed')
      } catch (err) {
        diag('Close error: ' + (err.message || err))
      }
      this._peering = null
    }

    this._registeredBases.clear()
    this._ready = false
  }

  /**
   * Get diagnostic info for debugging.
   */
  getDebugInfo () {
    let relaysConnected = 0
    let relaysTotal = 0
    try {
      for (const [, peer] of (this._peering && this._peering.blindPeers) || []) {
        relaysTotal++
        if (peer && peer.connected && peer.channel && !peer.channel.closed) relaysConnected++
      }
    } catch (e) { /* peering not ready; report zeros */ }
    return {
      enabled: this._ready && this.keys.length > 0,
      blindPeerCount: this.keys.length,
      registeredConversations: this._registeredBases.size,
      relaysConnected,
      relaysTotal: relaysTotal || this.keys.length,
      keys: this.keys.map(k => typeof k === 'string' ? k.substring(0, 12) + '...' : 'buffer')
    }
  }
}

module.exports = { BlindMirror, DEFAULT_BLIND_PEER_KEYS }
