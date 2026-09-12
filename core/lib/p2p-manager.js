/**
 * P2P Manager - Manages peer connections via Hyperswarm
 *
 * Uses topic-based discovery (swarm.join) for all connection types.
 * Supports direct chats and group chats.
 */

const { normalizePeerRecord } = require('./peer-record')

const Hyperswarm = require('hyperswarm')
const crypto = require('hypercore-crypto')
const HypercoreId = require('hypercore-id-encoding')
const EventEmitter = require('bare-events')
const b4a = require('b4a')
const { FramedSocket } = require('./socket-framing')
const { DHTHealthMonitor, HealthStatus } = require('./dht-health')
const {
  deriveDirectChatTopic,
  deriveGroupChatTopic,
  derivePersonalTopic
} = require('./rooms')
const config = require('./config')
const { registerPushEndpoint: registerPushEndpointRpc } = require('./push-register')
const { putInvite, drainInvites, throughDht, throughHttps } = require('./invite-mailbox')
const { ChatStore } = require('./chat-store')
const { runBounded } = require('./async-pool')
const { TaskScope } = require('./task-scope')
const { isMediaId, isPeerId } = require('./media-id')
const { createDiagnosticLogger } = require('./diagnostics')

const diag = createDiagnosticLogger('P2P')

/**
 * DHT Bootstrap Node Configuration - Multi-Tier Strategy
 *
 * Tier 1: Public Holepunch bootstrap nodes (default)
 * - Industry standard, proven infrastructure
 * - Best for most users with standard network configurations
 *
 * Tier 2: Custom Zapp bootstrap nodes (fallback)
 * - Provides redundancy if public nodes are unreachable
 * - Can be deployed on cloud infrastructure (AWS/GCP/Heroku)
 *
 * Tier 3: Peer-discovered nodes (dynamic)
 * - Successful peer connections become potential bootstrap candidates
 * - Implements "gossip protocol" for node discovery
 * - Provides resilience in restricted network environments
 */
// Bootstrap nodes loaded from config.js (overridable via --bootstrap-nodes=host:port,...)
const DEFAULT_BOOTSTRAP_NODES = config.DEFAULT_BOOTSTRAP_NODES
const ZAPP_BOOTSTRAP_NODES = config.CUSTOM_BOOTSTRAP_NODES

// Peer-discovered bootstrap nodes (populated at runtime, capped)
const discoveredBootstrapNodes = []
const MAX_DISCOVERED_NODES = config.MAX_DISCOVERED_NODES

// Heartbeat configuration (overridable via --heartbeat-interval= / --heartbeat-timeout=)
const HEARTBEAT_INTERVAL_MS = config.HEARTBEAT_INTERVAL_MS
const HEARTBEAT_TIMEOUT_MS = config.HEARTBEAT_TIMEOUT_MS
const MAX_PENDING_MESSAGES_PER_CONVERSATION = 5000
const MAX_MEDIA_REQUESTS_PER_MINUTE = 60
const DHT_NODE_CACHE_MAX = config.DHT_NODE_CACHE_MAX
const DHT_NODE_CACHE_FILE = 'dht-nodes.json'
// How long a conversation waits before it may re-offer itself through the
// mailbox. Every send to an unreachable peer is a candidate, so this is what
// keeps a long offline stretch from becoming a stream of deposits.
const MAILBOX_BOOTSTRAP_INTERVAL_MS = 10 * 60 * 1000
// Ceiling on one host-carried HTTPS round trip, including the IPC hop each way.
const PLATFORM_HTTP_TIMEOUT_MS = 15 * 1000

class P2PManager extends EventEmitter {
  constructor (opts = {}) {
    super()
    this._tasks = new TaskScope()
    this._generation = 0
    this._stopping = false
    this._stopPromise = null
    this._startPromise = null
    this.swarm = null
    this.keyPair = null
    this.isOnline = false
    this.peerCount = 0
    this._createSwarm = opts.createSwarm || (swarmOptions => new Hyperswarm(swarmOptions))
    this._blindPeerKeys = opts.blindPeerKeys || config.BLIND_PEER_KEYS
    this._mailboxStores = opts.mailboxStores || null
    this._mailboxTransports = opts.mailboxTransports || { throughDht, throughHttps }

    // Optional collaborators for blind-peer offline delivery. Both are nullable
    // so the swarm-only path keeps working in tests/stub setups.
    this.hypercoreManager = opts.hypercoreManager || null
    this.blindMirror = opts.blindMirror || null

    // Reads the current display name. This layer holds no identity, but the
    // bootstrap invites it composes carry one, and a stale copy would show a
    // peer the wrong name after a rename.
    this._displayName = opts.displayName || (() => null)

    // Applies a mailbox-delivered invite and reports whether it is finished
    // with. Awaited, unlike the socket path's fire-and-forget event, because the
    // mailbox copy is deleted on the strength of the answer. Without a handler
    // wired, nothing can be applied, so nothing is deleted.
    this._deliverInvite = opts.deliverInvite || (async () => false)

    // Hands an opaque JSON POST to the host app's HTTPS stack, returning false
    // when there is no host to take it. Bare's TLS addon is not dependable
    // across Android vendor builds and iOS has no equivalent, so the mailbox's
    // HTTPS transport is the host's socket rather than the worklet's.
    this._platformHttp = opts.platformHttp || (() => false)

    // Synchronous membership check backed by the chat store, wired by
    // index.js: (conversationId, peerKeyHex) → boolean. Covers conversations
    // that exist on disk but have not been joined at runtime yet (cold
    // start). Null in stub setups — runtime maps still gate then.
    this._isParticipant = opts.isParticipant || null
    this._getConversation = opts.getConversation || null
    this._resolveGroupTopic = opts.resolveGroupTopic || null
    this._peerRecordSink = null
    this._mayServeMedia = opts.mayServeMedia || null

    // Direct conversations: conversationId -> { peers: Map<peerKeyHex, { topic, topicHex, discovery, connections: [socket] }> }
    this.conversations = new Map()
    // peerKeyHex -> { conversationId, peerEntry } (reverse lookup for incoming direct connections)
    this.peerToConversation = new Map()

    // Group conversations: conversationId -> { groupTopicHex, groupId, discovery, participantKeys: [hex], connections: Map<peerKeyHex, [socket]> }
    this.groupConversations = new Map()
    // groupTopicHex -> conversationId (reverse lookup for incoming group messages)
    this.groupTopicToConversation = new Map()

    // All peer connections globally (for invite delivery)
    this.allPeerConnections = new Map() // peerKeyHex -> [socket]

    // Presence visibility. When false we advertise ourselves as offline: we ask
    // peers to retract our online dot (on each fresh connect and on toggle). The
    // reciprocal half — not seeing others while hidden — is enforced app-side.
    // Defaults true; native pushes the user's setting via set_presence_visible.
    this.presenceVisible = true

    // Invite management
    this.pendingInvites = new Map() // peerKeyHex -> [inviteData, ...]
    this._inviteTopics = new Map() // topicHex -> discovery
    this.personalDiscovery = null
    this._mailboxBootstrapInFlight = new Map()
    this._mailboxBootstrapAt = new Map()
    this._mailboxDrainInFlight = null
    this._platformHttpRequests = new Map()
    this._platformHttpRequestSequence = 0

    // Framed socket wrappers: raw socket -> FramedSocket
    this.framedSockets = new WeakMap()

    // Pending messages for offline peers: conversationId -> [{ message, timestamp }]
    this.pendingMessages = new Map()

    // Message deduplication: Set of recently seen message IDs (bounded to 1000 per conversation)
    // Map<conversationId, Set<messageId>>
    this._seenMessageIds = new Map()
    // Replay window: reject messages with timestamps older than 5 minutes
    this._replayWindowMs = 5 * 60 * 1000

    // DHT health monitor — uses the main swarm, no temp instances
    this.healthMonitor = new DHTHealthMonitor({
      checkInterval: 120000,  // 2 minutes
      checkTimeout: 8000
    })
    this.healthMonitor.setPeerCountFn(() => this.peerCount)

    // Set up health monitoring event handlers
    this._setupHealthMonitoring()

    // Heartbeat interval handle
    this._heartbeatInterval = null

    // Per-peer rate limiting (H10)
    this._peerMessageCounts = new Map() // peerKeyHex -> { count, windowStart }
    this._peerMediaRequestCounts = new Map()
    this._peerRateLimit = config.PEER_RATE_LIMIT_PER_MIN
    this._peerRateWindowMs = config.PEER_RATE_LIMIT_WINDOW_MS
  }

  /**
   * Check if a peer has exceeded the message rate limit.
   * @param {string} peerId
   * @returns {boolean} true if rate limit exceeded
   */
  _isRateLimited (peerId) {
    const now = Date.now()
    let entry = this._peerMessageCounts.get(peerId)
    if (!entry || (now - entry.windowStart) > this._peerRateWindowMs) {
      entry = { count: 0, windowStart: now }
      this._peerMessageCounts.set(peerId, entry)
    }
    entry.count++
    if (entry.count > this._peerRateLimit) {
      diag('Rate limit exceeded for peer ' + peerId.substring(0, 12) + ' (' + entry.count + ' msgs in window)')
      return true
    }
    return false
  }

  _isMediaRequestRateLimited (peerId) {
    const now = Date.now()
    let entry = this._peerMediaRequestCounts.get(peerId)
    if (!entry || (now - entry.windowStart) > this._peerRateWindowMs) {
      entry = { count: 0, windowStart: now }
      this._peerMediaRequestCounts.set(peerId, entry)
    }
    entry.count++
    return entry.count > MAX_MEDIA_REQUESTS_PER_MINUTE
  }

  /**
   * Set up DHT health monitoring event handlers (Phase 2)
   * @private
   */

  /**
   * May the authenticated peer announce a core for this conversation?
   * True only when the peer is a verified participant: via the live runtime
   * maps (joined direct/group conversations) or the chat-store-backed
   * membership callback for conversations known only on disk.
   * @private
   */
  _peerMayAccessConversation (conversationId, peerKeyHex) {
    // When the authoritative chat-store callback is available it must win over
    // runtime maps. Those maps are caches and may still contain a peer that has
    // just left a group.
    if (this._isParticipant) return this._isParticipant(conversationId, peerKeyHex)

    const direct = this.peerToConversation.get(peerKeyHex)
    if (direct && direct.conversationId === conversationId) return true
    // A group cache alone cannot authorize access.
    return false
  }

  setPeerRecordSink(fn) { this._peerRecordSink = fn }

  groupTopic(conversationId) {
    const conv = this._getConversation && this._getConversation(conversationId)
    if (conv && conv.type === 'group') return b4a.toString(deriveGroupChatTopic(conv.groupId), 'hex')
    return this.groupConversations.get(conversationId)?.groupTopicHex || null
  }

  resolveWireConversation(wireId) {
    const byTopic = (this._resolveGroupTopic && this._resolveGroupTopic(wireId)) || this.groupTopicToConversation.get(wireId)
    if (byTopic) return byTopic
    return wireId
  }

  _outgoingRecord(conversationId, message) {
    const topic = this.groupTopic(conversationId)
    const result = { ...message }
    delete result.status
    delete result.mediaLocalPath
    delete result.mediaTransferState
    delete result.mediaAuthorized
    if (topic) {
      result.groupTopicHex = topic
      result.conversationId = topic
    }
    if (result.type && result.type.startsWith('group_') && !result.id) {
      result.id = b4a.toString(crypto.randomBytes(16), 'hex')
    }
    return result
  }

  _peerMayAnnounceCore (conversationId, peerKeyHex) {
    return this._peerMayAccessConversation(conversationId, peerKeyHex)
  }

  /**
   * Accept a wire-supplied conversationId from an authenticated peer only
   * when it is verifiably theirs: either the deterministic direct-chat id for
   * (us, peer) — new or legacy form — or an existing conversation the peer is
   * a participant of. Everything else is dropped: honoring an arbitrary
   * wire id let a peer land authored messages in any conversation via the
   * first-contact recovery path.
   * @private
   */
  _acceptWireConversationId (wireConversationId, peerKeyHex) {
    if (!wireConversationId) return null
    if (this._isParticipant && this._isParticipant(wireConversationId, peerKeyHex)) {
      return wireConversationId
    }
    if (this.keyPair) {
      const myHex = b4a.toString(this.keyPair.publicKey, 'hex')
      if (wireConversationId === ChatStore.directChatId(myHex, peerKeyHex) ||
          wireConversationId === ChatStore._legacyDirectChatId(myHex, peerKeyHex)) {
        return wireConversationId
      }
    }
    diag('Dropped message with unverifiable conversationId ' +
      wireConversationId.substring(0, 12) + ' from ' + peerKeyHex.substring(0, 12))
    return null
  }

  /**
   * HIGH-02: Check whether an incoming message is a duplicate or outside the
   * replay-protection window. Returns true if the message should be dropped.
   * @private
   */
  _isDuplicateOrReplayed(conversationId, message) {
    const msgId = message.id
    // Note: No timestamp-based replay check here. Hyperswarm's Noise protocol
    // handles transport-level replay protection. A timestamp check would
    // incorrectly drop queued messages that arrive after DHT bootstrap delay.

    if (!msgId) return false

    if (!this._seenMessageIds.has(conversationId)) {
      this._seenMessageIds.set(conversationId, new Set())
    }
    const seen = this._seenMessageIds.get(conversationId)

    if (seen.has(msgId)) {
      diag('Dropping duplicate message: ' + msgId)
      return true
    }

    seen.add(msgId)
    // Bound the deduplication set to prevent unbounded memory growth
    if (seen.size > 1000) {
      const oldest = seen.values().next().value
      seen.delete(oldest)
    }

    return false
  }

  _setupHealthMonitoring() {
    this.healthMonitor.on('status_change', (event) => {
      diag('DHT health: ' + event.previous + ' -> ' + event.current +
        ' dht=' + event.dhtReady + ' peers=' + event.peerCount)

      if (event.current === 'critical') {
        this.isOnline = false
        this.emit('status', { online: false, peerCount: this.peerCount })
        this.emit('dht_warning', {
          reason: 'health_critical',
          message: 'DHT unreachable — P2P connectivity lost',
          consecutiveFailures: event.consecutiveFailures
        })
      } else if (event.current === 'degraded') {
        // DHT works but no peers — still "online" but warn
        this.isOnline = true
        this.emit('status', { online: true, peerCount: this.peerCount })
        this.emit('dht_warning', {
          reason: 'health_degraded',
          message: 'Connected to DHT but no peers reachable'
        })
      } else if (event.current === 'healthy' && event.previous !== 'healthy') {
        this.isOnline = true
        this.emit('status', { online: true, peerCount: this.peerCount })
        this.emit('dht_recovered', { previous: event.previous })
        diag('DHT recovered to healthy')
      }
    })
  }

  /**
   * Lightweight health check — delegates to the monitor which uses
   * the existing swarm (no temporary instances created).
   */
  async checkBootstrapHealth() {
    return this.healthMonitor.checkHealth()
  }

  /**
   * Start the P2P manager with the user's keypair
   * @param {Object} keyPair - Ed25519 keypair from Identity
   */
  runTask (fn) { return this._tasks.run(fn) }

  async start (keyPair) {
    if (this._stopPromise) await this._stopPromise
    if (this._startPromise) return this._startPromise
    if (this._tasks.closed) this._tasks = new TaskScope()
    this._stopping = false
    const operation = this.runTask(() => this._start(keyPair))
    this._startPromise = operation
    try { return await operation } finally {
      if (this._startPromise === operation) this._startPromise = null
    }
  }

  async _start(keyPair) {
    const generation = this._generation
    const current = () => !this._stopping && generation === this._generation
    if (this.swarm) {
      diag('P2P Manager already started')
      return
    }

    this.keyPair = keyPair

    try {
      diag('=== P2P Manager starting ===')

      // Create swarm with multi-tier bootstrap configuration
      const allBootstrapNodes = [
        ...DEFAULT_BOOTSTRAP_NODES,
        ...ZAPP_BOOTSTRAP_NODES,
        ...discoveredBootstrapNodes
      ]

      diag('Bootstrap nodes: ' + allBootstrapNodes.length)

      // Make a relay available from the first handshake. Hyperswarm's static
      // key form only enables it for randomized NATs or a small allow-list of
      // holepunch error codes. In particular, CANNOT_HOLEPUNCH and failed LAN
      // discovery can otherwise loop forever without ever trying the relay.
      //
      // HyperDHT races this relayed stream with direct/LAN holepunching and
      // upgrades an established raw stream when a direct route wins, so making
      // the relay available eagerly does not disable direct connections.
      //
      // The target must actually serve the `blind-relay` Protomux protocol.
      // A stock blind-peer-cli instance only mirrors cores and is not enough;
      // use server/blind-peer-relay.js for the configured key.
      let relayThroughFn
      const blindKeys = this._blindPeerKeys || []
      if (blindKeys.length > 0) {
        try {
          const relayKey = HypercoreId.decode(blindKeys[0])
          relayThroughFn = () => relayKey
          diag('Relay through blind peer (eager fallback): ' + blindKeys[0].slice(0, 12) + '…')
        } catch (e) {
          diag('Failed to decode blind peer key for relay: ' + (e.message || e))
        }
      } else {
        diag('NO blind peer keys — relay disabled (BLIND_PEER_KEYS empty)')
      }

      // Seed the routing table with known-good nodes persisted from the last
      // run so dht.ready() converges faster instead of paying a full cold
      // bootstrap every launch. Empty/first-run => undefined (normal bootstrap).
      const cachedNodes = this._loadDhtNodeCache()
      if (cachedNodes.length > 0) diag('Seeding DHT with ' + cachedNodes.length + ' cached node(s)')

      this.swarm = this._createSwarm({
        keyPair,
        bootstrap: allBootstrapNodes.length > 0 ? allBootstrapNodes : undefined,
        nodes: cachedNodes.length > 0 ? cachedNodes : undefined,
        relayThrough: relayThroughFn
      })

      // Let consumers (blind mirror) begin dialing known peers immediately, in
      // parallel with DHT bootstrap. dht.connect() self-seeds from bootstrap
      // nodes and blind-peering retries on its own backoff, so the blind-peer
      // connection does not need to wait for full bootstrap (dht_ready). This
      // overlaps the ~2s VPS connect with the ~1.5-3.5s bootstrap.
      this.emit('swarm_created')

      // Connection handler with capped peer discovery
      this.swarm.on('connection', (socket, peerInfo) => {
        if (!current()) { socket.destroy(); return }
        this.handleConnection(socket, peerInfo)

        // Tier 3: track peer-discovered nodes (capped)
        if (peerInfo.publicKey && socket.remoteAddress && socket.remotePort) {
          const peerNode = { host: socket.remoteAddress, port: socket.remotePort }
          const isDuplicate = discoveredBootstrapNodes.some(
            n => n.host === peerNode.host && n.port === peerNode.port
          )
          if (!isDuplicate && peerNode.host && peerNode.port && discoveredBootstrapNodes.length < MAX_DISCOVERED_NODES) {
            discoveredBootstrapNodes.push(peerNode)
            diag('Discovered bootstrap node: ' + peerNode.host + ':' + peerNode.port + ' (total: ' + discoveredBootstrapNodes.length + ')')
          }
        }
      })

      // Verify DHT connectivity with retry
      if (this.swarm.dht) {
        let dhtReady = false
        const maxRetries = 3
        const baseDelay = 1000

        for (let attempt = 1; attempt <= maxRetries; attempt++) {
          try {
            await this.swarm.dht.ready()
            if (!current()) return
            dhtReady = true
            diag('DHT ready (attempt ' + attempt + ')')
            // Consumers such as blind-peer replication can begin as soon as
            // the DHT is usable. They must not wait for the unrelated personal
            // invite topic to finish its full server announcement below.
            this.emit('dht_ready')
            break
          } catch (error) {
            if (!current()) return
            if (attempt < maxRetries) {
              const delay = baseDelay * Math.pow(2, attempt - 1)
              diag('DHT ready failed (attempt ' + attempt + '): ' + error.message + ', retrying in ' + delay + 'ms')
              await new Promise(resolve => setTimeout(resolve, delay))
              if (!current()) return
            } else {
              diag('DHT ready failed after ' + maxRetries + ' attempts')
              diag('[P2P] DHT may not be fully connected')
            }
          }
        }

        if (!dhtReady) {
          diag('Starting with limited DHT connectivity')
        }

        // Persist a warm snapshot of the routing table shortly after bootstrap
        // so the NEXT cold start can seed from it (see _loadDhtNodeCache). One
        // delayed save is enough; the table is stable for seeding purposes.
        if (this._nodeCacheSaveTimer) clearTimeout(this._nodeCacheSaveTimer)
        this._nodeCacheSaveTimer = setTimeout(() => this._saveDhtNodeCache(), 10000)
        if (this._nodeCacheSaveTimer && typeof this._nodeCacheSaveTimer.unref === 'function') {
          this._nodeCacheSaveTimer.unref()
        }

        // LAN peer discovery: probe the network gateway to see if it is a
        // Zapp node (responds to a DHT ping on port 49737).
        //
        // This is dynamic: any gateway gets probed — regular Wi-Fi routers
        // and cellular gateways don't speak HyperDHT and will time out; a
        // phone running Zapp (acting as a hotspot AP) will respond because its
        // HyperDHT binds to 0.0.0.0 and is reachable on the hotspot interface.
        // Only on a positive response do we add the gateway as a DHT seed, so
        // the routing table is never polluted with dead nodes.
        //
        // Why probe-then-seed works:
        //   - dht.ping() is a direct UDP packet — no routing table required.
        //     It works even when DHT is not yet bootstrapped (double-NAT case).
        //   - If the gateway IS phone1 running Zapp, the response seeds phone2's
        //     routing table with phone1's internet-connected DHT entries, enabling
        //     phone2 to reach the VPS and register with the blind peer even though
        //     phone2's direct internet UDP is blocked by double-NAT.
        //   - If the gateway is a regular router, the ping times out (~2s) and
        //     we log and skip — zero impact on normal-network operation.
        //
        // Run non-blocking so startup is not delayed for the common case.
        if (config.LOCAL_GATEWAY_IP) {
          const gatewayNode = { host: config.LOCAL_GATEWAY_IP, port: 49737 }
          diag('LAN probe: checking configured gateway')
          this.swarm.dht.ping(gatewayNode).then(() => {
            if (!current() || !this.swarm || !this.swarm.dht) return
            this.swarm.dht.addNode(gatewayNode)
            diag('LAN seed: configured gateway is a Zapp DHT node — seeded routing table')
          }).catch(() => {
            diag('LAN probe: configured gateway is not a DHT node — skipping')
          })
        }
      }

      // Mark online and join personal topic
      this.isOnline = true
      this.emit('status', { online: true, peerCount: 0 })
      diag('Joining personal topic...')
      await this.joinPersonalTopic()
      if (!current()) return

      // Wire health monitor to the live swarm (no temp instances)
      this.healthMonitor.setSwarm(this.swarm)
      await this.healthMonitor.startMonitoring()
      if (!current()) return
      diag('Health monitoring started (2 min interval)')

      // Start heartbeat to detect dead sockets
      this._startHeartbeat()
      diag('Heartbeat started (' + HEARTBEAT_INTERVAL_MS + 'ms interval)')

      diag('=== P2P Manager ready, key=' + b4a.toString(keyPair.publicKey, 'hex').substring(0, 12) + ' ===')
      diag('P2P Manager started successfully')

      // A brand-new recipient has no conversation core, so blind-peering has
      // nothing to register and cannot discover the mirrored message blocks.
      // Drain the identity-authenticated invite mailbox independently of DHT
      // peer discovery. The invite supplies the sender's first core key.
      this._drainInviteMailboxes().catch((e) => {
        diag('Invite mailbox drain failed: ' + (e.message || e))
      })

      // Periodic swarm state dump for debugging connection issues. Reveals
      // whether UDX bound a port, whether the DHT has a routing table, and
      // whether we're firewalled.
      this._swarmDumpInterval = setInterval(() => {
        try {
          const dht = this.swarm && this.swarm.dht
          const addr = dht && dht.remoteAddress ? dht.remoteAddress() : null
          const localAddr = dht && typeof dht.localAddress === 'function' ? dht.localAddress() : null
          const relaying = dht && dht.stats && dht.stats.relaying
          diag('SWARM conns=' + (this.swarm ? this.swarm.connections.size : 'n/a') +
            ' peers=' + this.peerCount +
            ' addr=' + (addr ? (addr.host + ':' + addr.port) : 'none') +
            ' localAddr=' + (localAddr ? (localAddr.host + ':' + localAddr.port) : 'none') +
            ' dhtReady=' + (dht ? !!dht.ready : false) +
            ' bootstrapped=' + (dht ? dht.bootstrapped : false) +
            ' firewalled=' + (dht ? dht.firewalled : 'n/a') +
            ' port=' + (dht && dht.io && dht.io.serverSocket ? dht.io.serverSocket.address().port : 0) +
            ' convs=' + this.conversations.size +
            ' globalConns=' + this.allPeerConnections.size +
            ' inviteTopics=' + this._inviteTopics.size +
            ' relay=' + (relaying
              ? (relaying.attempts + '/' + relaying.successes + '/' + relaying.aborts)
              : 'n/a'))
        } catch (e) {
          diag('SWARM dump error: ' + e.message)
        }
      }, 10000)
    } catch (error) {
      diag('Failed to start P2P Manager:', error)
      diag('START ERROR: ' + (error.stack || error.message || error))
      throw error
    }
  }

  /**
   * Ensure a local writable Hypercore exists for the conversation and is
   * registered with the blind mirror. Returns the core's public key (hex)
   * or null if hypercore support is not wired up.
   */
  async _ensureLocalCore (conversationId, recipientKeyHex = null) {
    if (!this.hypercoreManager) return null
    try {
      const core = await this.hypercoreManager.getOrCreateLocalCore(conversationId)
      // recipientKeyHex (direct chats) is the peer's identity pubkey: the blind
      // peer wakes them when we append a message to this writable core. Group
      // conversations pass null (multi-recipient wakeups are deferred).
      if (recipientKeyHex) this.hypercoreManager.setLocalCoreReferrer(conversationId, recipientKeyHex)
      if (this.blindMirror) this.blindMirror.addLocalCore(conversationId, core, recipientKeyHex)
      return b4a.toString(core.key, 'hex')
    } catch (err) {
      diag('Failed to ensure local core for ' + conversationId.substring(0, 12) + ': ' + (err.message || err))
      return null
    }
  }

  /**
   * Open a remote peer's read-only Hypercore (learned via invite or
   * `__core_keys` exchange) and register it with the blind mirror so we can
   * keep replicating it even when neither party is online.
   */
  /**
   * @returns {Promise<boolean>} whether the core is open. A mailbox-delivered
   *   core key is deleted on the strength of this, and it is the only copy, so
   *   failure has to be reported rather than logged and dropped.
   */
  async openRemoteCore (conversationId, peerKeyHex, coreKeyHex) {
    if (!this.hypercoreManager || !coreKeyHex) return false
    try {
      const recipientKeyHex = this.groupConversations.has(conversationId) || !this.keyPair
        ? null
        : b4a.toString(this.keyPair.publicKey, 'hex')
      const core = await this.hypercoreManager.openRemoteCore(conversationId, peerKeyHex, coreKeyHex, recipientKeyHex)
      if (this.blindMirror) {
        // A remote core carries messages the peer sends *to us*, so the blind
        // peer should wake our own identity on new blocks. Direct chats only --
        // a group core has many recipients and a single referrer can't express
        // that, so leave it unreferred until group wakeups land.
        //
        // Invariant: this same physical core is also registered by its writer
        // via addLocalCore(referrer = their peer). For a direct chat that peer
        // IS us, so both registrations set the identical referrer (our
        // identity). The blind peer keeps one referrer per core and does not
        // merge on re-registration -- relying on this convergence. If the two
        // sides ever set different referrers (e.g. groups, identity rotation),
        // the no-merge upsert would silently keep whichever landed first.
        this.blindMirror.addRemoteCore(conversationId, peerKeyHex, core, recipientKeyHex)
      }
      return true
    } catch (err) {
      diag('Failed to open remote core conv=' + conversationId.substring(0, 12) +
        ' peer=' + peerKeyHex.substring(0, 12) + ': ' + (err.message || err))
      return false
    }
  }

  /**
   * Register a Web-Push subscription with the blind peer(s) so the server can
   * doorbell-wake this device when a new message arrives for our identity. Uses
   * a dedicated, identity-authenticated connection per blind peer (the server
   * reads conn.remotePublicKey = our identity = the referrer it stores).
   *
   * @param {{endpoint:string, p256dh:string, auth:string}} sub
   * @returns {Promise<boolean>} true if at least one blind peer accepted it
   */
  async registerPushEndpoint (sub) {
    if (!this.swarm || !this.swarm.dht || !this.keyPair) {
      throw new Error('registerPushEndpoint: P2P not started')
    }
    const keys = (this.blindMirror && this.blindMirror.keys && this.blindMirror.keys.length)
      ? this.blindMirror.keys
      : config.BLIND_PEER_KEYS
    if (!keys || keys.length === 0) {
      throw new Error('registerPushEndpoint: no blind peer configured')
    }
    let ok = false
    let lastErr = null
    for (const key of keys) {
      try {
        await registerPushEndpointRpc(this.swarm.dht, this.keyPair, key, sub)
        ok = true
        diag('Registered push endpoint with blind peer ' + String(key).substring(0, 12))
      } catch (err) {
        lastErr = err
        diag('Push register failed (blind peer ' + String(key).substring(0, 12) + '): ' + (err.message || err))
      }
    }
    if (!ok && lastErr) throw lastErr
    return ok
  }

  /**
   * Send our local-core public keys (per conversation we share with this peer)
   * over an established connection. The peer can then open our cores and pull
   * any messages we've sent while they were offline.
   */
  _sendCoreKeys (framed, peerKeyHex) {
    if (!this.hypercoreManager) return
    const allKeys = this.hypercoreManager.getAllLocalCoreKeys()
    const relevant = {}
    // Direct conversation with this specific peer
    const direct = this.peerToConversation.get(peerKeyHex)
    if (direct && allKeys[direct.conversationId]) {
      relevant[direct.conversationId] = allKeys[direct.conversationId]
    }
    // Group conversations this peer belongs to
    for (const [convId, entry] of this.groupConversations) {
      if (this._peerMayAccessConversation(convId, peerKeyHex) && allKeys[convId]) {
        relevant[entry.groupTopicHex] = allKeys[convId]
      }
    }
    if (Object.keys(relevant).length === 0) return
    try {
      framed.writeJSON({ type: '__core_keys', cores: relevant })
    } catch (err) {
      diag('Failed to send __core_keys to ' + peerKeyHex.substring(0, 12) + ': ' + (err.message || err))
    }
  }

  /**
   * Join own personal topic to receive group invites from other peers
   */
  async joinPersonalTopic() {
    if (!this.swarm || !this.keyPair) return

    const topic = derivePersonalTopic(this.keyPair.publicKey)
    this.personalDiscovery = this.swarm.join(topic, { client: true, server: true })
    // Do NOT block startup on the announce landing. swarm.join() has already
    // registered the topic and begun announcing; flushed() only resolves once
    // the full DHT announce walk completes, which nothing on the message
    // delivery path needs. Awaiting it here previously serialized one full
    // announce ahead of every conversation join on cold start.
    this.personalDiscovery.flushed()
      .then(() => diag('Joined personal topic for receiving invites (announce landed)'))
      .catch((e) => diag('personal topic flush failed: ' + (e.message || e)))
  }

  /**
   * Absolute path to the persisted DHT node cache, or null if storage is
   * unavailable (e.g. tests without a data dir).
   */
  _dhtNodeCachePath () {
    // Only persist inside the Bare runtime (on-device). Avoids creating stray
    // data dirs during Node unit tests, which construct P2PManager with stub
    // swarms. storage.js pulls bare-* modules, so require it lazily here (behind
    // the Bare guard) rather than at module top where plain Node would fail.
    if (typeof Bare === 'undefined') return null
    try {
      const storage = require('./storage')
      const dir = storage.getDataDir()
      if (!dir) return null
      return (_diagPath ? _diagPath.join(dir, DHT_NODE_CACHE_FILE) : dir + '/' + DHT_NODE_CACHE_FILE)
    } catch (e) {
      return null
    }
  }

  /**
   * Load known-good DHT nodes persisted from a previous run. Returns [] on any
   * error so startup always proceeds with the normal bootstrap.
   */
  _loadDhtNodeCache () {
    try {
      const p = this._dhtNodeCachePath()
      if (!p) return []
      const storage = require('./storage')
      const data = storage.readJSON(p)
      if (!Array.isArray(data)) return []
      return data
        .filter(n => n && n.host && n.port)
        .map(n => ({ host: n.host, port: n.port }))
        .slice(0, DHT_NODE_CACHE_MAX)
    } catch (e) {
      return []
    }
  }

  /**
   * Persist a snapshot of the live routing table for the next cold start.
   * Best-effort: any failure is logged and swallowed.
   */
  _saveDhtNodeCache () {
    try {
      const dht = this.swarm && this.swarm.dht
      if (!dht || typeof dht.toArray !== 'function') return
      const p = this._dhtNodeCachePath()
      if (!p) return
      const nodes = dht.toArray({ limit: DHT_NODE_CACHE_MAX }) || []
      const clean = nodes.filter(n => n && n.host && n.port).map(n => ({ host: n.host, port: n.port }))
      if (clean.length === 0) return
      const storage = require('./storage')
      storage.writeJSON(p, clean)
      diag('Saved ' + clean.length + ' DHT node(s) to cache')
    } catch (e) {
      diag('DHT node cache save failed: ' + (e.message || e))
    }
  }

  /**
   * Join a direct conversation by deriving a shared topic with a peer's public key
   * @param {string} conversationId - Unique conversation identifier
   * @param {string} peerPublicKeyHex - Peer's public key (hex)
   * @returns {boolean} Success status
   */
  async joinConversation(conversationId, peerPublicKeyHex, { waitForDiscovery = true } = {}) {
    if (!this.swarm) {
      diag('joinConversation: swarm not started!')
      return false
    }

    if (!this.conversations.has(conversationId)) {
      this.conversations.set(conversationId, { peers: new Map() })
    }
    const conv = this.conversations.get(conversationId)

    if (conv.peers.has(peerPublicKeyHex)) {
      diag('joinConversation: already joined peer', peerPublicKeyHex.substring(0, 12))
      this._announceGroupCoreKeys(conversationId)
      return true
    }

    // Ensure a writable Hypercore exists for this conversation so messages
    // can be replicated through the blind peer for offline delivery. Pass the
    // peer's identity as the referrer so the blind peer can doorbell-wake them
    // when we send (direct chats only).
    await this._ensureLocalCore(conversationId, peerPublicKeyHex)

    try {
      const peerKey = b4a.from(peerPublicKeyHex, 'hex')
      const topic = deriveDirectChatTopic(
        b4a.toString(this.keyPair.publicKey, 'hex'),
        peerPublicKeyHex
      )
      const topicHex = b4a.toString(topic, 'hex')

      diag('Joining direct conversation:', conversationId.substring(0, 12), 'topic:', topicHex.substring(0, 12))

      const discovery = this.swarm.join(topic, { client: true, server: true })

      const peerEntry = {
        topic,
        topicHex,
        discovery,
        connections: []
      }
      conv.peers.set(peerPublicKeyHex, peerEntry)
      this.peerToConversation.set(peerPublicKeyHex, { conversationId, peerEntry })

      // Backfill any sockets that connected before joinConversation was called
      // (e.g. peer connected via personal topic for invite delivery)
      const existingConns = this.allPeerConnections.get(peerPublicKeyHex)
      if (existingConns && existingConns.length > 0) {
        for (const sock of existingConns) {
          if (!peerEntry.connections.includes(sock)) {
            peerEntry.connections.push(sock)
          }
        }
        this._flushPendingMessages(conversationId, peerEntry.connections)
        diag('Backfilled', peerEntry.connections.length, 'existing connection(s) for direct conversation', conversationId.substring(0, 12))
      }

      if (waitForDiscovery) await discovery.flushed()
      else discovery.flushed().catch(() => {})
      diag('Topic announced to DHT')

      return true
    } catch (error) {
      diag('joinConversation ERROR:', error)
      return false
    }
  }

  /**
   * Join a group conversation using the group's shared topic
   * @param {string} conversationId - Unique conversation identifier
   * @param {string} groupId - Group identifier (hex string)
   * @param {Array<string>} allParticipantKeyHexes - All participant public keys
   * @returns {boolean} Success status
   */
  async joinGroupConversation(conversationId, groupId, allParticipantKeyHexes, { waitForDiscovery = true } = {}) {
    if (!this._getConversation && !this._isParticipant) return false
    if (this._getConversation) {
      const stored = this._getConversation(conversationId)
      if (!stored || stored.type !== 'group' || stored.groupId !== groupId) return false
      const self = this.keyPair ? b4a.toString(this.keyPair.publicKey, 'hex') : null
      allParticipantKeyHexes = [...new Set([self, ...stored.participantIds].filter(Boolean))]
    } else if (this._isParticipant) {
      allParticipantKeyHexes = allParticipantKeyHexes.filter(key => this._peerMayAccessConversation(conversationId, key))
    }
    if (!this.swarm) {
      diag('joinGroupConversation: swarm not started!')
      return false
    }

    if (this.groupConversations.has(conversationId)) {
      const existing = this.groupConversations.get(conversationId)
      if (existing.groupId !== groupId) return false
      existing.participantKeys = [...allParticipantKeyHexes]
      for (const peer of existing.connections.keys()) {
        if (!allParticipantKeyHexes.includes(peer)) existing.connections.delete(peer)
      }
      this._announceGroupCoreKeys(conversationId)
      return true
    }

    // Ensure a writable Hypercore exists for blind-peer replication.
    await this._ensureLocalCore(conversationId)

    try {
      const topic = deriveGroupChatTopic(groupId)
      const topicHex = b4a.toString(topic, 'hex')

      diag('Joining group conversation:', conversationId.substring(0, 12), 'members:', allParticipantKeyHexes.length)

      const discovery = this.swarm.join(topic, { client: true, server: true })

      const groupEntry = {
        groupTopicHex: topicHex,
        groupId,
        discovery,
        participantKeys: allParticipantKeyHexes,
        connections: new Map() // peerKeyHex -> [socket]
      }

      this.groupConversations.set(conversationId, groupEntry)
      this.groupTopicToConversation.set(topicHex, conversationId)

      // Backfill any sockets that connected before joinGroupConversation was called
      const myKeyHex = this.keyPair ? b4a.toString(this.keyPair.publicKey, 'hex') : null
      for (const peerKeyHex of allParticipantKeyHexes) {
        if (peerKeyHex === myKeyHex) continue
        const existingConns = this.allPeerConnections.get(peerKeyHex)
        if (existingConns && existingConns.length > 0) {
          if (!groupEntry.connections.has(peerKeyHex)) {
            groupEntry.connections.set(peerKeyHex, [])
          }
          const arr = groupEntry.connections.get(peerKeyHex)
          for (const sock of existingConns) {
            if (!arr.includes(sock)) arr.push(sock)
          }
          diag('Backfilled', arr.length, 'existing connection(s) for group member', peerKeyHex.substring(0, 12))
        }
      }

      if (waitForDiscovery) await discovery.flushed()
      else discovery.flushed().catch(() => {})
      diag('Group topic announced to DHT')
      this._announceGroupCoreKeys(conversationId)

      return true
    } catch (error) {
      diag('joinGroupConversation ERROR:', error)
      return false
    }
  }

  /**
   * Flush queued messages once a conversation has usable sockets.
   * Works for both direct and group conversations.
   * @private
   */
  _announceGroupCoreKeys(conversationId) {
    for (const [peer, sockets] of this.allPeerConnections) {
      if (!this._peerMayAccessConversation(conversationId, peer)) continue
      const framed = this._firstLiveFramed(sockets)
      if (framed) this._sendCoreKeys(framed, peer)
    }
  }

  _flushPendingMessages(conversationId, sockets) {
    const pending = this.pendingMessages.get(conversationId)
    if (!pending || pending.length === 0 || !sockets || sockets.length === 0) {
      return
    }

    let flushed = 0
    // Add groupTopicHex for group conversations so receivers route correctly
    const groupConv = this.groupConversations.get(conversationId)
    const groupTopicHex = groupConv ? groupConv.groupTopicHex : null

    for (const { message } of pending) {
      const payload = groupTopicHex
        ? { ...message, groupTopicHex, conversationId: groupTopicHex }
        : { ...message, conversationId }
      let delivered = false

      for (const socket of sockets) {
        if (groupConv) {
          const peer = socket.remotePublicKey && b4a.toString(socket.remotePublicKey, 'hex')
          if (!peer || !this._peerMayAccessConversation(conversationId, peer)) continue
        }
        const framed = this._liveFramed(socket)
        if (!framed) continue
        try {
          if (framed.writeJSON(payload)) delivered = true
        } catch (error) {
          diag('Failed to flush queued message:', error.message)
        }
      }

      if (delivered) {
        flushed++
      }
    }

    if (flushed > 0) {
      diag('Flushed', flushed, 'queued message(s) for conversation', conversationId.substring(0, 12))
    }

  }

  /**
   * Start periodic heartbeat: pings all sockets, prunes dead ones.
   * @private
   */
  _startHeartbeat() {
    if (this._heartbeatInterval) return

    this._heartbeatInterval = setInterval(() => {
      this._runHeartbeat()
      // DHT discovery cannot wake two firewalled peers by itself. Poll the
      // encrypted bootstrap mailbox while foregrounded so an invite created
      // after startup does not wait for another app resume.
      this._drainInviteMailboxes().catch(e => {
        diag('Heartbeat invite mailbox drain failed: ' + (e.message || e))
      })
    }, HEARTBEAT_INTERVAL_MS)
  }

  _stopHeartbeat() {
    if (this._heartbeatInterval) {
      clearInterval(this._heartbeatInterval)
      this._heartbeatInterval = null
    }
  }

  /**
   * Ping every tracked socket. If a socket hasn't responded to a previous
   * ping within HEARTBEAT_TIMEOUT_MS, destroy it — this triggers the
   * normal 'close' handler which cleans up all maps.
   * @private
   */
  _runHeartbeat() {
    const now = Date.now()
    let pinged = 0
    let pruned = 0

    for (const [peerId, sockets] of this.allPeerConnections) {
      // Iterate in reverse so splice during iteration is safe
      for (let i = sockets.length - 1; i >= 0; i--) {
        const socket = sockets[i]
        const framed = this._liveFramed(socket)
        if (!framed) {
          // Already dead — drop it from every collection, not just this one.
          this._pruneSocket(socket, peerId)
          pruned++
          continue
        }

        // Check if the last pong is stale
        if (now - framed.lastPongTime > HEARTBEAT_TIMEOUT_MS) {
          diag('Heartbeat timeout for peer ' + peerId.substring(0, 12) + ', destroying socket')
          diag('[P2P] Heartbeat timeout — closing dead socket for', peerId.substring(0, 12))
          try { socket.destroy() } catch (e) { /* ignore */ }
          this._pruneSocket(socket, peerId)
          pruned++
          continue
        }

        // Send ping
        try {
          framed.writePing()
          pinged++
        } catch (e) {
          diag('Ping write failed for ' + peerId.substring(0, 12) + ': ' + e.message)
          try { socket.destroy() } catch (_) { /* ignore */ }
          this._pruneSocket(socket, peerId)
          pruned++
        }
      }

      if (sockets.length === 0) {
        this.allPeerConnections.delete(peerId)
      }
    }

    if (pruned > 0) {
      // _pruneSocket has already announced each lost peer and resynced the count.
      diag('Heartbeat: pinged=' + pinged + ' pruned=' + pruned + ' peers=' + this.peerCount)
    }
  }

  /**
   * Handle incoming peer connection
   * @param {Object} socket - Raw TCP socket
   * @param {Object} peerInfo - Peer information from Hyperswarm
   */
  handleConnection(socket, peerInfo) {
    if (this._stopping) { socket.destroy(); return }
    const generation = this._generation
    const current = () => !this._stopping && generation === this._generation
    const peerId = peerInfo.publicKey ? b4a.toString(peerInfo.publicKey, 'hex') : 'unknown'
    diag('New peer connection:', peerId.substring(0, 12))

    // Track in global connections map
    if (!this.allPeerConnections.has(peerId)) {
      this.allPeerConnections.set(peerId, [])
    }
    this.allPeerConnections.get(peerId).push(socket)

    // Wrap raw socket in FramedSocket for proper message boundaries
    const framed = new FramedSocket(socket)
    framed.peerId = peerId
    this.framedSockets.set(socket, framed)
    framed.canWriteChunk = hash => !this._mayServeMedia || this._mayServeMedia(hash, peerId)
    const writeChunk = framed.writeChunk.bind(framed)
    framed.writeChunk = (...args) => {
      if (!framed.canWriteChunk(args[0])) return false
      return writeChunk(...args)
    }

    // Send any pending invites to this peer
    const pending = this.pendingInvites.get(peerId)
    if (pending && pending.length > 0) {
      const failed = []
      for (const invite of pending) {
        try {
          if (framed.writeJSON(invite)) {
            diag('Sent pending invite to', peerId.substring(0, 12))
          } else {
            diag('Pending invite write did not land for', peerId.substring(0, 12))
            failed.push(invite)
          }
        } catch (e) {
          diag('Failed to send pending invite:', e.message)
          failed.push(invite)
        }
      }
      if (failed.length === 0) {
        this.pendingInvites.delete(peerId)
      } else {
        this.pendingInvites.set(peerId, failed)
      }
    }

    // Find direct conversation for this peer
    let directConversationId = null
    const directLookup = this.peerToConversation.get(peerId)
    if (directLookup) {
      directConversationId = directLookup.conversationId
      directLookup.peerEntry.connections.push(socket)
    }

    // Find group conversations this peer belongs to
    const groupConvIds = []
    for (const [convId, entry] of this.groupConversations) {
      if (this._peerMayAccessConversation(convId, peerId)) {
        if (!entry.connections.has(peerId)) {
          entry.connections.set(peerId, [])
        }
        entry.connections.get(peerId).push(socket)
        groupConvIds.push(convId)
      }
    }

    // Unique peers, not sockets. One peer can hold several, and the heartbeat
    // resyncs from the unique-peer map, so counting sockets here would drift
    // every time a peer opened a second connection.
    this.peerCount = this.allPeerConnections.size
    this.emit('status', { online: true, peerCount: this.peerCount })

    if (directConversationId) {
      this.emit('peer_online', directConversationId, peerId)
    }
    for (const convId of groupConvIds) {
      this.emit('peer_online', convId, peerId)
    }

    // Flush any messages queued while this peer was offline
    if (directConversationId) {
      const sockets = directLookup?.peerEntry?.connections || [socket]
      this._flushPendingMessages(directConversationId, sockets)
    }
    for (const convId of groupConvIds) {
      const entry = this.groupConversations.get(convId)
      if (entry) {
        const peerConns = entry.connections.get(peerId) || [socket]
        this._flushPendingMessages(convId, peerConns)
      }
    }

    // Tell this peer the public keys of our local Hypercores for shared
    // conversations so they can pull anything we sent while they were offline.
    this._sendCoreKeys(framed, peerId)

    // If we're hiding our presence, tell this peer straight away so they retract
    // the online dot our fresh socket just lit up on their side. Visible is the
    // default, so we only need to speak up when hidden.
    if (!this.presenceVisible) {
      try {
        framed.writeJSON({ type: '__presence', hidden: true })
      } catch (e) {
        diag('presence announce failed: ' + (e.message || e))
      }
    }

    // Handle incoming JSON messages via framed socket
    const onMessage = async (message) => {
      try {
        // Rate limit check (H10)
        while (this._isRateLimited(peerId)) {
          // FramedSocket pauses reads while this handler awaits. Throttle live
          // catch-up instead of acknowledging/dropping legitimate records.
          await new Promise(resolve => {
            const done = () => { clearTimeout(timer); socket.removeListener('close', done); resolve() }
            const entry = this._peerMessageCounts.get(peerId)
            const remaining = entry ? this._peerRateWindowMs - (Date.now() - entry.windowStart) + 1 : 1
            const timer = setTimeout(done, Math.max(1, remaining))
            socket.on('close', done)
          })
          if (!current() || socket.destroyed) return
        }
        message = normalizePeerRecord(message, peerId)
        // Core-key exchange: peer is announcing the public keys of its local
        // Hypercores. The conversationId set is attacker-controlled, so open
        // a remote core only for conversations this authenticated peer is a
        // verified participant of — otherwise a peer sharing one chat could
        // register a core (and inject authored rows) into any other chat
        // whose id it can compute.
        if (message.type === '__core_keys' && message.cores) {
          for (const [wireId, coreKeyHex] of Object.entries(message.cores)) {
            const convId = this.resolveWireConversation(wireId)
            if (!this._peerMayAnnounceCore(convId, peerId)) {
              diag('Rejected __core_keys for ' + String(convId).substring(0, 12) +
                ' from non-participant ' + peerId.substring(0, 12))
              continue
            }
            this.openRemoteCore(convId, peerId, coreKeyHex).catch((err) => {
              diag('openRemoteCore failed: ' + (err.message || err))
            })
          }
          return
        }

        // Bind receipts to a conversation shared with the authenticated peer.
        if (message.type === '__receipt') {
          const receiptLookup = this.peerToConversation.get(peerId)
          const wireId = message.groupTopicHex || message.conversationId
          const mapped = wireId && this.resolveWireConversation(wireId)
          // An explicit but unknown group route must never fall back into a DM.
          const receiptConvId = mapped || (receiptLookup && receiptLookup.conversationId) || directConversationId
          if (!receiptConvId || !this._peerMayAccessConversation(receiptConvId, peerId)) return
          if (this._peerRecordSink) await this._peerRecordSink(receiptConvId, peerId, message)
          else this.emit('receipt', receiptConvId, message, peerId)
          return
        }

        // Presence visibility: the peer is telling us whether to surface them as
        // online. Honor-system privacy toggle — when they hide we retract their
        // online dot for every conversation we share; when they unhide we restore
        // it. Bound to conversations we actually share with this authenticated
        // peer, never a wire-supplied conversationId.
        if (message.type === '__presence') {
          const evt = message.hidden === true ? 'peer_offline' : 'peer_online'
          const presenceLookup = this.peerToConversation.get(peerId)
          const presenceDirectId =
            (presenceLookup && presenceLookup.conversationId) || directConversationId
          if (presenceDirectId) {
            this.emit(evt, presenceDirectId, peerId)
          }
          for (const convId of groupConvIds) {
            if (this._peerMayAccessConversation(convId, peerId)) {
              this.emit(evt, convId, peerId)
            }
          }
          return
        }

        if (message.type && this._peerRecordSink) {
          const wireId = message.groupTopicHex || message.conversationId
          const convId = wireId ? this.resolveWireConversation(wireId) : null
          await this._peerRecordSink(convId, peerId, message)
          return
        }

        // Handle control messages before any mutation — these carry their own
        // senderKey/senderKey fields set by the remote peer.
        if (message.type === 'group_invite') {
          diag('Received group_invite from', peerId.substring(0, 12))
          this.emit('group_invite', message, peerId)
          return
        }
        if (message.type === 'direct_invite') {
          diag('Received direct_invite from', peerId.substring(0, 12))
          this.emit('direct_invite', message, peerId)
          return
        }
        if (message.type === 'group_leave') {
          diag('Received group_leave from', peerId.substring(0, 12))
          this.emit('group_leave', message, peerId)
          return
        }
        if (message.type === 'group_deleted') {
          diag('Received group_deleted from', peerId.substring(0, 12))
          this.emit('group_deleted', message, peerId)
          return
        }
        if (message.type === 'group_renamed') {
          diag('Received group_renamed from', peerId.substring(0, 12))
          this.emit('group_renamed', message, peerId)
          return
        }
        if (message.type === 'group_member_added') {
          diag('Received group_member_added from', peerId.substring(0, 12))
          this.emit('group_member_added', message, peerId)
          return
        }

        // Any unrecognized control record (reserved '__'-prefixed type) must never
        // fall through into chat-message handling, or it lands in the store as a
        // phantom row. A newer control type sent by an updated peer reaches an
        // older build here — drop it safely instead of persisting it as a message.
        if (typeof message.type === 'string' && message.type.startsWith('__')) {
          return
        }

        // HIGH-01: For chat messages, pin senderId to the authenticated Noise
        // public key so a malicious peer cannot spoof another user's identity.
        // Hyperswarm's Noise handshake guarantees this equals the remote
        // peer's long-term identity key.
        message.senderId = peerId

        // Route group messages by groupTopicHex
        if (message.groupTopicHex) {
          const convId = this.groupTopicToConversation.get(message.groupTopicHex)
          if (convId && this._peerMayAccessConversation(convId, peerId)) {
            // HIGH-02: Deduplicate by message ID to prevent the same message
            // appearing twice (e.g. flushed from pending queue + live delivery).
            if (this._peerRecordSink) {
              await this._peerRecordSink(convId, peerId, message)
            } else if (!this._isDuplicateOrReplayed(convId, message)) {
              this.emit('message', convId, message)
            }
          } else if (convId) {
            diag('Rejected group message from non-participant ' + peerId.substring(0, 12) +
              ' conv=' + convId.substring(0, 12))
          } else {
            diag('Received group message for unknown topic:', message.groupTopicHex.substring(0, 12))
          }
          return
        }

        // Route direct messages via live reverse lookup (avoids race where
        // directConversationId was null at connection time but joinConversation
        // was called shortly after). A wire-supplied conversationId is only
        // honored when it is verifiably the peer's own (deterministic DM id or
        // existing membership) — see _acceptWireConversationId.
        const liveLookup = this.peerToConversation.get(peerId)
        const convId = (liveLookup && liveLookup.conversationId) || directConversationId ||
          this._acceptWireConversationId(message.conversationId, peerId)
        if (convId) {
          // HIGH-02: Deduplicate by message ID
          if (this._peerRecordSink) {
            await this._peerRecordSink(convId, peerId, message)
          } else if (!this._isDuplicateOrReplayed(convId, message)) {
            this.emit('message', convId, message)
          }
        }
      } catch (error) {
        diag('Failed to handle peer message:', error)
      }
    }
    framed.onMessage = message => current()
      ? this.runTask(() => onMessage(message))
      : Promise.resolve()

    // Handle incoming media chunks
    framed.onChunk = (hashBuf, chunkIndex, totalChunks, chunkData) => {
      if (!current()) return
      this.emit('media_chunk', hashBuf, chunkIndex, totalChunks, chunkData, peerId)
    }

    // Handle media requests from peer
    framed.onRequest = (hashBuf, attempt) => {
      if (!current()) return
      if (this._isMediaRequestRateLimited(peerId)) return
      this.emit('media_request', hashBuf, peerId, framed, attempt)
    }

    framed.onError = (error) => {
      diag('Framing error from', peerId.substring(0, 12), ':', error.message)
    }

    socket.on('error', (error) => {
      diag('Socket error:', error)
    })

    socket.on('close', () => {
      if (!current()) return
      diag('Peer disconnected:', peerId.substring(0, 12))

      // Announces the loss and resyncs peerCount, once the last socket is gone.
      this._pruneSocket(socket, peerId)
      diag('Peer disconnected: ' + peerId.substring(0, 12) + ' remaining=' + this.peerCount)
    })
  }

  /**
   * Update whether we advertise our online presence to connected peers. When we
   * become hidden we tell every currently-connected peer to retract the online
   * dot our live socket lit up; when we become visible again we tell them to
   * restore it. Fresh connections are handled in handleConnection (which only
   * announces when hidden). Honor-system, mirroring read receipts — a peer's own
   * client decides whether to respect it.
   * @param {boolean} visible
   */
  setPresenceVisible (visible) {
    this.presenceVisible = visible !== false
    const hidden = !this.presenceVisible
    for (const sockets of this.allPeerConnections.values()) {
      for (const socket of sockets) {
        const framed = this.framedSockets.get(socket)
        if (!framed) continue
        try {
          framed.writeJSON({ type: '__presence', hidden })
        } catch (e) {
          diag('setPresenceVisible: writeJSON failed: ' + (e.message || e))
        }
      }
    }
  }

  /**
   * Get the FramedSocket wrapper for a raw socket
   * @param {Object} socket - Raw socket
   * @returns {FramedSocket|null}
   */
  _getFramed(socket) {
    return this.framedSockets.get(socket) || null
  }

  /**
   * Framed wrapper for a socket, or null when the socket is not usable.
   *
   * Liveness is resolved where a socket is used rather than trusted from the
   * connection maps. A socket that dies without a clean 'close' — the normal
   * case when the OS suspends us — lingers in those maps, so every send path
   * has to treat such an entry as absent instead of as a reachable peer.
   * @param {Object} socket
   * @returns {Object|null}
   */
  _liveFramed(socket) {
    const framed = this.framedSockets.get(socket)
    if (!framed || framed._destroyed) return null
    // `_destroyed` is our own teardown flag; it says nothing about a socket the
    // OS tore down without a close event, which is the case this exists for.
    if (socket.destroyed || socket.writable === false) return null
    return framed
  }

  /**
   * First usable framed wrapper in a socket list, or null when none are live.
   * Callers must not assume index 0 is reachable: a stale socket can sit ahead
   * of a working one.
   * @param {Array<Object>} sockets
   * @returns {Object|null}
   */
  _firstLiveFramed(sockets) {
    if (!Array.isArray(sockets)) return null
    for (const socket of sockets) {
      const framed = this._liveFramed(socket)
      if (framed) return framed
    }
    return null
  }

  /**
   * Drop a socket from every collection that can reference it. Idempotent, and
   * safe for a socket that was never fully registered.
   *
   * Socket references are denormalized across the global map, each direct
   * conversation and each group, so removal is centralized here; pruning only
   * the collection at hand is what lets the copies drift apart.
   * @param {Object} socket
   * @param {string|null} peerId
   */
  _pruneSocket(socket, peerId) {
    const framed = this.framedSockets.get(socket)
    if (framed) {
      try { framed.destroy() } catch (e) { /* already torn down */ }
      this.framedSockets.delete(socket)
    }

    // Whether this call is the one that took the peer's last socket, rather than
    // whether the peer has none now. The same socket is pruned more than once by
    // design — heartbeat pruning does not stop its eventual close callback — and
    // announcing from every call reported one loss twice.
    let lastSocketDropped = false
    if (peerId) {
      const conns = this.allPeerConnections.get(peerId)
      if (conns) {
        const idx = conns.indexOf(socket)
        if (idx !== -1) {
          conns.splice(idx, 1)
          lastSocketDropped = conns.length === 0
        }
        if (conns.length === 0) this.allPeerConnections.delete(peerId)
      }
    }

    // Every direct conversation, not just the one this socket was opened for:
    // joinConversation backfills already-connected sockets, so a socket can be
    // attached to a conversation that did not exist when it was established.
    for (const conv of this.conversations.values()) {
      for (const peerEntry of conv.peers.values()) {
        const idx = peerEntry.connections.indexOf(socket)
        if (idx !== -1) peerEntry.connections.splice(idx, 1)
      }
    }

    // Collected while removing, not derived afterwards: these are the groups this
    // socket actually served, and the entry is gone from the map by the end.
    const affectedGroups = []
    for (const [convId, entry] of this.groupConversations) {
      for (const [groupPeerId, peerConns] of entry.connections) {
        const idx = peerConns.indexOf(socket)
        if (idx === -1) continue
        peerConns.splice(idx, 1)
        if (peerConns.length === 0) entry.connections.delete(groupPeerId)
        if (groupPeerId === peerId) affectedGroups.push(convId)
      }
    }

    // Notified here rather than from the 'close' handler, because the socket this
    // method exists for is the one the OS killed without ever firing close.
    // Announcing only from close left the UI showing such a peer as reachable
    // indefinitely, with nothing to correct it.
    if (lastSocketDropped) this._announcePeerLost(peerId, affectedGroups)
  }

  /**
   * Report a peer as gone, but only once its last socket has been dropped: a peer
   * holding two connections that loses one is still reachable, and retracting it
   * flapped the online dot off and back on at the next heartbeat.
   * @param {string} peerId
   * @param {Array<string>} groupConversationIds groups the dropped socket served
   */
  _announcePeerLost(peerId, groupConversationIds = []) {
    if (this.allPeerConnections.has(peerId)) return

    const directLookup = this.peerToConversation.get(peerId)
    if (directLookup) this.emit('peer_offline', directLookup.conversationId, peerId)
    for (const convId of groupConversationIds) {
      this.emit('peer_offline', convId, peerId)
    }

    if (this.peerCount !== this.allPeerConnections.size) {
      this.peerCount = this.allPeerConnections.size
      this.emit('status', { online: this.isOnline, peerCount: this.peerCount })
    }
  }

  /**
   * Send a message to all connected peers in a conversation.
   * If no peers are connected the message is queued and flushed when a peer connects.
   * @param {string} conversationId - Conversation identifier
   * @param {Object} message - Message object to send
   * @returns {boolean} true if written to a live peer socket, false otherwise
   */
  sendToConversation(conversationId, message, { notificationEligible = false } = {}) {
    if (this._stopping) throw new Error('Messaging lifecycle stopped')
    const outboundMessage = this._outgoingRecord(conversationId, message)
    if (outboundMessage !== message) delete outboundMessage.status

    // Mirror message into the conversation's writable Hypercore. Visible user
    // messages request one proof-backed push only after that exact append is
    // durable; control records are persisted but never ring a notification.
    if (this.hypercoreManager) {
      this._persistOutgoing(conversationId, outboundMessage, notificationEligible).catch(() => {})
    }

    if (outboundMessage && outboundMessage.id && !outboundMessage.type) {
      if (!this.pendingMessages.has(conversationId)) {
        this.pendingMessages.set(conversationId, [])
      }
      const queue = this.pendingMessages.get(conversationId)
      if (!queue.some(entry => entry.message.id === outboundMessage.id)) {
        if (queue.length >= MAX_PENDING_MESSAGES_PER_CONVERSATION) {
          queue.shift()
        }
        queue.push({ message: outboundMessage })
      }
    }

    const sent = this._trySendToConversation(conversationId, outboundMessage)
    if (!sent) {
      const queueLength = this.pendingMessages.get(conversationId)?.length || 0
      diag('SEND QUEUED conv=' + conversationId.substring(0, 12) + ' queueLen=' + queueLength + ' isOnline=' + this.isOnline + ' peers=' + this.peerCount)
    } else {
      diag('SEND OK conv=' + conversationId.substring(0, 12))
    }
    return sent
  }

  /**
   * Persist an outgoing user-visible message before reporting success to the
   * native caller. Live socket delivery is intentionally attempted only after
   * the Hypercore append commits, so suspension can never turn a successful IPC
   * response into a message that existed only in memory.
   *
   * Blind-peer notification acknowledgement is advisory: the installed
   * blind-peering protocol acknowledges the proof-backed doorbell request, not
   * durable replication of the block. `relay` therefore reports what is known
   * without overstating the durability boundary.
   */
  async sendToConversationDurably (conversationId, message, { notificationEligible = false } = {}) {
    if (this._stopping) throw new Error('Messaging lifecycle stopped')
    const outboundMessage = this._outgoingRecord(conversationId, message)
    if (outboundMessage !== message) delete outboundMessage.status

    const persistence = this.hypercoreManager
      ? await this._persistOutgoing(conversationId, outboundMessage, notificationEligible)
      : { appended: false, relay: 'unavailable' }

    if (!persistence.appended) {
      const error = new Error('Outgoing message was not appended to Hypercore')
      error.code = 'MESSAGE_NOT_DURABLE'
      throw error
    }

    if (outboundMessage && outboundMessage.id && !outboundMessage.type) {
      if (!this.pendingMessages.has(conversationId)) this.pendingMessages.set(conversationId, [])
      const queue = this.pendingMessages.get(conversationId)
      if (!queue.some(entry => entry.message.id === outboundMessage.id)) {
        if (queue.length >= MAX_PENDING_MESSAGES_PER_CONVERSATION) queue.shift()
        queue.push({ message: outboundMessage })
      }
    }

    const sent = this._trySendToConversation(conversationId, outboundMessage)
    if (!sent) this._bootstrapConversationThroughMailbox(conversationId)
    const queueLength = this.pendingMessages.get(conversationId)?.length || 0
    diag('SEND DURABLE conv=' + conversationId.substring(0, 12) +
      ' appended=true relay=' + persistence.relay +
      ' live=' + sent + ' queueLen=' + queueLength)
    return { sent, ...persistence }
  }

  /**
   * Re-offer an unreachable conversation through the mailbox, so a peer that
   * never received the original invite can still open our core.
   *
   * Deliberately not awaited by callers: the transports below can take tens of
   * seconds on exactly the broken networks that trigger this, and the message
   * is already durable and queued by the time we get here.
   * @param {string} conversationId
   */
  _bootstrapConversationThroughMailbox (conversationId) {
    const last = this._mailboxBootstrapAt.get(conversationId) || 0
    if (Date.now() - last < MAILBOX_BOOTSTRAP_INTERVAL_MS) return
    if (this._mailboxBootstrapInFlight.has(conversationId)) return

    // Stamped before the attempt, not after it. Stamping only on success means
    // an unreachable mailbox is retried on every single send.
    this._mailboxBootstrapAt.set(conversationId, Date.now())

    const operation = this.runTask(async () => {
      const conversation = this.conversations.get(conversationId)
      const peerKeyHex = conversation && conversation.peers
        ? conversation.peers.keys().next().value
        : null
      if (!peerKeyHex || !this.keyPair || !this.hypercoreManager) return
      const localCoreKey = this.hypercoreManager.getLocalCoreKey(conversationId) ||
        await this._ensureLocalCore(conversationId, peerKeyHex)
      const stored = await this._putInviteMailbox(peerKeyHex, {
        type: 'direct_invite',
        conversationId,
        senderKey: b4a.toString(this.keyPair.publicKey, 'hex'),
        senderDisplayName: this._displayName() || undefined,
        localCoreKey
      })
      if (stored) {
        diag('Mailbox bootstrapped unreachable conversation ' + conversationId.substring(0, 12))
      }
    })

    this._mailboxBootstrapInFlight.set(conversationId, operation)
    operation
      .catch(e => diag('Mailbox bootstrap failed: ' + (e.message || e)))
      .finally(() => {
        if (this._mailboxBootstrapInFlight.get(conversationId) === operation) {
          this._mailboxBootstrapInFlight.delete(conversationId)
        }
      })
  }

  async _persistOutgoing (conversationId, message, notificationEligible) {
    let appended
    try {
      appended = await this.hypercoreManager.appendMessage(conversationId, message)
    } catch (err) {
      diag('Hypercore append failed: ' + (err.message || err))
      this.emit('message_persist_failed', {
        conversationId,
        messageId: message && message.id,
        error: err.message || String(err)
      })
      throw err
    }

    const isUserVisibleDirectMessage =
      notificationEligible && !!(message && message.id && !message.type)
    if (!isUserVisibleDirectMessage || !this.blindMirror || !appended) {
      return { appended: !!appended, relay: this.blindMirror ? 'not_requested' : 'unavailable' }
    }

    const generation = this._generation
    // Attach both outcomes immediately. Nothing after the append can turn a
    // notification failure into a failed durable send, or delay its live path.
    Promise.resolve().then(() => {
      if (this._stopping || generation !== this._generation) return
      return this.blindMirror.sendNotification(appended.core, appended.index)
    }).catch(err => {
      if (this._stopping || generation !== this._generation) return
      try {
        this.emit('push_notification_failed', {
          conversationId,
          messageId: message.id,
          error: err && err.code ? err.code : 'unavailable'
        })
      } catch (_) { /* IPC may already be unavailable; the append is durable. */ }
    })
    return { appended: true, relay: 'pending' }
  }

  restorePendingMessages(conversationId, messages) {
    if (!Array.isArray(messages) || messages.length === 0) return
    if (!this.pendingMessages.has(conversationId)) this.pendingMessages.set(conversationId, [])
    const queue = this.pendingMessages.get(conversationId)
    const knownIds = new Set(queue.map(entry => entry.message.id))
    for (const message of messages) {
      if (message && message.id && !knownIds.has(message.id)) {
        const wireMessage = { ...message }
        delete wireMessage.mediaLocalPath
        delete wireMessage.mediaTransferState
        if (queue.length >= MAX_PENDING_MESSAGES_PER_CONVERSATION) {
          const removed = queue.shift()
          if (removed) knownIds.delete(removed.message.id)
        }
        queue.push({ message: wireMessage })
        knownIds.add(message.id)
        this._trySendToConversation(conversationId, wireMessage)
      }
    }
  }

  confirmMessagesDelivered(conversationId, messageIds) {
    if (!Array.isArray(messageIds) || messageIds.length === 0) return
    const pending = this.pendingMessages.get(conversationId)
    if (!pending) return
    const delivered = new Set(messageIds)
    const remaining = pending.filter(entry => !delivered.has(entry.message.id))
    if (remaining.length === 0) this.pendingMessages.delete(conversationId)
    else this.pendingMessages.set(conversationId, remaining)
  }

  /**
   * Send a read receipt for a conversation back to the original sender.
   *
   * Rides both transport layers, same as a normal message in reverse:
   *  - appended to our writable Hypercore so it replicates to the sender via
   *    the blind peer even when they're offline (the cross-timezone case), and
   *  - written live to any connected peer for an instant tick.
   *
   * The receipt is a control record, never a chat row: the sender routes it out
   * by `type` on both the live and replication paths.
   *
   * @param {string} conversationId
   * @param {string} upToMessageId - Newest of the sender's messages we've read
   */
  sendReadReceipt(conversationId, upToMessageId) {
    if (this._stopping) return false
    const receipt = this._outgoingRecord(conversationId, { type: '__receipt', kind: 'read', conversationId, upTo: upToMessageId })

    if (this.hypercoreManager) {
      this.runTask(() => this.hypercoreManager.appendMessage(conversationId, receipt)).catch((err) => {
        diag('Receipt append failed conv=' + conversationId.substring(0, 12) + ': ' + (err.message || err))
      })
    }

    this._trySendToConversation(conversationId, receipt)
  }

  async sendDeliveryReceipt(conversationId, upToMessageId, senderId, opts = {}) {
    if (!senderId) return false
    const receipt = this._outgoingRecord(conversationId, { type: '__receipt', kind: 'delivered', conversationId, upTo: upToMessageId, to: senderId })
    const requireDurable = opts.requireDurable === true

    if (this.hypercoreManager) {
      const append = this.runTask(() => this.hypercoreManager.appendMessage(conversationId, receipt))
      if (requireDurable) {
        try {
          await append
        } catch (err) {
          diag('Delivery receipt append failed conv=' + conversationId.substring(0, 12) + ': ' + (err.message || err))
          throw err
        }
      } else {
        append.catch((err) => {
          diag('Delivery receipt append failed conv=' + conversationId.substring(0, 12) + ': ' + (err.message || err))
        })
      }
    } else if (requireDurable) {
      throw new Error('Hypercore unavailable for durable delivery receipt')
    }

    return this._trySendToConversation(conversationId, receipt)
  }

  /**
   * Internal: attempt immediate send without queuing
   * @param {string} conversationId - Conversation identifier
   * @param {Object} message - Message object to send
   * @returns {boolean} Success status
   */
  _trySendToConversation(conversationId, message) {
    // Check group conversations first
    const groupConv = this.groupConversations.get(conversationId)
    if (groupConv) {
      const payload = { ...message, groupTopicHex: groupConv.groupTopicHex, conversationId: groupConv.groupTopicHex }
      let sent = false
      const sentPeers = new Set()
      let writeErrors = 0

      for (const [peerKeyHex, sockets] of groupConv.connections) {
        if (!this._peerMayAccessConversation(conversationId, peerKeyHex)) continue
        const framed = this._firstLiveFramed(sockets)
        if (!framed) continue
        try {
          if (framed.writeJSON(payload)) {
            sent = true
            sentPeers.add(peerKeyHex)
          }
        } catch (error) {
          writeErrors++
          diag('Group write failed peer=' + peerKeyHex.substring(0, 12) + ': ' + error.message)
        }
      }

      // Fallback: try allPeerConnections for unreached participants
      const myKey = this.keyPair ? b4a.toString(this.keyPair.publicKey, 'hex') : null
      for (const peerKeyHex of groupConv.participantKeys) {
        if (!this._peerMayAccessConversation(conversationId, peerKeyHex)) continue
        if (peerKeyHex === myKey) continue
        if (sentPeers.has(peerKeyHex)) continue

        const framed = this._firstLiveFramed(this.allPeerConnections.get(peerKeyHex))
        if (!framed) continue
        try {
          if (framed.writeJSON(payload)) sent = true
        } catch (error) {
          writeErrors++
          diag('Group fallback write failed peer=' + peerKeyHex.substring(0, 12) + ': ' + error.message)
        }
      }

      if (!sent) {
        diag('Group send FAILED conv=' + conversationId.substring(0, 12) +
          ' groupConns=' + groupConv.connections.size + ' writeErrors=' + writeErrors)
      }
      return sent
    }

    // Direct conversation
    const conv = this.conversations.get(conversationId)
    if (!conv) {
      diag('Send FAILED: no conversation map entry for ' + conversationId.substring(0, 12))
      return false
    }

    const payload = { ...message, conversationId }
    let sent = false
    let socketCount = 0
    let writeErrors = 0

    for (const [peerKeyHex, peerEntry] of conv.peers) {
      // Snapshot: a dead entry is pruned as we go, which mutates this array.
      for (const socket of [...peerEntry.connections]) {
        const framed = this._liveFramed(socket)
        if (!framed) {
          this._pruneSocket(socket, peerKeyHex)
          continue
        }
        socketCount++
        try {
          if (framed.writeJSON(payload)) sent = true
        } catch (error) {
          writeErrors++
          diag('Direct write failed peer=' + peerKeyHex.substring(0, 12) + ': ' + error.message)
        }
      }
    }

    if (!sent) {
      diag('Direct send FAILED conv=' + conversationId.substring(0, 12) +
        ' peers=' + conv.peers.size + ' sockets=' + socketCount + ' writeErrors=' + writeErrors)
    }
    return sent
  }

  /**
   * Get framed sockets for a conversation (for media transfer)
   * @param {string} conversationId - Conversation identifier
   * @returns {Array<FramedSocket>}
   */
  getConversationFramedSockets(conversationId) {
    const sockets = []

    const groupConv = this.groupConversations.get(conversationId)
    if (groupConv) {
      const reachedPeers = new Set()
      for (const [peerKeyHex, peerSockets] of groupConv.connections) {
        if (!this._peerMayAccessConversation(conversationId, peerKeyHex)) continue
        for (const socket of peerSockets) {
          const framed = this._liveFramed(socket)
          if (framed) {
            sockets.push(framed)
            reachedPeers.add(peerKeyHex)
          }
        }
      }

      // Fallback: try allPeerConnections for participants not reached via group topic
      const myKey = this.keyPair ? b4a.toString(this.keyPair.publicKey, 'hex') : null
      for (const peerKeyHex of groupConv.participantKeys) {
        if (!this._peerMayAccessConversation(conversationId, peerKeyHex)) continue
        if (peerKeyHex === myKey) continue
        if (reachedPeers.has(peerKeyHex)) continue

        const framed = this._firstLiveFramed(this.allPeerConnections.get(peerKeyHex))
        if (framed) sockets.push(framed)
      }

      return sockets
    }

    const conv = this.conversations.get(conversationId)
    if (conv) {
      for (const [, peerEntry] of conv.peers) {
        for (const socket of peerEntry.connections) {
          const framed = this._liveFramed(socket)
          if (framed) sockets.push(framed)
        }
      }
    }

    return sockets
  }

  /**
   * Ask connected peers to send us a media blob we are missing. Prefers the
   * authoring peer's own sockets, then falls back to any socket in the
   * conversation. Returns true if at least one request frame went out.
   *
   * The serving side is already wired (onRequest → 'media_request' →
   * mediaTransfer.handleRequest → sendMedia); this is the missing initiator.
   *
   * @param {string} conversationId
   * @param {string} mediaIdHex - media content hash (hex)
   * @param {string|null} preferredPeerId - author's identity key, tried first
   * @returns {boolean}
   */
  requestMedia (conversationId, mediaIdHex, preferredPeerId = null, attempt = 1) {
    if (typeof conversationId !== 'string' || conversationId.length === 0) return false
    if (!isMediaId(mediaIdHex)) return false
    if (preferredPeerId !== null && !isPeerId(preferredPeerId)) return false

    const hashBuf = b4a.from(mediaIdHex, 'hex')
    const seen = new Set()
    const requestFrom = (framed) => {
      if (!framed || seen.has(framed) || typeof framed.writeRequest !== 'function') return false
      seen.add(framed)
      try {
        return framed.writeRequest(hashBuf, attempt) === true
      } catch (err) {
        diag('media request write failed: ' + (err.message || err))
        return false
      }
    }

    // First attempt prefers the author. Subsequent attempts rotate through
    // reachable recipients/sockets: an author can be online but lack the file.
    const candidates = []
    if (preferredPeerId) {
      for (const socket of this.allPeerConnections.get(preferredPeerId) || []) {
        const framed = this._liveFramed(socket)
        if (framed) candidates.push(framed)
      }
    }
    for (const framed of this.getConversationFramedSockets(conversationId)) {
      if (!candidates.includes(framed)) candidates.push(framed)
    }
    const offset = Number.isSafeInteger(attempt) && attempt > 0 ? (attempt - 1) % Math.max(1, candidates.length) : 0
    for (let index = 0; index < candidates.length; index++) {
      if (requestFrom(candidates[(index + offset) % candidates.length])) return true
    }
    return false
  }

  /**
   * Send a group invite to a peer
   * @param {string} peerPublicKeyHex - Peer's public key (hex)
   * @param {Object} inviteData - Invite data to send
   * @returns {boolean} Success status
   */
  async sendInvite(peerPublicKeyHex, inviteData) {
    if (!this.swarm) return false

    // Try sending via existing connections first. A write that did not land must
    // fall through to the mailbox below, not report the invite as sent: this is
    // the only path that carries the core key.
    try {
      const framed = this._firstLiveFramed(this.allPeerConnections.get(peerPublicKeyHex))
      if (framed && framed.writeJSON(inviteData)) {
        diag('Sent invite to', peerPublicKeyHex.substring(0, 12), 'via existing connection')
        return true
      }
    } catch (e) {
      diag('Failed to send via existing connection:', e.message)
    }

    // Store as pending invite (delivered when peer connects)
    if (!this.pendingInvites.has(peerPublicKeyHex)) {
      this.pendingInvites.set(peerPublicKeyHex, [])
    }
    this.pendingInvites.get(peerPublicKeyHex).push(inviteData)

    // Persist the bootstrap control message at the blind peer before relying
    // on topic discovery. The body is end-to-end encrypted to the recipient
    // and signed by this identity; the server only routes the opaque envelope.
    // Once accepted, the direct pending copy is redundant (and would produce
    // a duplicate invite if DHT connectivity later recovers).
    try {
      if (await this._putInviteMailbox(peerPublicKeyHex, inviteData)) {
        const pending = this.pendingInvites.get(peerPublicKeyHex) || []
        const index = pending.indexOf(inviteData)
        if (index !== -1) pending.splice(index, 1)
        if (pending.length === 0) this.pendingInvites.delete(peerPublicKeyHex)
        diag('Invite stored in blind mailbox for ' + peerPublicKeyHex.substring(0, 12))
        return true
      }
    } catch (e) {
      diag('Invite mailbox put failed for ' + peerPublicKeyHex.substring(0, 12) +
        ': ' + (e.message || e))
    }

    // Join the peer's personal topic to establish connection
    const peerTopic = derivePersonalTopic(b4a.from(peerPublicKeyHex, 'hex'))
    const topicHex = b4a.toString(peerTopic, 'hex')

    if (!this._inviteTopics.has(topicHex)) {
      // Older builds incorrectly announced the inviter on the recipient's
      // personal topic. Those signed DHT records can outlive a force-stopped
      // process, so remove our own stale record before the client-only lookup.
      // The unannounce is signed by our key and cannot remove the recipient.
      if (this.keyPair && this.swarm.dht && typeof this.swarm.dht.unannounce === 'function') {
        try {
          await this.swarm.dht.unannounce(peerTopic, this.keyPair)
          diag('Cleared stale inviter announcement from personal topic')
        } catch (e) {
          diag('Failed to clear stale inviter announcement: ' + (e.message || e))
        }
      }

      // The recipient owns and announces its personal topic. Inviters must
      // only look it up; announcing ourselves here can crowd the recipient
      // out of DHT results and makes other inviters connect to each other.
      const discovery = this.swarm.join(peerTopic, { client: true, server: false })
      this._inviteTopics.set(topicHex, discovery)
      await discovery.flushed()
      diag('Joined personal topic for invite delivery')
    }

    return false // invite is pending
  }

  /**
   * Run one mailbox operation over the first transport that answers.
   *
   * HTTPS goes first when configured: it is the transport that survives the
   * networks this whole mechanism exists for, the ones that drop HyperDHT's
   * UDP outright. The Protomux mailbox on each configured blind peer is the
   * fallback for when the HTTPS endpoint is unreachable or unset.
   * @param {Function} operation (request) => Promise<any>
   * @returns {Promise<{ok: boolean, value?: any}>} ok is false when no transport answered
   */
  _configuredMailboxStores () {
    if (this._mailboxStores) return this._mailboxStores
    const stores = [...new Set(this._blindPeerKeys || [])].map(key => ({ key }))
    if (config.INVITE_MAILBOX_URL) {
      // A URL alone does not prove which Noise identity owns its store. Only
      // collapse transports when the deployment explicitly supplies that link.
      const same = stores.find(store => store.key === config.INVITE_MAILBOX_PEER_KEY)
      if (same) same.url = config.INVITE_MAILBOX_URL
      else stores.unshift({ url: config.INVITE_MAILBOX_URL })
    }
    return stores
  }

  async _throughMailboxStore (store, operation) {
    const keyPair = this.keyPair
    const generation = this._generation
    if (!keyPair || this._stopping) return { ok: false }
    const guarded = request => operation(async (method, body) => {
      if (this._stopping || generation !== this._generation) throw new Error('mailbox stopped')
      const result = await request(method, body)
      if (this._stopping || generation !== this._generation) throw new Error('mailbox stopped')
      return result
    })
    if (store.url) {
      try {
        const value = await this._mailboxTransports.throughHttps(store.url, keyPair, guarded, {
          postJson: (url, body) => this._platformPostJson(url, body)
        })
        return { ok: true, value }
      } catch (e) { diag('HTTPS invite mailbox unavailable: ' + e.message) }
    }
    if (store.key && this.swarm && this.swarm.dht && !this._stopping) {
      try {
        const value = await this._mailboxTransports.throughDht(
          this.swarm.dht, keyPair, store.key, store.address || config.BLIND_PEER_ADDRESS, guarded)
        return { ok: true, value }
      } catch (e) { diag('DHT invite mailbox unavailable: ' + e.message) }
    }
    return { ok: false }
  }

  // Deposits need acceptance from one store. Reads must visit every store.
  async _throughMailbox (operation) {
    for (const store of this._configuredMailboxStores()) {
      const result = await this._throughMailboxStore(store, operation)
      if (result.ok) return result
    }
    return { ok: false }
  }

  async _putInviteMailbox (recipientKeyHex, inviteData) {
    const { ok } = await this._throughMailbox(
      request => putInvite(request, this.keyPair, recipientKeyHex, inviteData)
    )
    return ok
  }

  async _drainInviteMailboxes () {
    if (this._mailboxDrainInFlight) return await this._mailboxDrainInFlight

    const operation = this._drainInviteMailboxesOnce()
    this._mailboxDrainInFlight = operation
    try {
      return await operation
    } finally {
      if (this._mailboxDrainInFlight === operation) this._mailboxDrainInFlight = null
    }
  }

  async _drainInviteMailboxesOnce () {
    let delivered = 0
    const seenInvites = new Map()
    const applicationChains = new Map()
    const keyPair = this.keyPair
    if (!keyPair) return 0

    // Runs before the entry is acknowledged, and its answer decides whether the
    // server-side copy may be deleted. Returning true for a rejected invite is
    // deliberate: it is finished with, and retrying it would starve real ones.
    const deliver = async (entry) => {
      if (this._stopping) return false
      const invite = entry.invite
      const sender = entry.senderKeyHex
      if (!invite || (invite.type !== 'direct_invite' && invite.type !== 'group_invite')) {
        diag('Discarded unsupported blind mailbox message')
        return true
      }
      const claimedSender = String(invite.senderKey || '').toLowerCase().replace(/^0x/, '')
      if (claimedSender !== sender) {
        diag('Discarded blind mailbox invite with mismatched sender')
        return true
      }
      // Multiple sends before the recipient polls can enqueue the same bootstrap
      // invite more than once. Process it once per drain so we do not create
      // duplicate cores, joins, or core-key replies.
      const inviteKey = sender + '\n' + JSON.stringify(invite)
      if (seenInvites.has(inviteKey)) return seenInvites.get(inviteKey)
      const scope = invite.groupId || sender
      const previous = applicationChains.get(scope) || Promise.resolve()
      const applying = previous.catch(() => {}).then(() => {
        if (this._stopping) return false
        return this._deliverInvite(invite, sender)
      })
      applicationChains.set(scope, applying)
      seenInvites.set(inviteKey, applying)
      try {
        const finished = await applying
        if (finished) delivered++
        else seenInvites.delete(inviteKey)
        return finished
      } catch (error) {
        seenInvites.delete(inviteKey)
        return false
      }
    }

    await runBounded(this._configuredMailboxStores(), 3, store =>
      this._throughMailboxStore(store, request => drainInvites(request, keyPair, deliver)))
    if (delivered > 0) diag('Invite mailbox delivered ' + delivered + ' invite(s)')
    return delivered
  }

  /**
   * Hand an opaque JSON POST to the host app's HTTPS stack and wait for it to
   * hand back the reply. Resolves with the parsed response body.
   * @param {string} url
   * @param {Object} body
   * @returns {Promise<Object>}
   */
  _platformPostJson (url, body) {
    const requestId = Date.now().toString(36) + '-' +
      (++this._platformHttpRequestSequence).toString(36)
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this._platformHttpRequests.delete(requestId)
        reject(new Error('platform HTTPS request timed out'))
      }, PLATFORM_HTTP_TIMEOUT_MS)
      this._platformHttpRequests.set(requestId, { resolve, reject, timeout })
      const dispatched = this._platformHttp({
        requestId,
        url,
        body,
        timeoutMs: PLATFORM_HTTP_TIMEOUT_MS
      })
      // Fail now rather than after the timeout: with no host to answer, waiting
      // only delays the fallback to the DHT mailbox.
      if (dispatched === false) {
        clearTimeout(timeout)
        this._platformHttpRequests.delete(requestId)
        reject(new Error('platform HTTPS transport unavailable'))
      }
    })
  }

  completePlatformHttpRequest (payload) {
    const pending = this._platformHttpRequests.get(payload.requestId)
    if (!pending) return false
    this._platformHttpRequests.delete(payload.requestId)
    clearTimeout(pending.timeout)
    if (payload.success) pending.resolve(payload.response || {})
    else pending.reject(new Error(payload.error || 'platform HTTPS request failed'))
    return true
  }

  /**
   * Leave a conversation and stop discovery for all its topics
   * @param {string} conversationId - Conversation identifier
   */
  async leaveConversation(conversationId) {
    // Check group conversations
    const groupConv = this.groupConversations.get(conversationId)
    if (groupConv) {
      try {
        if (groupConv.discovery) await groupConv.discovery.destroy()
        for (const [, sockets] of groupConv.connections) {
          for (const socket of sockets) socket.destroy()
        }
        this.groupTopicToConversation.delete(groupConv.groupTopicHex)
      } catch (error) {
        diag('Error leaving group conversation:', error)
      }
      this.groupConversations.delete(conversationId)
      return
    }

    // Direct conversation
    const conv = this.conversations.get(conversationId)
    if (!conv) return

    try {
      for (const [peerKeyHex, peerEntry] of conv.peers) {
        if (peerEntry.discovery) {
          await peerEntry.discovery.destroy()
        }
        for (const socket of peerEntry.connections) {
          socket.destroy()
        }
        this.peerToConversation.delete(peerKeyHex)
      }
    } catch (error) {
      diag('Error leaving conversation:', error)
    }

    this.conversations.delete(conversationId)
  }

  /** Remove a departed peer from the live group routing cache. */
  removeGroupParticipant (conversationId, peerKeyHex) {
    const group = this.groupConversations.get(conversationId)
    if (!group) return false
    const peer = (peerKeyHex || '').toLowerCase()
    group.participantKeys = group.participantKeys.filter(key => (key || '').toLowerCase() !== peer)
    group.connections.delete(peerKeyHex)
    for (const key of group.connections.keys()) {
      if ((key || '').toLowerCase() === peer) group.connections.delete(key)
    }
    return true
  }

  /**
   * Suspend P2P manager (for mobile background)
   */
  suspend() {
    if (this.blindMirror && this.blindMirror.cancelNotifications) this.blindMirror.cancelNotifications()
    this._stopHeartbeat()
    if (this.swarm && typeof this.swarm.suspend === 'function') {
      this.swarm.suspend()
      diag('Suspended')
    }
  }

  /**
   * Resume P2P manager from mobile background.
   * Re-starts heartbeat and immediately prunes dead sockets so the next
   * send doesn't silently fail on connections that died while suspended.
   */
  resume() {
    if (!this.swarm) return

    if (this.swarm && typeof this.swarm.resume === 'function') {
      this.swarm.resume()
      diag('Resumed')
    }

    // Re-start heartbeat
    this._startHeartbeat()

    // Immediately prune dead sockets — connections may have died during suspend
    this._runHeartbeat()

    // Re-flush topic announcements by triggering a health check
    this.healthMonitor.checkHealth().catch(e => {
      diag('Post-resume health check failed: ' + e.message)
    })

    this._drainInviteMailboxes().catch(e => {
      diag('Post-resume invite mailbox drain failed: ' + (e.message || e))
    })
  }

  /**
   * Stop the P2P manager and cleanup all connections
   * Includes Phase 2: DHT health monitoring cleanup
   */
  async stop () {
    if (this._stopPromise) return this._stopPromise
    const operation = this._stop()
    this._stopPromise = operation
    try { return await operation } finally { this._stopPromise = null }
  }

  async _stop () {
    this._stopping = true
    this._generation++
    this._tasks.closed = true
    this._stopHeartbeat()
    this.healthMonitor.stopMonitoring()
    clearInterval(this._swarmDumpInterval)
    clearTimeout(this._nodeCacheSaveTimer)
    this._swarmDumpInterval = null
    this._nodeCacheSaveTimer = null
    if (this.blindMirror && this.blindMirror.cancelNotifications) this.blindMirror.cancelNotifications()
    for (const pending of this._platformHttpRequests.values()) {
      clearTimeout(pending.timeout)
      pending.reject(new Error('P2P manager stopped'))
    }
    this._platformHttpRequests.clear()
    this._saveDhtNodeCache()
    const swarm = this.swarm
    this.swarm = null
    try {
      if (swarm) await swarm.destroy()
    } finally {
      await this._tasks.drain()
      this.healthMonitor.stopMonitoring()
      this.healthMonitor.setSwarm(null)
      for (const map of [this.conversations, this.peerToConversation,
        this.groupConversations, this.groupTopicToConversation, this.allPeerConnections,
        this.pendingInvites, this.pendingMessages, this._mailboxBootstrapInFlight,
        this._mailboxBootstrapAt, this._peerMessageCounts, this._peerMediaRequestCounts,
        this._inviteTopics, this._seenMessageIds]) map.clear()
      this._mailboxDrainInFlight = null
      this.framedSockets = new WeakMap()
      this.personalDiscovery = null
      this.keyPair = null
      this.isOnline = false
      this.peerCount = 0
      this.presenceVisible = true
      this.emit('status', { online: false, peerCount: 0 })
    }
  }

  // Network stop preserves application handlers. Only final disposal removes
  // them; restart never installs another copy of an existing handler.
  async dispose () {
    await this.stop()
    this.removeAllListeners()
    this.healthMonitor.removeAllListeners()
  }

  /**
   * Get current DHT health status from monitor
   * @returns {Object} Current health status
   */
  getDHTHealthStatus() {
    return this.healthMonitor.getStatus()
  }

  /**
   * Return a snapshot of connection state for diagnostics.
   */
  getConnectionDiagnostics() {
    const directConvs = {}
    for (const [convId, conv] of this.conversations) {
      const peers = {}
      for (const [peerKey, entry] of conv.peers) {
        peers[peerKey.substring(0, 12)] = entry.connections.length
      }
      directConvs[convId.substring(0, 12)] = peers
    }

    const groupConvs = {}
    for (const [convId, entry] of this.groupConversations) {
      const peers = {}
      for (const [peerKey, sockets] of entry.connections) {
        peers[peerKey.substring(0, 12)] = sockets.length
      }
      groupConvs[convId.substring(0, 12)] = peers
    }

    // Undelivered invites queued for peers that aren't connected yet (cleared on
    // delivery). Distinct from _inviteTopics, which tracks advertised invite
    // topics and only clears on stop.
    let pendingInviteCount = 0
    for (const [, queue] of this.pendingInvites) pendingInviteCount += queue.length

    return {
      isOnline: this.isOnline,
      peerCount: this.peerCount,
      globalConnections: this.allPeerConnections.size,
      directConversations: directConvs,
      groupConversations: groupConvs,
      pendingQueues: this.pendingMessages.size,
      pendingInvites: pendingInviteCount,
      health: this.healthMonitor.getStatus(),
      dht: this._dhtDiagnostics()
    }
  }

  /**
   * Snapshot of the live HyperDHT node: routing-table size (the real DHT-health
   * signal), NAT/firewall classification, and bootstrap state. All read from
   * the existing swarm.dht handle; no new sockets. Every field is guarded
   * because these are version-dependent internals of hyperdht.
   */
  _dhtDiagnostics () {
    const dht = this.swarm && this.swarm.dht
    if (!dht) return { available: false }
    let rtNodes = 0
    try {
      if (dht.table && typeof dht.table.size === 'number') rtNodes = dht.table.size
      else if (dht.nodes && typeof dht.nodes.length === 'number') rtNodes = dht.nodes.length
    } catch (e) { /* internals shifted; leave 0 */ }
    const relaying = dht.stats && dht.stats.relaying
    return {
      available: true,
      bootstrapped: !!dht.bootstrapped,
      firewalled: typeof dht.firewalled === 'boolean' ? dht.firewalled : null,
      randomized: typeof dht.randomized === 'boolean' ? dht.randomized : null,
      rtNodes,
      relayAttempts: relaying && Number.isSafeInteger(relaying.attempts) ? relaying.attempts : 0,
      relaySuccesses: relaying && Number.isSafeInteger(relaying.successes) ? relaying.successes : 0,
      relayAborts: relaying && Number.isSafeInteger(relaying.aborts) ? relaying.aborts : 0
    }
  }
}

// Track asynchronous network operations so stop() can quiesce them before
// identity storage is changed. Internal calls share the same admission barrier.
for (const name of [
  'checkBootstrapHealth',
  '_ensureLocalCore',
  'openRemoteCore',
  'registerPushEndpoint',
  'joinPersonalTopic',
  'joinConversation',
  'joinGroupConversation',
  'sendToConversationDurably',
  '_persistOutgoing',
  'sendDeliveryReceipt',
  'sendInvite',
  '_throughMailboxStore',
  '_throughMailbox',
  '_putInviteMailbox',
  '_drainInviteMailboxes',
  '_drainInviteMailboxesOnce',
  'leaveConversation'
]) {
  const method = P2PManager.prototype[name]
  P2PManager.prototype[name] = function (...args) {
    return this.runTask(() => method.apply(this, args))
  }
}

module.exports = { P2PManager }
