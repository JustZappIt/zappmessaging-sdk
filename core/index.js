/**
 * ZappMessaging Core - Main entry point
 * 
 * This is the core JavaScript implementation that runs in the Bare runtime
 * and communicates with the Swift/Kotlin wrapper via IPC.
 */

const { createDiagnosticLogger } = require('./lib/diagnostics')
const diag = createDiagnosticLogger('CORE')

// Catch uncaught errors to prevent SIGABRT - MUST be first
if (typeof Bare !== 'undefined') {
  Bare.on('uncaughtException', (err) => {
    diag('Uncaught exception:', err)
  })

  Bare.on('unhandledRejection', (reason) => {
    diag('Unhandled rejection:', reason)
  })
}

diag('ZappMessaging worklet starting...')

const { createPeerReceiver, serveAuthorizedMedia, acceptAuthorizedMediaChunk } = require('./lib/peer-receiver')

const { runBounded } = require('./lib/async-pool')
const { STARTUP_JOIN_CONCURRENCY } = require('./lib/config')
const { MediaRequests } = require('./lib/media-requests')
const b4a = require('b4a')

let _coldStartBeganAt = 0
const _coldStartMilestones = new Set()

// Identifier-free phase markers for correlating the native tap timestamp with
// worklet startup. Emit each milestone once per initialize() invocation.
function coldStartMilestone (name, fields = {}) {
  if (!_coldStartBeganAt || _coldStartMilestones.has(name)) return
  _coldStartMilestones.add(name)
  const details = Object.entries(fields)
    .map(([key, value]) => key + '=' + value)
    .join(' ')
  diag('[COLD_START] milestone=' + name +
    ' elapsed_ms=' + (Date.now() - _coldStartBeganAt) +
    (details ? ' ' + details : ''))
}

diag('=== Worklet starting ===')

// Guard each require to prevent a single module failure from killing the worklet
let Identity, ChatStore, ContactStore, MediaStore, MediaTransfer, P2PManager, IPCHandler
let HypercoreManager, BlindMirror

try {
  Identity = require('./lib/identity').Identity
  diag('Identity loaded: true')
} catch (e) {
  diag('Failed to load identity:', e)
  diag('Identity loaded: false - ' + e.message)
}

try {
  ChatStore = require('./lib/chat-store').ChatStore
  diag('ChatStore loaded: true')
} catch (e) {
  diag('Failed to load chat-store:', e)
  diag('ChatStore loaded: false - ' + e.message)
}

try {
  ContactStore = require('./lib/contact-store').ContactStore
  diag('ContactStore loaded: true')
} catch (e) {
  diag('Failed to load contact-store:', e)
  diag('ContactStore loaded: false - ' + e.message)
}

try {
  MediaStore = require('./lib/media-store').MediaStore
  diag('MediaStore loaded: true')
} catch (e) {
  diag('Failed to load media-store:', e)
  diag('MediaStore loaded: false - ' + e.message)
}

try {
  MediaTransfer = require('./lib/media-transfer').MediaTransfer
  diag('MediaTransfer loaded: true')
} catch (e) {
  diag('Failed to load media-transfer:', e)
  diag('MediaTransfer loaded: false - ' + e.message)
}

try {
  P2PManager = require('./lib/p2p-manager').P2PManager
  diag('P2PManager loaded: true')
} catch (e) {
  diag('Failed to load p2p-manager:', e)
  diag('P2PManager loaded: false - ' + e.message)
}

try {
  IPCHandler = require('./lib/ipc-handler').IPCHandler
  diag('IPCHandler loaded: true')
} catch (e) {
  diag('Failed to load ipc-handler:', e)
  diag('IPCHandler loaded: false - ' + e.message)
}

// Optional: blind-peer offline relay support. If these fail to load (e.g.
// blind-peering or corestore missing), the worklet still runs in direct-only
// mode and the user only loses offline delivery.
try {
  HypercoreManager = require('./lib/hypercore-manager').HypercoreManager
  diag('HypercoreManager loaded: true')
} catch (e) {
  diag('Failed to load hypercore-manager:', e)
  diag('HypercoreManager loaded: false - ' + e.message)
}

try {
  BlindMirror = require('./lib/blind-mirror').BlindMirror
  diag('BlindMirror loaded: true')
} catch (e) {
  diag('Failed to load blind-mirror:', e)
  diag('BlindMirror loaded: false - ' + e.message)
}

if (!P2PManager) {
  const EventEmitter = require('bare-events')
  P2PManager = class StubP2PManager extends EventEmitter {
    constructor() { super(); this.isOnline = false; this.peerCount = 0 }
    async start() { diag('StubP2PManager: P2P disabled (native addon load failed)'); diag('StubP2PManager: P2P disabled') }
    async joinConversation() { return false }
    async joinGroupConversation() { return false }
    async joinPersonalTopic() {}
    setPeerRecordSink() {}
    sendToConversation() { return false }
    getConversationFramedSockets() { return [] }
    requestMedia() { return false }
    async ensureLocalMediaCore() { return null }
    async openRemoteMediaCore() { return null }
    async sendInvite() { return false }
    async leaveConversation() {}
    suspend() {}
    resume() {}
    async stop() {}
  }
  diag('Using stub P2PManager - P2P networking is disabled')
  diag('Using stub P2PManager - P2P networking is disabled')
}

if (!Identity || !ChatStore || !ContactStore || !IPCHandler) {
  diag('Critical modules failed to load, worklet running in degraded mode')
  diag('CRITICAL: Core modules failed to load - Identity:' + !!Identity + ' ChatStore:' + !!ChatStore + ' ContactStore:' + !!ContactStore + ' IPCHandler:' + !!IPCHandler)
}

// Global instances
let identity = null
let chatStore = null
let contactStore = null
let mediaStore = null
let mediaTransfer = null
let p2pManager = null
let ipcHandler = null
let hypercoreManager = null
let blindMirror = null
let blindMirrorInitPromise = null

// Bytes behind inbound media messages are pulled from the author over a live
// socket and, when the record says where they sit in the author's media
// core, from the blind peer. Dependencies are read lazily because core
// restore can start before IPC/P2P and the mirror is rebuilt on swarm restart.
const mediaRequests = new MediaRequests(() => ({
  chatStore, mediaStore, mediaTransfer, p2pManager, hypercoreManager, blindMirror, ipcHandler
}))

const receivePeerRecord = createPeerReceiver(() => ({
  chatStore, p2pManager, ipcHandler, identity, processReceipt,
  onMessage: (conversationId, stored, record, peer) => {
    if (stored) coldStartMilestone('first_authentic_message_persisted')
    mediaRequests.request(conversationId, record, peer)
    if (stored && ipcHandler) ipcHandler.pushEvent('message.received', { conversationId, message: stored })
  }
}))

async function processReceipt (conversationId, receipt) {
  if (!receipt || !receipt.upTo) return
  if (!chatStore) return
  if (receipt.kind === 'delivered') {
    if (!identity || receipt.to !== identity.publicKeyHex) return
    const changedIds = chatStore.markDeliveredUpTo(conversationId, receipt.upTo)
    if (p2pManager) p2pManager.confirmMessagesDelivered(conversationId, changedIds)
    if (ipcHandler) {
      for (const messageId of changedIds) {
        ipcHandler.pushEvent('message.status', { messageId, conversationId, status: 'delivered' })
      }
    }
    return
  }
  if (receipt.kind !== 'read') return
  if (ipcHandler && ipcHandler.readReceiptsEnabled === false) return
  // DMs only: never let a receipt naming a group conversation mark group
  // messages read (a single member can't speak for the whole room).
  const conversation = await chatStore.getConversation(conversationId)
  if (!conversation || conversation.type !== 'direct') return
  const changedIds = chatStore.markReadUpTo(conversationId, receipt.upTo)
  if (ipcHandler) {
    for (const messageId of changedIds) {
      ipcHandler.pushEvent('message.status', { messageId, conversationId, status: 'read' })
    }
  }
}

/**
 * Idempotently bring up the blind-peer mirror once a swarm + corestore exist.
 * Safe to call after every successful p2pManager.start() — including identity
 * create / restore-from-seed flows which tear down and restart the swarm.
 */
async function ensureBlindMirror () {
  if (!BlindMirror || !hypercoreManager || !p2pManager || !p2pManager.swarm) return

  // DHT readiness and the normal post-start fallback can race each other.
  // Share one initialization so only one mirror and one set of replication
  // sessions can be created for a swarm.
  if (blindMirrorInitPromise) return blindMirrorInitPromise
  blindMirrorInitPromise = initializeBlindMirror()
  try {
    return await blindMirrorInitPromise
  } finally {
    blindMirrorInitPromise = null
  }
}

async function initializeBlindMirror () {
  // If the mirror already exists but its swarm reference is stale (a previous
  // p2pManager.stop() destroyed it), tear it down so we can rebuild fresh.
  if (blindMirror && blindMirror.swarm !== p2pManager.swarm) {
    // In-flight image fetches are registered with the old relay session; they
    // start over once the new mirror is up.
    mediaRequests.cancelFetches()
    try { await blindMirror.close() } catch (e) { diag('blindMirror close (rebuild): ' + e.message) }
    blindMirror = null
  }

  let created = false
  if (!blindMirror) {
    try {
      blindMirror = new BlindMirror(p2pManager.swarm, hypercoreManager.store)
      await blindMirror.ready()
      created = true
      p2pManager.blindMirror = blindMirror
      if (ipcHandler) ipcHandler.blindMirror = blindMirror
      diag('BlindMirror initialized with ' +
        (blindMirror.getDebugInfo().blindPeerCount || 0) + ' peer key(s)')
    } catch (err) {
      diag('BlindMirror init failed: ' + (err.message || err))
      blindMirror = null
      return
    }
  }

  // Re-register every core we know about (idempotent — addLocalCore /
  // addRemoteCore short-circuit if already registered).
  for (const [convId, core] of hypercoreManager.localCores) {
    blindMirror.addLocalCore(convId, core, hypercoreManager.getLocalCoreReferrer(convId))
  }
  for (const [convId, core] of hypercoreManager.localMediaCores) {
    blindMirror.addLocalMediaCore(convId, core, hypercoreManager.getLocalCoreReferrer(convId))
  }
  for (const [convId, peerMap] of hypercoreManager.remoteCores) {
    for (const [peerKey, core] of peerMap) {
      blindMirror.addRemoteCore(convId, peerKey, core, hypercoreManager.getRemoteCoreReferrer(convId, peerKey))
    }
  }

  // Restore any previously-known remote cores from disk on first init so we
  // resume pulling them via the blind peer immediately on launch.
  try {
    await hypercoreManager.loadCoreKeyIndex()
    for (const [convId, peerMap] of hypercoreManager.remoteCores) {
      for (const [peerKey, core] of peerMap) {
        blindMirror.addRemoteCore(convId, peerKey, core, hypercoreManager.getRemoteCoreReferrer(convId, peerKey))
      }
    }
  } catch (err) {
    diag('Failed to restore core key index: ' + (err.message || err))
  }

  // Images sent while we were offline are waiting on the relay.
  if (created && blindMirror.enabled) mediaRequests.requestMissingEverywhere()

  coldStartMilestone('blind_mirror_ready', {
    local_cores: hypercoreManager.localCores.size,
    remote_conversations: hypercoreManager.remoteCores.size
  })
}

/**
 * Initialize the ZappMessaging core
 */
async function initialize() {
  try {
    _coldStartBeganAt = Date.now()
    _coldStartMilestones.clear()
    coldStartMilestone('initialize_started')
    diag('Initializing ZappMessaging Core...')
    diag('Initializing ZappMessaging Core...')

    // Initialize storage and identity
    if (Identity) {
      identity = new Identity()
      await identity.load()
      diag('Identity loaded: ' + !!identity.keyPair)
      coldStartMilestone('identity_loaded', { has_identity: !!identity.keyPair })
    }

    // Initialize stores
    if (ChatStore) chatStore = new ChatStore()
    if (ContactStore) contactStore = new ContactStore()
    if (MediaStore) mediaStore = new MediaStore()
    if (MediaTransfer && mediaStore) mediaTransfer = new MediaTransfer(mediaStore)

    // Initialize Hypercore Manager (optional, for blind-peer offline relay)
    if (HypercoreManager) {
      try {
        hypercoreManager = new HypercoreManager()
        // Key derivation and remote-core authorization need the identity and
        // conversation records. Wire before initialize()/loadCoreKeyIndex()
        // so every core open — including startup restore — goes through them.
        hypercoreManager.setKeyContext({
          getIdentityKeyPair: () => (identity && identity.keyPair) || null,
          getConversation: (id) => (chatStore && chatStore.conversations.get(id)) || null
        })
        await hypercoreManager.initialize()
        diag('HypercoreManager initialized')
        coldStartMilestone('hypercore_initialized')

        // Ingest blocks arriving via blind-peer replication into the chat
        // store, mirroring the live-P2P message handler below. This is an
        // AWAITABLE sink (not a fire-and-forget event): the hypercore manager
        // advances its persisted per-core cursor only after this resolves, so
        // the block is durably stored before it is marked processed. A throw
        // means "not ingested" — the cursor stays and bounded retry begins
        // immediately (addMessage dedups any replay by id).
        hypercoreManager.setRemoteMessageSink((conversationId, peer, message) =>
          receivePeerRecord(conversationId, peer, message, { replicated: true }))

        hypercoreManager.setRemoteDrainCompleteSink(async (conversationId, peerKeyHex, result) => {
          const senderId = result.senderId || peerKeyHex
          if (p2pManager && typeof p2pManager.sendDeliveryReceipt === 'function') {
            await p2pManager.sendDeliveryReceipt(
              conversationId,
              result.messageId,
              senderId,
              { requireDurable: true }
            )
          } else {
            // The P2P module can be unavailable while Corestore still works.
            // Preserve the durability boundary in degraded mode; the receipt
            // will replicate when blind/direct networking recovers.
            await hypercoreManager.appendMessage(conversationId, {
              type: '__receipt',
              kind: 'delivered',
              conversationId,
              upTo: result.messageId,
              to: senderId
            })
          }
          coldStartMilestone('first_delivery_receipt_queued')
        })

        // Reopen persisted remote cores while the DHT bootstraps. Watchers are
        // then installed before either direct or blind replication can deliver
        // a queued block. ensureBlindMirror() awaits this same idempotent load
        // before registering the restored cores with the blind peer.
        hypercoreManager.loadCoreKeyIndex()
          .then(() => coldStartMilestone('core_index_restored', {
            remote_conversations: hypercoreManager.remoteCores.size
          }))
          .catch((err) => {
            diag('Early core key restore failed: ' + (err.message || err))
          })
      } catch (err) {
        diag('HypercoreManager init failed: ' + (err.message || err))
        hypercoreManager = null
      }
    }

    // Initialize P2P manager with hypercore deps so it can mirror sends and
    // exchange core keys. Both deps are optional; P2P works without them.
    // isParticipant backs the wire-input gates (__core_keys announcements,
    // wire-supplied conversationIds) with the persisted conversation records.
    p2pManager = new P2PManager({
      hypercoreManager,
      // P2PManager builds bootstrap invites itself and holds no identity, so it
      // reads the current display name through here. Without it a mailbox
      // bootstrap would introduce us by a truncated key.
      displayName: () => identity && identity.displayName,
      // A mailbox invite is deleted server-side only once it has been applied,
      // so this is awaited rather than emitted and forgotten.
      deliverInvite: async (invite, senderKeyHex) => {
        if (!ipcHandler) return false
        return await ipcHandler.deliverMailboxInvite(invite, senderKeyHex)
      },
      // The worklet keeps the mailbox cryptography and hands the host only
      // already-opaque JSON. False means there is nobody to hand it to yet.
      platformHttp: (request) => {
        if (!ipcHandler) return false
        ipcHandler.pushEvent('platform.http_request', request)
        return true
      },
      resolveGroupTopic: topic => chatStore && chatStore.conversationForGroupTopic(topic),
      mayServeMedia: (hash, peer) => !!chatStore && b4a.isBuffer(hash) && hash.length === 32 &&
        chatStore.canServeMedia(b4a.toString(hash, 'hex'), peer),
      getConversation: id => chatStore && !chatStore.hasLeftConversation(id) && chatStore.conversations.get(id),
      isParticipant: (id, peer) => !!chatStore && chatStore.isPeerAuthorized(id, peer)
    })

    // Begin blind-mirror bring-up as soon as the swarm/DHT object exists — in
    // parallel with DHT bootstrap. dht.connect() self-seeds from bootstrap nodes
    // and blind-peering retries on its own backoff, so dialing the blind peer
    // does not need to wait for full bootstrap. This overlaps the ~2s VPS
    // connect with the ~1.5-3.5s bootstrap on cold start. ensureBlindMirror is
    // idempotent (shared init promise), so the dht_ready + post-start paths
    // below are harmless fallbacks.
    p2pManager.on('swarm_created', () => {
      coldStartMilestone('swarm_created')
      ensureBlindMirror().catch((err) => {
        diag('Early (swarm_created) BlindMirror init failed: ' + (err.message || err))
      })
    })

    // Blind replication only needs a ready DHT and Corestore. Start it at that
    // point instead of waiting for the personal invite topic's full DHT
    // announcement. The post-start await below remains a fallback and shares
    // the same initialization promise.
    p2pManager.on('dht_ready', () => {
      coldStartMilestone('dht_ready')
      ensureBlindMirror().catch((err) => {
        diag('Early BlindMirror init failed: ' + (err.message || err))
      })
    })

    // Initialize IPC handler BEFORE starting P2P so it can handle
    // requests from the native side immediately (P2P start can take 8-10s
    // on emulators due to DHT bootstrap).
    if (IPCHandler && identity && chatStore && contactStore) {
      ipcHandler = new IPCHandler({
        identity,
        chatStore,
        contactStore,
        p2pManager,
        mediaStore,
        mediaTransfer,
        // blindMirror is set on the handler after init below
        hypercoreManager,
        ensureBlindMirror
      })
      diag('IPC handler initialized')
    } else {
      diag('Cannot start IPC handler - missing dependencies')
      diag('Cannot start IPC handler - missing dependencies')
    }

    // Forward P2P connection status changes to the UI
    p2pManager.on('status', (status) => {
      diag('P2P status changed: online=' + status.online + ' peers=' + status.peerCount)
      if (ipcHandler) {
        ipcHandler.pushEvent('connection.status', status)
      }
    })

    // Forward Hypercore persist failures to the UI so it can surface a warning
    p2pManager.on('message_persist_failed', (detail) => {
      diag('message_persist_failed conv=' + (detail.conversationId || '').substring(0, 12) +
        ' msg=' + (detail.messageId || '?') + ' err=' + detail.error)
      if (ipcHandler) {
        ipcHandler.pushEvent('message.persist_failed', detail)
      }
    })

    p2pManager.on('push_notification_failed', (detail) => {
      diag('push_notification_failed conv=' + (detail.conversationId || '').substring(0, 12) +
        ' msg=' + (detail.messageId || '?') + ' err=' + detail.error)
      if (ipcHandler) {
        ipcHandler.pushEvent('push.notification_failed', detail)
      }
    })

    if (hypercoreManager) {
      hypercoreManager.on('push-topics-changed', () => {
        if (ipcHandler) ipcHandler.pushEvent('push.topics_changed', { version: 1 })
      })
    }

    // Receipts use the live socket when available and Hypercore otherwise.
    p2pManager.on('receipt', (conversationId, receipt) => {
      processReceipt(conversationId, receipt).catch((err) => {
        diag('receipt handler failed: ' + (err.message || err))
      })
    })

    // Forward DHT health warnings to the UI
    p2pManager.on('dht_warning', (data) => {
      diag('DHT warning: ' + data.reason + ' — ' + data.message)
      if (ipcHandler) {
        ipcHandler.pushEvent('connection.dht_health', {
          status: data.reason === 'health_critical' ? 'critical' : 'degraded',
          message: data.message
        })
      }
    })

    p2pManager.on('dht_recovered', (data) => {
      diag('DHT recovered from: ' + data.previous)
      if (ipcHandler) {
        ipcHandler.pushEvent('connection.dht_health', {
          status: 'healthy',
          message: 'DHT recovered'
        })
      }
    })

    // Forward per-conversation peer online/offline events to the UI
    p2pManager.on('peer_online', (conversationId, peerId) => {
      if (ipcHandler) {
        ipcHandler.pushEvent('connection.peer_status', {
          conversationId,
          peerId: peerId.substring(0, 12),
          status: 'online'
        })
      }
      // A peer just became reachable — pull any media we're still missing for
      // this conversation (e.g. sent while we were offline, the doorbell case).
      mediaRequests.requestMissing(conversationId, peerId)
    })

    p2pManager.on('peer_offline', (conversationId, peerId) => {
      if (ipcHandler) {
        ipcHandler.pushEvent('connection.peer_status', {
          conversationId,
          peerId: peerId.substring(0, 12),
          status: 'offline'
        })
      }
    })

    p2pManager.setPeerRecordSink(receivePeerRecord)

    // Set up media transfer event handlers
    if (mediaTransfer) {
      // Emitted only after hash verification.
      mediaTransfer.on('complete', (hashHex, fullData) => mediaRequests.complete(hashHex, fullData))

      mediaTransfer.on('progress', (hashHex, progress) => {
        if (ipcHandler) {
          ipcHandler.pushEvent('media.transfer_progress', {
            mediaId: hashHex,
            progress
          })
        }
      })

      // A partial download stalled (peer vanished mid-stream). The buffers were
      // already evicted; re-request from the author so it can resume from zero.
      mediaTransfer.on('timeout', (hashHex) => mediaRequests.retry(hashHex))
      mediaTransfer.on('error', (hashHex, err) => {
        diag('Rejected corrupt media ' + String(hashHex).substring(0, 12) + ': ' +
          (err && err.message ? err.message : err))
        mediaRequests.retry(hashHex)
      })
    }

    // Handle media requests from peers
    p2pManager.on('media_request', async (hashBuf, peerId, framedSocket) => {
      try {
        await serveAuthorizedMedia(chatStore, mediaTransfer, hashBuf, peerId, framedSocket)
      } catch (err) {
        diag('Failed to handle media request:', err)
        diag('Failed to handle media request: ' + (err.message || err))
      }
    })

    // Handle media chunks from peers
    p2pManager.on('media_chunk', (hashBuf, chunkIndex, totalChunks, chunkData, peerId) => {
      try {
        acceptAuthorizedMediaChunk(chatStore, mediaTransfer, mediaRequests,
          hashBuf, chunkIndex, totalChunks, chunkData, peerId)
      } catch (err) {
        diag('Failed to handle media chunk:', err)
        diag('Failed to handle media chunk: ' + (err.message || err))
      }
    })

    // Start only after every receive-side handler is installed. A peer can
    // connect and flush queued frames while start() is still waiting for the
    // personal topic announcement; EventEmitter does not replay events that
    // were emitted before a listener existed.
    if (identity && identity.keyPair) {
      diag('Starting P2P manager...')
      await p2pManager.start(identity.keyPair)
      coldStartMilestone('p2p_start_returned')
      diag('P2P manager started OK, isOnline=' + p2pManager.isOnline)
      diag('P2P Manager started with existing identity')
      await ensureBlindMirror()
    } else {
      diag('No identity found, waiting for creation')
    }

    // Auto-reconnect all saved conversations so messages arrive
    // even before the user navigates into each chat.
    // listConversations() already excludes left conversations.
    if (chatStore && identity && identity.keyPair) {
      try {
        const conversations = await chatStore.listConversations()
        // Join conversations with bounded concurrency. This loop previously awaited each
        // joinConversation() — and therefore a full DHT announce
        // (discovery.flushed()) — before starting the next, so the Nth
        // conversation's topic join began N-1 announces late and the message
        // that woke us could be last in line. A small pool overlaps those walks
        // without flooding the DHT or Corestore on accounts with a large history.
        // listConversations() is sorted most-recent-first, so likely targets are
        // claimed by the pool first.
        const joins = []
        for (const conv of conversations) {
          if (conv.type === 'group' && conv.groupId) {
            const allKeys = [identity.publicKeyHex, ...conv.participantIds]
            joins.push({
              conversationId: conv.id,
              run: () => p2pManager.joinGroupConversation(conv.id, conv.groupId, allKeys)
            })
          } else if (conv.participantIds) {
            for (const pid of conv.participantIds) {
              if (pid !== identity.publicKeyHex) {
                joins.push({
                  conversationId: conv.id,
                  run: () => p2pManager.joinConversation(conv.id, pid)
                })
              }
            }
          }
        }
        coldStartMilestone('conversation_reconnect_started', {
          conversations: conversations.length,
          joins: joins.length,
          concurrency: STARTUP_JOIN_CONCURRENCY
        })
        await runBounded(joins, STARTUP_JOIN_CONCURRENCY, async (join) => {
          try {
            return await join.run()
          } catch (e) {
            diag('Auto-reconnect failed for ' + join.conversationId.substring(0, 12) + ': ' + (e.message || e))
            return false
          }
        })
        coldStartMilestone('conversation_reconnect_finished')
        diag('Auto-reconnected ' + conversations.length + ' conversation(s)')
      } catch (e) {
        diag('Failed to auto-reconnect conversations: ' + (e.message || e))
      }
    }

    diag('ZappMessaging Core initialized successfully')
    diag('ZappMessaging Core initialized successfully')
    coldStartMilestone('initialize_finished')
    return true
  } catch (error) {
    diag('Failed to initialize ZappMessaging Core:', error)
    diag('INIT ERROR: ' + (error.stack || error.message || error))
    // Don't throw - let the worklet continue in degraded mode
    return false
  }
}

/**
 * Shutdown the ZappMessaging core
 */
async function shutdown() {
  diag('Shutting down ZappMessaging Core...')

  mediaRequests.cancelFetches()
  if (blindMirror) {
    try { await blindMirror.close() } catch (e) { diag('blindMirror close: ' + e.message) }
  }
  if (p2pManager) {
    await p2pManager.stop()
  }
  if (hypercoreManager) {
    try { await hypercoreManager.close() } catch (e) { diag('hypercoreManager close: ' + e.message) }
  }

  diag('ZappMessaging Core shutdown complete')
}

// Handle app lifecycle events
if (typeof Bare !== 'undefined') {
  Bare.on('suspend', () => {
    diag('Worklet suspending...')
    diag('Worklet suspending...')
    // NOTE: do NOT suspend the swarm — Android fires `suspend` aggressively
    // during early startup and a suspended swarm never recovers. The blind
    // mirror does have its own connections; pause those to save battery.
    if (blindMirror) blindMirror.suspend().catch(() => {})
  })

  Bare.on('resume', () => {
    diag('Worklet resuming...')
    diag('Worklet resuming...')
    if (p2pManager) p2pManager.resume()
    if (blindMirror) blindMirror.resume().catch(() => {})
  })
}

// Auto-initialize when running in Bare runtime
if (typeof Bare !== 'undefined') {
  initialize().catch(err => {
    // Don't exit - log and continue in degraded mode
    diag('Fatal error during initialization:', err)
    diag('FATAL INIT ERROR: ' + (err.stack || err.message || err))
  })
}

diag('ZappMessaging worklet initialized')

// Export for testing and manual initialization
module.exports = {
  initialize,
  shutdown,
  // Core modules
  Identity,
  ChatStore,
  ContactStore,
  MediaStore,
  MediaTransfer,
  P2PManager,
  IPCHandler,
  // Instances (for testing)
  getInstances: () => ({
    identity,
    chatStore,
    contactStore,
    mediaStore,
    mediaTransfer,
    p2pManager,
    ipcHandler
  })
}
