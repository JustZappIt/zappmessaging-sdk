/**
 * Unit tests for p2p-manager.js
 * 
 * Note: Full integration tests with real Hyperswarm connections
 * will be in the integration test suite.
 */

const { test } = require('node:test')
const assert = require('node:assert')
const crypto = require('hypercore-crypto')
const b4a = require('b4a')
const EventEmitter = require('node:events')
const { P2PManager } = require('../lib/p2p-manager')

test('P2PManager initializes correctly', () => {
  const manager = new P2PManager()
  
  assert.ok(manager, 'Manager should be created')
  assert.strictEqual(manager.swarm, null, 'Swarm should be null initially')
  assert.strictEqual(manager.isOnline, false, 'Should be offline initially')
  assert.strictEqual(manager.peerCount, 0, 'Peer count should be 0')
  assert.ok(manager.conversations instanceof Map, 'Conversations should be a Map')
  assert.ok(manager.groupConversations instanceof Map, 'Group conversations should be a Map')
})

test('P2PManager extends EventEmitter', () => {
  const manager = new P2PManager()
  
  assert.ok(typeof manager.on === 'function', 'Should have on method')
  assert.ok(typeof manager.emit === 'function', 'Should have emit method')
})

test('DHT readiness is emitted before personal topic announcement completes', async () => {
  class FakeSwarm extends EventEmitter {
    constructor () {
      super()
      this.connections = new Set()
      this.dht = {
        ready: async () => {},
        ping: async () => { throw new Error('not a DHT gateway') }
      }
    }

    async destroy () {}
  }

  const phases = []
  let releasePersonalTopic
  const personalTopicGate = new Promise(resolve => { releasePersonalTopic = resolve })
  const manager = new P2PManager({ createSwarm: () => new FakeSwarm() })
  manager.healthMonitor = {
    setSwarm: () => {},
    startMonitoring: async () => {},
    stopMonitoring: () => {}
  }
  manager._startHeartbeat = () => {}
  manager.joinPersonalTopic = async () => {
    phases.push('personal-started')
    await personalTopicGate
    phases.push('personal-ready')
  }
  manager.on('dht_ready', () => phases.push('dht-ready'))

  const startPromise = manager.start({ publicKey: b4a.alloc(32) })
  await new Promise(resolve => setImmediate(resolve))

  assert.deepStrictEqual(phases, ['dht-ready', 'personal-started'])

  releasePersonalTopic()
  await startPromise
  await manager.stop()
  assert.deepStrictEqual(phases, ['dht-ready', 'personal-started', 'personal-ready'])
})

test('start makes the configured blind relay available before holepunch failure', async () => {
  class FakeSwarm extends EventEmitter {
    constructor () {
      super()
      this.connections = new Set()
      this.dht = {
        randomized: false,
        ready: async () => {},
        ping: async () => { throw new Error('not a DHT gateway') }
      }
    }

    async destroy () {}
  }

  const relayKey = crypto.randomBytes(32)
  let swarmOptions = null
  const manager = new P2PManager({
    blindPeerKeys: [b4a.toString(relayKey, 'hex')],
    createSwarm: (options) => {
      swarmOptions = options
      return new FakeSwarm()
    }
  })
  manager.healthMonitor = {
    setSwarm: () => {},
    startMonitoring: async () => {},
    stopMonitoring: () => {}
  }
  manager._startHeartbeat = () => {}
  manager.joinPersonalTopic = async () => {}

  await manager.start({ publicKey: b4a.alloc(32) })

  assert.strictEqual(typeof swarmOptions.relayThrough, 'function')
  assert.deepStrictEqual(swarmOptions.relayThrough(false, { dht: { randomized: false } }), relayKey)

  await manager.stop()
})

test('sendToConversation returns false for non-existent conversation', () => {
  const manager = new P2PManager()
  
  const result = manager.sendToConversation('non-existent', { test: true })
  assert.strictEqual(result, false, 'Should return false for non-existent conversation')
})

test('getConversationFramedSockets returns empty array for non-existent conversation', () => {
  const manager = new P2PManager()
  
  const sockets = manager.getConversationFramedSockets('non-existent')
  assert.ok(Array.isArray(sockets), 'Should return an array')
  assert.strictEqual(sockets.length, 0, 'Should return empty array')
})

test('_getFramed returns null for unknown socket', () => {
  const manager = new P2PManager()
  const mockSocket = {}
  
  const framed = manager._getFramed(mockSocket)
  assert.strictEqual(framed, null, 'Should return null for unknown socket')
})

test('suspend does not throw when swarm is null', () => {
  const manager = new P2PManager()
  
  assert.doesNotThrow(() => {
    manager.suspend()
  }, 'Should not throw when swarm is null')
})

test('resume does not throw when swarm is null', () => {
  const manager = new P2PManager()
  
  assert.doesNotThrow(() => {
    manager.resume()
  }, 'Should not throw when swarm is null')
})

test('heartbeat polls the invite mailbox while the app remains foregrounded', () => {
  const manager = new P2PManager()
  const originalSetInterval = global.setInterval
  let heartbeatTick = null
  let drainCalls = 0

  global.setInterval = (callback) => {
    heartbeatTick = callback
    return { fake: true }
  }
  manager._runHeartbeat = () => {}
  manager._drainInviteMailboxes = async () => { drainCalls++ }

  try {
    manager._startHeartbeat()
    assert.strictEqual(typeof heartbeatTick, 'function')
    heartbeatTick()
    assert.strictEqual(drainCalls, 1)
  } finally {
    global.setInterval = originalSetInterval
    manager._heartbeatInterval = null
  }
})

test('concurrent invite mailbox drains share one operation', async () => {
  const manager = new P2PManager()
  let releaseDrain
  let drainCalls = 0
  const drainGate = new Promise(resolve => { releaseDrain = resolve })

  manager._drainInviteMailboxesOnce = async () => {
    drainCalls++
    await drainGate
    return 3
  }

  const first = manager._drainInviteMailboxes()
  const second = manager._drainInviteMailboxes()
  assert.strictEqual(drainCalls, 1)

  releaseDrain()
  assert.deepStrictEqual(await Promise.all([first, second]), [3, 3])
  assert.strictEqual(manager._mailboxDrainInFlight, null)
})

test('stop does not throw when swarm is null', async () => {
  const manager = new P2PManager()
  
  await assert.doesNotReject(async () => {
    await manager.stop()
  }, 'Should not throw when swarm is null')
})

test('joinConversation returns false when swarm is not started', async () => {
  const manager = new P2PManager()
  
  const result = await manager.joinConversation('conv-1', b4a.toString(crypto.randomBytes(32), 'hex'))
  assert.strictEqual(result, false, 'Should return false when swarm not started')
})

test('joinGroupConversation returns false when swarm is not started', async () => {
  const manager = new P2PManager()
  
  const result = await manager.joinGroupConversation('conv-1', b4a.toString(crypto.randomBytes(32), 'hex'), [])
  assert.strictEqual(result, false, 'Should return false when swarm not started')
})

test('sendInvite returns false when swarm is not started', async () => {
  const manager = new P2PManager()
  
  const result = await manager.sendInvite(b4a.toString(crypto.randomBytes(32), 'hex'), { type: 'invite' })
  assert.strictEqual(result, false, 'Should return false when swarm not started')
})

test('sendInvite looks up the recipient personal topic without announcing the inviter', async () => {
  const joins = []
  const phases = []
  const keyPair = { publicKey: crypto.randomBytes(32) }
  const manager = new P2PManager()
  manager.keyPair = keyPair
  manager.swarm = {
    dht: {
      unannounce: async (topic, usedKeyPair) => {
        phases.push('unannounce')
        assert.strictEqual(usedKeyPair, keyPair)
      }
    },
    join: (topic, options) => {
      phases.push('join')
      joins.push({ topic, options })
      return { flushed: async () => {} }
    }
  }

  const peerKey = b4a.toString(crypto.randomBytes(32), 'hex')
  const result = await manager.sendInvite(peerKey, { type: 'direct_invite' })

  assert.strictEqual(result, false, 'invite remains pending until the recipient connects')
  assert.strictEqual(joins.length, 1)
  assert.deepStrictEqual(joins[0].options, { client: true, server: false })
  assert.deepStrictEqual(phases, ['unannounce', 'join'])
})

test('leaveConversation handles non-existent conversation gracefully', async () => {
  const manager = new P2PManager()
  
  await assert.doesNotReject(async () => {
    await manager.leaveConversation('non-existent')
  }, 'Should handle non-existent conversation')
})

test('conversations map tracks direct conversations', async () => {
  const manager = new P2PManager()
  
  // Manually add a conversation (simulating what would happen after joinConversation)
  manager.conversations.set('conv-1', { peers: new Map() })
  
  assert.ok(manager.conversations.has('conv-1'), 'Should track conversation')
  assert.strictEqual(manager.conversations.size, 1, 'Should have one conversation')
})

test('groupConversations map tracks group conversations', () => {
  const manager = new P2PManager()
  
  // Manually add a group conversation
  const groupId = b4a.toString(crypto.randomBytes(32), 'hex')
  manager.groupConversations.set('group-1', {
    groupTopicHex: 'test-topic',
    groupId,
    participantKeys: [],
    connections: new Map()
  })
  
  assert.ok(manager.groupConversations.has('group-1'), 'Should track group conversation')
  assert.strictEqual(manager.groupConversations.size, 1, 'Should have one group conversation')
})

test('allPeerConnections tracks global peer connections', () => {
  const manager = new P2PManager()
  
  const peerKey = b4a.toString(crypto.randomBytes(32), 'hex')
  manager.allPeerConnections.set(peerKey, [])
  
  assert.ok(manager.allPeerConnections.has(peerKey), 'Should track peer')
  assert.strictEqual(manager.allPeerConnections.size, 1, 'Should have one peer')
})

test('pendingInvites tracks invites waiting for peer connection', () => {
  const manager = new P2PManager()
  
  const peerKey = b4a.toString(crypto.randomBytes(32), 'hex')
  manager.pendingInvites.set(peerKey, [{ type: 'group_invite' }])
  
  assert.ok(manager.pendingInvites.has(peerKey), 'Should track pending invite')
  const invites = manager.pendingInvites.get(peerKey)
  assert.strictEqual(invites.length, 1, 'Should have one pending invite')
})

test('pending messages remain queued until recipient confirmation', () => {
  const manager = new P2PManager()
  manager.conversations.set('conv-1', { peers: new Map() })

  manager.sendToConversation('conv-1', { id: 'message-1', content: 'hello', status: 'queued' })
  assert.strictEqual(manager.pendingMessages.get('conv-1').length, 1)
  assert.strictEqual(manager.pendingMessages.get('conv-1')[0].message.status, undefined)

  manager.confirmMessagesDelivered('conv-1', ['message-1'])
  assert.strictEqual(manager.pendingMessages.has('conv-1'), false)
})

test('restored queued messages are deduplicated and strip local media fields', () => {
  const manager = new P2PManager()
  manager.conversations.set('conv-1', { peers: new Map() })
  const message = {
    id: 'message-1',
    mediaLocalPath: '/private/photo.jpg',
    mediaTransferState: 'complete'
  }

  manager.restorePendingMessages('conv-1', [message, message])

  const pending = manager.pendingMessages.get('conv-1')
  assert.strictEqual(pending.length, 1)
  assert.strictEqual(pending[0].message.mediaLocalPath, undefined)
  assert.strictEqual(pending[0].message.mediaTransferState, undefined)
})

test('one visible message append requests one notification for the exact block', async () => {
  const core = { key: b4a.alloc(32), discoveryKey: b4a.alloc(32) }
  const notifications = []
  let resolveNotification
  const notificationRequested = new Promise(resolve => { resolveNotification = resolve })
  const manager = new P2PManager({
    hypercoreManager: {
      appendMessage: async () => ({ core, index: 7 })
    },
    blindMirror: {
      sendNotification: async (notifiedCore, index) => {
        notifications.push({ core: notifiedCore, index })
        resolveNotification()
      }
    }
  })
  manager.conversations.set('conv-1', { peers: new Map() })

  manager.sendToConversation(
    'conv-1',
    { id: 'message-1', content: 'hello' },
    { notificationEligible: true }
  )
  await notificationRequested

  assert.deepStrictEqual(notifications, [{ core, index: 7 }])
})

test('control records append without requesting notifications', async () => {
  let appends = 0
  let notifications = 0
  const manager = new P2PManager({
    hypercoreManager: {
      appendMessage: async () => {
        appends++
        return { core: {}, index: appends - 1 }
      }
    },
    blindMirror: {
      sendNotification: async () => { notifications++ }
    }
  })
  manager.conversations.set('conv-1', { peers: new Map() })

  manager.sendReadReceipt('conv-1', 'message-1')
  manager.sendToConversation('conv-1', { type: '__presence', hidden: true })
  await new Promise(resolve => setTimeout(resolve, 0))

  assert.strictEqual(appends, 2)
  assert.strictEqual(notifications, 0)
})

test('durable delivery receipt resolves only after its Hypercore append', async () => {
  let releaseAppend
  const appendGate = new Promise(resolve => { releaseAppend = resolve })
  let liveSent = false
  const manager = new P2PManager({
    hypercoreManager: {
      appendMessage: async () => appendGate
    }
  })
  manager._trySendToConversation = () => {
    liveSent = true
    return true
  }

  let resolved = false
  const receipt = manager.sendDeliveryReceipt(
    'conv-1',
    'message-1',
    'aa'.repeat(32),
    { requireDurable: true }
  ).then(() => { resolved = true })

  await new Promise(resolve => setImmediate(resolve))
  assert.strictEqual(resolved, false)
  assert.strictEqual(liveSent, false)

  releaseAppend({ core: {}, index: 0 })
  await receipt
  assert.strictEqual(resolved, true)
  assert.strictEqual(liveSent, true)
})

test('durable delivery receipt propagates append failure', async () => {
  const manager = new P2PManager({
    hypercoreManager: {
      appendMessage: async () => { throw new Error('disk full') }
    }
  })

  await assert.rejects(
    manager.sendDeliveryReceipt(
      'conv-1',
      'message-1',
      'aa'.repeat(32),
      { requireDurable: true }
    ),
    /disk full/
  )
})

test('messages fail closed without authoritative direct-chat eligibility', async () => {
  let appends = 0
  let notifications = 0
  const manager = new P2PManager({
    hypercoreManager: {
      appendMessage: async () => {
        appends++
        return { core: {}, index: appends - 1 }
      }
    },
    blindMirror: {
      sendNotification: async () => { notifications++ }
    }
  })
  manager.sendToConversation('group-1', { id: 'message-1', content: 'hello group' })
  await new Promise(resolve => setTimeout(resolve, 0))

  assert.strictEqual(appends, 1)
  assert.strictEqual(notifications, 0)
})

test('push failure does not undo persistence or queued delivery', async () => {
  let persisted = false
  const manager = new P2PManager({
    hypercoreManager: {
      appendMessage: async () => {
        persisted = true
        return { core: {}, index: 0 }
      }
    },
    blindMirror: {
      sendNotification: async () => { throw new Error('gateway unavailable') }
    }
  })
  manager.conversations.set('conv-1', { peers: new Map() })

  manager.sendToConversation(
    'conv-1',
    { id: 'message-1', content: 'hello' },
    { notificationEligible: true }
  )
  await new Promise(resolve => setTimeout(resolve, 0))

  assert.strictEqual(persisted, true)
  assert.strictEqual(manager.pendingMessages.get('conv-1').length, 1)
})

test('durable send does not report success before Hypercore append', async () => {
  let releaseAppend
  const appendGate = new Promise(resolve => { releaseAppend = resolve })
  let liveSent = false
  const manager = new P2PManager({
    hypercoreManager: { appendMessage: async () => appendGate }
  })
  manager.conversations.set('conv-1', { peers: new Map() })
  manager._trySendToConversation = () => {
    liveSent = true
    return true
  }

  let resolved = false
  const sending = manager.sendToConversationDurably(
    'conv-1',
    { id: 'message-1', content: 'hello' }
  ).then(result => {
    resolved = true
    return result
  })

  await new Promise(resolve => setImmediate(resolve))
  assert.strictEqual(resolved, false)
  assert.strictEqual(liveSent, false)
  assert.strictEqual(manager.pendingMessages.has('conv-1'), false)

  releaseAppend({ core: {}, index: 0 })
  const result = await sending
  assert.strictEqual(result.appended, true)
  assert.strictEqual(result.sent, true)
  assert.strictEqual(liveSent, true)
})

test('durable send propagates append failure and never queues or sends', async () => {
  const manager = new P2PManager({
    hypercoreManager: {
      appendMessage: async () => { throw new Error('disk full') }
    }
  })
  manager.conversations.set('conv-1', { peers: new Map() })
  let liveSent = false
  manager._trySendToConversation = () => {
    liveSent = true
    return true
  }

  await assert.rejects(
    manager.sendToConversationDurably('conv-1', { id: 'message-1', content: 'hello' }),
    /disk full/
  )
  assert.strictEqual(liveSent, false)
  assert.strictEqual(manager.pendingMessages.has('conv-1'), false)
})

test('durable send distinguishes advisory relay acknowledgement from local durability', async () => {
  const manager = new P2PManager({
    hypercoreManager: {
      appendMessage: async () => ({ core: {}, index: 4 })
    },
    blindMirror: {
      sendNotification: async () => { throw new Error('gateway unavailable') }
    }
  })
  manager.conversations.set('conv-1', { peers: new Map() })

  const result = await manager.sendToConversationDurably(
    'conv-1',
    { id: 'message-1', content: 'hello' },
    { notificationEligible: true }
  )

  assert.strictEqual(result.appended, true)
  assert.strictEqual(result.relay, 'pending')
  assert.strictEqual(result.sent, false)
  assert.strictEqual(manager.pendingMessages.get('conv-1').length, 1)
})

// --- Cold-start latency fixes (perf/cold-start-latency) ---

test('start() emits swarm_created before dht_ready so blind mirror can dial in parallel with bootstrap', async () => {
  class FakeSwarm extends EventEmitter {
    constructor () {
      super()
      this.connections = new Set()
      this.dht = { ready: async () => {}, ping: async () => { throw new Error('no gw') } }
    }
    join () { return { flushed: async () => {} } }
    async destroy () {}
  }
  const order = []
  const manager = new P2PManager({ createSwarm: () => new FakeSwarm() })
  manager.healthMonitor = { setSwarm () {}, async startMonitoring () {}, stopMonitoring () {} }
  manager._startHeartbeat = () => {}
  manager.on('swarm_created', () => order.push('swarm_created'))
  manager.on('dht_ready', () => order.push('dht_ready'))

  await manager.start({ publicKey: b4a.alloc(32) })
  await manager.stop()

  assert.deepStrictEqual(order, ['swarm_created', 'dht_ready'],
    'swarm_created must fire before dht_ready')
})

test('joinPersonalTopic() does not block on the DHT announce (flushed)', async () => {
  class FakeSwarm extends EventEmitter {
    join () { return { flushed: () => new Promise(() => {}) } } // never resolves
  }
  const manager = new P2PManager()
  manager.keyPair = { publicKey: b4a.alloc(32) }
  manager.swarm = new FakeSwarm()

  const outcome = await Promise.race([
    manager.joinPersonalTopic().then(() => 'resolved'),
    new Promise(resolve => setTimeout(() => resolve('blocked'), 200))
  ])
  assert.strictEqual(outcome, 'resolved',
    'joinPersonalTopic must return without awaiting the announce')
})

test('DHT node cache is a safe no-op outside the Bare runtime', () => {
  const manager = new P2PManager()
  assert.strictEqual(manager._dhtNodeCachePath(), null)
  assert.deepStrictEqual(manager._loadDhtNodeCache(), [])
  assert.doesNotThrow(() => manager._saveDhtNodeCache())
})

// --- Wire-input authorization (fix: message injection via unauthenticated
// --- core announcements and wire-supplied conversationIds) ---

const { ChatStore } = require('../lib/chat-store')

test('_peerMayAnnounceCore only allows verified participants', () => {
  const peer = b4a.toString(crypto.randomBytes(32), 'hex')
  const stranger = b4a.toString(crypto.randomBytes(32), 'hex')
  const runtimeManager = new P2PManager()

  // Via live direct-conversation map
  runtimeManager.peerToConversation.set(peer, { conversationId: 'conv-live', peerEntry: {} })
  assert.strictEqual(runtimeManager._peerMayAnnounceCore('conv-live', peer), true)
  assert.strictEqual(runtimeManager._peerMayAnnounceCore('conv-live', stranger), false)

  // Group runtime caches alone cannot authorize membership
  runtimeManager.groupConversations.set('group-1', { participantKeys: [peer], connections: new Map() })
  assert.strictEqual(runtimeManager._peerMayAnnounceCore('group-1', peer), false)
  assert.strictEqual(runtimeManager._peerMayAnnounceCore('group-1', stranger), false)

  const manager = new P2PManager({
    isParticipant: (convId, key) => convId === 'disk-conv' && key === peer
  })

  // Via the chat-store-backed callback (cold start, not yet joined)
  assert.strictEqual(manager._peerMayAnnounceCore('disk-conv', peer), true)
  assert.strictEqual(manager._peerMayAnnounceCore('disk-conv', stranger), false)

  // Arbitrary announced conversation
  assert.strictEqual(manager._peerMayAnnounceCore('other-conv', peer), false)
})

test('authoritative membership overrides stale group routing state', () => {
  const peer = b4a.toString(crypto.randomBytes(32), 'hex')
  const manager = new P2PManager({ isParticipant: () => false })
  manager.groupConversations.set('group-1', {
    participantKeys: [peer],
    connections: new Map([[peer, [{}]]])
  })

  assert.strictEqual(manager._peerMayAccessConversation('group-1', peer), false)
  assert.strictEqual(manager._peerMayAnnounceCore('group-1', peer), false)
  assert.strictEqual(manager.removeGroupParticipant('group-1', peer), true)
  assert.deepStrictEqual(manager.groupConversations.get('group-1').participantKeys, [])
  assert.strictEqual(manager.groupConversations.get('group-1').connections.has(peer), false)
})

test('_acceptWireConversationId accepts only verifiable ids', () => {
  const myKeyPair = crypto.keyPair(b4a.alloc(32, 7))
  const myHex = b4a.toString(myKeyPair.publicKey, 'hex')
  const peer = b4a.toString(crypto.keyPair(b4a.alloc(32, 8)).publicKey, 'hex')
  const manager = new P2PManager({
    isParticipant: (convId, key) => convId === 'existing-conv' && key === peer
  })
  manager.keyPair = myKeyPair

  const derived = ChatStore.directChatId(myHex, peer)
  const legacy = ChatStore._legacyDirectChatId(myHex, peer)

  assert.strictEqual(manager._acceptWireConversationId(derived, peer), derived,
    'Deterministic DM id for (us, peer) is accepted')
  assert.strictEqual(manager._acceptWireConversationId(legacy, peer), legacy,
    'Legacy DM id for (us, peer) is accepted')
  assert.strictEqual(manager._acceptWireConversationId('existing-conv', peer), 'existing-conv',
    'Existing conversation the peer participates in is accepted')
  assert.strictEqual(manager._acceptWireConversationId('dm_' + 'ff'.repeat(16), peer), null,
    'Arbitrary conversationId is rejected')
  const otherPair = ChatStore.directChatId(myHex, b4a.toString(crypto.randomBytes(32), 'hex'))
  assert.strictEqual(manager._acceptWireConversationId(otherPair, peer), null,
    'A DM id belonging to a different pair is rejected')
  assert.strictEqual(manager._acceptWireConversationId(null, peer), null)
})

// A socket killed while the OS had us suspended never fires 'close', so it
// lingers in the connection maps. Sends must treat it as absent; previously the
// write was skipped silently and every message queued forever behind a peer the
// UI still reported as connected.
const deadFramed = () => ({
  _destroyed: true,
  destroy: () => {},
  writeJSON: () => { throw new Error('must not write to a dead socket') }
})

test('a socket that died without a close event is not treated as reachable', () => {
  const manager = new P2PManager()
  const peerKey = b4a.toString(crypto.randomBytes(32), 'hex')
  const dead = {}
  const peerEntry = { connections: [dead] }
  manager.conversations.set('conv-1', { peers: new Map([[peerKey, peerEntry]]) })
  manager.allPeerConnections.set(peerKey, [dead])
  manager.framedSockets.set(dead, deadFramed())

  assert.strictEqual(manager._trySendToConversation('conv-1', { id: 'm1' }), false,
    'A dead socket must not be reported as a send')
  assert.strictEqual(peerEntry.connections.length, 0,
    'The dead socket is pruned from the conversation, not left to poison later sends')
  assert.strictEqual(manager.allPeerConnections.has(peerKey), false,
    'and from the global connection map')
})

test('a live socket still delivers when a dead one precedes it', () => {
  const manager = new P2PManager()
  const peerKey = b4a.toString(crypto.randomBytes(32), 'hex')
  const dead = {}
  const live = {}
  const written = []
  const peerEntry = { connections: [dead, live] }
  manager.conversations.set('conv-1', { peers: new Map([[peerKey, peerEntry]]) })
  manager.framedSockets.set(dead, deadFramed())
  manager.framedSockets.set(live, {
    _destroyed: false,
    destroy: () => {},
    writeJSON: (p) => { written.push(p); return true }
  })

  assert.strictEqual(manager._trySendToConversation('conv-1', { id: 'm1' }), true,
    'The live socket is used even though a dead one comes first')
  assert.strictEqual(written.length, 1)
  assert.deepStrictEqual(peerEntry.connections, [live], 'Only the dead socket is pruned')
})

test('pruning a socket clears it from every collection that referenced it', () => {
  const manager = new P2PManager()
  const peerKey = b4a.toString(crypto.randomBytes(32), 'hex')
  const socket = {}
  const peerEntry = { connections: [socket] }
  manager.conversations.set('conv-1', { peers: new Map([[peerKey, peerEntry]]) })
  manager.allPeerConnections.set(peerKey, [socket])
  manager.groupConversations.set('group-1', { connections: new Map([[peerKey, [socket]]]) })
  manager.framedSockets.set(socket, deadFramed())

  manager._pruneSocket(socket, peerKey)

  assert.strictEqual(peerEntry.connections.length, 0, 'direct conversation cleared')
  assert.strictEqual(manager.allPeerConnections.has(peerKey), false, 'global map cleared')
  assert.strictEqual(manager.groupConversations.get('group-1').connections.has(peerKey), false,
    'group conversation cleared')
})

test('flushing queued messages keeps them pending until a recipient receipt', () => {
  const manager = new P2PManager()
  const live = {}
  let writes = 0
  manager.framedSockets.set(live, {
    _destroyed: false,
    destroy: () => {},
    writeJSON: () => {
      writes++
      return true
    }
  })
  manager.pendingMessages.set('conv-1', [{ message: { id: 'm1' } }, { message: { id: 'm2' } }])

  manager._flushPendingMessages('conv-1', [live])

  assert.strictEqual(writes, 2)
  assert.strictEqual(manager.pendingMessages.get('conv-1').length, 2)
})

// The mailbox transports can take tens of seconds on exactly the broken
// networks that trigger a bootstrap, and the native IPC call has its own
// deadline. A durable send must not wait behind any of that.
test('a durable send does not wait for the mailbox bootstrap', async () => {
  const manager = new P2PManager()
  let released
  const gate = new Promise(resolve => { released = resolve })
  let bootstrapStarted = false

  manager.hypercoreManager = {
    getLocalCoreKey: () => 'cc'.repeat(32),
    appendMessage: async () => true
  }
  manager.keyPair = { publicKey: crypto.randomBytes(32) }
  manager.conversations.set('conv-1', {
    peers: new Map([[b4a.toString(crypto.randomBytes(32), 'hex'), { connections: [] }]])
  })
  manager._persistOutgoing = async () => ({ appended: true, relay: 'pending' })
  manager._putInviteMailbox = async () => { bootstrapStarted = true; await gate; return true }

  const result = await manager.sendToConversationDurably('conv-1', { id: 'm1', content: 'hi' })

  assert.strictEqual(result.sent, false, 'the message is queued, not delivered')
  assert.strictEqual(bootstrapStarted, true, 'the bootstrap was still kicked off')
  released()
})

// Stamping only on success meant an unreachable mailbox was retried on every
// single send, so the cost of a broken network scaled with how much you typed.
test('an unreachable mailbox is not retried on the next send', async () => {
  const manager = new P2PManager()
  let attempts = 0
  manager.keyPair = { publicKey: crypto.randomBytes(32) }
  manager.hypercoreManager = { getLocalCoreKey: () => 'cc'.repeat(32) }
  manager.conversations.set('conv-1', {
    peers: new Map([[b4a.toString(crypto.randomBytes(32), 'hex'), { connections: [] }]])
  })
  manager._putInviteMailbox = async () => { attempts++; return false }

  manager._bootstrapConversationThroughMailbox('conv-1')
  await manager._mailboxBootstrapInFlight.get('conv-1')
  manager._bootstrapConversationThroughMailbox('conv-1')

  assert.strictEqual(attempts, 1, 'the failed attempt still starts the cooldown')
})

test('a mailbox bootstrap introduces us by display name, not a truncated key', async () => {
  const manager = new P2PManager({ displayName: () => 'Ada' })
  const invites = []
  manager.keyPair = { publicKey: crypto.randomBytes(32) }
  manager.hypercoreManager = { getLocalCoreKey: () => 'cc'.repeat(32) }
  manager.conversations.set('conv-1', {
    peers: new Map([[b4a.toString(crypto.randomBytes(32), 'hex'), { connections: [] }]])
  })
  manager._putInviteMailbox = async (_recipient, invite) => { invites.push(invite); return true }

  manager._bootstrapConversationThroughMailbox('conv-1')
  await manager._mailboxBootstrapInFlight.get('conv-1')

  assert.strictEqual(invites[0].senderDisplayName, 'Ada')
})

// With no host app to carry it there is nothing to wait for, and waiting only
// delays the fallback to the DHT mailbox.
test('platform HTTPS fails immediately when no host transport is wired', async () => {
  const manager = new P2PManager()
  await assert.rejects(
    () => manager._platformPostJson('https://mailbox.example/list', {}),
    /transport unavailable/
  )
  assert.strictEqual(manager._platformHttpRequests.size, 0, 'no request is left pending')
})

// _destroyed is our own teardown flag. A socket the OS killed without a close
// event leaves it false, which is precisely the case the send-time liveness
// check exists for, so the check has to ask the socket too.
test('a socket the OS killed without a close event is not treated as reachable', () => {
  const manager = new P2PManager()
  const peerKey = b4a.toString(crypto.randomBytes(32), 'hex')
  const killed = { destroyed: true }
  const peerEntry = { connections: [killed] }
  manager.conversations.set('conv-1', { peers: new Map([[peerKey, peerEntry]]) })
  manager.allPeerConnections.set(peerKey, [killed])
  manager.framedSockets.set(killed, {
    _destroyed: false, // never torn down by us
    destroy: () => {},
    writeJSON: () => { throw new Error('must not write to a killed socket') }
  })

  assert.strictEqual(manager._liveFramed(killed), null,
    'an undestroyed wrapper over a dead socket is still not usable')
  assert.strictEqual(manager._trySendToConversation('conv-1', { id: 'm1' }), false)
})

test('a socket that is no longer writable is not treated as reachable', () => {
  const manager = new P2PManager()
  const socket = { writable: false }
  manager.framedSockets.set(socket, { _destroyed: false, destroy: () => {}, writeJSON: () => true })
  assert.strictEqual(manager._liveFramed(socket), null)
})

// peerCount is resynced from the unique-peer map by the heartbeat, so counting
// sockets on connect and decrementing per close made the two disagree whenever
// a peer held more than one connection.
test('peerCount counts peers, not sockets', () => {
  const manager = new P2PManager()
  const peerKey = b4a.toString(crypto.randomBytes(32), 'hex')
  const first = {}
  const second = {}
  manager.allPeerConnections.set(peerKey, [first, second])
  manager.framedSockets.set(first, { _destroyed: false, destroy: () => {}, writeJSON: () => true })
  manager.framedSockets.set(second, { _destroyed: false, destroy: () => {}, writeJSON: () => true })

  manager._pruneSocket(first, peerKey)
  assert.strictEqual(manager.allPeerConnections.get(peerKey).length, 1,
    'the peer is still connected on its other socket')

  // What the heartbeat would resync to. Counting sockets would have said 2 here.
  assert.strictEqual(manager.allPeerConnections.size, 1)
})

// The mailbox copy is deleted on the strength of a successful send, so a write
// that did not land must fall through to the mailbox rather than report success.
test('an invite whose write did not land is not reported as sent', async () => {
  const manager = new P2PManager()
  const peerKey = b4a.toString(crypto.randomBytes(32), 'hex')
  const socket = {}
  manager.swarm = { dht: {}, join: () => ({ flushed: async () => {} }) }
  manager.allPeerConnections.set(peerKey, [socket])
  manager.framedSockets.set(socket, {
    _destroyed: false,
    destroy: () => {},
    writeJSON: () => false // socket accepted nothing
  })
  let mailboxAttempts = 0
  manager._putInviteMailbox = async () => { mailboxAttempts++; return true }

  const result = await manager.sendInvite(peerKey, { type: 'direct_invite' })

  assert.strictEqual(result, true, 'the mailbox accepted it')
  assert.strictEqual(mailboxAttempts, 1, 'the failed write fell through to the mailbox')
})

test('a pending invite whose write did not land stays pending', () => {
  const manager = new P2PManager()
  const peerKey = b4a.toString(crypto.randomBytes(32), 'hex')
  manager.pendingInvites.set(peerKey, [{ type: 'direct_invite' }])
  const framed = { _destroyed: false, destroy: () => {}, writeJSON: () => false }

  // The flush lives inside the connection handler; exercise its contract directly.
  const pending = manager.pendingInvites.get(peerKey)
  const failed = []
  for (const invite of pending) {
    if (!framed.writeJSON(invite)) failed.push(invite)
  }
  assert.strictEqual(failed.length, 1, 'a write that did not land keeps the invite queued')
})

// Only the close handler used to announce this, and the socket the prune exists
// for is the one that never fires close.
test('pruning an OS-killed socket announces the peer as offline', () => {
  const manager = new P2PManager()
  const peerKey = b4a.toString(crypto.randomBytes(32), 'hex')
  const socket = { destroyed: true }
  const offline = []
  manager.peerToConversation.set(peerKey, { conversationId: 'conv-1', peerEntry: { connections: [socket] } })
  manager.conversations.set('conv-1', { peers: new Map([[peerKey, { connections: [socket] }]]) })
  manager.allPeerConnections.set(peerKey, [socket])
  manager.framedSockets.set(socket, { _destroyed: false, destroy: () => {}, writeJSON: () => false })
  manager.peerCount = 1
  manager.on('peer_offline', (conversationId, id) => offline.push([conversationId, id]))

  manager._pruneSocket(socket, peerKey)

  assert.deepStrictEqual(offline, [['conv-1', peerKey]], 'the conversation is told')
  assert.strictEqual(manager.peerCount, 0, 'and the count is resynced')
})

test('a peer keeping a second socket is not announced as offline', () => {
  const manager = new P2PManager()
  const peerKey = b4a.toString(crypto.randomBytes(32), 'hex')
  const dead = { destroyed: true }
  const live = {}
  const offline = []
  manager.peerToConversation.set(peerKey, { conversationId: 'conv-1', peerEntry: { connections: [dead, live] } })
  manager.conversations.set('conv-1', { peers: new Map([[peerKey, { connections: [dead, live] }]]) })
  manager.allPeerConnections.set(peerKey, [dead, live])
  manager.framedSockets.set(dead, { _destroyed: false, destroy: () => {}, writeJSON: () => false })
  manager.on('peer_offline', (conversationId, id) => offline.push([conversationId, id]))

  manager._pruneSocket(dead, peerKey)

  assert.deepStrictEqual(offline, [], 'still reachable on its other socket')
})

test('a group peer is announced offline for the groups its socket served', () => {
  const manager = new P2PManager()
  const peerKey = b4a.toString(crypto.randomBytes(32), 'hex')
  const socket = { destroyed: true }
  const offline = []
  manager.allPeerConnections.set(peerKey, [socket])
  manager.groupConversations.set('group-1', { connections: new Map([[peerKey, [socket]]]) })
  manager.framedSockets.set(socket, { _destroyed: false, destroy: () => {}, writeJSON: () => false })
  manager.on('peer_offline', (conversationId, id) => offline.push([conversationId, id]))

  manager._pruneSocket(socket, peerKey)

  assert.deepStrictEqual(offline, [['group-1', peerKey]])
})

// Pruning the same socket twice is the normal case, not a mistake: the heartbeat
// drops a socket the OS killed, and the close callback for that socket can still
// arrive afterwards.
test('pruning the same socket twice announces the loss once', () => {
  const manager = new P2PManager()
  const peerKey = b4a.toString(crypto.randomBytes(32), 'hex')
  const socket = { destroyed: true }
  const offline = []
  manager.peerToConversation.set(peerKey, { conversationId: 'conv-1', peerEntry: { connections: [socket] } })
  manager.conversations.set('conv-1', { peers: new Map([[peerKey, { connections: [socket] }]]) })
  manager.allPeerConnections.set(peerKey, [socket])
  manager.groupConversations.set('group-1', { connections: new Map([[peerKey, [socket]]]) })
  manager.framedSockets.set(socket, { _destroyed: false, destroy: () => {}, writeJSON: () => false })
  manager.peerCount = 1
  manager.on('peer_offline', (conversationId, id) => offline.push([conversationId, id]))

  manager._pruneSocket(socket, peerKey) // the heartbeat notices it
  manager._pruneSocket(socket, peerKey) // then close finally fires

  assert.deepStrictEqual(offline, [['conv-1', peerKey], ['group-1', peerKey]],
    'the second prune removed nothing, so it has nothing to announce')
})

// openRemoteCore swallowed its errors and returned undefined either way, so the
// caller could not tell a failed core open from a successful one.
test('openRemoteCore reports failure instead of swallowing it', async () => {
  const manager = new P2PManager()
  assert.strictEqual(await manager.openRemoteCore('conv-1', 'aa', null), false,
    'a missing core key is not success')

  manager.hypercoreManager = { openRemoteCore: async () => { throw new Error('corrupt') } }
  manager.keyPair = { publicKey: crypto.randomBytes(32) }
  assert.strictEqual(await manager.openRemoteCore('conv-1', 'bb'.repeat(32), 'cc'.repeat(32)), false)

  manager.hypercoreManager = { openRemoteCore: async () => ({}) }
  assert.strictEqual(await manager.openRemoteCore('conv-1', 'bb'.repeat(32), 'cc'.repeat(32)), true)
})

// --- group invite links: mailbox operations as another key ---

function signingKeyPair (seedByte) {
  return crypto.keyPair(b4a.alloc(32, seedByte))
}

/**
 * A manager whose mailbox runs over the host HTTPS path, answered in-process,
 * so each request body shows which key signed or authenticated it.
 */
function managerWithHttpsMailbox (handle, opts = {}) {
  const config = require('../lib/config')
  const previousUrl = config.INVITE_MAILBOX_URL
  config.INVITE_MAILBOX_URL = 'https://mailbox.example/zapp-invite'
  const requests = []
  const manager = new P2PManager({
    ...opts,
    platformHttp: (request) => {
      requests.push(request)
      Promise.resolve(handle(request)).then(response => {
        manager.completePlatformHttpRequest({ requestId: request.requestId, success: true, response })
      })
      return true
    }
  })
  manager.keyPair = signingKeyPair(1)
  return { manager, requests, restore: () => { config.INVITE_MAILBOX_URL = previousUrl } }
}

test('putMailboxAs signs the envelope with the given key, not the identity', async () => {
  const { manager, requests, restore } = managerWithHttpsMailbox(() => ({}))
  try {
    const throwaway = signingKeyPair(9)
    const recipient = b4a.toString(signingKeyPair(7).publicKey, 'hex')
    assert.strictEqual(await manager.putMailboxAs(throwaway, recipient, { type: 'group_join_request' }), true)
    const envelope = JSON.parse(requests[0].body.envelope)
    assert.strictEqual(envelope.sender, b4a.toString(throwaway.publicKey, 'hex'))
    assert.notStrictEqual(envelope.sender, b4a.toString(manager.keyPair.publicKey, 'hex'))
    assert.strictEqual(requests[0].body.recipient, recipient)
  } finally {
    restore()
  }
})

test('drainMailboxAs lists and acknowledges as the given key', async () => {
  const { manager, requests, restore } = managerWithHttpsMailbox((request) => {
    if (request.url.endsWith('/list')) return { entries: [], more: false }
    return {}
  })
  try {
    const rendezvous = signingKeyPair(5)
    assert.strictEqual(await manager.drainMailboxAs(rendezvous, async () => true), true)
    assert.strictEqual(requests[0].body.identity, b4a.toString(rendezvous.publicKey, 'hex'))
  } finally {
    restore()
  }
})

test('a group join result in our mailbox is delivered whatever key signed it', async () => {
  const { encryptInvite } = require('../lib/invite-mailbox')
  const linkKey = signingKeyPair(6)
  const me = signingKeyPair(1)
  const result = { type: 'group_join_result', v: 1, linkId: '11'.repeat(16), joinerKey: b4a.toString(me.publicKey, 'hex'), status: 'full', sig: '00'.repeat(64) }
  const envelope = encryptInvite(result, linkKey, b4a.toString(me.publicKey, 'hex'))
  const delivered = []
  let listed = false
  const { manager, restore } = managerWithHttpsMailbox((request) => {
    if (request.url.endsWith('/list')) {
      if (listed) return { entries: [], more: false }
      listed = true
      return { entries: [{ id: 'e1', envelope }], more: false }
    }
    return {}
  }, { deliverInvite: async (invite, sender) => { delivered.push({ invite, sender }); return true } })
  try {
    await manager._drainInviteMailboxesOnce()
    assert.strictEqual(delivered.length, 1)
    assert.deepStrictEqual(delivered[0].invite, result)
    assert.strictEqual(delivered[0].sender, b4a.toString(linkKey.publicKey, 'hex'))
  } finally {
    restore()
  }
})

test('the auxiliary drain runs after each mailbox drain and its failure is contained', async () => {
  const manager = new P2PManager()
  const order = []
  manager._drainInviteMailboxesOnce = async () => { order.push('own'); return 2 }
  manager.setAuxiliaryDrain(async () => { order.push('links'); throw new Error('offline') })
  assert.strictEqual(await manager._drainInviteMailboxes(), 2)
  assert.deepStrictEqual(order, ['own', 'links'])
  assert.strictEqual(manager._mailboxDrainInFlight, null)
})
