const { test } = require('node:test')
const assert = require('node:assert')
const EventEmitter = require('node:events')
const b4a = require('b4a')
const { BlindMirror } = require('../lib/blind-mirror')

function core () {
  const value = new EventEmitter()
  value.key = b4a.alloc(32, 1)
  value.closing = false
  return value
}

test('closed cores do not leave stale conversation registrations', () => {
  let registrations = 0
  const mirror = new BlindMirror({}, {}, { keys: ['first'] })
  mirror._peering = {
    addCoreBackground: () => { registrations++ }
  }
  const first = core()

  mirror.addLocalCore('dm-1', first)
  first.emit('close')
  mirror.addLocalCore('dm-1', core())

  assert.strictEqual(registrations, 2)
  assert.strictEqual(mirror.hasConversation('dm-1'), true)
})

test('removing a conversation clears local and every remote registration', () => {
  const mirror = new BlindMirror({}, {}, { keys: ['first'] })
  mirror._peering = { addCoreBackground: () => {} }

  mirror.addLocalCore('dm-1', core())
  mirror.addRemoteCore('dm-1', 'peer-a', core())
  mirror.addRemoteCore('dm-1', 'peer-b', core())
  mirror.removeConversation('dm-1')

  assert.strictEqual(mirror.hasConversation('dm-1'), false)
  assert.strictEqual(mirror._registeredBases.size, 0)
})

test('removing a remote core keeps the rest of the conversation registered', () => {
  const mirror = new BlindMirror({}, {}, { keys: ['first'] })
  mirror._peering = { addCoreBackground: () => {} }
  mirror.addLocalCore('group-1', core())
  mirror.addRemoteCore('group-1', 'departed', core())
  mirror.addRemoteCore('group-1', 'remaining', core())

  mirror.removeRemoteCore('group-1', 'departed')

  assert.strictEqual(mirror._registeredBases.has('remote:group-1:departed'), false)
  assert.strictEqual(mirror._registeredBases.has('remote:group-1:remaining'), true)
  assert.strictEqual(mirror.hasConversation('group-1'), true)
})

test('changing blind-peer keys re-registers live cores', () => {
  let registrations = 0
  let configuredKeys = null
  const mirror = new BlindMirror({}, {}, { keys: ['first'] })
  mirror._peering = {
    pick: 1,
    setKeys: keys => { configuredKeys = keys },
    addCoreBackground: () => { registrations++ }
  }

  mirror.addLocalCore('dm-1', core())
  mirror.setKeys(['second'])

  assert.deepStrictEqual(configuredKeys, ['second'])
  assert.strictEqual(registrations, 2)
})

test('notification connection failures receive bounded retries', async () => {
  let attempts = 0
  const mirror = new BlindMirror({}, {}, {
    keys: ['first'],
    notificationRetryDelays: [0, 0, 0]
  })
  mirror._peering = {
    sendNotification: async () => {
      attempts++
      if (attempts < 3) throw new Error('not connected')
    }
  }

  await mirror.sendNotification(core(), 4)

  assert.strictEqual(attempts, 3)
})

test('_buildDialKeys returns keys unchanged when no dial address is configured', () => {
  const keys = ['aaaa', 'bbbb']
  const mirror = new BlindMirror({ dht: {}, keyPair: {} }, {}, { keys })
  assert.strictEqual(mirror._buildDialKeys(), keys,
    'default deployment (no --blind-peer-address) must pass keys through untouched')
})
