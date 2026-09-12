'use strict'

/**
 * Forgets mirrored images a week after they stopped arriving, and keeps
 * message history out of the sweep.
 *
 * The relay's records carry no notion of what a core holds, and priority is
 * not enough on its own: the relay records a priority only for cores it has
 * not seen, so every core that existed before this module — all of message
 * history — sits at 0, and phones on older builds keep registering message
 * cores at 0. Two things make the sweep safe:
 *
 * - Message-core owners are the only registrations that ask to announce
 *   (core/lib/blind-mirror.js, since the first release), and the relay emits
 *   'add-cores-downgrade-announce' for each such request. Those keys are
 *   promoted to priority 1 through the database's own add path, which the
 *   sweep never selects.
 * - On its first attach on a relay every existing record is promoted the
 *   same way, once, and a marker file in the storage directory records that
 *   it happened. Media cores registered from then on are the only records
 *   left at 0.
 *
 * A media core whose newest block is older than maxAgeMs is cleared whole,
 * the way blind-peer's own storage-pressure GC clears a core: the record
 * stays, with blocksCleared at the core's length, so a phone that
 * re-registers the core makes the relay pull only blocks appended since.
 * Deleting the record instead would make every reconnect re-upload the lot.
 *
 * There is no timer. A pass runs when a block moves through the relay
 * ('core-activity'), at most once per minIntervalMs, plus once at attach: an
 * idle relay has nothing arriving and therefore nothing to forget.
 *
 * Reaches into the same BlindPeer members its _gc uses (db, store,
 * activeReplication, lock) plus rocks.path. blind-peer is pinned at 3.12.2 in
 * package.json for exactly that reason.
 */

const fs = require('fs')
const path = require('path')
const b4a = require('b4a')

const DEFAULTS = {
  maxAgeMs: 7 * 24 * 60 * 60 * 1000,
  minIntervalMs: 60 * 60 * 1000
}

const MESSAGE_CORE_PRIORITY = 1
const STATE_FILE = 'media-retention.json'

/**
 * @param {import('blind-peer')} blindPeer ready instance, not yet listening
 * @param {object} [opts]
 * @param {number} [opts.maxAgeMs] age of a core's newest block before it is cleared
 * @param {number} [opts.minIntervalMs] least time between two passes
 * @param {string} [opts.stateFile] where the one-shot promotion marker lives
 * @param {() => number} [opts.now] clock, for tests
 * @param {(message: string) => void} [opts.log]
 * @returns {Promise<{ close: () => Promise<void> }>} resolves once existing
 *   records are protected and the first pass has been scheduled
 */
async function attachMediaRetention (blindPeer, opts = {}) {
  if (!blindPeer || !blindPeer.opened || !blindPeer.db) {
    throw new Error('media retention requires a ready blind peer')
  }
  const maxAgeMs = positiveInteger(opts.maxAgeMs, DEFAULTS.maxAgeMs)
  const minIntervalMs = positiveInteger(opts.minIntervalMs, DEFAULTS.minIntervalMs)
  const stateFile = opts.stateFile || path.join(blindPeer.rocks.path, STATE_FILE)
  const now = opts.now || Date.now
  const log = opts.log || (() => {})

  const promoted = new Set() // key hex already at MESSAGE_CORE_PRIORITY, this process
  let lastPassAt = 0
  let pass = null
  let closed = false

  // Same lock as flush(), so nothing here interleaves with the relay's GC.
  // A debounced acquire can resolve false; that means try again, not skip.
  async function withLock (fn) {
    while (!(await blindPeer.lock.lock())) {
      if (closed || blindPeer.closing) return
    }
    try {
      if (!closed && !blindPeer.closing) await fn()
    } finally {
      blindPeer.lock.unlock()
    }
  }

  function promote (key, referrer) {
    const hex = b4a.toString(key, 'hex')
    if (promoted.has(hex)) return false
    promoted.add(hex)
    blindPeer.db.addCore({ key, priority: MESSAGE_CORE_PRIORITY, announce: false, referrer: referrer || null })
    return true
  }

  // Ahead of the relay's own record for the batch, so a message core is never
  // written at 0 first and then found by a pass.
  function onAddCores ({ request }) {
    let added = false
    for (const core of request.cores) added = promote(core.key, request.referrer) || added
    if (added) blindPeer.flush()
  }

  function onAddCore ({ record }) {
    if (promote(record.key, record.referrer)) blindPeer.flush()
  }

  async function promoteExisting () {
    if (readState(stateFile).promotedAt) return
    let count = 0
    await withLock(async () => {
      for await (const record of blindPeer.db.find('@blind-peer/cores')) {
        if (record.priority >= MESSAGE_CORE_PRIORITY) {
          promoted.add(b4a.toString(record.key, 'hex'))
          continue
        }
        if (promote(record.key, record.referrer)) count++
      }
      if (blindPeer.db.updated()) await blindPeer.db.flush()
      writeState(stateFile, { version: 1, promotedAt: now(), cores: count })
    })
    log('media retention protected ' + count + ' existing core(s)')
  }

  function onActivity () {
    if (closed || pass || now() - lastPassAt < minIntervalMs) return
    pass = sweep()
      .catch(error => log('media retention pass failed: ' + (error.message || error)))
      .finally(() => { pass = null })
  }

  async function sweep () {
    let cores = 0
    let bytes = 0
    await withLock(async () => {
      lastPassAt = now()
      // Promotions still waiting for the relay's periodic flush must land
      // before anything is selected.
      if (blindPeer.db.updated()) await blindPeer.db.flush()
      const cutoff = lastPassAt - maxAgeMs
      for await (const record of blindPeer.db.find('@blind-peer/cores-by-activity')) {
        if (closed || blindPeer.closing) break
        if (record.priority !== 0 || record.announce) continue
        if (record.bytesAllocated === 0 || record.updated >= cutoff) continue
        if (promoted.has(b4a.toString(record.key, 'hex'))) continue
        bytes += await clear(record)
        cores++
      }
      if (cores > 0) await blindPeer.db.flush()
    })
    log('media retention pass cleared ' + cores + ' core(s), ' + bytes + ' byte(s)')
  }

  async function clear (record) {
    const core = blindPeer.store.get({ key: record.key })
    await core.ready()
    try {
      const tracker = blindPeer.activeReplication.get(b4a.toString(core.discoveryKey, 'hex'))
      if (!tracker) return 0
      await tracker.refresh()
      return tracker.gc()
    } finally {
      await core.close().catch(() => {})
    }
  }

  blindPeer.on('add-cores-downgrade-announce', onAddCores)
  blindPeer.on('downgrade-announce', onAddCore)
  await promoteExisting()
  blindPeer.on('core-activity', onActivity)
  onActivity()

  return {
    async close () {
      closed = true
      blindPeer.removeListener('add-cores-downgrade-announce', onAddCores)
      blindPeer.removeListener('downgrade-announce', onAddCore)
      blindPeer.removeListener('core-activity', onActivity)
      if (pass) await pass
    }
  }
}

function readState (file) {
  try {
    const state = JSON.parse(fs.readFileSync(file, 'utf8'))
    return state && typeof state === 'object' ? state : {}
  } catch (_) {
    return {}
  }
}

function writeState (file, state) {
  const tmp = file + '.tmp'
  fs.writeFileSync(tmp, JSON.stringify(state) + '\n', { mode: 0o600 })
  fs.renameSync(tmp, file)
}

function positiveInteger (value, fallback) {
  return Number.isSafeInteger(value) && value > 0 ? value : fallback
}

module.exports = { attachMediaRetention, DEFAULTS, MESSAGE_CORE_PRIORITY }
