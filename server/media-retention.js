'use strict'

/**
 * Forgets mirrored images a week after they stopped arriving.
 *
 * Phones register message cores at priority 1 and media cores at priority 0
 * (core/lib/blind-mirror.js), so priority is what tells the two apart here.
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
 * activeReplication, lock). blind-peer is pinned at 3.12.2 in package.json
 * for exactly that reason.
 */

const b4a = require('b4a')

const DEFAULTS = {
  maxAgeMs: 7 * 24 * 60 * 60 * 1000,
  minIntervalMs: 60 * 60 * 1000
}

/**
 * @param {import('blind-peer')} blindPeer ready instance
 * @param {object} [opts]
 * @param {number} [opts.maxAgeMs] age of a core's newest block before it is cleared
 * @param {number} [opts.minIntervalMs] least time between two passes
 * @param {() => number} [opts.now] clock, for tests
 * @param {(message: string) => void} [opts.log]
 * @returns {{ close: () => Promise<void> }}
 */
function attachMediaRetention (blindPeer, opts = {}) {
  if (!blindPeer || !blindPeer.opened || !blindPeer.db) {
    throw new Error('media retention requires a ready blind peer')
  }
  const maxAgeMs = positiveInteger(opts.maxAgeMs, DEFAULTS.maxAgeMs)
  const minIntervalMs = positiveInteger(opts.minIntervalMs, DEFAULTS.minIntervalMs)
  const now = opts.now || Date.now
  const log = opts.log || (() => {})

  let lastPassAt = 0
  let pass = null
  let closed = false

  function onActivity () {
    if (closed || pass || now() - lastPassAt < minIntervalMs) return
    pass = sweep()
      .catch(error => log('media retention pass failed: ' + (error.message || error)))
      .finally(() => { pass = null })
  }

  async function sweep () {
    // Same lock as flush(), so a pass never interleaves with the relay's GC.
    if (!(await blindPeer.lock.lock())) return
    let cores = 0
    let bytes = 0
    try {
      lastPassAt = now()
      const cutoff = lastPassAt - maxAgeMs
      for await (const record of blindPeer.db.find('@blind-peer/cores-by-activity')) {
        if (closed || blindPeer.closing) break
        if (record.priority !== 0 || record.announce) continue
        if (record.bytesAllocated === 0 || record.updated >= cutoff) continue
        bytes += await clear(record)
        cores++
      }
      if (cores > 0) await blindPeer.db.flush()
    } finally {
      blindPeer.lock.unlock()
    }
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

  blindPeer.on('core-activity', onActivity)
  onActivity()

  return {
    async close () {
      closed = true
      blindPeer.removeListener('core-activity', onActivity)
      if (pass) await pass
    }
  }
}

function positiveInteger (value, fallback) {
  return Number.isSafeInteger(value) && value > 0 ? value : fallback
}

module.exports = { attachMediaRetention, DEFAULTS }
