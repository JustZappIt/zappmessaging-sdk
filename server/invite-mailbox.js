'use strict'

/**
 * Server half of the bootstrap invite mailbox.
 *
 * A recipient with no conversations has no Hypercore for blind-peering to
 * replicate, so the first invite has nowhere to live. This mailbox holds it.
 * The body is encrypted to the recipient and signed by the sender; the server
 * only routes an opaque envelope and never learns conversation contents.
 *
 * Two transports reach the same three operations. Protomux rides the existing
 * authenticated Noise connection, so the caller's identity is the connection
 * key. HTTPS is for networks that drop HyperDHT's UDP entirely; it has no
 * connection identity, so reads and deletes carry a signed request instead.
 */

const fs = require('fs')
const path = require('path')
const crypto = require('crypto')
const http = require('http')
const b4a = require('b4a')
const c = require('compact-encoding')
const ProtomuxRPC = require('protomux-rpc')
const sodium = require('sodium-universal')

const PROTOCOL = 'zapp-invite-mailbox'
const CHANNEL_ID = b4a.from('zi')
const SIGNING_PREFIX = b4a.from('zapp-invite-mailbox:v1')

// A direct invite encodes to ~1 KB, but a group invite carries every
// participant's key, so it grows with the group: ~134 bytes each, which puts a
// 100-member group near 15 KB. Sizing this for DMs alone silently pushed larger
// groups onto the DHT path, which is the path that does not work for the
// firewalled peers the mailbox exists to serve.
//
// Headroom is what an attacker gets to spend filling a stranger's mailbox, so
// the per-recipient budget below bounds bytes rather than entries. That is the
// quantity that actually matters, and it lets one large invite cost what many
// small ones would.
const MAX_ENVELOPE_BYTES = 16 * 1024
const MAX_HTTP_BODY_BYTES = 32 * 1024

// The client refuses a listing larger than 256 KB, so the server must never
// build one. A mailbox that cannot be listed can never be acked either, which
// would leave a poisoned mailbox poisoned forever. Pages stay well under the
// client limit and `more` tells the client to come back for the rest.
const MAX_LIST_BYTES = 128 * 1024

const AUTH_MAX_AGE_MS = 5 * 60 * 1000
const RECIPIENT_KEY = /^[0-9a-f]{64}$/

// Replay protection only has to remember one freshness window, but anyone can
// mint an identity, so the table needs a ceiling of its own.
const MAX_TRACKED_NONCES = 10000

const DEFAULTS = {
  maxPerRecipient: 100,
  maxPerSender: 8,
  maxBytesPerRecipient: 256 * 1024,
  maxTotal: 10000,
  ttlMs: 30 * 24 * 60 * 60 * 1000,
  httpPort: 49739,
  httpHost: '127.0.0.1',
  maxHttpRequestsPerMinute: 600
}

/**
 * One JSON file per recipient. A put rewrites only that recipient's file, so
 * mailbox size never turns a single store into a multi-megabyte synchronous
 * write on the event loop that also serves core replication.
 */
class MailboxStore {
  constructor (opts = {}) {
    const directory = opts.directory
    if (!directory || !path.isAbsolute(directory)) {
      throw new Error('invite mailbox directory must be absolute')
    }
    this.directory = directory
    this.maxPerRecipient = positiveInteger(opts.maxPerRecipient, DEFAULTS.maxPerRecipient)
    this.maxPerSender = positiveInteger(opts.maxPerSender, DEFAULTS.maxPerSender)
    this.maxBytesPerRecipient = positiveInteger(
      opts.maxBytesPerRecipient, DEFAULTS.maxBytesPerRecipient
    )
    this.maxTotal = positiveInteger(opts.maxTotal, DEFAULTS.maxTotal)
    this.ttlMs = positiveInteger(opts.ttlMs, DEFAULTS.ttlMs)
    fs.mkdirSync(this.directory, { recursive: true, mode: 0o700 })
    this._total = 0
    this._maintenanceRecipients = new Set()
    this._cleanupBatchSize = positiveInteger(opts.cleanupBatchSize, 16)
    this._log = opts.log || (() => {})
    this._importLegacyFile()
    this._total = this._countAll()
    this._maintenanceTimer = setInterval(() => this.reclaimExpired(),
      positiveInteger(opts.cleanupIntervalMs, 1000))
    this._maintenanceTimer.unref()

  }

  put (recipient, envelope, expectedSender) {
    const sender = verifyEnvelope(envelope, recipient, expectedSender)
    const entries = this._read(recipient)
    if (entries.length >= this.maxPerRecipient) {
      throw new Error('invite mailbox recipient is full')
    }
    if (entries.filter(entry => entry.sender === sender).length >= this.maxPerSender) {
      throw new Error('invite mailbox sender quota exceeded')
    }
    const storedBytes = entries.reduce((total, entry) => total + entry.envelope.length, 0)
    if (storedBytes + envelope.length > this.maxBytesPerRecipient) {
      throw new Error('invite mailbox recipient byte quota exceeded')
    }
    if (this._total >= this.maxTotal) this.reclaimExpired()
    if (this._total >= this.maxTotal) {
      throw new Error('invite mailbox is full')
    }
    const previous = entries.length
    entries.push({
      id: crypto.randomBytes(16).toString('hex'),
      sender,
      envelope,
      createdAt: Date.now()
    })
    this._write(recipient, entries, previous)
    return sender
  }

  /**
   * A bounded page of the recipient's mailbox. `more` is true when entries
   * were withheld, so a client draining junk makes progress every round
   * instead of stalling on an unlistable mailbox.
   */
  list (recipient) {
    const entries = this._read(recipient)
    const page = []
    let bytes = 0
    for (const entry of entries) {
      bytes += entry.envelope.length + entry.id.length + 32
      // Always yield at least one entry. An empty page reads to the client as
      // "nothing to do", so a single entry over budget would stall the drain
      // rather than page it.
      if (bytes > MAX_LIST_BYTES && page.length > 0) break
      page.push({ id: entry.id, envelope: entry.envelope })
    }
    return { entries: page, more: page.length < entries.length }
  }

  ack (recipient, ids) {
    const accepted = new Set(ids)
    if (accepted.size === 0) return 0
    const entries = this._read(recipient)
    const remaining = entries.filter(entry => !accepted.has(entry.id))
    if (remaining.length === entries.length) return 0
    this._write(recipient, remaining, entries.length)
    return entries.length - remaining.length
  }

  /**
   * The only place a recipient key becomes a filesystem path, so the key format
   * is enforced here rather than relying on each caller. A recipient is a 32-byte
   * hex key and can therefore never contain a path separator.
   */
  _pathFor (recipient) {
    if (!RECIPIENT_KEY.test(recipient)) {
      throw new Error('invite mailbox recipient must be a 32-byte hex key')
    }
    return path.join(this.directory, recipient + '.json')
  }

  /**
   * Reads a recipient's entries, dropping any that have aged out.
   *
   * The drop is persisted rather than filtered in memory. `_total` is only
   * adjusted by a write, so an in-memory-only prune would leave expired entries
   * counted against the global ceiling forever: once it filled, every deposit
   * for every recipient would fail until the process restarted, even with
   * nothing but expired entries stored.
   */
  _read (recipient) {
    let parsed
    try {
      parsed = JSON.parse(fs.readFileSync(this._pathFor(recipient), 'utf8'))
    } catch (error) {
      // Missing is empty; unreadable/corrupt is not. Never overwrite accepted
      // invitations after a failed read or silently discount their capacity.
      if (error.code === 'ENOENT' || !RECIPIENT_KEY.test(recipient)) return []
      throw error
    }
    if (!Array.isArray(parsed)) throw new Error('invalid mailbox file')
    const cutoff = Date.now() - this.ttlMs
    const current = parsed.filter(entry => (
      entry &&
      typeof entry.id === 'string' &&
      typeof entry.envelope === 'string' &&
      Number.isFinite(entry.createdAt) &&
      entry.createdAt >= cutoff
    ))
    if (current.length !== parsed.length) this._write(recipient, current, parsed.length)
    return current
  }

  _write (recipient, entries, previous) {
    const target = this._pathFor(recipient)
    if (entries.length === 0) {
      try { fs.unlinkSync(target) } catch (error) {
        if (error.code !== 'ENOENT') throw error
      }
    } else {
      const temp = target + '.tmp'
      fs.writeFileSync(temp, JSON.stringify(entries), { mode: 0o600 })
      fs.renameSync(temp, target)
    }
    if (entries.length) this._maintenanceRecipients.add(recipient)
    else this._maintenanceRecipients.delete(recipient)
    this._total += entries.length - previous
    if (this._total < 0) this._total = 0
  }

  _countAll () {
    let total = 0
    for (const recipient of this._recipients()) {
      const count = this._read(recipient).length
      total += count
      if (count) this._maintenanceRecipients.add(recipient)
    }
    return total
  }

  // Rotate a bounded in-memory recipient index, populated once at startup
  // and maintained only after successful writes. No per-request directory scan.
  reclaimExpired () {
    const count = Math.min(this._cleanupBatchSize, this._maintenanceRecipients.size)
    for (let i = 0; i < count; i++) {
      const recipient = this._maintenanceRecipients.values().next().value
      this._maintenanceRecipients.delete(recipient)
      this._maintenanceRecipients.add(recipient)
      try { this._read(recipient) } catch (error) {
        this._log('invite mailbox expiration failed: ' + error.message)
      }
    }
  }

  close () {
    clearInterval(this._maintenanceTimer)
    this._maintenanceTimer = null
  }

  _recipients () {
    return fs.readdirSync(this.directory)
      .filter(name => /^[0-9a-f]{64}\.json$/.test(name))
      .map(name => name.slice(0, 64))
  }

  /**
   * Earlier builds kept every mailbox in one `invite-mailbox.json` beside this
   * directory. Fold it in once so an upgrade does not silently drop invites
   * that were already accepted.
   */
  _importLegacyFile () {
    const legacyPath = this.directory + '.json'
    let legacy
    try {
      legacy = JSON.parse(fs.readFileSync(legacyPath, 'utf8'))
    } catch (_) {
      return
    }
    if (legacy && typeof legacy === 'object' && !Array.isArray(legacy)) {
      for (const [recipient, entries] of Object.entries(legacy)) {
        if (!RECIPIENT_KEY.test(recipient) || !Array.isArray(entries)) continue
        this._write(recipient, entries.slice(0, this.maxPerRecipient), 0)
      }
    }
    try { fs.unlinkSync(legacyPath) } catch (_) {}
  }
}

/**
 * Serve the mailbox on a blind peer's swarm, and optionally over HTTP for
 * clients whose network drops UDP. Bind HTTP to localhost and let the existing
 * TLS terminator publish it.
 * @param {Object} blindPeer ready blind-peer instance
 * @param {Object} opts
 * @returns {{ store: MailboxStore, listenHttp: Function, close: Function }}
 */
function attachInviteMailbox (blindPeer, opts = {}) {
  if (!blindPeer || !blindPeer.swarm) {
    throw new Error('invite mailbox requires a ready blind peer')
  }
  const store = opts.store || new MailboxStore(opts)
  const log = opts.log || (() => {})
  const rpcs = new Set()
  const usedNonces = new Map()
  const allowHttpRequest = createRateLimiter(
    positiveInteger(opts.maxHttpRequestsPerMinute, DEFAULTS.maxHttpRequestsPerMinute)
  )
  let httpServer = null

  function onConnection (connection) {
    const remote = connection.remotePublicKey
    if (!remote || remote.byteLength !== sodium.crypto_sign_PUBLICKEYBYTES) return
    const identity = b4a.toString(remote, 'hex')

    let rpc
    try {
      rpc = new ProtomuxRPC(connection, {
        protocol: PROTOCOL,
        id: CHANNEL_ID,
        valueEncoding: c.string
      })
    } catch (_) {
      return
    }
    rpcs.add(rpc)
    const forget = () => rpcs.delete(rpc)
    rpc.once('close', forget)
    rpc.once('destroy', forget)

    // The Noise connection already proves who the caller is, so put binds the
    // envelope to it and list/ack need no further authentication.
    respond(rpc, 'put', c.none, (request) => {
      store.put(readRecipient(request), readEnvelope(request), identity)
      log('invite mailbox stored')
    })
    respond(rpc, 'list', c.string, () => JSON.stringify(store.list(identity)))
    respond(rpc, 'ack', c.none, (request) => {
      if (store.ack(identity, readIds(request))) log('invite mailbox acknowledged')
    })
  }

  async function onHttpRequest (request, response) {
    try {
      if (request.method !== 'POST') {
        return sendJson(response, 405, { error: 'method not allowed' })
      }
      // This endpoint faces the public internet and `put` cannot authenticate
      // its caller. The quotas bound what a flood can store; this bounds what
      // it can cost. Deliberately in aggregate rather than per address: the
      // reverse proxy is the only thing that sees a real client address.
      if (!allowHttpRequest()) {
        return sendJson(response, 429, { error: 'rate limited' })
      }
      const action = String(request.url || '').split('?')[0].replace(/^\/+|\/+$/g, '')
      const body = await readJson(request)

      // Anyone may deposit: a sender cannot prove anything to this endpoint
      // before the recipient has ever been online. The envelope's own
      // signature is what binds it to a sender.
      if (action === 'put') {
        store.put(readRecipient(body), readEnvelope(body), null)
        log('invite mailbox stored')
        return sendJson(response, 200, { ok: true })
      }

      // Reads and deletes expose or destroy someone's mail, so they carry a
      // signature from the identity that owns the mailbox.
      if (action === 'list') {
        return sendJson(response, 200, store.list(verifyAuth(body, 'list', usedNonces)))
      }
      if (action === 'ack') {
        const identity = verifyAuth(body, 'ack', usedNonces)
        if (store.ack(identity, body.ids)) log('invite mailbox acknowledged')
        return sendJson(response, 200, { ok: true })
      }

      return sendJson(response, 404, { error: 'not found' })
    } catch (_) {
      return sendJson(response, 400, { error: 'invalid mailbox request' })
    }
  }

  blindPeer.swarm.on('connection', onConnection)

  return {
    store,
    listenHttp (port = DEFAULTS.httpPort, host = DEFAULTS.httpHost) {
      if (httpServer) throw new Error('invite mailbox HTTP server already started')
      httpServer = http.createServer(onHttpRequest)
      return new Promise((resolve, reject) => {
        httpServer.once('error', reject)
        httpServer.listen(port, host, () => {
          httpServer.removeListener('error', reject)
          resolve()
        })
      })
    },
    /** The bound address, so a log line reports what is listening rather than what was asked for. */
    httpAddress () {
      return httpServer ? httpServer.address() : null
    },
    async close () {
      store.close()
      blindPeer.swarm.removeListener('connection', onConnection)
      for (const rpc of rpcs) {
        try { rpc.destroy() } catch (_) {}
      }
      rpcs.clear()
      usedNonces.clear()
      if (httpServer) {
        const server = httpServer
        httpServer = null
        await new Promise(resolve => server.close(resolve))
      }
    }
  }
}

function respond (rpc, method, responseEncoding, handler) {
  rpc.respond(method, { requestEncoding: c.string, responseEncoding }, (raw) => (
    handler(raw ? JSON.parse(raw) : {})
  ))
}

function readRecipient (request) {
  const recipient = normalizeKey(request && request.recipient)
  if (!recipient) throw new Error('invalid invite mailbox recipient')
  return recipient
}

function readEnvelope (request) {
  const envelope = request && request.envelope
  if (typeof envelope !== 'string' || Buffer.byteLength(envelope) > MAX_ENVELOPE_BYTES) {
    throw new Error('invalid invite mailbox envelope')
  }
  return envelope
}

function readIds (request) {
  const ids = request && request.ids
  return Array.isArray(ids) ? ids.filter(validId) : []
}

/**
 * Check an envelope's shape and its sender signature.
 * @param {string} envelopeString
 * @param {string} recipientHex mailbox the envelope is being filed under
 * @param {string|null} expectedSender connection identity, when the transport has one
 * @returns {string} verified sender key, hex
 */
function verifyEnvelope (envelopeString, recipientHex, expectedSender) {
  let envelope
  try {
    envelope = JSON.parse(envelopeString)
  } catch (_) {
    throw new Error('invalid invite mailbox envelope')
  }
  if (!envelope || envelope.v !== 1) throw new Error('unsupported invite mailbox envelope')

  const recipient = b4a.from(recipientHex, 'hex')
  const sender = b4a.from(envelope.sender || '', 'hex')
  const ephemeralPublicKey = b4a.from(envelope.epk || '', 'hex')
  const nonce = b4a.from(envelope.nonce || '', 'hex')
  const ciphertext = b4a.from(envelope.ciphertext || '', 'hex')
  const signature = b4a.from(envelope.signature || '', 'hex')
  const senderHex = b4a.toString(sender, 'hex')

  if (recipient.byteLength !== sodium.crypto_sign_PUBLICKEYBYTES ||
      sender.byteLength !== sodium.crypto_sign_PUBLICKEYBYTES ||
      ephemeralPublicKey.byteLength !== sodium.crypto_box_PUBLICKEYBYTES ||
      nonce.byteLength !== sodium.crypto_box_NONCEBYTES ||
      ciphertext.byteLength < sodium.crypto_box_MACBYTES ||
      signature.byteLength !== sodium.crypto_sign_BYTES ||
      (expectedSender && senderHex !== expectedSender)) {
    throw new Error('malformed invite mailbox envelope')
  }

  const signed = b4a.concat([SIGNING_PREFIX, recipient, ephemeralPublicKey, nonce, ciphertext])
  if (!sodium.crypto_sign_verify_detached(signature, signed, sender)) {
    throw new Error('invite mailbox signature verification failed')
  }
  return senderHex
}

function authBytes (action, identity, timestamp, nonce, ids) {
  return b4a.from(JSON.stringify({ v: 1, action, identity, timestamp, nonce, ids }))
}

/**
 * Verify a signed HTTP list/ack request and record its nonce, so a captured
 * request cannot be replayed inside the freshness window.
 * @returns {string} the authenticated identity, hex
 */
function verifyAuth (request, expectedAction, usedNonces) {
  const identity = normalizeKey(request && request.identity)
  const timestamp = request && request.timestamp
  const nonce = typeof request.nonce === 'string' ? request.nonce.toLowerCase() : ''
  const signature = b4a.from((request && request.signature) || '', 'hex')
  const ids = request && request.ids

  if (request.v !== 1 || request.action !== expectedAction || !identity ||
      !Number.isSafeInteger(timestamp) || Math.abs(Date.now() - timestamp) > AUTH_MAX_AGE_MS ||
      !/^[0-9a-f]{32}$/.test(nonce) || signature.byteLength !== sodium.crypto_sign_BYTES ||
      !Array.isArray(ids) || ids.some(id => !validId(id))) {
    throw new Error('invalid invite mailbox authentication')
  }

  // Signature first, so an unauthenticated caller cannot reach the nonce table
  // at all, let alone grow it.
  if (!sodium.crypto_sign_verify_detached(
    signature,
    authBytes(expectedAction, identity, timestamp, nonce, ids),
    b4a.from(identity, 'hex')
  )) {
    throw new Error('invite mailbox authentication failed')
  }

  const cutoff = Date.now() - AUTH_MAX_AGE_MS
  for (const [key, seenAt] of usedNonces) {
    if (seenAt >= cutoff) break // insertion-ordered, so the rest are current
    usedNonces.delete(key)
  }
  // Anyone can mint a keypair, so freshness alone does not bound this table.
  // Evicting the oldest only ever discards entries that are closest to aging
  // out anyway, and the rate limit keeps the cap out of reach in practice.
  while (usedNonces.size >= MAX_TRACKED_NONCES) {
    usedNonces.delete(usedNonces.keys().next().value)
  }

  const replayKey = identity + ':' + nonce
  if (usedNonces.has(replayKey)) throw new Error('replayed invite mailbox request')
  usedNonces.set(replayKey, Date.now())
  return identity
}

function readJson (request) {
  return new Promise((resolve, reject) => {
    const chunks = []
    let length = 0
    request.on('data', chunk => {
      length += chunk.length
      if (length > MAX_HTTP_BODY_BYTES) {
        reject(new Error('invite mailbox request too large'))
        request.destroy()
        return
      }
      chunks.push(chunk)
    })
    request.on('error', reject)
    request.on('end', () => {
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')))
      } catch (error) {
        reject(error)
      }
    })
  })
}

function sendJson (response, status, body) {
  const raw = JSON.stringify(body)
  response.writeHead(status, {
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(raw),
    'cache-control': 'no-store'
  })
  response.end(raw)
}

function normalizeKey (value) {
  const key = typeof value === 'string' ? value.toLowerCase() : ''
  return RECIPIENT_KEY.test(key) ? key : null
}

function validId (value) {
  return typeof value === 'string' && /^[0-9a-f]{32}$/.test(value)
}

function positiveInteger (value, fallback) {
  return Number.isSafeInteger(value) && value > 0 ? value : fallback
}

/** Fixed-window counter. Returns false once the window's allowance is spent. */
function createRateLimiter (maxPerMinute) {
  let windowStartedAt = Date.now()
  let count = 0
  return () => {
    const now = Date.now()
    if (now - windowStartedAt >= 60_000) {
      windowStartedAt = now
      count = 0
    }
    if (count >= maxPerMinute) return false
    count++
    return true
  }
}

module.exports = {
  attachInviteMailbox,
  MailboxStore,
  DEFAULTS,
  MAX_ENVELOPE_BYTES,
  MAX_LIST_BYTES
}
