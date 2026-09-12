'use strict'

/**
 * Client half of the bootstrap invite mailbox.
 *
 * A recipient with no conversations has no Hypercore for blind-peering to
 * replicate, so the first invite has nowhere to live. This tiny control-plane
 * mailbox carries it, along with the sender's core key, which is what breaks
 * the bootstrap cycle between two firewalled peers.
 *
 * The blind peer sees sender and recipient identity keys for routing. The
 * invite body is encrypted to the recipient and signed by the sender, so the
 * server can verify who deposited what without reading any of it.
 *
 * Two transports, one set of operations. Protomux rides the existing
 * authenticated Noise connection; HTTPS is for networks that drop HyperDHT's
 * UDP transport, and signs each read or delete instead. Only the transport
 * differs, so `putInvite` and `drainInvites` are written once against an
 * abstract `request` and composed with either.
 */

const ProtomuxRPC = require('protomux-rpc')
const c = require('compact-encoding')
const b4a = require('b4a')
const sodium = require('sodium-universal')
const HypercoreId = require('hypercore-id-encoding')
const HyperDHTAddress = require('hyperdht-address')

const PROTOCOL = 'zapp-invite-mailbox'
const CHANNEL_ID = b4a.from('zi')
const SIGNING_PREFIX = b4a.from('zapp-invite-mailbox:v1')
const PUT_METHOD = 'put'
const LIST_METHOD = 'list'
const ACK_METHOD = 'ack'
const DEFAULT_TIMEOUT_MS = 15000
const DEFAULT_DHT_PORT = 49737

// Matches the server. A direct invite is ~1 KB; a group invite grows by ~134
// bytes per participant, so this has to hold a realistic group.
const MAX_ENVELOPE_BYTES = 16 * 1024

// The server pages listings so a flooded mailbox stays drainable. Bound the
// rounds anyway: a server that always reports more must not spin us forever.
const MAX_DRAIN_PAGES = 8

// ── Operations ───────────────────────────────────────────────────────────────

/**
 * Deposit one invite. `request` comes from a transport below.
 * @param {Function} request (method, body) => Promise<Object>
 * @param {{publicKey: Buffer, secretKey: Buffer}} keyPair sender identity
 * @param {string} recipientKeyHex
 * @param {Object} invite
 * @returns {Promise<boolean>}
 */
async function putInvite (request, keyPair, recipientKeyHex, invite) {
  await request(PUT_METHOD, {
    recipient: recipientKeyHex,
    envelope: encryptInvite(invite, keyPair, recipientKeyHex)
  })
  return true
}

/**
 * Take everything addressed to us, hand each entry to `deliver`, and only then
 * acknowledge it.
 *
 * The order matters: acknowledging is a delete, and the mailbox holds the only
 * copy of a bootstrap invite. Acknowledging first means a lost ack response, or
 * a conversation that fails to initialize, destroys the invite with no way to
 * ask for it again.
 *
 * Undecryptable entries are acknowledged without delivery: a mailbox anyone can
 * deposit into is one anyone can pollute, and leaving junk in place would starve
 * later real invites.
 * @param {Function} request
 * @param {{publicKey: Buffer, secretKey: Buffer}} keyPair our identity
 * @param {Function} [deliver] async (entry) => boolean; entry is finished with on true
 * @returns {Promise<Array<{id: string, invite?: Object, senderKeyHex?: string, invalid?: boolean}>>}
 */
async function drainInvites (request, keyPair, deliver) {
  const accepted = []
  for (let round = 0; round < MAX_DRAIN_PAGES; round++) {
    const page = asPage(await request(LIST_METHOD, { ids: [] }))
    const decoded = decodeEntries(page.entries, keyPair)
    if (decoded.length === 0) break

    const finished = []
    for (const entry of decoded) {
      try {
        if (entry.invalid || !deliver || await deliver(entry)) finished.push(entry.id)
      } catch (_) {
        // A transient application failure belongs to this entry. Other entries
        // can still be applied and acknowledged on their originating store.
      }
    }
    accepted.push(...decoded)

    // Nothing could be finished, so nothing can be deleted. Stop rather than
    // re-list the same entries; a later drain retries them.
    if (finished.length === 0) break
    await request(ACK_METHOD, { ids: finished })
    if (!page.more) break
  }
  return accepted
}

/**
 * Listings used to be a bare array, before the server paged them. Accept both
 * shapes: an app can reach a blind peer that has not been upgraded yet, and
 * reading the new shape off the old reply would silently find no invites.
 */
function asPage (reply) {
  if (Array.isArray(reply)) return { entries: reply, more: false }
  return { entries: (reply && reply.entries) || [], more: !!(reply && reply.more) }
}

function decodeEntries (entries, keyPair) {
  const decoded = []
  if (JSON.stringify(entries).length > 256 * 1024) throw new Error('mailbox page too large')
  for (const entry of (Array.isArray(entries) ? entries : []).slice(0, 100)) {
    if (!entry || typeof entry.id !== 'string') continue
    try {
      decoded.push({ id: entry.id, ...decryptInvite(entry.envelope, keyPair) })
    } catch (_) {
      decoded.push({ id: entry.id, invalid: true })
    }
  }
  return decoded
}

// ── Transports ───────────────────────────────────────────────────────────────

/**
 * Run `operation` over a Protomux channel on a fresh Noise connection to the
 * blind peer. The connection key is the caller's identity, so nothing here
 * needs to be signed separately.
 */
async function throughDht (dht, keyPair, blindPeerKey, address, operation, opts = {}) {
  const timeout = opts.timeout || DEFAULT_TIMEOUT_MS
  const deadline = Date.now() + (opts.operationTimeout || 30000)
  const connection = dht.connect(encodeTarget(blindPeerKey, address), { keyPair })
  const rpc = new ProtomuxRPC(connection, {
    protocol: PROTOCOL,
    id: CHANNEL_ID,
    valueEncoding: c.string
  })
  const request = async (method, body) => {
    const remaining = Math.min(timeout, deadline - Date.now())
    if (remaining <= 0) throw new Error('mailbox operation timeout')
    const raw = await rpc.request(method, JSON.stringify(body), {
      requestEncoding: c.string,
      responseEncoding: method === LIST_METHOD ? c.string : c.none,
      timeout: remaining
    })
    return method === LIST_METHOD ? JSON.parse(raw) : {}
  }
  try {
    return await operation(request)
  } finally {
    try { rpc.destroy() } catch (_) {}
    try { connection.destroy() } catch (_) {}
  }
}

/**
 * Run `operation` over plain HTTPS. There is no connection identity here, so
 * reads and deletes carry an Ed25519 signature over the request; deposits rely
 * on the envelope's own signature.
 *
 * Nothing leaves this module unencrypted, but the socket belongs to the host:
 * Bare's TLS addon is not dependable across Android vendor builds and iOS has
 * no equivalent, so the host app carries the already-opaque JSON.
 */
async function throughHttps (baseUrl, keyPair, operation, opts = {}) {
  if (typeof opts.postJson !== 'function') {
    throw new Error('invite mailbox HTTPS requires a host transport')
  }
  const base = String(baseUrl || '').replace(/\/+$/, '')
  if (!/^https:\/\//i.test(base)) throw new Error('invite mailbox URL must use HTTPS')
  const deadline = Date.now() + (opts.operationTimeout || 30000)
  const request = async (method, body) => {
    const payload = method === PUT_METHOD
      ? body
      : createAuthRequest(method, keyPair, body.ids || [])
    const timeout = Math.min(opts.timeout || DEFAULT_TIMEOUT_MS, deadline - Date.now())
    if (timeout <= 0) throw new Error('mailbox operation timeout')
    let timer
    try {
      return await Promise.race([
        opts.postJson(base + '/' + method, payload),
        new Promise((resolve, reject) => {
          timer = setTimeout(() => reject(new Error('mailbox request timeout')), timeout)
        })
      ]) || {}
    } finally { clearTimeout(timer) }
  }
  return await operation(request)
}

// ── Envelope ─────────────────────────────────────────────────────────────────

function signedBytes (recipient, ephemeralPublicKey, nonce, ciphertext) {
  return b4a.concat([SIGNING_PREFIX, recipient, ephemeralPublicKey, nonce, ciphertext])
}

function encryptInvite (invite, keyPair, recipientKeyHex) {
  const recipient = b4a.from(recipientKeyHex, 'hex')
  if (recipient.byteLength !== sodium.crypto_sign_PUBLICKEYBYTES) {
    throw new Error('invite mailbox recipient must be a 32-byte Ed25519 key')
  }
  requireKeyPair(keyPair)

  const recipientCurve = b4a.alloc(sodium.crypto_box_PUBLICKEYBYTES)
  sodium.crypto_sign_ed25519_pk_to_curve25519(recipientCurve, recipient)

  const ephemeralPublicKey = b4a.alloc(sodium.crypto_box_PUBLICKEYBYTES)
  const ephemeralSecretKey = b4a.alloc(sodium.crypto_box_SECRETKEYBYTES)
  sodium.crypto_box_keypair(ephemeralPublicKey, ephemeralSecretKey)

  const nonce = b4a.alloc(sodium.crypto_box_NONCEBYTES)
  sodium.randombytes_buf(nonce)
  const plaintext = b4a.from(JSON.stringify(invite))
  const ciphertext = b4a.alloc(plaintext.byteLength + sodium.crypto_box_MACBYTES)
  sodium.crypto_box_easy(ciphertext, plaintext, nonce, recipientCurve, ephemeralSecretKey)

  const signature = b4a.alloc(sodium.crypto_sign_BYTES)
  sodium.crypto_sign_detached(
    signature,
    signedBytes(recipient, ephemeralPublicKey, nonce, ciphertext),
    keyPair.secretKey
  )

  const envelope = JSON.stringify({
    v: 1,
    sender: b4a.toString(keyPair.publicKey, 'hex'),
    epk: b4a.toString(ephemeralPublicKey, 'hex'),
    nonce: b4a.toString(nonce, 'hex'),
    ciphertext: b4a.toString(ciphertext, 'hex'),
    signature: b4a.toString(signature, 'hex')
  })
  if (b4a.byteLength(envelope) > MAX_ENVELOPE_BYTES) {
    throw new Error('invite mailbox envelope too large')
  }
  return envelope
}

function decryptInvite (envelopeString, keyPair) {
  if (typeof envelopeString !== 'string' || b4a.byteLength(envelopeString) > MAX_ENVELOPE_BYTES) {
    throw new Error('invalid invite mailbox envelope')
  }
  const envelope = JSON.parse(envelopeString)
  if (!envelope || envelope.v !== 1) throw new Error('unsupported invite mailbox envelope')

  const sender = b4a.from(envelope.sender || '', 'hex')
  const ephemeralPublicKey = b4a.from(envelope.epk || '', 'hex')
  const nonce = b4a.from(envelope.nonce || '', 'hex')
  const ciphertext = b4a.from(envelope.ciphertext || '', 'hex')
  const signature = b4a.from(envelope.signature || '', 'hex')
  if (sender.byteLength !== sodium.crypto_sign_PUBLICKEYBYTES ||
      ephemeralPublicKey.byteLength !== sodium.crypto_box_PUBLICKEYBYTES ||
      nonce.byteLength !== sodium.crypto_box_NONCEBYTES ||
      ciphertext.byteLength < sodium.crypto_box_MACBYTES ||
      signature.byteLength !== sodium.crypto_sign_BYTES) {
    throw new Error('malformed invite mailbox envelope')
  }

  if (!sodium.crypto_sign_verify_detached(
    signature,
    signedBytes(keyPair.publicKey, ephemeralPublicKey, nonce, ciphertext),
    sender
  )) {
    throw new Error('invite mailbox signature verification failed')
  }

  const recipientCurveSecret = b4a.alloc(sodium.crypto_box_SECRETKEYBYTES)
  sodium.crypto_sign_ed25519_sk_to_curve25519(recipientCurveSecret, keyPair.secretKey)
  const plaintext = b4a.alloc(ciphertext.byteLength - sodium.crypto_box_MACBYTES)
  if (!sodium.crypto_box_open_easy(
    plaintext, ciphertext, nonce, ephemeralPublicKey, recipientCurveSecret
  )) {
    throw new Error('invite mailbox decryption failed')
  }

  return { invite: JSON.parse(b4a.toString(plaintext)), senderKeyHex: b4a.toString(sender, 'hex') }
}

// ── Request authentication (HTTPS only) ──────────────────────────────────────

function authBytes (action, identityKeyHex, timestamp, nonceHex, ids = []) {
  return b4a.from(JSON.stringify({
    v: 1,
    action,
    identity: identityKeyHex,
    timestamp,
    nonce: nonceHex,
    ids
  }))
}

function createAuthRequest (action, keyPair, ids = []) {
  requireKeyPair(keyPair)
  const identity = b4a.toString(keyPair.publicKey, 'hex')
  const timestamp = Date.now()
  const nonceBuffer = b4a.alloc(16)
  sodium.randombytes_buf(nonceBuffer)
  const nonce = b4a.toString(nonceBuffer, 'hex')
  const signature = b4a.alloc(sodium.crypto_sign_BYTES)
  sodium.crypto_sign_detached(
    signature,
    authBytes(action, identity, timestamp, nonce, ids),
    keyPair.secretKey
  )
  return {
    v: 1,
    action,
    identity,
    timestamp,
    nonce,
    ids,
    signature: b4a.toString(signature, 'hex')
  }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function encodeTarget (blindPeerKey, address) {
  const raw = b4a.isBuffer(blindPeerKey) ? blindPeerKey : HypercoreId.decode(blindPeerKey)
  if (!address || !address.host) return raw
  return HyperDHTAddress.encode(raw, [{
    host: address.host,
    port: address.port || DEFAULT_DHT_PORT
  }])
}

function requireKeyPair (keyPair) {
  if (!keyPair || !keyPair.publicKey || !keyPair.secretKey) {
    throw new Error('invite mailbox requires an identity key pair')
  }
}

module.exports = {
  PROTOCOL,
  CHANNEL_ID,
  PUT_METHOD,
  LIST_METHOD,
  ACK_METHOD,
  MAX_ENVELOPE_BYTES,
  MAX_DRAIN_PAGES,
  authBytes,
  createAuthRequest,
  encryptInvite,
  decryptInvite,
  decodeEntries,
  putInvite,
  drainInvites,
  throughDht,
  throughHttps
}
