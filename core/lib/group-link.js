'use strict'

/**
 * Group invite links: the link codec and every signature the join protocol
 * uses. Pure functions only, no I/O, so each rule is testable under Node and
 * identical on iOS and Android (both run this file inside the worklet).
 *
 * A link is https://join.justzappit.xyz/g/v1#<base64url(payload)>. The payload
 * rides in the fragment, which an HTTP client never sends, so the bearer secret
 * reaches no server, proxy, Referer header or preview crawler.
 *
 * Payload, version 1:
 *   version (1) | flags (1) | secret (16) | rendezvousPublicKey (32)
 *   | expiresAt u32 BE (4, if FLAG_EXPIRY) | nameLength (1) + name (if FLAG_NAME)
 *   | checksum (4)
 *
 * The link deliberately carries neither the groupId (the group's encryption
 * key seed) nor the owner's identity key. The rendezvous key is random per
 * link and only its owner holds the secret half, so link holders can deposit
 * join requests for it but cannot read or delete each other's.
 *
 * Never log a link, a payload, a secret or a record built from one.
 */

const b4a = require('b4a')
const sodium = require('sodium-universal')

const VERSION = 1
const LINK_HOST = 'join.justzappit.xyz'
const LINK_PATH = '/g/v1'
const HANDOFF_SCHEME = 'xyz.justzappit.zapp'
const MAX_URI_BYTES = 16 * 1024
const SECRET_BYTES = 16
const LINK_ID_BYTES = 16
const NONCE_BYTES = 16
const KEY_BYTES = 32
const CHECKSUM_BYTES = 4
const MAX_NAME_BYTES = 48
const MAX_JOINER_NAME_BYTES = 255
const FLAG_NAME = 0x01
const FLAG_EXPIRY = 0x02
const KNOWN_FLAGS = FLAG_NAME | FLAG_EXPIRY

// Result statuses travel as one byte inside the signed result message.
const RESULT_STATUS = Object.freeze({
  inactive: 1,
  expired: 2,
  full: 3,
  declined: 4,
  pending_approval: 5
})

const TAG = Object.freeze({
  check: 'zapp/group-link/v1/check',
  id: 'zapp/group-link/v1/id',
  proof: 'zapp/group-link/v1/proof',
  topic: 'zapp/group-link/v1/topic',
  requestLink: 'zapp/group-link/v1/request/link',
  requestJoiner: 'zapp/group-link/v1/request/joiner',
  admit: 'zapp/group-link/v1/admit',
  result: 'zapp/group-link/v1/result'
})

class GroupLinkError extends Error {
  /**
   * @param {'MALFORMED'|'TOO_LARGE'|'UNSUPPORTED_VERSION'|'NEWER_FORMAT'} code
   */
  constructor (code) {
    // The message is the code alone: a message quoting the input would quote
    // the bearer secret.
    super('Group link rejected: ' + code)
    this.code = code
  }
}

function fail (code) {
  throw new GroupLinkError(code)
}

// ── Encoding helpers ────────────────────────────────────────────────────────

function toBase64Url (buf) {
  return b4a.toString(buf, 'base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

/**
 * Strict base64url: the alphabet only, no padding, and canonical, so one
 * payload has exactly one string. Checked by re-encoding, which also rejects
 * non-zero trailing bits.
 */
function fromBase64Url (text) {
  if (typeof text !== 'string' || text.length === 0 || !/^[A-Za-z0-9_-]+$/.test(text)) fail('MALFORMED')
  if (text.length % 4 === 1) fail('MALFORMED')
  const standard = text.replace(/-/g, '+').replace(/_/g, '/')
  const padded = standard + '='.repeat((4 - (standard.length % 4)) % 4)
  const bytes = b4a.from(padded, 'base64')
  if (toBase64Url(bytes) !== text) fail('MALFORMED')
  return bytes
}

function ascii (text) {
  return b4a.from(text, 'utf8')
}

function tagged (tag, message) {
  return b4a.concat([ascii(tag), b4a.from([0]), message])
}

function keyedHash (outputBytes, tag, key) {
  const out = b4a.alloc(outputBytes)
  sodium.crypto_generichash(out, ascii(tag), key)
  return out
}

function checksum (body) {
  const out = b4a.alloc(32)
  sodium.crypto_generichash(out, tagged(TAG.check, body))
  return out.subarray(0, CHECKSUM_BYTES)
}

function writeU32 (value) {
  const out = b4a.alloc(4)
  out[0] = (value >>> 24) & 0xff
  out[1] = (value >>> 16) & 0xff
  out[2] = (value >>> 8) & 0xff
  out[3] = value & 0xff
  return out
}

function readU32 (buf, offset) {
  return ((buf[offset] << 24) >>> 0) + (buf[offset + 1] << 16) + (buf[offset + 2] << 8) + buf[offset + 3]
}

function writeU64 (value) {
  if (!Number.isSafeInteger(value) || value < 0) throw new TypeError('u64 out of range')
  const high = Math.floor(value / 0x100000000)
  const low = value % 0x100000000
  return b4a.concat([writeU32(high), writeU32(low)])
}

function normalizeName (name) {
  return typeof name.normalize === 'function' ? name.normalize('NFC') : name
}

/** UTF-8 bytes of a name, or null when the name is not allowed. */
function nameBytes (name, maxBytes) {
  if (typeof name !== 'string') return null
  const normalized = normalizeName(name)
  // No control characters: a name is shown on screen and must not reshape it.
  if (/[\u0000-\u001f\u007f]/.test(normalized)) return null
  const bytes = ascii(normalized)
  if (bytes.byteLength < 1 || bytes.byteLength > maxBytes) return null
  return bytes
}

/** Decodes UTF-8 and returns null for anything that does not round trip. */
function decodeUtf8 (bytes) {
  const text = b4a.toString(bytes, 'utf8')
  if (!b4a.equals(ascii(text), bytes)) return null
  if (/[\u0000-\u001f\u007f]/.test(text)) return null
  return text
}

function isValidPublicKey (key) {
  if (!b4a.isBuffer(key) || key.byteLength !== KEY_BYTES) return false
  // The mailbox converts the recipient to Curve25519, which fails for keys
  // that are not points on the curve. Refuse those at the link, not later.
  try {
    const curve = b4a.alloc(sodium.crypto_box_PUBLICKEYBYTES)
    sodium.crypto_sign_ed25519_pk_to_curve25519(curve, key)
    return true
  } catch (_) {
    return false
  }
}

function hexKey (value, bytes) {
  if (typeof value !== 'string' || value.length !== bytes * 2 || !/^[0-9a-f]+$/i.test(value)) return null
  return b4a.from(value.toLowerCase(), 'hex')
}

// ── Payload ─────────────────────────────────────────────────────────────────

/**
 * @param {{secret: Buffer, rendezvousPublicKey: Buffer, expiresAt?: number|null, nameHint?: string|null}} fields
 * @returns {Buffer}
 */
function encodePayload ({ secret, rendezvousPublicKey, expiresAt = null, nameHint = null }) {
  if (!b4a.isBuffer(secret) || secret.byteLength !== SECRET_BYTES) throw new TypeError('secret must be 16 bytes')
  if (!isValidPublicKey(rendezvousPublicKey)) throw new TypeError('invalid rendezvous key')
  let flags = 0
  const parts = [null, null, secret, rendezvousPublicKey]
  if (expiresAt != null) {
    if (!Number.isInteger(expiresAt) || expiresAt < 0 || expiresAt > 0xffffffff) throw new TypeError('expiresAt out of range')
    flags |= FLAG_EXPIRY
    parts.push(writeU32(expiresAt))
  }
  if (nameHint != null) {
    const bytes = nameBytes(nameHint, MAX_NAME_BYTES)
    if (!bytes) throw new TypeError('name hint must be 1 to 48 bytes with no control characters')
    flags |= FLAG_NAME
    parts.push(b4a.from([bytes.byteLength]), bytes)
  }
  parts[0] = b4a.from([VERSION])
  parts[1] = b4a.from([flags])
  const body = b4a.concat(parts)
  return b4a.concat([body, checksum(body)])
}

/**
 * @param {Buffer} payload
 * @returns {{version: number, secret: Buffer, rendezvousPublicKey: Buffer, expiresAt: number|null, nameHint: string|null}}
 */
function decodePayload (payload) {
  if (!b4a.isBuffer(payload) || payload.byteLength < 2 + SECRET_BYTES + KEY_BYTES + CHECKSUM_BYTES) fail('MALFORMED')
  const body = payload.subarray(0, payload.byteLength - CHECKSUM_BYTES)
  if (!b4a.equals(checksum(body), payload.subarray(payload.byteLength - CHECKSUM_BYTES))) fail('MALFORMED')

  // Version before flags, so a version 2 link reports its version rather
  // than its unknown flags.
  const version = body[0]
  if (version !== VERSION) fail('UNSUPPORTED_VERSION')
  const flags = body[1]
  if ((flags & ~KNOWN_FLAGS) !== 0) fail('NEWER_FORMAT')

  let offset = 2
  const secret = b4a.from(body.subarray(offset, offset + SECRET_BYTES))
  offset += SECRET_BYTES
  const rendezvousPublicKey = b4a.from(body.subarray(offset, offset + KEY_BYTES))
  offset += KEY_BYTES
  if (!isValidPublicKey(rendezvousPublicKey)) fail('MALFORMED')

  let expiresAt = null
  if (flags & FLAG_EXPIRY) {
    if (offset + 4 > body.byteLength) fail('MALFORMED')
    expiresAt = readU32(body, offset)
    offset += 4
  }

  let nameHint = null
  if (flags & FLAG_NAME) {
    if (offset + 1 > body.byteLength) fail('MALFORMED')
    const length = body[offset]
    offset += 1
    if (length < 1 || length > MAX_NAME_BYTES || offset + length > body.byteLength) fail('MALFORMED')
    nameHint = decodeUtf8(body.subarray(offset, offset + length))
    if (nameHint === null) fail('MALFORMED')
    offset += length
  }

  if (offset !== body.byteLength) fail('MALFORMED')
  return { version, secret, rendezvousPublicKey, expiresAt, nameHint }
}

// ── Links ───────────────────────────────────────────────────────────────────

/** The shareable link. */
function buildLink (fields) {
  return 'https://' + LINK_HOST + LINK_PATH + '#' + toBase64Url(encodePayload(fields))
}

/**
 * The in-app handoff form the landing page opens after an explicit tap. A
 * custom scheme URI is never fetched over HTTP, so the payload may sit in the
 * path here.
 */
function buildHandoff (fields) {
  return HANDOFF_SCHEME + '://g/v1/' + toBase64Url(encodePayload(fields))
}

function withinSizeLimit (uri) {
  return typeof uri === 'string' && uri.length <= MAX_URI_BYTES && b4a.byteLength(uri) <= MAX_URI_BYTES
}

/** Cheap shape test for routing: does this URI claim to be a group link? */
function isGroupLink (uri) {
  if (typeof uri !== 'string') return false
  return /^https:\/\/join\.justzappit\.xyz\/g\//i.test(uri) ||
    new RegExp('^' + HANDOFF_SCHEME.replace(/\./g, '\\.') + ':\\/\\/g\\/', 'i').test(uri)
}

/**
 * Parses either link form. A query on the canonical form is ignored, never
 * read: apps append tracking parameters, and the secret is in the fragment.
 * @param {string} uri
 */
function parseLink (uri) {
  if (!withinSizeLimit(uri)) fail('TOO_LARGE')

  const https = /^https:\/\/([^/?#]+)(\/[^?#]*)?(?:\?[^#]*)?#(.*)$/i.exec(uri)
  if (https) {
    if (https[1].toLowerCase() !== LINK_HOST) fail('MALFORMED')
    const path = https[2] || ''
    if (path !== LINK_PATH) {
      if (/^\/g\/v\d+$/.test(path)) fail('UNSUPPORTED_VERSION')
      fail('MALFORMED')
    }
    return decodePayload(fromBase64Url(https[3]))
  }

  const handoff = new RegExp('^' + HANDOFF_SCHEME.replace(/\./g, '\\.') + ':\\/\\/g\\/(v\\d+)\\/([^/?#]*)$', 'i').exec(uri)
  if (handoff) {
    if (handoff[1] !== 'v1') fail('UNSUPPORTED_VERSION')
    return decodePayload(fromBase64Url(handoff[2]))
  }

  fail('MALFORMED')
}

/**
 * Display fields only, for previews. Never returns the secret or any key.
 * @returns {{status: string, version?: number, nameHint?: string|null, expiresAt?: number|null, linkId?: string}}
 */
function inspectLink (uri, nowSeconds = Math.floor(Date.now() / 1000)) {
  let parsed
  try {
    parsed = parseLink(uri)
  } catch (err) {
    if (!(err instanceof GroupLinkError)) throw err
    const status = { TOO_LARGE: 'malformed', MALFORMED: 'malformed', UNSUPPORTED_VERSION: 'unsupported_version', NEWER_FORMAT: 'newer_format' }[err.code]
    return { status }
  }
  const expired = parsed.expiresAt !== null && parsed.expiresAt <= nowSeconds
  return {
    status: expired ? 'expired' : 'ok',
    version: parsed.version,
    nameHint: parsed.nameHint,
    expiresAt: parsed.expiresAt,
    linkId: b4a.toString(deriveLinkId(parsed.secret), 'hex')
  }
}

// ── Derived values ──────────────────────────────────────────────────────────

function deriveLinkId (secret) {
  return keyedHash(LINK_ID_BYTES, TAG.id, secret)
}

function keyPairFromSeed (seed) {
  const publicKey = b4a.alloc(sodium.crypto_sign_PUBLICKEYBYTES)
  const secretKey = b4a.alloc(sodium.crypto_sign_SECRETKEYBYTES)
  sodium.crypto_sign_seed_keypair(publicKey, secretKey, seed)
  return { publicKey, secretKey }
}

/** P: joiners sign with it to prove they hold the link. */
function deriveProofKeyPair (secret) {
  return keyPairFromSeed(keyedHash(32, TAG.proof, secret))
}

/** Topic for the optional direct path. Unused in v1, fixed here so it never changes meaning. */
function deriveTopic (secret) {
  return keyedHash(32, TAG.topic, secret)
}

/** M: the owner's per-link mailbox and signing key. */
function rendezvousKeyPair (seed) {
  if (!b4a.isBuffer(seed) || seed.byteLength !== 32) throw new TypeError('rendezvous seed must be 32 bytes')
  return keyPairFromSeed(seed)
}

/**
 * Fresh link material for an owner.
 * @returns {{secret: Buffer, rendezvousSeed: Buffer, rendezvous: {publicKey: Buffer, secretKey: Buffer}, linkId: Buffer, proofPublicKey: Buffer}}
 */
function createLinkMaterial () {
  const secret = b4a.alloc(SECRET_BYTES)
  sodium.randombytes_buf(secret)
  const rendezvousSeed = b4a.alloc(32)
  sodium.randombytes_buf(rendezvousSeed)
  return {
    secret,
    rendezvousSeed,
    rendezvous: rendezvousKeyPair(rendezvousSeed),
    linkId: deriveLinkId(secret),
    proofPublicKey: deriveProofKeyPair(secret).publicKey
  }
}

// ── Signatures ──────────────────────────────────────────────────────────────

function sign (tag, message, secretKey) {
  const signature = b4a.alloc(sodium.crypto_sign_BYTES)
  sodium.crypto_sign_detached(signature, tagged(tag, message), secretKey)
  return signature
}

function verify (tag, message, signatureHex, publicKey) {
  const signature = hexKey(signatureHex, sodium.crypto_sign_BYTES)
  if (!signature || !b4a.isBuffer(publicKey) || publicKey.byteLength !== KEY_BYTES) return false
  try {
    return sodium.crypto_sign_verify_detached(signature, tagged(tag, message), publicKey)
  } catch (_) {
    return false
  }
}

function requestMessage ({ rendezvousPublicKey, linkId, joinerKey, requestedAt, nonce, joinerName }) {
  const name = nameBytes(joinerName, MAX_JOINER_NAME_BYTES)
  if (!name) throw new TypeError('joiner name must be 1 to 255 bytes with no control characters')
  return b4a.concat([rendezvousPublicKey, linkId, joinerKey, writeU64(requestedAt), nonce, b4a.from([name.byteLength]), name])
}

/**
 * Builds a signed join request. After this the caller no longer needs the
 * secret and should drop it.
 */
function createJoinRequest ({ secret, rendezvousPublicKey, joinerKeyPair, joinerName, requestedAt = Date.now(), nonce = null }) {
  const requestNonce = nonce || (() => { const n = b4a.alloc(NONCE_BYTES); sodium.randombytes_buf(n); return n })()
  const linkId = deriveLinkId(secret)
  const joinerKey = joinerKeyPair.publicKey
  const message = requestMessage({ rendezvousPublicKey, linkId, joinerKey, requestedAt, nonce: requestNonce, joinerName })
  return {
    type: 'group_join_request',
    v: 1,
    linkId: b4a.toString(linkId, 'hex'),
    joinerKey: b4a.toString(joinerKey, 'hex'),
    joinerName: normalizeName(joinerName),
    requestedAt,
    nonce: b4a.toString(requestNonce, 'hex'),
    linkSig: b4a.toString(sign(TAG.requestLink, message, deriveProofKeyPair(secret).secretKey), 'hex'),
    joinerSig: b4a.toString(sign(TAG.requestJoiner, message, joinerKeyPair.secretKey), 'hex')
  }
}

/**
 * True when both signatures hold: the link signature under the owner's stored
 * proof key, and the joiner signature under the key the request names.
 */
function verifyJoinRequest (request, { rendezvousPublicKey, proofPublicKey }) {
  if (!request || request.type !== 'group_join_request' || request.v !== 1) return false
  const linkId = hexKey(request.linkId, LINK_ID_BYTES)
  const joinerKey = hexKey(request.joinerKey, KEY_BYTES)
  const nonce = hexKey(request.nonce, NONCE_BYTES)
  if (!linkId || !joinerKey || !nonce || !Number.isSafeInteger(request.requestedAt) || request.requestedAt < 0) return false
  let message
  try {
    message = requestMessage({ rendezvousPublicKey, linkId, joinerKey, requestedAt: request.requestedAt, nonce, joinerName: request.joinerName })
  } catch (_) {
    return false
  }
  return verify(TAG.requestLink, message, request.linkSig, proofPublicKey) &&
    verify(TAG.requestJoiner, message, request.joinerSig, joinerKey)
}

function admitMessage ({ linkId, joinerKey, groupId, creatorKey }) {
  return b4a.concat([linkId, joinerKey, groupId, creatorKey])
}

function toBuffers (fields) {
  return {
    linkId: b4a.isBuffer(fields.linkId) ? fields.linkId : hexKey(fields.linkId, LINK_ID_BYTES),
    joinerKey: b4a.isBuffer(fields.joinerKey) ? fields.joinerKey : hexKey(fields.joinerKey, KEY_BYTES),
    groupId: fields.groupId == null ? null : (b4a.isBuffer(fields.groupId) ? fields.groupId : hexKey(fields.groupId, KEY_BYTES)),
    creatorKey: fields.creatorKey == null ? null : (b4a.isBuffer(fields.creatorKey) ? fields.creatorKey : hexKey(fields.creatorKey, KEY_BYTES))
  }
}

/** Owner side: the proof, carried on the group invite, that the link's owner admitted this joiner. */
function signAdmit (rendezvous, fields) {
  const f = toBuffers(fields)
  return b4a.toString(sign(TAG.admit, admitMessage(f), rendezvous.secretKey), 'hex')
}

function verifyAdmit (admitSig, rendezvousPublicKey, fields) {
  const f = toBuffers(fields)
  if (!f.linkId || !f.joinerKey || !f.groupId || !f.creatorKey) return false
  return verify(TAG.admit, admitMessage(f), admitSig, rendezvousPublicKey)
}

function resultMessage ({ linkId, joinerKey }, status) {
  const code = RESULT_STATUS[status]
  if (!code) throw new TypeError('unknown result status')
  return b4a.concat([linkId, joinerKey, b4a.from([code])])
}

/** Owner side: a signed answer that is not an admission. */
function createJoinResult (rendezvous, { linkId, joinerKey, status }) {
  const f = toBuffers({ linkId, joinerKey })
  return {
    type: 'group_join_result',
    v: 1,
    linkId: b4a.toString(f.linkId, 'hex'),
    joinerKey: b4a.toString(f.joinerKey, 'hex'),
    status,
    sig: b4a.toString(sign(TAG.result, resultMessage(f, status), rendezvous.secretKey), 'hex')
  }
}

function verifyJoinResult (result, rendezvousPublicKey) {
  if (!result || result.type !== 'group_join_result' || result.v !== 1 || !RESULT_STATUS[result.status]) return false
  const f = toBuffers({ linkId: result.linkId, joinerKey: result.joinerKey })
  if (!f.linkId || !f.joinerKey) return false
  return verify(TAG.result, resultMessage(f, result.status), result.sig, rendezvousPublicKey)
}

module.exports = {
  VERSION,
  LINK_HOST,
  LINK_PATH,
  HANDOFF_SCHEME,
  MAX_URI_BYTES,
  MAX_NAME_BYTES,
  MAX_JOINER_NAME_BYTES,
  RESULT_STATUS,
  GroupLinkError,
  toBase64Url,
  fromBase64Url,
  encodePayload,
  decodePayload,
  buildLink,
  buildHandoff,
  isGroupLink,
  parseLink,
  inspectLink,
  deriveLinkId,
  deriveProofKeyPair,
  deriveTopic,
  rendezvousKeyPair,
  createLinkMaterial,
  createJoinRequest,
  verifyJoinRequest,
  signAdmit,
  verifyAdmit,
  createJoinResult,
  verifyJoinResult
}
