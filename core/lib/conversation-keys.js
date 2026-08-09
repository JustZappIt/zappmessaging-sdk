/**
 * Conversation encryption-key derivation (v2) — keys require a secret only
 * participants hold.
 *
 * v1 derived the Hypercore encryption key as blake2b('zapp-enc:' + convId), a
 * pure function of the conversation id. Direct-chat ids are themselves
 * blake2b(sortedPubA:pubB) of public keys, so any peer who knew two public
 * keys could derive the key of a conversation it was never part of — and,
 * combined with the unauthenticated openRemoteCore path, decrypt mirrored
 * ciphertext and inject authored messages into it.
 *
 * v2:
 * - direct: X25519 ECDH between the two identity keys (Ed25519 converted to
 *   Curve25519, the same conversion Hyperswarm's Noise transport applies), so
 *   both sides derive an identical key with zero extra protocol messages.
 * - group: keyed by the 32-byte random groupId, which is minted by the
 *   creator and only ever transits inside invites over Noise-authenticated
 *   sockets. The swarm topic is discoveryKey(groupId) — one-way — so the
 *   public topic announcement does not reveal the key seed. Deliberately NOT
 *   bound to the conversationId: each member stores the group under its own
 *   locally-random conversation id, so a conversationId-bound key could never
 *   interoperate across members.
 * - store/city rooms are open — anyone can join from a public identifier — so
 *   their cores keep a deterministic key and carry no confidentiality claim.
 *
 * Rollout is a clean cutover: blocks written under v1 keys are unreadable
 * under v2 and are skipped by the per-block error handling in the remote-core
 * watcher. Message history already ingested into the chat store is unaffected.
 */

const sodium = require('sodium-universal')
const b4a = require('b4a')

const KEY_BYTES = 32

function _epochSuffix (epoch) {
  return epoch ? ':e' + epoch : ''
}

/** blake2b(labelStr) keyed with keyBuf → 32-byte encryption key. */
function _keyedHash (keyBuf, labelStr) {
  const out = b4a.alloc(KEY_BYTES)
  sodium.crypto_generichash(out, b4a.from(labelStr), keyBuf)
  return out
}

/**
 * Derive the direct-chat encryption key from our Ed25519 identity keypair and
 * the peer's Ed25519 public key. Symmetric: both sides compute the same key.
 * Throws on malformed keys or a degenerate ECDH result (fail closed).
 *
 * @param {{publicKey: Buffer, secretKey: Buffer}} keyPair - own identity
 * @param {string} peerPubHex - peer identity public key (hex)
 * @param {string} conversationId - deterministic direct-chat id (shared)
 * @param {number} epoch - key rotation epoch
 */
function deriveDirectKey (keyPair, peerPubHex, conversationId, epoch = 0) {
  const peerPub = b4a.from((peerPubHex || '').toLowerCase(), 'hex')
  if (peerPub.byteLength !== 32) throw new Error('deriveDirectKey: bad peer public key')
  if (!keyPair || !keyPair.secretKey || keyPair.secretKey.byteLength !== 64) {
    throw new Error('deriveDirectKey: bad identity keypair')
  }

  const mySk = b4a.alloc(sodium.crypto_scalarmult_SCALARBYTES)
  sodium.crypto_sign_ed25519_sk_to_curve25519(mySk, keyPair.secretKey)
  const theirPk = b4a.alloc(sodium.crypto_scalarmult_BYTES)
  sodium.crypto_sign_ed25519_pk_to_curve25519(theirPk, peerPub)

  const shared = b4a.alloc(sodium.crypto_scalarmult_BYTES)
  sodium.crypto_scalarmult(shared, mySk, theirPk)
  mySk.fill(0)

  // Bind the raw ECDH output to both identities (sorted, so A→B and B→A
  // agree) per libsodium guidance, then to the conversation label.
  const myPubHex = b4a.toString(keyPair.publicKey, 'hex')
  const sorted = [myPubHex, b4a.toString(peerPub, 'hex')].sort()
  const secret = b4a.alloc(KEY_BYTES)
  sodium.crypto_generichash(secret, b4a.concat([shared, b4a.from(sorted[0] + sorted[1], 'hex')]))
  shared.fill(0)

  const key = _keyedHash(secret, 'zapp-enc:v2:dm:' + conversationId + _epochSuffix(epoch))
  secret.fill(0)
  return key
}

/**
 * Derive the group encryption key from the invite-distributed groupId.
 * Identical for every member regardless of their local conversation id.
 *
 * @param {string} groupIdHex - 32-byte group id (hex), member-secret
 * @param {number} epoch - key rotation epoch
 */
function deriveGroupKey (groupIdHex, epoch = 0) {
  const groupId = b4a.from((groupIdHex || '').toLowerCase(), 'hex')
  if (groupId.byteLength !== 32) throw new Error('deriveGroupKey: bad groupId')
  return _keyedHash(groupId, 'zapp-enc:v2:group' + _epochSuffix(epoch))
}

/**
 * Deterministic key for open rooms (store/city). These are joinable from a
 * public identifier, so the key hides nothing from anyone who can join — it
 * only keeps the blind peer's stored blocks opaque.
 */
function derivePublicRoomKey (kind, identifier, epoch = 0) {
  const out = b4a.alloc(KEY_BYTES)
  sodium.crypto_generichash(out, b4a.from('zapp-enc:v2:public:' + kind + ':' + identifier + _epochSuffix(epoch)))
  return out
}

module.exports = { deriveDirectKey, deriveGroupKey, derivePublicRoomKey }
