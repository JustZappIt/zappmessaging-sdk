/**
 * Rooms - Topic derivation for different chat types
 * 
 * All topics are 32-byte buffers derived deterministically so both parties
 * compute the same value independently.
 */

const crypto = require('hypercore-crypto')
const b4a = require('b4a')

/**
 * Derive topic for a store room
 * @param {string} storeId - Store identifier (UUID or similar)
 * @returns {Buffer} 32-byte topic
 */
function deriveStoreRoomTopic(storeId) {
  const data = b4a.from('store:' + storeId, 'utf8')
  const seed = crypto.data(data) // Blake2b hash
  return crypto.discoveryKey(seed)
}

/**
 * Derive topic for a city channel
 * @param {string} citySlug - City identifier (e.g., 'new-york', 'london')
 * @returns {Buffer} 32-byte topic
 */
function deriveCityChannelTopic(citySlug) {
  const data = b4a.from('city:' + citySlug.toLowerCase(), 'utf8')
  const seed = crypto.data(data) // Blake2b hash
  return crypto.discoveryKey(seed)
}

/**
 * Derive topic for a direct chat between two users
 * Order-independent: both users derive the same topic
 * @param {string} pubKeyHexA - First user's public key (hex)
 * @param {string} pubKeyHexB - Second user's public key (hex)
 * @returns {Buffer} 32-byte topic
 */
function deriveDirectChatTopic(pubKeyHexA, pubKeyHexB) {
  // Sort keys to ensure order-independence
  const sorted = [pubKeyHexA, pubKeyHexB].sort()
  const combined = b4a.from(sorted[0] + sorted[1], 'hex')
  const seed = crypto.data(combined) // Blake2b hash
  return crypto.discoveryKey(seed)
}

/**
 * Derive topic for a group chat
 * @param {string} groupId - Group identifier (hex string)
 * @returns {Buffer} 32-byte topic
 */
function deriveGroupChatTopic(groupId) {
  const seed = b4a.from(groupId, 'hex')
  return crypto.discoveryKey(seed)
}

/**
 * Derive personal topic for receiving invites
 * @param {Buffer|string} publicKey - User's public key (Buffer or hex string)
 * @returns {Buffer} 32-byte topic
 */
function derivePersonalTopic(publicKey) {
  const pubKeyBuf = b4a.isBuffer(publicKey) 
    ? publicKey 
    : b4a.from(publicKey, 'hex')
  return crypto.discoveryKey(pubKeyBuf)
}

/**
 * Generate a random group ID
 * @returns {string} Random 32-byte hex string
 */
function generateGroupId() {
  const randomBytes = crypto.randomBytes(32)
  return b4a.toString(randomBytes, 'hex')
}

module.exports = {
  deriveStoreRoomTopic,
  deriveCityChannelTopic,
  deriveDirectChatTopic,
  deriveGroupChatTopic,
  derivePersonalTopic,
  generateGroupId
}
