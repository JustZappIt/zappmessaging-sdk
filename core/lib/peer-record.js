// One schema for both Noise frames and records authored by a registered core.
const b4a = require('b4a')
const crypto = require('bare-crypto')

class InvalidPeerRecord extends Error {
  constructor (field) {
    super('Invalid peer record: ' + field)
    this.code = 'INVALID_PEER_RECORD'
  }
}
const MAX_CONTENT = 256 * 1024
const MAX_THUMBNAIL = 1024 * 1024
const MAX_RECORD = MAX_THUMBNAIL + 2 * MAX_CONTENT
const GROUP_CONTROLS = new Set(['group_invite', 'group_leave', 'group_deleted', 'group_renamed', 'group_member_added'])
function object (value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value) &&
    (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null)
}
function string (value, field, max, required = false) {
  if (value == null && !required) return
  if (typeof value !== 'string' || (required && !value.length) || b4a.byteLength(value) > max) throw new InvalidPeerRecord(field)
}
function identifier (value, field, required = false) {
  string(value, field, 128, required)
  if (value != null && !/^[a-zA-Z0-9_-]+$/.test(value)) throw new InvalidPeerRecord(field)
}
function key (value, field, required = false) {
  string(value, field, 66, required)
  if (value != null && !/^(0x)?[a-fA-F0-9]{64}$/.test(value)) throw new InvalidPeerRecord(field)
  return value == null ? value : value.toLowerCase().replace(/^0x/, '')
}
function keys (value, field) {
  if (!Array.isArray(value) || value.length === 0 || value.length > 256) throw new InvalidPeerRecord(field)
  return [...new Set(value.map(k => key(k, field, true)))]
}
const MEDIA_DESCRIPTOR_FIELDS = ['mediaCoreKey', 'mediaBlockOffset', 'mediaBlockLength']
const MIME_TYPE = /^[\w.+-]+\/[\w.+-]+(?:;[^\r\n]*)?$/
// Where an image's bytes sit in the sender's media core. Meaningless without
// the mediaId that verifies them, so it is all three fields or none.
function validateMediaDescriptor (record) {
  const present = MEDIA_DESCRIPTOR_FIELDS.filter(field => record[field] != null)
  if (present.length === 0) return
  if (present.length !== MEDIA_DESCRIPTOR_FIELDS.length || record.mediaId == null) throw new InvalidPeerRecord('media descriptor')
  key(record.mediaCoreKey, 'mediaCoreKey', true)
  if (!Number.isSafeInteger(record.mediaBlockOffset) || record.mediaBlockOffset < 0) throw new InvalidPeerRecord('mediaBlockOffset')
  if (!Number.isSafeInteger(record.mediaBlockLength) || record.mediaBlockLength < 1) throw new InvalidPeerRecord('mediaBlockLength')
}
function validateMessage (record) {
  if (!object(record) || record.type != null) throw new InvalidPeerRecord('message shape')
  identifier(record.id, 'id')
  string(record.senderId, 'senderId', 128, true)
  for (const field of ['content', 'replyToContent']) string(record[field], field, MAX_CONTENT)
  for (const field of ['senderName', 'replyToSenderName']) string(record[field], field, 1024)
  for (const field of ['contentType', 'replyToContentType']) {
    string(record[field], field, 256)
    if (record[field] != null && !MIME_TYPE.test(record[field])) throw new InvalidPeerRecord(field)
  }
  identifier(record.replyToId, 'replyToId')
  string(record.mediaId, 'mediaId', 128)
  validateMediaDescriptor(record)
  // Preserve existing numeric and thumbnail normalization in ChatStore.
  for (const field of ['mediaLocalPath', 'mediaTransferState', 'status']) string(record[field], field, 4096)
}
function normalizePeerRecord (input, peer) {
  if (!object(input)) throw new InvalidPeerRecord('shape')
  peer = key(peer, 'authenticated peer', true)
  let encoded
  try { encoded = JSON.stringify(input) } catch (_) { throw new InvalidPeerRecord('encoding') }
  if (b4a.byteLength(encoded) > MAX_RECORD) throw new InvalidPeerRecord('size')
  identifier(input.conversationId, 'conversationId')
  key(input.groupTopicHex, 'groupTopicHex')
  const record = {}
  if (input.conversationId) record.conversationId = input.conversationId
  if (input.groupTopicHex) record.groupTopicHex = key(input.groupTopicHex, 'groupTopicHex')
  string(input.type, 'type', 64)
  if (input.type != null) {
    record.type = input.type
    identifier(input.id, 'id')
    if (input.id) record.id = input.id
    if (input.type === '__receipt') {
      if (!['read', 'delivered'].includes(input.kind)) throw new InvalidPeerRecord('receipt kind')
      identifier(input.upTo, 'upTo', true)
      record.kind = input.kind
      record.upTo = input.upTo
      if (input.to != null) record.to = key(input.to, 'to', true)
      if (input.kind === 'delivered' && !record.to) throw new InvalidPeerRecord('to')
    } else if (input.type === '__core_keys') {
      if (!object(input.cores) || Object.keys(input.cores).length > 256) throw new InvalidPeerRecord('cores')
      record.cores = Object.create(null)
      for (const [id, core] of Object.entries(input.cores)) {
        identifier(id, 'core conversation', true)
        record.cores[id] = key(core, 'core key', true)
      }
    } else if (input.type === '__presence') {
      if (typeof input.hidden !== 'boolean') throw new InvalidPeerRecord('hidden')
      record.hidden = input.hidden
    } else if (input.type === 'direct_invite' || input.type === 'group_invite') {
      record.senderKey = key(input.senderKey, 'senderKey', true)
      if (record.senderKey !== peer) throw new InvalidPeerRecord('invite sender')
      if (input.localCoreKey != null) record.localCoreKey = key(input.localCoreKey, 'localCoreKey', true)
      for (const field of ['senderDisplayName', 'groupName']) {
        string(input[field], field, 1024)
        if (input[field] != null) record[field] = input[field]
      }
      if (input.type === 'group_invite') {
        record.groupId = key(input.groupId, 'groupId', true)
        record.creatorKey = key(input.creatorKey, 'creatorKey', true)
        record.participants = keys(input.participants, 'participants')
      }
    } else if (input.type === 'group_renamed') {
      string(input.newName, 'newName', 1024, true)
      record.newName = input.newName
    } else if (input.type === 'group_member_added') {
      record.newMemberKey = key(input.newMemberKey, 'newMemberKey', true)
      record.updatedParticipants = keys(input.updatedParticipants, 'updatedParticipants')
      string(input.newMemberName, 'newMemberName', 1024)
      if (input.newMemberName != null) record.newMemberName = input.newMemberName
    }
    // Unrecognized types remain controls and are never stored as chat rows.
    return record
  }
  for (const field of ['id', 'senderName', 'content', 'contentType', 'timestamp', 'mediaId', 'mediaSize', 'mediaWidth', 'mediaHeight', 'thumbnailData', 'replyToId', 'replyToSenderName', 'replyToContent', 'replyToContentType', ...MEDIA_DESCRIPTOR_FIELDS]) {
    if (input[field] != null) record[field] = input[field]
  }
  record.senderId = peer
  record.isFromMe = false
  validateMessage(record)
  identifier(record.id, 'id', true)
  if (record.mediaId != null) record.mediaId = key(record.mediaId, 'mediaId', true)
  if (record.mediaCoreKey != null) record.mediaCoreKey = key(record.mediaCoreKey, 'mediaCoreKey', true)
  for (const field of ['timestamp', 'mediaSize', 'mediaWidth', 'mediaHeight']) {
    if (record[field] != null && (typeof record[field] !== 'number' || !Number.isFinite(record[field]))) throw new InvalidPeerRecord(field)
  }
  string(record.thumbnailData, 'thumbnailData', MAX_THUMBNAIL)
  return record
}
function controlKey (record, peer) {
  // IDs, unlike payload fingerprints, distinguish an intentional repeated
  // operation from transport redelivery after an intervening state change.
  return crypto.createHash('sha256').update(JSON.stringify([peer, record.id])).digest('hex')
}
module.exports = { InvalidPeerRecord, normalizePeerRecord, validateMessage, controlKey, GROUP_CONTROLS, MEDIA_DESCRIPTOR_FIELDS }
