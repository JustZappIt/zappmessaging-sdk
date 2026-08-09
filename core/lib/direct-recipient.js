class IPCRequestError extends Error {
  constructor(code, message) {
    super(message)
    this.code = code
  }
}

function normalizePublicKey(value) {
  return typeof value === 'string' ? value.toLowerCase().replace(/^0x/, '') : ''
}

function isValidPublicKey(value) {
  return /^[0-9a-f]{64}$/.test(value)
}

function validateDirectParticipant(myKey, participants) {
  if (!Array.isArray(participants) || participants.length === 0) {
    throw new IPCRequestError('MISSING_PARTICIPANT', 'A direct conversation requires a participant')
  }
  if (participants.length !== 1) {
    throw new IPCRequestError('INVALID_PARTICIPANTS', 'A direct conversation requires exactly one participant')
  }

  const theirKey = normalizePublicKey(participants[0])
  if (!isValidPublicKey(theirKey)) {
    throw new IPCRequestError('INVALID_PUBLIC_KEY', 'The direct conversation participant key is invalid')
  }
  if (theirKey === normalizePublicKey(myKey)) {
    throw new IPCRequestError('OWN_PUBLIC_KEY', 'A direct conversation cannot use your own messaging key')
  }
  return theirKey
}

module.exports = { IPCRequestError, isValidPublicKey, normalizePublicKey, validateDirectParticipant }
