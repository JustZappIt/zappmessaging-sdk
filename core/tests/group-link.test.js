const { test } = require('node:test')
const assert = require('node:assert')
const b4a = require('b4a')
const sodium = require('sodium-universal')
const gl = require('../lib/group-link')
const vectors = require('../../test/group-link-vectors.json')

const secret = b4a.from(vectors.inputs.secretHex, 'hex')
const rendezvous = gl.rendezvousKeyPair(b4a.from(vectors.inputs.rendezvousSeedHex, 'hex'))
const joiner = gl.rendezvousKeyPair(b4a.from(vectors.inputs.joinerSeedHex, 'hex'))
const owner = gl.rendezvousKeyPair(b4a.from(vectors.inputs.ownerSeedHex, 'hex'))
const hex = (buf) => b4a.toString(buf, 'hex')

function keyPair () {
  const publicKey = b4a.alloc(32)
  const secretKey = b4a.alloc(64)
  sodium.crypto_sign_keypair(publicKey, secretKey)
  return { publicKey, secretKey }
}

function errorCode (fn) {
  try {
    fn()
  } catch (err) {
    assert.ok(err instanceof gl.GroupLinkError, 'expected a GroupLinkError, got ' + err)
    return err.code
  }
  assert.fail('expected the link to be rejected')
}

test('derived values match the vectors', () => {
  assert.strictEqual(hex(gl.deriveLinkId(secret)), vectors.derived.linkIdHex)
  assert.strictEqual(hex(gl.deriveProofKeyPair(secret).publicKey), vectors.derived.proofPublicKeyHex)
  assert.strictEqual(hex(gl.deriveTopic(secret)), vectors.derived.topicHex)
  assert.strictEqual(hex(rendezvous.publicKey), vectors.derived.rendezvousPublicKeyHex)
  assert.strictEqual(hex(joiner.publicKey), vectors.derived.joinerPublicKeyHex)
  assert.strictEqual(hex(owner.publicKey), vectors.derived.ownerPublicKeyHex)
})

test('every valid vector encodes, parses and round trips in both forms', () => {
  for (const vector of vectors.links) {
    const fields = { secret, rendezvousPublicKey: rendezvous.publicKey }
    if (vector.nameHint !== null) fields.nameHint = vector.nameHint
    if (vector.expiresAt !== null) fields.expiresAt = vector.expiresAt
    assert.strictEqual(hex(gl.encodePayload(fields)), vector.payloadHex, vector.id)
    assert.strictEqual(gl.buildLink(fields), vector.link, vector.id)
    assert.strictEqual(gl.buildHandoff(fields), vector.handoff, vector.id)

    for (const uri of [vector.link, vector.handoff]) {
      const parsed = gl.parseLink(uri)
      assert.strictEqual(parsed.version, 1)
      assert.strictEqual(hex(parsed.secret), vectors.inputs.secretHex)
      assert.strictEqual(hex(parsed.rendezvousPublicKey), vectors.derived.rendezvousPublicKeyHex)
      assert.strictEqual(parsed.nameHint, vector.nameHint)
      assert.strictEqual(parsed.expiresAt, vector.expiresAt)
    }
  }
})

test('every invalid vector is refused with its expected error', () => {
  for (const vector of vectors.invalid) {
    assert.strictEqual(errorCode(() => gl.parseLink(vector.link)), vector.error, vector.id)
  }
})

test('a query and host case do not change the link', () => {
  const minimal = gl.parseLink(vectors.links[0].link)
  for (const variant of vectors.accepted_variants) {
    const parsed = gl.parseLink(variant.link)
    assert.deepStrictEqual(parsed, minimal, variant.id)
  }
})

test('flipping any single bit of a payload is always detected', () => {
  for (const vector of vectors.links) {
    const payload = b4a.from(vector.payloadHex, 'hex')
    for (let i = 0; i < payload.byteLength; i++) {
      for (let bit = 0; bit < 8; bit++) {
        const tampered = b4a.from(payload)
        tampered[i] ^= 1 << bit
        errorCode(() => gl.decodePayload(tampered))
      }
    }
  }
})

test('names are bounded, normalized and free of control characters', () => {
  const base = { secret, rendezvousPublicKey: rendezvous.publicKey }
  const max = 'a'.repeat(gl.MAX_NAME_BYTES)
  assert.strictEqual(gl.parseLink(gl.buildLink({ ...base, nameHint: max })).nameHint, max)
  assert.throws(() => gl.encodePayload({ ...base, nameHint: max + 'a' }), TypeError)
  assert.throws(() => gl.encodePayload({ ...base, nameHint: '' }), TypeError)
  assert.throws(() => gl.encodePayload({ ...base, nameHint: 'line\nbreak' }), TypeError)
  // Decomposed é becomes the composed form, so one name has one encoding.
  const decomposed = 'Café'
  assert.strictEqual(gl.parseLink(gl.buildLink({ ...base, nameHint: decomposed })).nameHint, 'Café')
})

test('a payload with invalid UTF-8 in the name is refused', () => {
  const good = b4a.from(vectors.links[3].payloadHex, 'hex') // name "A"
  const body = b4a.from(good.subarray(0, good.byteLength - 4))
  body[body.byteLength - 1] = 0xff
  const check = b4a.alloc(32)
  sodium.crypto_generichash(check, b4a.concat([b4a.from('zapp/group-link/v1/check'), b4a.from([0]), body]))
  assert.strictEqual(errorCode(() => gl.decodePayload(b4a.concat([body, check.subarray(0, 4)]))), 'MALFORMED')
})

test('oversized input is refused before any decoding', () => {
  const huge = 'https://join.justzappit.xyz/g/v1#' + 'A'.repeat(gl.MAX_URI_BYTES)
  assert.strictEqual(errorCode(() => gl.parseLink(huge)), 'TOO_LARGE')
})

test('a rendezvous key that is not a curve point is refused', () => {
  const payload = b4a.from(vectors.links[0].payloadHex, 'hex')
  const body = b4a.from(payload.subarray(0, payload.byteLength - 4))
  body.fill(0xff, 18, 50)
  const check = b4a.alloc(32)
  sodium.crypto_generichash(check, b4a.concat([b4a.from('zapp/group-link/v1/check'), b4a.from([0]), body]))
  assert.strictEqual(errorCode(() => gl.decodePayload(b4a.concat([body, check.subarray(0, 4)]))), 'MALFORMED')
})

test('inspect returns display fields only and reports expiry', () => {
  const withExpiry = vectors.links.find(v => v.id === 'with_expiry_and_name')
  const before = gl.inspectLink(withExpiry.link, withExpiry.expiresAt - 1)
  assert.deepStrictEqual(Object.keys(before).sort(), ['expiresAt', 'linkId', 'nameHint', 'status', 'version'])
  assert.strictEqual(before.status, 'ok')
  assert.strictEqual(before.nameHint, 'Café Crew')
  assert.strictEqual(before.linkId, vectors.derived.linkIdHex)
  assert.strictEqual(gl.inspectLink(withExpiry.link, withExpiry.expiresAt).status, 'expired')
  const statuses = Object.fromEntries(vectors.invalid.map(v => [v.id, gl.inspectLink(v.link).status]))
  assert.strictEqual(statuses.bad_checksum, 'malformed')
  assert.strictEqual(statuses.version_2, 'unsupported_version')
  assert.strictEqual(statuses.reserved_flag, 'newer_format')
})

test('isGroupLink recognizes only our hosts and scheme', () => {
  assert.ok(gl.isGroupLink(vectors.links[0].link))
  assert.ok(gl.isGroupLink(vectors.links[0].handoff))
  assert.ok(!gl.isGroupLink('https://gift.justzappit.xyz/c/v1#k=abc'))
  assert.ok(!gl.isGroupLink('https://join.justzappit.xyz.evil.example/g/v1#x'))
  assert.ok(!gl.isGroupLink(null))
})

test('the join request vector reproduces and verifies', () => {
  const request = gl.createJoinRequest({
    secret,
    rendezvousPublicKey: rendezvous.publicKey,
    joinerKeyPair: joiner,
    joinerName: vectors.request.joinerName,
    requestedAt: vectors.request.requestedAt,
    nonce: b4a.from(vectors.request.nonce, 'hex')
  })
  assert.deepStrictEqual(request, vectors.request)
  const proofPublicKey = gl.deriveProofKeyPair(secret).publicKey
  assert.ok(gl.verifyJoinRequest(request, { rendezvousPublicKey: rendezvous.publicKey, proofPublicKey }))
})

test('a join request fails when any signed field or key is wrong', () => {
  const proofPublicKey = gl.deriveProofKeyPair(secret).publicKey
  const context = { rendezvousPublicKey: rendezvous.publicKey, proofPublicKey }
  const request = vectors.request
  const other = keyPair()

  // Someone holding the link cannot name a victim's key: the joiner signature fails.
  const forged = gl.createJoinRequest({ secret, rendezvousPublicKey: rendezvous.publicKey, joinerKeyPair: other, joinerName: 'Ana' })
  forged.joinerKey = request.joinerKey
  assert.ok(!gl.verifyJoinRequest(forged, context))

  for (const [field, value] of [['joinerName', 'Eve'], ['requestedAt', request.requestedAt + 1], ['nonce', '66'.repeat(16)], ['linkId', '00'.repeat(16)]]) {
    assert.ok(!gl.verifyJoinRequest({ ...request, [field]: value }, context), field)
  }
  // A request for another link's rendezvous key, or signed without the link.
  assert.ok(!gl.verifyJoinRequest(request, { ...context, rendezvousPublicKey: other.publicKey }))
  assert.ok(!gl.verifyJoinRequest(request, { ...context, proofPublicKey: other.publicKey }))
  assert.ok(!gl.verifyJoinRequest({ ...request, v: 2 }, context))
  assert.ok(!gl.verifyJoinRequest({ ...request, linkSig: 'zz' }, context))
})

test('admission and result signatures reproduce and verify only under the link key', () => {
  const fields = {
    linkId: vectors.derived.linkIdHex,
    joinerKey: vectors.derived.joinerPublicKeyHex,
    groupId: vectors.inputs.groupIdHex,
    creatorKey: vectors.derived.ownerPublicKeyHex
  }
  assert.strictEqual(gl.signAdmit(rendezvous, fields), vectors.admit.admitSig)
  assert.ok(gl.verifyAdmit(vectors.admit.admitSig, rendezvous.publicKey, fields))
  assert.ok(!gl.verifyAdmit(vectors.admit.admitSig, keyPair().publicKey, fields))
  assert.ok(!gl.verifyAdmit(vectors.admit.admitSig, rendezvous.publicKey, { ...fields, groupId: '55'.repeat(32) }))

  for (const result of vectors.results) {
    const again = gl.createJoinResult(rendezvous, { linkId: result.linkId, joinerKey: result.joinerKey, status: result.status })
    assert.deepStrictEqual(again, result)
    assert.ok(gl.verifyJoinResult(result, rendezvous.publicKey), result.status)
    assert.ok(!gl.verifyJoinResult(result, keyPair().publicKey), result.status)
    assert.ok(!gl.verifyJoinResult({ ...result, status: result.status === 'full' ? 'declined' : 'full' }, rendezvous.publicKey))
  }
  assert.throws(() => gl.createJoinResult(rendezvous, { linkId: fields.linkId, joinerKey: fields.joinerKey, status: 'maybe' }), TypeError)
})

test('fresh link material is consistent with itself', () => {
  const material = gl.createLinkMaterial()
  const link = gl.buildLink({ secret: material.secret, rendezvousPublicKey: material.rendezvous.publicKey, nameHint: 'Book club' })
  const parsed = gl.parseLink(link)
  assert.ok(b4a.equals(gl.deriveLinkId(parsed.secret), material.linkId))
  assert.ok(b4a.equals(gl.deriveProofKeyPair(parsed.secret).publicKey, material.proofPublicKey))
  assert.ok(b4a.equals(gl.rendezvousKeyPair(material.rendezvousSeed).publicKey, parsed.rendezvousPublicKey))
})

test('errors never quote the link', () => {
  const link = vectors.invalid.find(v => v.id === 'bad_checksum').link
  try {
    gl.parseLink(link)
  } catch (err) {
    assert.ok(!err.message.includes(link.split('#')[1]))
    assert.ok(!err.message.includes(vectors.inputs.secretHex))
  }
})
