const { test } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { ChatStore } = require('../lib/chat-store')
const { MediaStore } = require('../lib/media-store')
const { MediaTransfer } = require('../lib/media-transfer')
const { createPeerReceiver, serveAuthorizedMedia, acceptAuthorizedMediaChunk } = require('../lib/peer-receiver')
const ME = '11'.repeat(32), MEMBER = '22'.repeat(32), ATTACKER = '33'.repeat(32)

async function setup(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sdk-media-auth-'))
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }))
  const store = new ChatStore()
  store.storagePath = dir
  store.conversations.clear()
  store.leftConversations.clear()
  const privateChat = await store.createConversationWithId('private', 'group', [MEMBER], {
    groupId: '44'.repeat(32), creatorKey: ME
  })
  const attackerChat = await store.createConversationWithId('attacker-dm', 'direct', [ATTACKER])
  const media = new MediaStore()
  media.mediaDir = path.join(dir, 'media')
  media._ensureDir()
  const transfer = new MediaTransfer(media)
  transfer.on('error', () => {})
  const receiver = createPeerReceiver(() => ({ chatStore: store, identity: { publicKeyHex: ME } }))
  const data = Buffer.from('private group photo bytes')
  const saved = media.saveMedia(data, 'jpg')
  const hash = Buffer.from(saved.hashHex, 'hex')
  const frames = []
  const socket = { writeChunk: (...args) => frames.push(args) }
  return { store, privateChat, attackerChat, media, transfer, receiver, data, saved, hash, frames, socket }
}

test('peer cannot authorize known cached bytes by planting a media reference through live or replicated delivery', async t => {
  const h = await setup(t)
  await h.store.addMessage(h.privateChat.id, { id: 'local-share', senderId: ME, isFromMe: true, mediaId: h.saved.hashHex })
  assert.equal(await serveAuthorizedMedia(h.store, h.transfer, h.hash, MEMBER, h.socket), true)
  for (const replicated of [false, true]) {
    await h.receiver(h.attackerChat.id, ATTACKER, {
      id: 'planted-' + replicated, mediaId: h.saved.hashHex, mediaAuthorized: true,
      isFromMe: true, mediaLocalPath: h.saved.filePath, mediaTransferState: 'complete'
    }, { replicated })
    assert.equal(await serveAuthorizedMedia(h.store, h.transfer, h.hash, ATTACKER, h.socket), false)
  }
  assert.equal(h.frames.length, 1)
  assert.ok((await h.store.getMessages(h.attackerChat.id)).every(m => m.mediaAuthorized === false))
})

test('verified incoming bytes authorize only their requested conversation and survive restart', async t => {
  const h = await setup(t)
  await h.receiver(h.privateChat.id, MEMBER, { id: 'incoming', mediaId: h.saved.hashHex })
  await h.receiver(h.attackerChat.id, ATTACKER, { id: 'planted', mediaId: h.saved.hashHex })
  const requests = new Map([[h.saved.hashHex, { conversationId: h.privateChat.id, senderId: MEMBER }]])
  h.transfer.on('complete', hash => h.store.authorizeReceivedMedia(requests.get(hash).conversationId, hash))
  assert.equal(acceptAuthorizedMediaChunk(h.store, h.transfer, requests, h.hash, 0, 1, h.data, ATTACKER), false)
  assert.equal(h.store.canServeMedia(h.saved.hashHex, MEMBER), false)
  assert.equal(acceptAuthorizedMediaChunk(h.store, h.transfer, requests, h.hash, 0, 1, Buffer.from('wrong bytes'), MEMBER), false)
  assert.equal(h.store.canServeMedia(h.saved.hashHex, MEMBER), false)
  assert.equal(acceptAuthorizedMediaChunk(h.store, h.transfer, requests, h.hash, 0, 1, h.data, MEMBER), true)
  assert.equal(await serveAuthorizedMedia(h.store, h.transfer, h.hash, MEMBER, h.socket), true)
  assert.equal(await serveAuthorizedMedia(h.store, h.transfer, h.hash, ATTACKER, h.socket), false)
  h.store.conversations.clear()
  h.store.loadConversations()
  assert.equal(h.store.canServeMedia(h.saved.hashHex, MEMBER), true)
  assert.equal(h.store.canServeMedia(h.saved.hashHex, ATTACKER), false)
  await h.store.updateConversation(h.privateChat.id, { participantIds: [] })
  assert.equal(h.store.canServeMedia(h.saved.hashHex, MEMBER), false)
})

test('local explicit sharing grants each conversation independently; legacy references do not imply grants', async t => {
  const h = await setup(t)
  await h.store.addMessage(h.privateChat.id, { id: 'first-share', senderId: ME, isFromMe: true, mediaId: h.saved.hashHex })
  const file = path.join(h.store.storagePath, h.privateChat.id + '.json')
  const legacy = JSON.parse(fs.readFileSync(file))
  delete legacy[0].mediaAuthorized
  fs.writeFileSync(file, JSON.stringify(legacy))
  assert.equal(h.store.canServeMedia(h.saved.hashHex, MEMBER), false)
  await h.store.addMessage(h.privateChat.id, { id: 'reshared', senderId: ME, isFromMe: true, mediaId: h.saved.hashHex })
  await h.store.addMessage(h.attackerChat.id, { id: 'shared-deliberately', senderId: ME, isFromMe: true, mediaId: h.saved.hashHex })
  assert.equal(h.store.canServeMedia(h.saved.hashHex, MEMBER), true)
  assert.equal(h.store.canServeMedia(h.saved.hashHex, ATTACKER), true)
  h.store.markConversationAsLeft(h.privateChat.id)
  assert.equal(h.store.canServeMedia(h.saved.hashHex, MEMBER), false)
  assert.equal(h.store.canServeMedia(h.saved.hashHex, ATTACKER), true)
})
