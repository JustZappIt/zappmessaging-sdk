const { normalizePeerRecord, controlKey, GROUP_CONTROLS, CREATOR_CONTROLS } = require('./peer-record')
const { ChatStore } = require('./chat-store')
const { deriveGroupChatTopic } = require('./rooms')
const b4a = require('b4a')

// A single awaitable persistence boundary for live frames and replicated blocks.
// Dependencies are read lazily because core restore can start before IPC/P2P.
function createPeerReceiver (getDependencies) {
  const chains = new Map()
  return async function receive (conversationId, peer, input, { replicated = false } = {}) {
    const record = normalizePeerRecord(input, peer)
    const findInvitedGroup = store => record.type === 'group_invite' && store &&
      [...store.conversations.values()].find(conv => conv.type === 'group' && conv.groupId === record.groupId)
    const invitedGroup = findInvitedGroup(getDependencies().chatStore)
    // Live invite IDs belong to the sender; established groups are identified
    // by their shared groupId. Replication keeps its registered core scope.
    if (!replicated && invitedGroup) conversationId = invitedGroup.id
    const lock = conversationId || (record.groupId ? 'invite:' + record.groupId : peer)
    const previous = chains.get(lock) || Promise.resolve()
    const operation = previous.catch(() => {}).then(async () => {
      const { chatStore, p2pManager, ipcHandler, identity, processReceipt, onMessage } = getDependencies()
      if (!chatStore || !identity) throw new Error('Peer receiver not ready')
      let conv = conversationId ? await chatStore.getConversation(conversationId) : (findInvitedGroup(chatStore) || null)
      if (conversationId && chatStore.hasLeftConversation(conversationId)) return
      if (replicated && !conv) return
      // Removed from this group: nothing more arrives in it.
      if (conv && conv.removedAt) return
      if (conv) {
        if (!chatStore.isPeerAuthorized(conv.id, peer)) return
        if (conv.type === 'group') {
          const topic = b4a.toString(deriveGroupChatTopic(conv.groupId), 'hex')
          // Records written before the group got a new secret carry an
          // earlier topic; they still belong here.
          const knownTopics = [topic, ...(conv.pastGroupIds || []).map(p => b4a.toString(deriveGroupChatTopic(p.groupId), 'hex'))]
          if (record.groupTopicHex && !knownTopics.includes(record.groupTopicHex)) return
          // Legacy replicated controls have a sender-local id or no routing.
          // The registered core supplies the authoritative conversation scope.
          if (record.groupId && record.groupId !== conv.groupId) return
          record.groupTopicHex = topic
        } else if (record.groupTopicHex || GROUP_CONTROLS.has(record.type)) return
        record.conversationId = conv.id
      }
      if (record.type != null) {
        if (record.type === '__receipt') {
          if (conv) await processReceipt(conv.id, record, peer)
          return
        }
        if (record.type === '__caps') {
          if (conv && conv.type === 'group' && ipcHandler) ipcHandler.onMemberCaps(conv.id, peer, record.features)
          return
        }
        if (!GROUP_CONTROLS.has(record.type) && record.type !== 'direct_invite') return
        if (!conv && !['group_invite', 'direct_invite'].includes(record.type)) return
        if (conv && conv.type === 'group') {
          if (record.type === 'direct_invite') return
          if (CREATOR_CONTROLS.has(record.type) && conv.creatorKey !== peer) return
        }
        if (record.type === 'group_invite' &&
            (record.creatorKey !== peer || !record.participants.includes(peer) ||
             !record.participants.includes(identity.publicKeyHex))) return
        if (record.type === 'group_member_added' &&
            !record.updatedParticipants.includes(record.newMemberKey)) return
        if (!ipcHandler) throw new Error('Control receiver not ready')
        const key = controlKey(record, peer)
        if (record.id && conv && chatStore.hasAppliedControl(conv.id, key)) return
        await ipcHandler.handlePeerControl(record, peer)
        const appliedConversation = conv || findInvitedGroup(chatStore)
        if (record.id && appliedConversation) chatStore.markControlApplied(appliedConversation.id, key)
        return
      }
      if (!conv) {
        // Only authenticated deterministic DMs can recover a missing live chat.
        if (replicated || record.groupTopicHex ||
            conversationId !== ChatStore.directChatId(identity.publicKeyHex, peer)) return
        conv = await chatStore.createConversationWithId(conversationId, 'direct', [peer])
        if (p2pManager) await p2pManager.joinConversation(conversationId, peer)
        if (ipcHandler) ipcHandler.pushEvent('conversation.invite_received', { conversation: conv })
      }
      const stored = await chatStore.addMessage(conv.id, record)
      // Native events use normalized persisted fields, never the wire payload.
      if (onMessage) onMessage(conv.id, stored, record, peer)
      if (!replicated && p2pManager) await p2pManager.sendDeliveryReceipt(conv.id, record.id, peer)
      return { deliveryReceipt: { messageId: record.id, senderId: peer } }
    })
    chains.set(lock, operation)
    try { return await operation } finally {
      if (chains.get(lock) === operation) chains.delete(lock)
    }
  }
}

async function serveAuthorizedMedia (chatStore, mediaTransfer, hash, peer, socket) {
  if (!mediaTransfer || !b4a.isBuffer(hash) || hash.length !== 32) return false
  if (!chatStore || !chatStore.canServeMedia(b4a.toString(hash, 'hex'), peer)) return false
  await mediaTransfer.handleRequest(hash, socket)
  return true
}
function acceptAuthorizedMediaChunk(chatStore, mediaTransfer, requests, hash, chunkIndex, totalChunks, chunkData, peer) {
  if (!mediaTransfer || !b4a.isBuffer(hash) || hash.length !== 32) return false
  const request = requests.get(b4a.toString(hash, 'hex'))
  if (!request || !chatStore || !chatStore.isPeerAuthorized(request.conversationId, peer)) return false
  return mediaTransfer.onChunkReceived(hash, chunkIndex, totalChunks, chunkData)
}
module.exports = { createPeerReceiver, serveAuthorizedMedia, acceptAuthorizedMediaChunk }
