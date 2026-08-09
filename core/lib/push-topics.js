const b4a = require('b4a')

/** Derive an inbound capability snapshot from already-opened remote writers. */
function getInboundPushTopics (remoteCores, conversationId) {
  const remotes = remoteCores.get(conversationId)
  if (!remotes) return []
  const topics = []
  for (const [writerPublicKey, core] of remotes) {
    if (!core || !core.discoveryKey) continue
    topics.push({
      topic: b4a.toString(core.discoveryKey, 'hex'),
      writerPublicKey
    })
  }
  return topics
}

module.exports = { getInboundPushTopics }
