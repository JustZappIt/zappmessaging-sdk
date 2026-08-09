package xyz.justzappit.zappmessaging.models

data class ZMPushTopicSnapshot(
    val version: Int,
    val hydrated: Boolean,
    val supportsGroups: Boolean,
    val conversations: List<ZMPushConversationTopics>,
)

data class ZMPushConversationTopics(
    val conversationId: String,
    val lifecycle: String,
    val inboundTopics: List<ZMPushTopic>,
)

data class ZMPushTopic(
    val topic: String,
    val writerPublicKey: String,
)
