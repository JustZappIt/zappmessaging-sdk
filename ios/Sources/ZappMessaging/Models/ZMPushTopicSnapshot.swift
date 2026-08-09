import Foundation

public struct ZMPushTopicSnapshot: Sendable {
    public let version: Int
    public let hydrated: Bool
    public let supportsGroups: Bool
    public let conversations: [ZMPushConversationTopics]
}

public struct ZMPushConversationTopics: Sendable {
    public let conversationId: String
    public let lifecycle: String
    public let inboundTopics: [ZMPushTopic]
}

public struct ZMPushTopic: Sendable {
    public let topic: String
    public let writerPublicKey: String
}
