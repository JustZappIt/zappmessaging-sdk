//
//  ZMConversation.swift
//  ZappMessaging
//
//  Represents a direct or group chat conversation
//

import Foundation

/// A chat conversation
public struct ZMConversation: Codable, Identifiable, Equatable, Sendable {
    /// Unique conversation identifier
    public let id: String
    
    /// Type of conversation
    public let type: ConversationType
    
    /// Participant public keys (excluding self for groups)
    public let participantIds: [String]
    
    /// Group ID (for group chats only)
    public let groupId: String?
    
    /// Creator's public key (for groups only)
    public let creatorKey: String?
    
    /// Display name for the conversation
    public var displayName: String
    
    /// Preview of last message
    public var lastMessage: String?
    
    /// Timestamp of last message
    public var lastMessageTimestamp: Date?
    
    /// When the conversation was created
    public let createdAt: Date
    
    /// Whether current user is the group owner
    public let isOwner: Bool?
    
    public init(
        id: String,
        type: ConversationType,
        participantIds: [String],
        groupId: String? = nil,
        creatorKey: String? = nil,
        displayName: String,
        lastMessage: String? = nil,
        lastMessageTimestamp: Date? = nil,
        createdAt: Date = Date(),
        isOwner: Bool? = nil
    ) {
        self.id = id
        self.type = type
        self.participantIds = participantIds
        self.groupId = groupId
        self.creatorKey = creatorKey
        self.displayName = displayName
        self.lastMessage = lastMessage
        self.lastMessageTimestamp = lastMessageTimestamp
        self.createdAt = createdAt
        self.isOwner = isOwner
    }
}

/// Type of conversation
public enum ConversationType: String, Codable, Equatable, Sendable {
    case direct
    case group
}
