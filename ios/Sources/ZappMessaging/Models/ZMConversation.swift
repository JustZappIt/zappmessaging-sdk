//
//  ZMConversation.swift
//  ZappMessaging
//
//  Represents a chat conversation (direct, group, store, or city)
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
    
    /// Store ID (for store rooms only)
    public let storeId: String?
    
    /// City slug (for city channels only)
    public let citySlug: String?
    
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
        storeId: String? = nil,
        citySlug: String? = nil,
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
        self.storeId = storeId
        self.citySlug = citySlug
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
    case store
    case city
}
