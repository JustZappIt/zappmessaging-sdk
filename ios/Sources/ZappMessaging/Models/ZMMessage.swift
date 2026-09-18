//
//  ZMMessage.swift
//  ZappMessaging
//
//  Represents a chat message
//

import Foundation

/// A chat message
public struct ZMMessage: Codable, Identifiable, Equatable, Sendable {
    /// Unique message identifier
    public let id: String
    
    /// Conversation this message belongs to
    public let conversationId: String
    
    /// Sender's public key
    public let senderId: String
    
    /// Sender's display name
    public let senderName: String?
    
    /// Message content (text or caption)
    public let content: String
    
    /// Content type (MIME type)
    public let contentType: String
    
    /// When the message was sent
    public let timestamp: Date
    
    /// Whether this message was sent by the current user
    public let isFromMe: Bool
    
    /// Media hash ID (for media messages)
    public let mediaId: String?
    
    /// Media file size in bytes
    public let mediaSize: Int?
    
    /// Media width in pixels
    public let mediaWidth: Int?
    
    /// Media height in pixels
    public let mediaHeight: Int?
    
    /// Base64-encoded thumbnail data
    public let thumbnailData: String?
    
    /// Local file path to media
    public var mediaLocalPath: String?
    
    /// Media transfer state
    public var mediaTransferState: MediaTransferState?

    /// Persisted outgoing delivery state (`queued`, `sent`, `delivered`, or `read`).
    public let status: String?

    /// Id of the message this one replies to
    public let replyToId: String?

    /// Sender name of the replied-to message, denormalised at send time
    public let replyToSenderName: String?

    /// Content preview of the replied-to message, denormalised at send time
    public let replyToContent: String?

    /// MIME type of the replied-to message, denormalised at send time. `nil` from clients
    /// that predate the field, which readers treat as a text quote.
    public let replyToContentType: String?

    public init(
        id: String,
        conversationId: String,
        senderId: String,
        senderName: String? = nil,
        content: String,
        contentType: String = "text/plain",
        timestamp: Date = Date(),
        isFromMe: Bool,
        mediaId: String? = nil,
        mediaSize: Int? = nil,
        mediaWidth: Int? = nil,
        mediaHeight: Int? = nil,
        thumbnailData: String? = nil,
        mediaLocalPath: String? = nil,
        mediaTransferState: MediaTransferState? = nil,
        status: String? = nil,
        replyToId: String? = nil,
        replyToSenderName: String? = nil,
        replyToContent: String? = nil,
        replyToContentType: String? = nil
    ) {
        self.id = id
        self.conversationId = conversationId
        self.senderId = senderId
        self.senderName = senderName
        self.content = content
        self.contentType = contentType
        self.timestamp = timestamp
        self.isFromMe = isFromMe
        self.mediaId = mediaId
        self.mediaSize = mediaSize
        self.mediaWidth = mediaWidth
        self.mediaHeight = mediaHeight
        self.thumbnailData = thumbnailData
        self.mediaLocalPath = mediaLocalPath
        self.mediaTransferState = mediaTransferState
        self.status = status
        self.replyToId = replyToId
        self.replyToSenderName = replyToSenderName
        self.replyToContent = replyToContent
        self.replyToContentType = replyToContentType
    }
}

public extension ZMMessage {
    /// A copy with `thumbnailData` replaced. Used by the SDK to hydrate the
    /// thumbnail that `message.list` omits to keep the IPC frame small.
    func withThumbnailData(_ data: String?) -> ZMMessage {
        ZMMessage(
            id: id,
            conversationId: conversationId,
            senderId: senderId,
            senderName: senderName,
            content: content,
            contentType: contentType,
            timestamp: timestamp,
            isFromMe: isFromMe,
            mediaId: mediaId,
            mediaSize: mediaSize,
            mediaWidth: mediaWidth,
            mediaHeight: mediaHeight,
            thumbnailData: data,
            mediaLocalPath: mediaLocalPath,
            mediaTransferState: mediaTransferState,
            status: status,
            replyToId: replyToId,
            replyToSenderName: replyToSenderName,
            replyToContent: replyToContent,
            replyToContentType: replyToContentType
        )
    }
}

/// Media transfer state
public enum MediaTransferState: String, Codable, Equatable, Sendable {
    case sending
    case sent
    case receiving
    case complete
    case failed
}
