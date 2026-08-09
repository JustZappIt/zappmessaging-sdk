//
//  ZMParse.swift
//  ZappMessaging
//
//  Decoding of worklet payloads into the public models.
//
//  Every parser returns nil on a malformed record rather than throwing, and
//  callers use `compactMap`. This mirrors Kotlin's `mapNotNull` (IPCBridge.kt):
//  one bad record must not fail the whole list, or a single corrupt message
//  takes down the entire chat room.
//
//  These are free functions, not actor methods, so IPCBridge (request
//  responses) and ZappMessagingSDK (pushed events) decode through the same
//  code. They used to hold divergent copies of it.
//

import Foundation

enum ZMParse {
    static func identity(from data: [String: Any]) -> ZMIdentity? {
        guard let publicKey = data["publicKey"] as? String,
              let displayName = data["displayName"] as? String else {
            return nil
        }

        return ZMIdentity(
            publicKey: publicKey,
            displayName: displayName,
            createdAt: zmDate(data["createdAt"]) ?? Date()
        )
    }

    static func conversation(from data: [String: Any]) -> ZMConversation? {
        guard let id = data["id"] as? String,
              let typeString = data["type"] as? String,
              let type = ConversationType(rawValue: typeString) else {
            return nil
        }

        return ZMConversation(
            id: id,
            type: type,
            participantIds: data["participantIds"] as? [String] ?? [],
            groupId: data["groupId"] as? String,
            creatorKey: data["creatorKey"] as? String,
            displayName: data["displayName"] as? String ?? "",
            lastMessage: data["lastMessage"] as? String,
            lastMessageTimestamp: zmDate(data["lastMessageTimestamp"]),
            createdAt: zmDate(data["createdAt"]) ?? Date(),
            isOwner: data["isOwner"] as? Bool
        )
    }

    static func message(from data: [String: Any]) -> ZMMessage? {
        guard let id = data["id"] as? String,
              let conversationId = data["conversationId"] as? String,
              let senderId = data["senderId"] as? String else {
            return nil
        }

        let mediaTransferState = (data["mediaTransferState"] as? String)
            .flatMap(MediaTransferState.init(rawValue:))

        return ZMMessage(
            id: id,
            conversationId: conversationId,
            senderId: senderId,
            senderName: data["senderName"] as? String,
            content: data["content"] as? String ?? "",
            contentType: data["contentType"] as? String ?? "text/plain",
            timestamp: zmDate(data["timestamp"]) ?? Date(),
            isFromMe: data["isFromMe"] as? Bool ?? false,
            mediaId: data["mediaId"] as? String,
            mediaSize: zmInt(data["mediaSize"]),
            mediaWidth: zmInt(data["mediaWidth"]),
            mediaHeight: zmInt(data["mediaHeight"]),
            thumbnailData: data["thumbnailData"] as? String,
            mediaLocalPath: data["mediaLocalPath"] as? String,
            mediaTransferState: mediaTransferState,
            status: data["status"] as? String,
            replyToId: data["replyToId"] as? String,
            replyToSenderName: data["replyToSenderName"] as? String,
            replyToContent: data["replyToContent"] as? String
        )
    }

    static func contact(from data: [String: Any]) -> ZMContact? {
        guard let publicKey = data["publicKey"] as? String,
              let name = data["name"] as? String else {
            return nil
        }

        return ZMContact(
            publicKey: publicKey,
            name: name,
            addedAt: zmDate(data["addedAt"]) ?? Date(),
            walletAddress: data["walletAddress"] as? String,
            addressType: data["addressType"] as? String
        )
    }
}
