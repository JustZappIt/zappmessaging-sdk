//
//  ModelTests.swift
//  ZappMessagingTests
//
//  Tests for data models
//

import XCTest
@testable import ZappMessaging

final class ModelTests: XCTestCase {
    
    // MARK: - Identity Tests
    
    func testIdentityCreation() {
        let identity = ZMIdentity(
            publicKey: "abc123",
            displayName: "Alice",
            createdAt: Date()
        )
        
        XCTAssertEqual(identity.id, "abc123")
        XCTAssertEqual(identity.publicKey, "abc123")
        XCTAssertEqual(identity.displayName, "Alice")
    }
    
    func testIdentityCodable() throws {
        let identity = ZMIdentity(
            publicKey: "abc123",
            displayName: "Alice",
            createdAt: Date()
        )
        
        let encoder = JSONEncoder()
        let data = try encoder.encode(identity)
        
        let decoder = JSONDecoder()
        let decoded = try decoder.decode(ZMIdentity.self, from: data)
        
        XCTAssertEqual(decoded.publicKey, identity.publicKey)
        XCTAssertEqual(decoded.displayName, identity.displayName)
    }
    
    // MARK: - Conversation Tests
    
    func testConversationCreation() {
        let conversation = ZMConversation(
            id: "conv-123",
            type: .direct,
            participantIds: ["bob-key"],
            displayName: "Bob"
        )
        
        XCTAssertEqual(conversation.id, "conv-123")
        XCTAssertEqual(conversation.type, .direct)
        XCTAssertEqual(conversation.participantIds, ["bob-key"])
        XCTAssertEqual(conversation.displayName, "Bob")
    }
    
    func testConversationTypes() {
        XCTAssertEqual(ConversationType.direct.rawValue, "direct")
        XCTAssertEqual(ConversationType.group.rawValue, "group")
        XCTAssertEqual(ConversationType.store.rawValue, "store")
        XCTAssertEqual(ConversationType.city.rawValue, "city")
    }
    
    func testGroupConversation() {
        let conversation = ZMConversation(
            id: "conv-456",
            type: .group,
            participantIds: ["bob-key", "charlie-key"],
            groupId: "group-789",
            creatorKey: "alice-key",
            displayName: "Team Chat",
            isOwner: true
        )
        
        XCTAssertEqual(conversation.type, .group)
        XCTAssertEqual(conversation.groupId, "group-789")
        XCTAssertEqual(conversation.creatorKey, "alice-key")
        XCTAssertEqual(conversation.isOwner, true)
    }
    
    // MARK: - Message Tests
    
    func testMessageCreation() {
        let message = ZMMessage(
            id: "msg-123",
            conversationId: "conv-123",
            senderId: "alice-key",
            senderName: "Alice",
            content: "Hello!",
            contentType: "text/plain",
            isFromMe: true
        )
        
        XCTAssertEqual(message.id, "msg-123")
        XCTAssertEqual(message.conversationId, "conv-123")
        XCTAssertEqual(message.content, "Hello!")
        XCTAssertEqual(message.isFromMe, true)
    }
    
    func testMediaMessage() {
        let message = ZMMessage(
            id: "msg-456",
            conversationId: "conv-123",
            senderId: "alice-key",
            content: "Check this out",
            contentType: "image/jpeg",
            isFromMe: true,
            mediaId: "hash-abc",
            mediaSize: 123456,
            mediaWidth: 1920,
            mediaHeight: 1080,
            mediaTransferState: .complete
        )
        
        XCTAssertEqual(message.mediaId, "hash-abc")
        XCTAssertEqual(message.mediaSize, 123456)
        XCTAssertEqual(message.mediaTransferState, .complete)
    }
    
    func testMediaTransferStates() {
        XCTAssertEqual(MediaTransferState.sending.rawValue, "sending")
        XCTAssertEqual(MediaTransferState.sent.rawValue, "sent")
        XCTAssertEqual(MediaTransferState.receiving.rawValue, "receiving")
        XCTAssertEqual(MediaTransferState.complete.rawValue, "complete")
        XCTAssertEqual(MediaTransferState.failed.rawValue, "failed")
    }
    
    // MARK: - Contact Tests
    
    func testContactCreation() {
        let contact = ZMContact(
            publicKey: "bob-key",
            name: "Bob",
            addedAt: Date()
        )
        
        XCTAssertEqual(contact.id, "bob-key")
        XCTAssertEqual(contact.publicKey, "bob-key")
        XCTAssertEqual(contact.name, "Bob")
    }
    
    func testContactCodable() throws {
        let contact = ZMContact(
            publicKey: "bob-key",
            name: "Bob",
            addedAt: Date()
        )
        
        let encoder = JSONEncoder()
        let data = try encoder.encode(contact)
        
        let decoder = JSONDecoder()
        let decoded = try decoder.decode(ZMContact.self, from: data)
        
        XCTAssertEqual(decoded.publicKey, contact.publicKey)
        XCTAssertEqual(decoded.name, contact.name)
    }
    
    // MARK: - Error Tests
    
    func testErrorDescriptions() {
        XCTAssertNotNil(ZMError.notInitialized.errorDescription)
        XCTAssertNotNil(ZMError.identityNotFound.errorDescription)
        XCTAssertNotNil(ZMError.conversationNotFound.errorDescription)
        XCTAssertNotNil(ZMError.connectionFailed("test").errorDescription)
        XCTAssertNotNil(ZMError.invalidData("test").errorDescription)
    }
}
