//
//  GroupLinkTests.swift
//  ZappMessagingTests
//
//  Group invite links and member removal: the decode contract with the JS
//  core, and one pass through the real worklet.
//

import XCTest
@testable import ZappMessaging

final class GroupLinkTests: XCTestCase {
    /// Decode through `AnyCodable`, exactly as production does.
    private func decodePayload(_ json: String) throws -> [String: Any] {
        let envelope = #"{"id":"t","success":true,"data":\#(json)}"#
        let response = try JSONDecoder().decode(IPCResponse.self, from: Data(envelope.utf8))
        return response.data?.mapValues { $0.value } ?? [:]
    }

    func testOwnersLinkDecodesAndIsNeverPrinted() throws {
        let data = try decodePayload(#"""
        {"conversationId":"g1","state":"active","link":"https://join.justzappit.xyz/g/v1#SECRET","linkId":"11",
         "createdAt":1789800000000,"expiresAt":1790000000000,"maxJoins":5,"joins":2,"includeName":false,
         "approval":"owner","approvalReason":"rate","pendingRequests":3}
        """#)
        let info = ZMParse.groupLink(from: data)
        XCTAssertEqual(info.state, .active)
        XCTAssertEqual(info.approval, .owner)
        XCTAssertEqual(info.maxJoins, 5)
        XCTAssertEqual(info.joins, 2)
        XCTAssertFalse(info.includeName)
        XCTAssertEqual(info.pendingRequests, 3)
        XCTAssertEqual(info.expiresAt, Date(timeIntervalSince1970: 1_790_000_000))
        XCTAssertFalse(String(describing: info).contains("SECRET"))
        XCTAssertFalse(String(reflecting: info).contains("SECRET"))
    }

    func testUnknownValuesFallBackSafely() throws {
        let link = ZMParse.groupLink(from: try decodePayload(#"{"state":"later","approval":"committee"}"#))
        XCTAssertEqual(link.state, .unknown)
        XCTAssertEqual(link.approval, .auto)
        XCTAssertEqual(ZMParse.joinResult(from: try decodePayload(#"{"status":"new_thing"}"#)).status, .malformed)
        XCTAssertEqual(ZMParse.joinUpdate(from: try decodePayload(#"{"linkId":"l","status":"new_thing"}"#))?.status, .unknown)
    }

    func testOptionsBecomeThePayloadTheCoreExpects() {
        let clearing = ZMParse.groupLinkPayload(conversationId: "g1", options: ZMGroupLinkOptions(clearExpiry: true, maxJoins: 4, approval: .owner))
        XCTAssertTrue(clearing["expiresAt"] is NSNull)
        XCTAssertEqual(clearing["maxJoins"] as? Int, 4)
        XCTAssertEqual(clearing["approval"] as? String, "owner")
        XCTAssertNil(clearing["includeName"])

        let setting = ZMParse.groupLinkPayload(conversationId: "g1", options: ZMGroupLinkOptions(expiresAt: Date(timeIntervalSince1970: 1_790_000_000)))
        XCTAssertEqual(setting["expiresAt"] as? Int64, 1_790_000_000_000)
    }

    func testInspectionExpiryIsInSeconds() throws {
        let inspection = ZMParse.inspection(from: try decodePayload(#"{"status":"ok","nameHint":"Crew","expiresAt":1790000000,"linkId":"c0"}"#))
        XCTAssertEqual(inspection.status, .ok)
        XCTAssertEqual(inspection.nameHint, "Crew")
        XCTAssertEqual(inspection.expiresAt, Date(timeIntervalSince1970: 1_790_000_000))
    }

    func testJoinUpdatesAndRequestsDecode() throws {
        let update = ZMParse.joinUpdate(from: try decodePayload(#"{"linkId":"l1","status":"pending_approval"}"#))
        XCTAssertEqual(update?.status, .pendingApproval)
        XCTAssertEqual(update?.status.isWaiting, true)
        let request = ZMParse.approvalRequest(conversationId: "g1", from: try decodePayload(
            #"{"joinerKey":"cdcd","joinerName":"Ben","requestedAt":1789800000000,"previouslyRemoved":true}"#))
        XCTAssertEqual(request?.joinerName, "Ben")
        XCTAssertEqual(request?.previouslyRemoved, true)
        XCTAssertNil(ZMParse.approvalRequest(conversationId: "g1", from: [:]))
    }

    func testRemovedGroupCarriesItsRemovalTime() throws {
        let conversation = ZMParse.conversation(from: try decodePayload(
            #"{"id":"g1","type":"group","displayName":"Crew","participantIds":[],"removedAt":1789800000000}"#))
        XCTAssertEqual(conversation?.removedAt, Date(timeIntervalSince1970: 1_789_800_000))
    }

    /// The whole link path through the real worklet: an owner makes a group
    /// and a link, inspects it, and opening their own link needs nothing.
    @MainActor
    func testOwnerLinkThroughTheRealWorklet() async throws {
        let container = FileManager.default.temporaryDirectory
            .appendingPathComponent("zm-link-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: container, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: container) }

        let sdk = ZappMessagingSDK(config: ZappMessagingConfig(dataDir: container))
        try await sdk.initialize()
        defer { Task { await sdk.shutdown() } }

        let phrase = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon "
                   + "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon art"
        _ = try await sdk.restoreFromSeedPhrase(phrase, displayName: "owner")

        let member = String(repeating: "ab", count: 32)
        let group = try await sdk.createConversation(type: .group, participants: [member], displayName: "Hiking Crew")

        let info = try await sdk.enableGroupLink(conversationId: group.id, options: ZMGroupLinkOptions(approval: .owner))
        XCTAssertEqual(info.state, .active)
        XCTAssertEqual(info.approval, .owner)
        let link = try XCTUnwrap(info.link)
        XCTAssertTrue(link.hasPrefix("https://join.justzappit.xyz/g/v1#"))

        let inspection = try await sdk.inspectGroupLink(link)
        XCTAssertEqual(inspection.status, .ok)
        XCTAssertEqual(inspection.nameHint, "Hiking Crew")
        XCTAssertEqual(inspection.linkId, info.linkId)

        let own = try await sdk.joinGroupViaLink(link)
        XCTAssertEqual(own.status, .alreadyMember)
        XCTAssertEqual(own.conversationId, group.id)

        let reset = try await sdk.resetGroupLink(conversationId: group.id)
        XCTAssertNotEqual(reset.link, link)
        let requests = try await sdk.groupJoinRequests(conversationId: group.id)
        XCTAssertTrue(requests.isEmpty)
        let olderMembers = try await sdk.olderMemberCount(conversationId: group.id)
        XCTAssertEqual(olderMembers, 1, "the member has never announced support")
    }
}
