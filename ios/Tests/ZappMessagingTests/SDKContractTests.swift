//
//  SDKContractTests.swift
//  ZappMessagingTests
//
//  Regression tests for the decode contract between the JS core and Swift, and
//  for the SDK facade's boot path.
//
//  The decode tests below are not hypothetical. Every one of them failed before
//  the fix they guard, and each failure was SILENT — no crash, no error, just a
//  chat that quietly did the wrong thing.
//

import XCTest
@testable import ZappMessaging

final class SDKContractTests: XCTestCase {

    /// Decode a JSON object exactly the way production does: through
    /// `AnyCodable`, then unwrapped to `[String: Any]`. Reproducing the real path
    /// matters — hand-building a `[String: Any]` with Swift `Double`s would hide
    /// the very bug these tests exist to catch.
    private func decodePayload(_ json: String) throws -> [String: Any] {
        let envelope = #"{"id":"t","success":true,"data":\#(json)}"#
        let response = try JSONDecoder().decode(IPCResponse.self, from: Data(envelope.utf8))
        return response.data?.mapValues { $0.value } ?? [:]
    }

    func testStableIPCErrorCodesRemainTyped() {
        XCTAssertEqual(ZMErrorCode(rawValue: "OWN_PUBLIC_KEY"), .ownPublicKey)
        XCTAssertEqual(ZMErrorCode(rawValue: "MISSING_PARTICIPANT"), .missingParticipant)
        XCTAssertEqual(ZMErrorCode(rawValue: "FUTURE_CODE"), .unknown("FUTURE_CODE"))
        XCTAssertEqual(ZMErrorCode.ownPublicKey.rawValue, "OWN_PUBLIC_KEY")
    }

    func testOperationalFailureIdentifiersAreStableAtTheDisplayBoundary() {
        let failure = ZMOperationalFailure(
            operation: .conversationRefresh(.messageReceived),
            code: .refreshFailed,
            message: "offline"
        )

        XCTAssertEqual(failure.operation.identifier, "conversation.refresh.message.received")
        XCTAssertEqual(failure.code.identifier, "REFRESH_FAILED")
        XCTAssertEqual(ZMOperationalFailure.Operation.workletRecovery.identifier, "worklet.recovery")
        XCTAssertEqual(ZMOperationalFailure.Code.workletRestarted.identifier, "WORKLET_RESTARTED")
        XCTAssertEqual(ZMOperationalFailure.Code.workletRestartExhausted.identifier, "WORKLET_RESTART_EXHAUSTED")
    }

    func testBareIPCReadClassificationMatchesUpstreamSemantics() {
        XCTAssertEqual(BareIPCReadResult.classify(nil), .wouldBlock)
        XCTAssertEqual(BareIPCReadResult.classify(Data()), .endOfStream)
        XCTAssertEqual(BareIPCReadResult.classify(Data([0x0A])), .data(Data([0x0A])))
    }

    // MARK: - Numeric coercion
    //
    // JS sends `Date.now()` — an integer. `AnyCodable` decodes an integral JSON
    // number as `Int`, and Swift's `as?` does not bridge `Int` to `Double`, so
    // `any as? TimeInterval` returned nil for EVERY timestamp and every parser
    // fell back to `Date()`. Effect: the conversation list could not sort and the
    // chat room could not order messages.

    func testIntegralJSONNumberIsReadableAsDouble() throws {
        let payload = try decodePayload(#"{"timestamp":1751500000000,"progress":1}"#)

        // The precise shape of the bug: the raw cast fails...
        XCTAssertNil(payload["timestamp"] as? TimeInterval,
                     "Guard: an integral JSON number boxes as Int and must NOT cast to Double. "
                     + "If this ever starts passing, AnyCodable changed and the coercion helpers "
                     + "may no longer be load-bearing.")

        // ...and the coercion is what makes it readable.
        XCTAssertEqual(zmDouble(payload["timestamp"]), 1_751_500_000_000)
        XCTAssertEqual(zmInt(payload["timestamp"]), 1_751_500_000_000)

        // `progress` lands on exactly 1 at completion — an integer on the wire.
        XCTAssertEqual(zmDouble(payload["progress"]), 1.0)
    }

    func testFractionalJSONNumberStillCoerces() throws {
        let payload = try decodePayload(#"{"progress":0.5}"#)
        XCTAssertEqual(zmDouble(payload["progress"]), 0.5)
    }

    func testOutOfRangeAndFractionalIntegersAreRejectedWithoutTrapping() throws {
        let payload = try decodePayload(#"{"huge":1e19,"fractional":1.5}"#)

        XCTAssertNil(zmInt(payload["huge"]))
        XCTAssertNil(zmInt(payload["fractional"]))
    }

    func testMillisecondTimestampBecomesDate() throws {
        let payload = try decodePayload(#"{"timestamp":1751500000000}"#)
        let date = try XCTUnwrap(zmDate(payload["timestamp"]))
        XCTAssertEqual(date.timeIntervalSince1970, 1_751_500_000, accuracy: 0.001)
    }

    // MARK: - Parsers

    func testMessageTimestampSurvivesTheWire() throws {
        let payload = try decodePayload("""
        {"id":"m1","conversationId":"c1","senderId":"abc","content":"hi",
         "contentType":"text/plain","timestamp":1751500000000,"isFromMe":false}
        """)

        let message = try XCTUnwrap(ZMParse.message(from: payload))
        XCTAssertEqual(message.timestamp.timeIntervalSince1970, 1_751_500_000, accuracy: 0.001)
        XCTAssertNotEqual(message.timestamp.timeIntervalSince1970,
                          Date().timeIntervalSince1970,
                          accuracy: 5,
                          "Timestamp fell back to Date() — the wire value was dropped.")
    }

    func testConversationSortKeySurvivesTheWire() throws {
        let older = try decodePayload("""
        {"id":"c1","type":"direct","participantIds":["a"],"displayName":"A",
         "createdAt":1751000000000,"lastMessageTimestamp":1751000000000}
        """)
        let newer = try decodePayload("""
        {"id":"c2","type":"direct","participantIds":["b"],"displayName":"B",
         "createdAt":1751500000000,"lastMessageTimestamp":1751500000000}
        """)

        let parsed = [older, newer].compactMap(ZMParse.conversation(from:))
        XCTAssertEqual(parsed.count, 2)

        // The whole point of the timestamp: the list is ordered by it.
        let sorted = parsed.sorted {
            ($0.lastMessageTimestamp ?? .distantPast) > ($1.lastMessageTimestamp ?? .distantPast)
        }
        XCTAssertEqual(sorted.map(\.id), ["c2", "c1"],
                       "Conversations did not sort by recency — lastMessageTimestamp was lost.")
    }

    func testReplyContextIsParsed() throws {
        let payload = try decodePayload("""
        {"id":"m2","conversationId":"c1","senderId":"abc","content":"re",
         "timestamp":1751500000000,"isFromMe":true,
         "replyToId":"m1","replyToSenderName":"alice","replyToContent":"hi"}
        """)

        let message = try XCTUnwrap(ZMParse.message(from: payload))
        XCTAssertEqual(message.replyToId, "m1")
        XCTAssertEqual(message.replyToSenderName, "alice")
        XCTAssertEqual(message.replyToContent, "hi")
    }

    func testContactWalletAddressIsParsed() throws {
        let payload = try decodePayload("""
        {"publicKey":"ab12","name":"bob","addedAt":1751500000000,
         "walletAddress":"u1abc","addressType":"unified"}
        """)

        let contact = try XCTUnwrap(ZMParse.contact(from: payload))
        XCTAssertEqual(contact.walletAddress, "u1abc")
        XCTAssertEqual(contact.addressType, "unified")
    }

    /// One malformed record must not take out the list. Swift used to `try map`
    /// with throwing parsers, so a single bad message failed the whole chat room;
    /// Kotlin has always used `mapNotNull`.
    func testOneMalformedRecordDoesNotKillTheList() throws {
        let good = try decodePayload("""
        {"id":"m1","conversationId":"c1","senderId":"abc","content":"hi","timestamp":1751500000000,"isFromMe":false}
        """)
        let malformed = try decodePayload(#"{"conversationId":"c1","content":"no id or sender"}"#)

        let parsed = [good, malformed, good].compactMap(ZMParse.message(from:))
        XCTAssertEqual(parsed.count, 2, "A malformed record should be skipped, not fatal.")
    }

    // MARK: - Facade boot

    /// Boots the real worklet through the public facade — which exercises the
    /// `protocol.init` readiness probe that replaced a blind 500ms sleep — and
    /// derives a golden-vector identity.
    ///
    /// This is also the standing check that the facade does not touch the
    /// Keychain: a host-less test bundle has no keychain entitlement, so if
    /// `restoreFromSeedPhrase` ever starts writing one again, this fails with
    /// `errSecMissingEntitlement (-34018)`.
    @MainActor
    func testFacadeBootsAndDerivesGoldenVector() async throws {
        let container = FileManager.default.temporaryDirectory
            .appendingPathComponent("zm-facade-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: container, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: container) }

        let sdk = ZappMessagingSDK(config: ZappMessagingConfig(dataDir: container))
        try await sdk.initialize()
        defer { Task { await sdk.shutdown() } }

        // Cold start with no identity is a normal outcome, not an error.
        XCTAssertNil(sdk.identity, "A fresh data dir should have no identity yet.")

        let phrase = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon "
                   + "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon art"

        let identity = try await sdk.restoreFromSeedPhrase(phrase, displayName: "tester")

        XCTAssertEqual(identity.publicKey,
                       "7afa7190d9f5daeaa45d9650ed3ce7c0973bb0e35f7361bf858389a8cf1c3f3c",
                       "Facade derivation diverged from the golden vector.")
        XCTAssertEqual(identity.displayName, "tester")
        XCTAssertEqual(sdk.identity?.publicKey, identity.publicKey)

        let details = try await sdk.getConnectionDetails()
        XCTAssertGreaterThanOrEqual(details.pendingMessageCount, 0)
        XCTAssertGreaterThanOrEqual(details.pendingQueues, 0)
        XCTAssertGreaterThanOrEqual(details.rtNodes, 0)
    }

    /// The JS appends `zappmessaging` to whatever `--data-dir` names, so the flag
    /// must carry the CONTAINER. Passing the store path yields
    /// `…/zappmessaging/zappmessaging`.
    func testDataDirArgvNamesTheContainerNotTheStore() throws {
        let container = FileManager.default.temporaryDirectory
            .appendingPathComponent("zm-datadir-\(UUID().uuidString)", isDirectory: true)
        let config = ZappMessagingConfig(dataDir: container)

        let dataDirArg = try XCTUnwrap(config.argv.first { $0.hasPrefix("--data-dir=") })
        XCTAssertFalse(
            dataDirArg.hasSuffix("/\(ZappMessagingConfig.storeDirectoryName)"),
            "--data-dir must name the container; the JS core appends "
            + "'\(ZappMessagingConfig.storeDirectoryName)' itself."
        )

        XCTAssertEqual(
            ZappMessagingConfig.storeDirectory(inContainer: container).lastPathComponent,
            ZappMessagingConfig.storeDirectoryName
        )
    }

    /// Offline delivery dies silently without these, so assert they actually
    /// reach argv rather than trusting the caller.
    func testOptionalArgvFlagsAreEmittedWhenSet() {
        let config = ZappMessagingConfig(
            dataDir: URL(fileURLWithPath: "/tmp/x"),
            blindPeerKeys: "KEY1",
            bootstrapNodes: "1.2.3.4:1234",
            blindPeerAddress: "5.6.7.8:49737",
            logLevel: "info"
        )

        XCTAssertTrue(config.argv.contains("--blind-peer-keys=KEY1"))
        XCTAssertTrue(config.argv.contains("--bootstrap-nodes=1.2.3.4:1234"))
        XCTAssertTrue(config.argv.contains("--blind-peer-address=5.6.7.8:49737"))
        XCTAssertTrue(config.argv.contains("--log-level=info"))

        // Absent flags must be omitted entirely, not passed empty.
        let bare = ZappMessagingConfig(dataDir: URL(fileURLWithPath: "/tmp/x"))
        XCTAssertEqual(bare.argv.count, 1)
        XCTAssertTrue(bare.argv[0].hasPrefix("--data-dir="))
    }

    func testIPCBridgeBuffersUTF8SplitAcrossChunks() async throws {
        let bridge = IPCBridge()
        let delivered = expectation(description: "split UTF-8 event delivered")

        await bridge.setEventHandler { type, payload in
            XCTAssertEqual(type, "test.event")
            XCTAssertEqual(payload["text"] as? String, "hello 😀")
            delivered.fulfill()
        }

        let frame = Data(#"{"type":"test.event","payload":{"text":"hello 😀"},"timestamp":1}"#.utf8) + Data([0x0A])
        let emojiStart = try XCTUnwrap(frame.range(of: Data("😀".utf8))?.lowerBound)

        await bridge.handleIncomingData(Data(frame[..<(emojiStart + 2)]))
        await bridge.handleIncomingData(Data(frame[(emojiStart + 2)...]))

        await fulfillment(of: [delivered], timeout: 1.0)
    }

    func testIPCBridgeSkipsCompleteOversizedFrameAndPreservesNextFrame() async {
        let bridge = IPCBridge(maxLineBytes: 128)
        let oversized = expectation(description: "oversized event is not delivered")
        oversized.isInverted = true
        let delivered = expectation(description: "frame after oversized event is delivered")

        await bridge.setEventHandler { type, _ in
            if type == "oversized.event" { oversized.fulfill() }
            if type == "valid.event" { delivered.fulfill() }
        }

        let huge = Data(#"{"type":"oversized.event","payload":{"pad":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"},"timestamp":1}"#.utf8) + Data([0x0A])
        let valid = Data(#"{"type":"valid.event","payload":{},"timestamp":1}"#.utf8) + Data([0x0A])
        XCTAssertGreaterThan(huge.count - 1, 128)

        await bridge.handleIncomingData(huge + valid)

        await fulfillment(of: [oversized, delivered], timeout: 0.2)
    }

    func testIPCBridgeSkipsChunkedOversizedFrameAndPreservesNextFrame() async {
        let bridge = IPCBridge(maxLineBytes: 128)
        let oversized = expectation(description: "chunked oversized event is not delivered")
        oversized.isInverted = true
        let delivered = expectation(description: "frame after chunked oversized event is delivered")

        await bridge.setEventHandler { type, _ in
            if type == "oversized.event" { oversized.fulfill() }
            if type == "valid.event" { delivered.fulfill() }
        }

        let huge = Data(#"{"type":"oversized.event","payload":{"pad":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"},"timestamp":1}"#.utf8)
        let valid = Data(#"{"type":"valid.event","payload":{},"timestamp":1}"#.utf8) + Data([0x0A])
        XCTAssertGreaterThan(huge.count, 129)

        await bridge.handleIncomingData(Data(huge.prefix(129)))
        await bridge.handleIncomingData(Data(huge.dropFirst(129)) + Data([0x0A]) + valid)

        await fulfillment(of: [oversized, delivered], timeout: 0.2)
    }

    func testIPCBridgeSurfacesSendFailureImmediately() async {
        let bridge = IPCBridge()

        do {
            _ = try await bridge.sendRequest(type: "protocol.init", timeout: 5.0)
            XCTFail("An IPC bridge without a worklet manager must fail")
        } catch ZMError.notInitialized {
            // Expected: the transport error is surfaced, not a later timeout.
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }

    func testIPCBridgeReplacesEventHandlerOnInitializationRetry() async {
        let bridge = IPCBridge()
        let staleHandler = expectation(description: "stale handler is not called")
        staleHandler.isInverted = true
        let currentHandler = expectation(description: "current handler is called once")

        await bridge.setEventHandler { _, _ in staleHandler.fulfill() }
        await bridge.setEventHandler { _, _ in currentHandler.fulfill() }

        let frame = Data(#"{"type":"test.event","payload":{},"timestamp":1}"#.utf8) + Data([0x0A])
        await bridge.handleIncomingData(frame)

        await fulfillment(of: [staleHandler, currentHandler], timeout: 0.2)
    }
}
