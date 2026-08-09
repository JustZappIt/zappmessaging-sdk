//
//  IPCBridge.swift
//  ZappMessaging
//
//  Handles NDJSON IPC protocol communication with JavaScript worklet
//

import Foundation

/// Handles IPC communication with JavaScript worklet using NDJSON protocol
actor IPCBridge {
    private var pendingRequests: [String: CheckedContinuation<[String: Any], Error>] = [:]
    private var timeoutTasks: [String: Task<Void, Never>] = [:]
    private var eventHandler: ((String, [String: Any]) -> Void)?
    private var receiveBuffer = Data()
    // True while dropping the remainder of a single over-cap frame, up to its
    // next newline, so one huge frame can't take the whole stream down.
    private var skippingOversizedLine = false
    private weak var workletManager: BareWorkletManager?

    /// Per-frame ceiling. Any larger NDJSON line is skipped while the rest of
    /// the stream is preserved. Generous because frames are small now that
    /// message.list no longer inlines base64 thumbnails.
    private let maxLineBytes: Int

    private let encoder = JSONEncoder()
    private let decoder = JSONDecoder()

    /// Protocol version this client speaks. Must match Kotlin's
    /// `IPCBridge.CLIENT_PROTOCOL_VERSION`.
    static let clientProtocolVersion = "1.0"

    static let defaultTimeout: TimeInterval = 30.0
    static let protocolInitTimeout: TimeInterval = 5.0

    init(maxLineBytes: Int = 16 * 1024 * 1024) {
        precondition(maxLineBytes > 0)
        self.maxLineBytes = maxLineBytes
        encoder.outputFormatting = .sortedKeys
    }

    /// Set the worklet manager for sending data
    func setWorkletManager(_ manager: BareWorkletManager) {
        self.workletManager = manager
    }

    // MARK: - Request/Response

    /// Send a request and wait for response
    func sendRequest(
        type: String,
        payload: [String: Any] = [:],
        timeout: TimeInterval = IPCBridge.defaultTimeout
    ) async throws -> [String: Any] {
        let request = IPCRequest(type: type, payload: payload)
        let requestData = try encoder.encode(request)
        let framedRequest = requestData + Data([0x0A])

        return try await withCheckedThrowingContinuation { continuation in
            pendingRequests[request.id] = continuation

            // Held so a completed request can cancel it. Without this every
            // request leaked a live 30s timer until it fired.
            timeoutTasks[request.id] = Task { [weak self] in
                try? await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
                guard !Task.isCancelled else { return }
                await self?.timeOut(request.id)
            }

            Task { [weak self] in
                await self?.sendToWorklet(framedRequest, requestID: request.id)
            }
        }
    }

    private func timeOut(_ id: String) {
        timeoutTasks.removeValue(forKey: id)
        guard let pending = pendingRequests.removeValue(forKey: id) else { return }
        pending.resume(throwing: ZMError.ipcTimeout)
    }

    /// Negotiate the wire protocol with the worklet.
    ///
    /// Doubles as the readiness probe: the worklet only answers once its IPC
    /// handler is mounted, so a successful `protocol.init` is the signal that it
    /// is safe to send anything else.
    func negotiateProtocol() async throws {
        let response = try await sendRequest(
            type: "protocol.init",
            payload: ["protocolVersion": IPCBridge.clientProtocolVersion],
            timeout: IPCBridge.protocolInitTimeout
        )

        guard response["compatible"] as? Bool == true else {
            let workletVersion = response["protocolVersion"] as? String ?? "unknown"
            throw ZMError.ipcError(
                code: .unknown("INCOMPATIBLE_PROTOCOL"),
                message: "Worklet protocol \(workletVersion) is incompatible with client \(IPCBridge.clientProtocolVersion)"
            )
        }
    }

    /// Fail every in-flight request. Called on shutdown so callers awaiting a
    /// response get an error instead of hanging until their timeout.
    func cancelAllPendingRequests(error: Error = ZMError.notInitialized) {
        for task in timeoutTasks.values {
            task.cancel()
        }
        timeoutTasks.removeAll()

        let pending = pendingRequests
        pendingRequests.removeAll()
        for continuation in pending.values {
            continuation.resume(throwing: error)
        }
    }

    func clearEventHandlers() {
        eventHandler = nil
    }

    /// Handle incoming data from worklet
    func handleIncomingData(_ data: Data) {
        receiveBuffer.append(data)

        // Process all complete newline-delimited messages
        while let newlineIndex = receiveBuffer.firstIndex(of: 0x0A) {
            if skippingOversizedLine {
                // These bytes are the tail of an over-cap frame we already gave
                // up on; drop them and resume at the next frame.
                receiveBuffer.removeSubrange(...newlineIndex)
                skippingOversizedLine = false
                continue
            }

            // Check before copying or decoding the line. A complete huge frame
            // may arrive in one callback, so the unterminated-tail check below
            // is not sufficient on its own.
            let lineByteCount = receiveBuffer.distance(from: receiveBuffer.startIndex, to: newlineIndex)
            if lineByteCount > maxLineBytes {
                print("[IPCBridge] IPC frame exceeds \(maxLineBytes) bytes; skipping it")
                receiveBuffer.removeSubrange(...newlineIndex)
                continue
            }

            let lineData = Data(receiveBuffer[..<newlineIndex])
            receiveBuffer.removeSubrange(...newlineIndex)
            guard !lineData.isEmpty else { continue }
            guard let line = String(data: lineData, encoding: .utf8) else {
                print("[IPCBridge] Discarding invalid UTF-8 frame of \(lineData.count) bytes")
                continue
            }
            processLine(line)
        }

        // An un-terminated frame past the cap: drop just this frame and skip to
        // its next newline, preserving the rest of the stream. Previously the
        // whole buffer was cleared — killing every in-flight request and
        // deterministically bricking a media-heavy conversation on retry.
        if receiveBuffer.count > maxLineBytes {
            print("[IPCBridge] IPC frame exceeds \(maxLineBytes) bytes without a newline; skipping it")
            skippingOversizedLine = true
            receiveBuffer.removeAll(keepingCapacity: false)
        }
    }

    private func processLine(_ line: String) {
        guard let data = line.data(using: .utf8) else { return }

        // Try to decode as response first (has "id" and "success" fields)
        if let response = try? decoder.decode(IPCResponse.self, from: data) {
            handleResponse(response)
            return
        }

        // Try to decode as event (has "type" field but no "id")
        if let event = try? decoder.decode(IPCEvent.self, from: data) {
            handleEvent(event)
            return
        }

        // Log unrecognized messages for debugging
        print("[IPCBridge] Skipping non-JSON or unrecognized message: \(line.prefix(120))")
    }

    private func handleResponse(_ response: IPCResponse) {
        timeoutTasks.removeValue(forKey: response.id)?.cancel()

        guard let continuation = pendingRequests.removeValue(forKey: response.id) else {
            return
        }

        if response.success {
            let data = response.data?.mapValues { $0.value } ?? [:]
            continuation.resume(returning: data)
        } else {
            let errorCode = ZMErrorCode(rawValue: response.error?.code ?? "ERROR")
            let errorMessage = response.error?.message ?? "Unknown error"
            continuation.resume(throwing: ZMError.ipcError(code: errorCode, message: errorMessage))
        }
    }

    private func handleEvent(_ event: IPCEvent) {
        let payload = event.payload.mapValues { $0.value }
        eventHandler?(event.type, payload)
    }

    // MARK: - Event Handling

    /// Set the SDK event handler. Replacing rather than appending makes SDK
    /// initialization retry-safe.
    func setEventHandler(_ handler: @escaping (String, [String: Any]) -> Void) {
        eventHandler = handler
    }

    // MARK: - Worklet Communication

    /// Send data to the worklet and fail the matching request immediately if
    /// the transport rejects it.
    private func sendToWorklet(_ data: Data, requestID: String) async {
        do {
            guard let workletManager else { throw ZMError.notInitialized }
            try await workletManager.sendData(data)
        } catch {
            timeoutTasks.removeValue(forKey: requestID)?.cancel()
            guard let continuation = pendingRequests.removeValue(forKey: requestID) else { return }
            continuation.resume(throwing: error)
        }
    }
}

// MARK: - Typed request helpers

extension IPCBridge {
    func createIdentity(displayName: String) async throws -> ZMIdentity {
        let response = try await sendRequest(type: "identity.create", payload: ["displayName": displayName])
        guard let identity = ZMParse.identity(from: response) else {
            throw ZMError.invalidData("Invalid identity data")
        }
        return identity
    }

    /// The worklet answers `{}` when no identity has been derived yet — that is a
    /// normal cold-start outcome, not an error.
    func getIdentity() async throws -> ZMIdentity? {
        let response = try await sendRequest(type: "identity.get")
        guard response["publicKey"] as? String != nil else {
            return nil
        }
        return ZMParse.identity(from: response)
    }

    func createConversation(
        type: ConversationType,
        participants: [String],
        displayName: String?,
        storeId: String? = nil,
        citySlug: String? = nil
    ) async throws -> ZMConversation {
        var payload: [String: Any] = [
            "type": type.rawValue,
            "participants": participants
        ]
        payload["displayName"] = displayName
        payload["storeId"] = storeId
        payload["citySlug"] = citySlug

        let response = try await sendRequest(type: "conversation.create", payload: payload)
        guard let conversationData = response["conversation"] as? [String: Any],
              let conversation = ZMParse.conversation(from: conversationData) else {
            throw ZMError.invalidData("Missing conversation in response")
        }
        return conversation
    }

    func listConversations() async throws -> [ZMConversation] {
        let response = try await sendRequest(type: "conversation.list")
        guard let conversations = response["conversations"] as? [[String: Any]] else {
            throw ZMError.invalidData("Missing conversations in response")
        }
        return conversations.compactMap(ZMParse.conversation(from:))
    }

    /// Join the conversation's swarm topic and flush anything queued for it.
    ///
    /// Must be sent before any `message.send`. Without it the message persists
    /// locally, `p2pManager.sendToConversation` has no socket, and it sits in the
    /// queue forever — a send that silently never arrives.
    func ensureConversationConnected(conversationId: String) async throws {
        _ = try await sendRequest(
            type: "connection.connect",
            payload: ["conversationId": conversationId]
        )
    }

    func sendMessage(
        conversationId: String,
        content: String,
        contentType: String = "text/plain",
        replyTo: ZMReplyContext? = nil
    ) async throws -> ZMMessage {
        var payload: [String: Any] = [
            "conversationId": conversationId,
            "content": content,
            "contentType": contentType
        ]
        if let replyTo {
            payload["replyToId"] = replyTo.id
            payload["replyToSenderName"] = replyTo.senderName
            payload["replyToContent"] = replyTo.content
        }

        let response = try await sendRequest(type: "message.send", payload: payload)
        guard let messageData = response["message"] as? [String: Any],
              let message = ZMParse.message(from: messageData) else {
            throw ZMError.invalidData("Missing message in response")
        }
        return message
    }

    func listMessages(conversationId: String, limit: Int = 50) async throws -> [ZMMessage] {
        let payload: [String: Any] = [
            "conversationId": conversationId,
            "limit": limit
        ]
        let response = try await sendRequest(type: "message.list", payload: payload)
        guard let raw = response["messages"] as? [[String: Any]] else {
            throw ZMError.invalidData("Missing messages in response")
        }
        let messages = raw.compactMap(ZMParse.message(from:))
        return await hydrateThumbnails(conversationId: conversationId, messages: messages, raw: raw)
    }

    /// message.list omits inline base64 thumbnails (they can push the frame past
    /// the IPC receive cap). Re-fetch each stripped thumbnail via
    /// message.get_thumbnail so the returned model matches the pre-strip
    /// behaviour and callers need no changes.
    private func hydrateThumbnails(
        conversationId: String,
        messages: [ZMMessage],
        raw: [[String: Any]]
    ) async -> [ZMMessage] {
        let needing = Set(raw.compactMap { dict -> String? in
            let hasThumb = (dict["hasThumbnail"] as? Bool) ?? false
            guard hasThumb, let id = dict["id"] as? String else { return nil }
            return id
        })
        if needing.isEmpty { return messages }

        var thumbnails: [String: String] = [:]
        for id in needing {
            if let thumb = try? await fetchThumbnail(conversationId: conversationId, messageId: id) {
                thumbnails[id] = thumb
            }
        }

        return messages.map { message in
            needing.contains(message.id) ? message.withThumbnailData(thumbnails[message.id]) : message
        }
    }

    private func fetchThumbnail(conversationId: String, messageId: String) async throws -> String? {
        let response = try await sendRequest(
            type: "message.get_thumbnail",
            payload: ["conversationId": conversationId, "messageId": messageId]
        )
        return response["thumbnailData"] as? String
    }

    func markRead(conversationId: String) async throws {
        _ = try await sendRequest(
            type: "message.mark_read",
            payload: ["conversationId": conversationId]
        )
    }

    /// Payload key is `enabled`, not `visible` — see core/lib/ipc-handler.js.
    func setReadReceiptsEnabled(_ enabled: Bool) async throws {
        _ = try await sendRequest(
            type: "message.set_read_receipts",
            payload: ["enabled": enabled]
        )
    }

    func setPresenceVisible(_ enabled: Bool) async throws {
        _ = try await sendRequest(
            type: "message.set_presence_visible",
            payload: ["enabled": enabled]
        )
    }

    func addContact(publicKey: String, name: String) async throws {
        let payload: [String: Any] = [
            "publicKey": publicKey,
            "name": name
        ]
        _ = try await sendRequest(type: "contacts.add", payload: payload)
    }

    func listContacts() async throws -> [ZMContact] {
        let response = try await sendRequest(type: "contacts.list")
        guard let contacts = response["contacts"] as? [[String: Any]] else {
            throw ZMError.invalidData("Missing contacts in response")
        }
        return contacts.compactMap(ZMParse.contact(from:))
    }
}

/// The message being replied to, denormalised at send time so the recipient can
/// render the quote without fetching it.
public struct ZMReplyContext: Equatable, Sendable {
    public let id: String
    public let senderName: String
    public let content: String

    public init(id: String, senderName: String, content: String) {
        self.id = id
        self.senderName = senderName
        self.content = content
    }
}
