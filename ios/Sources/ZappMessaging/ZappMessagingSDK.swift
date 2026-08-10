//
//  ZappMessagingSDK.swift
//  ZappMessaging
//
//  Main public API for ZappMessaging SDK
//

import Foundation
import Combine

/// Main SDK class for ZappMessaging
@MainActor
public final class ZappMessagingSDK: ObservableObject {
    
    // MARK: - Published Properties
    
    /// Current user identity
    @Published public private(set) var identity: ZMIdentity?
    
    /// All conversations
    @Published public private(set) var conversations: [ZMConversation] = []
    
    /// All contacts
    @Published public private(set) var contacts: [ZMContact] = []
    
    /// Connection status
    @Published public private(set) var isOnline: Bool = false

    /// Peer count
    @Published public private(set) var peerCount: Int = 0

    /// DHT health: "healthy" | "degraded" | "critical". Drives the chat list's
    /// connection banner.
    @Published public private(set) var dhtHealth: String = "healthy"

    // MARK: - Private Properties

    private let ipcBridge = IPCBridge()
    private let workletManager = BareWorkletManager()

    private var isInitialized = false
    private var eventCancellables: Set<AnyCancellable> = []
    private var conversationRefreshTask: Task<Void, Never>?
    private var conversationRefreshNeedsFollowup = false
    
    // MARK: - Event Publishers
    
    /// Publisher for incoming messages (conversationId, message)
    public let messageReceived = PassthroughSubject<(conversationId: String, message: ZMMessage), Never>()
    
    /// Publisher for conversation invites
    public let inviteReceived = PassthroughSubject<ZMConversation, Never>()
    
    /// Publisher for a member leaving a conversation
    public let memberLeft = PassthroughSubject<(conversationId: String, leaverKey: String), Never>()
    
    /// Publisher for a group being deleted
    public let groupDeleted = PassthroughSubject<String, Never>()
    
    /// Publisher for a group being renamed
    public let groupRenamed = PassthroughSubject<(conversationId: String, newName: String), Never>()
    
    /// Publisher for a new member added to a group
    public let memberAdded = PassthroughSubject<(conversationId: String, memberKey: String, memberName: String), Never>()
    
    /// Publisher for message delivery status updates
    public let messageStatus = PassthroughSubject<(messageId: String, conversationId: String, status: String), Never>()
    
    /// Publisher for media download progress
    public let mediaDownloadProgress = PassthroughSubject<(mediaId: String, progress: Double), Never>()
    
    /// Publisher for media download completion
    public let mediaDownloadComplete = PassthroughSubject<(mediaId: String, filePath: String), Never>()
    
    /// Publisher for media transfer progress (sending)
    public let mediaTransferProgress = PassthroughSubject<(mediaId: String, progress: Double), Never>()
    
    /// Publisher for media transfer completion (sending)
    public let mediaTransferComplete = PassthroughSubject<String, Never>()

    /// Per-conversation peer presence. `peerId` is truncated to 12 chars by the
    /// core and is NOT comparable to a public key.
    public let peerStatus = PassthroughSubject<(conversationId: String, peerId: String, status: String), Never>()

    /// Emitted whenever direct-chat inbound push capabilities change.
    public let pushTopicsChanged = PassthroughSubject<Void, Never>()

    /// Failures that arrive asynchronously, after the initiating request has
    /// already returned. Hosts should surface these; logging alone makes a
    /// queued or persistence failure indistinguishable from success in the UI.
    public let operationalFailure = PassthroughSubject<ZMOperationalFailure, Never>()

    // MARK: - Initialization

    /// Startup configuration handed to the worklet as argv.
    private let config: ZappMessagingConfig

    /// Worklet readiness probe, mirroring Kotlin's ZappMessagingSDK.
    /// Cumulative envelope if every attempt fails: 300+600+900+1200+1500 = 4.5s.
    private static let startupProbeDelayMs: UInt64 = 300
    private static let startupMaxRetries = 5

    /// - Parameter config: worklet startup config. Supply `blindPeerKeys` and
    ///   `bootstrapNodes` from the host app's build config, or offline delivery
    ///   will not work. See `ZappMessagingConfig`.
    public init(config: ZappMessagingConfig) {
        self.config = config
        ZMLog.configure(level: config.logLevel)
    }

    /// Initialize the SDK
    public func initialize() async throws {
        guard !isInitialized else { return }

        // Connect IPC bridge to worklet manager
        await ipcBridge.setWorkletManager(workletManager)
        await workletManager.setFailureHandler { [weak self] error, recovered in
            Task { @MainActor [weak self] in
                self?.operationalFailure.send(
                    ZMOperationalFailure(
                        operation: .workletRecovery,
                        code: recovered ? .workletRestarted : .workletRestartExhausted,
                        message: error.localizedDescription
                    )
                )
            }
        }

        // Start worklet
        try await workletManager.start(config: config, ipcBridge: ipcBridge)

        try await negotiateProtocolWithRetry()

        // Install the single SDK event sink only after readiness succeeds. A
        // failed initialization can then be retried without duplicating every
        // event handler.
        await setupEventHandler()

        // A missing identity is the normal first-run outcome, not a failure: the
        // app derives one from the wallet seed once the user picks a name.
        do {
            identity = try await ipcBridge.getIdentity()
        } catch {
            ZMLog.debug("SDK", "No existing identity available during initialization")
        }

        if identity != nil {
            do {
                try await refreshConversations()
                try await refreshContacts()
                _ = try await getConnectionStatus()
            } catch {
                ZMLog.warning("SDK", "Initial data refresh failed")
            }
        }

        isInitialized = true
    }

    /// Probe the worklet until it answers `protocol.init`.
    ///
    /// The worklet only replies once its IPC handler is mounted, so this doubles
    /// as the readiness gate. A fixed sleep raced it: a slow cold start meant
    /// `identity.get` went out too early, came back empty, and the app concluded
    /// the user had no chat identity.
    private func negotiateProtocolWithRetry() async throws {
        for attempt in 1...Self.startupMaxRetries {
            try await Task.sleep(nanoseconds: Self.startupProbeDelayMs * UInt64(attempt) * 1_000_000)

            do {
                try await ipcBridge.negotiateProtocol()
                return
            } catch {
                ZMLog.warning("SDK", "Worklet readiness probe \(attempt) failed")
            }
        }

        ZMLog.error("SDK", "Worklet readiness probes exhausted")
        throw ZMError.notInitialized
    }

    /// Shutdown the SDK
    public func shutdown() async {
        conversationRefreshTask?.cancel()
        conversationRefreshTask = nil
        conversationRefreshNeedsFollowup = false
        await ipcBridge.cancelAllPendingRequests()
        await workletManager.stop()
        await ipcBridge.clearEventHandlers()
        isInitialized = false
    }
    
    // MARK: - Identity Management

    /// Get current identity
    public func getIdentity() async throws -> ZMIdentity? {
        return identity
    }

    /// Update the display name of the current identity.
    public func updateDisplayName(_ displayName: String) async throws {
        guard let currentIdentity = identity else {
            throw ZMError.identityNotFound
        }

        let response = try await ipcBridge.sendRequest(
            type: "identity.update",
            payload: ["displayName": displayName]
        )
        guard let persistedDisplayName = response["displayName"] as? String else {
            throw ZMError.invalidData("Invalid identity update response")
        }
        identity = ZMIdentity(
            publicKey: currentIdentity.publicKey,
            displayName: persistedDisplayName,
            createdAt: currentIdentity.createdAt
        )
    }

    /// Derive the chat identity from the wallet's BIP-39 seed phrase.
    ///
    /// The seed is passed through, never stored: the worklet persists the derived
    /// identity in `identity.json` inside its data dir, and the identity is always
    /// re-derivable from the wallet. This SDK deliberately keeps no copy of the
    /// wallet seed — Android keeps none either, and a second at-rest copy would be
    /// a strictly larger blast radius than the wallet itself.
    ///
    /// Idempotent for the same seed. A *different* seed destructively clears the
    /// chat and contact stores (see core/lib/ipc-handler.js).
    public func restoreFromSeedPhrase(_ seedPhrase: String, displayName: String) async throws -> ZMIdentity {
        let payload: [String: Any] = [
            "seedPhrase": seedPhrase,
            "displayName": displayName
        ]

        let response = try await ipcBridge.sendRequest(type: "migration.restore_from_seed_phrase", payload: payload)

        guard let publicKey = response["publicKey"] as? String,
              let displayName = response["displayName"] as? String else {
            throw ZMError.invalidData("Invalid restore response")
        }

        let restoredIdentity = ZMIdentity(publicKey: publicKey, displayName: displayName)
        self.identity = restoredIdentity

        do {
            try await refreshConversations()
            try await refreshContacts()
        } catch {
            // The identity restore has already succeeded and is durable. A
            // transient list failure must not make the host report that restore
            // itself failed.
            ZMLog.warning("SDK", "Post-restore data refresh failed")
        }

        return restoredIdentity
    }
    
    // MARK: - Conversation Management

    /// Returns whether the deterministic direct conversation with this peer was explicitly
    /// removed on this device. Hosts can use this to confirm before an intentional rejoin.
    public func hasLeftDirectConversation(publicKey: String) async throws -> Bool {
        guard identity != nil else {
            throw ZMError.identityNotFound
        }

        let response = try await ipcBridge.sendRequest(
            type: "conversation.direct_status",
            payload: ["participants": [publicKey]]
        )
        return response["isLeft"] as? Bool ?? false
    }

    /// Create a new conversation
    public func createConversation(
        type: ConversationType,
        participants: [String],
        displayName: String? = nil
    ) async throws -> ZMConversation {
        guard identity != nil else {
            throw ZMError.identityNotFound
        }
        
        let conversation = try await ipcBridge.createConversation(
            type: type,
            participants: participants,
            displayName: displayName
        )

        do {
            try await ipcBridge.ensureConversationConnected(conversationId: conversation.id)
        } catch {
            reportOperationalFailure(
                operation: .conversationConnect,
                code: .connectFailed,
                error: error
            )
        }
        await refreshConversationsAfterMutation(.create)

        return conversation
    }

    /// Join a conversation's swarm topic. Safe to call repeatedly.
    public func ensureConversationConnected(_ conversationId: String) async throws {
        try await ipcBridge.ensureConversationConnected(conversationId: conversationId)
    }
    
    /// Get all conversations
    public func getConversations() async throws -> [ZMConversation] {
        return conversations
    }
    
    /// Refresh conversations from core
    public func refreshConversations() async throws {
        conversations = try await ipcBridge.listConversations()
    }
    
    /// Leave a group conversation
    public func leaveConversation(_ conversationId: String) async throws {
        let payload = ["conversationId": conversationId]
        _ = try await ipcBridge.sendRequest(type: "conversation.leave", payload: payload)
        await refreshConversationsAfterMutation(.leave)
    }
    
    /// Delete a group conversation (owner only)
    public func deleteConversation(_ conversationId: String) async throws {
        let payload = ["conversationId": conversationId]
        _ = try await ipcBridge.sendRequest(type: "conversation.delete", payload: payload)
        await refreshConversationsAfterMutation(.delete)
    }
    
    /// Remove a conversation locally (works for both direct and group)
    public func removeConversation(_ conversationId: String) async throws {
        let payload = ["conversationId": conversationId]
        _ = try await ipcBridge.sendRequest(type: "conversation.remove", payload: payload)
        await refreshConversationsAfterMutation(.remove)
    }
    
    /// Rename a group conversation
    public func renameGroup(conversationId: String, name: String) async throws {
        let payload: [String: Any] = [
            "conversationId": conversationId,
            "name": name
        ]
        _ = try await ipcBridge.sendRequest(type: "conversation.rename", payload: payload)
        await refreshConversationsAfterMutation(.rename)
    }
    
    /// Add a member to a group conversation
    public func addMember(conversationId: String, publicKey: String, displayName: String? = nil) async throws {
        var payload: [String: Any] = [
            "conversationId": conversationId,
            "publicKey": publicKey
        ]
        if let displayName = displayName {
            payload["displayName"] = displayName
        }
        _ = try await ipcBridge.sendRequest(type: "conversation.add_member", payload: payload)
        await refreshConversationsAfterMutation(.addMember)
    }
    
    // MARK: - Message Management

    /// Send a text message.
    ///
    /// Joins the swarm topic first — without that the message persists locally and
    /// never leaves the device.
    public func sendMessage(
        conversationId: String,
        content: String,
        replyTo: ZMReplyContext? = nil
    ) async throws -> ZMMessage {
        guard identity != nil else {
            throw ZMError.identityNotFound
        }

        try await ipcBridge.ensureConversationConnected(conversationId: conversationId)

        return try await ipcBridge.sendMessage(
            conversationId: conversationId,
            content: content,
            replyTo: replyTo
        )
    }

    /// Get messages for a conversation
    public func getMessages(conversationId: String, limit: Int = 50) async throws -> [ZMMessage] {
        return try await ipcBridge.listMessages(conversationId: conversationId, limit: limit)
    }

    /// Mark every message in a conversation as read, emitting read receipts to
    /// the sender when the user has them enabled.
    public func markRead(conversationId: String) async throws {
        try await ipcBridge.markRead(conversationId: conversationId)
    }

    /// The worklet optimistically defaults read receipts ON at identity
    /// create/restore, before the app's preferences have loaded. The app must
    /// re-assert the user's real setting once identity lands, or a
    /// receipts-off user leaks read receipts in the cold-start window.
    public func setReadReceiptsEnabled(_ enabled: Bool) async throws {
        try await ipcBridge.setReadReceiptsEnabled(enabled)
    }

    public func setPresenceVisible(_ visible: Bool) async throws {
        try await ipcBridge.setPresenceVisible(visible)
    }
    
    /// Send a media message
    public func sendMediaMessage(
        conversationId: String,
        mediaPath: String,
        contentType: String,
        caption: String = "",
        thumbnailData: String? = nil,
        replyTo: ZMReplyContext? = nil
    ) async throws -> ZMMessage {
        guard identity != nil else {
            throw ZMError.identityNotFound
        }

        try await ipcBridge.ensureConversationConnected(conversationId: conversationId)

        // Prepare media (hash and store)
        let preparePayload: [String: Any] = [
            "filePath": mediaPath,
            "extension": contentType.components(separatedBy: "/").last ?? "jpg"
        ]
        let prepareResponse = try await ipcBridge.sendRequest(type: "media.prepare_send", payload: preparePayload)

        guard let mediaId = prepareResponse["mediaId"] as? String,
              let mediaSize = zmInt(prepareResponse["mediaSize"]),
              let mediaLocalPath = prepareResponse["mediaLocalPath"] as? String else {
            throw ZMError.mediaError("Failed to prepare media")
        }

        var sendPayload: [String: Any] = [
            "conversationId": conversationId,
            "content": caption,
            "contentType": contentType,
            "mediaId": mediaId,
            "mediaSize": mediaSize,
            "mediaLocalPath": mediaLocalPath
        ]
        sendPayload["thumbnailData"] = thumbnailData
        if let replyTo {
            sendPayload["replyToId"] = replyTo.id
            sendPayload["replyToSenderName"] = replyTo.senderName
            sendPayload["replyToContent"] = replyTo.content
        }

        let response = try await ipcBridge.sendRequest(type: "media.send_message", payload: sendPayload)

        guard let messageData = response["message"] as? [String: Any],
              let message = ZMParse.message(from: messageData) else {
            throw ZMError.invalidData("Missing message in response")
        }

        return message
    }
    
    // MARK: - Contact Management
    
    /// Add a contact
    public func addContact(publicKey: String, name: String) async throws {
        guard identity != nil else {
            throw ZMError.identityNotFound
        }
        
        try await ipcBridge.addContact(publicKey: publicKey, name: name)
        try await refreshContacts()
    }
    
    /// Get all contacts
    public func getContacts() async throws -> [ZMContact] {
        return contacts
    }
    
    /// Refresh contacts from core
    public func refreshContacts() async throws {
        contacts = try await ipcBridge.listContacts()
    }
    
    /// Update a contact
    public func updateContact(publicKey: String, name: String) async throws {
        let payload: [String: Any] = [
            "publicKey": publicKey,
            "updates": ["name": name]
        ]
        _ = try await ipcBridge.sendRequest(type: "contacts.update", payload: payload)
        try await refreshContacts()
    }
    
    /// Delete a contact
    public func deleteContact(publicKey: String) async throws {
        let payload = ["publicKey": publicKey]
        _ = try await ipcBridge.sendRequest(type: "contacts.remove", payload: payload)
        try await refreshContacts()
    }
    
    /// Update a contact's wallet address
    public func updateContactWalletAddress(publicKey: String, walletAddress: String) async throws {
        let payload: [String: Any] = [
            "publicKey": publicKey,
            "walletAddress": walletAddress
        ]
        _ = try await ipcBridge.sendRequest(type: "contacts.updateWalletAddress", payload: payload)
        try await refreshContacts()
    }
    
    // MARK: - Payment Messages
    
    /// Send a payment-related message (transaction, request, or wallet address)
    public func sendPaymentMessage(
        conversationId: String,
        message: [String: Any],
        type: PaymentMessageType
    ) async throws -> ZMMessage {
        guard identity != nil else {
            throw ZMError.identityNotFound
        }

        try await ipcBridge.ensureConversationConnected(conversationId: conversationId)

        let payload: [String: Any] = [
            "conversationId": conversationId,
            "message": message
        ]

        let response = try await ipcBridge.sendRequest(type: type.ipcType, payload: payload)
        guard let messageData = response["message"] as? [String: Any],
              let sent = ZMParse.message(from: messageData) else {
            throw ZMError.invalidData("Missing message in response")
        }

        return sent
    }
    
    // MARK: - Connection Management
    
    /// Get connection status
    public func getConnectionStatus() async throws -> (online: Bool, peerCount: Int) {
        let response = try await ipcBridge.sendRequest(type: "connection.status")
        let online = response["online"] as? Bool ?? false
        let peerCount = zmInt(response["peerCount"]) ?? 0

        self.isOnline = online
        self.peerCount = peerCount

        return (online, peerCount)
    }

    /// Return the same full transport snapshot exposed by the Android facade.
    public func getConnectionDetails() async throws -> ZMConnectionDetails {
        let response = try await ipcBridge.sendRequest(type: "connection.details")
        let lastCheck = response["dhtLastCheck"].flatMap { value -> String? in
            if value is NSNull { return nil }
            return value as? String ?? String(describing: value)
        }

        return ZMConnectionDetails(
            online: response["online"] as? Bool ?? false,
            peerCount: zmInt(response["peerCount"]) ?? 0,
            globalConnections: zmInt(response["globalConnections"]) ?? 0,
            pendingQueues: zmInt(response["pendingQueues"]) ?? 0,
            pendingMessageCount: zmInt(response["pendingMessageCount"]) ?? 0,
            pendingInvites: zmInt(response["pendingInvites"]) ?? 0,
            dhtHealth: response["dhtHealth"] as? String ?? "unknown",
            dhtLastCheck: lastCheck,
            consecutiveFailures: zmInt(response["consecutiveFailures"]) ?? 0,
            directConversations: zmInt(response["directConversations"]) ?? 0,
            groupConversations: zmInt(response["groupConversations"]) ?? 0,
            dhtBootstrapped: response["dhtBootstrapped"] as? Bool ?? false,
            dhtFirewalled: response["dhtFirewalled"] as? Bool,
            dhtRandomized: response["dhtRandomized"] as? Bool,
            rtNodes: zmInt(response["rtNodes"]) ?? 0,
            relayEnabled: response["relayEnabled"] as? Bool ?? false,
            relaysConnected: zmInt(response["relaysConnected"]) ?? 0,
            relaysTotal: zmInt(response["relaysTotal"]) ?? 0
        )
    }
    
    // MARK: - Lifecycle
    
    /// Suspend SDK (for app backgrounding)
    public func suspend() async {
        await workletManager.suspend()
    }
    
    /// Resume SDK (from app backgrounding)
    public func resume() async {
        await workletManager.resume()
        do {
            _ = try await getConnectionStatus()
        } catch {
            operationalFailure.send(
                ZMOperationalFailure(
                    operation: .connectionResume,
                    code: .statusFailed,
                    message: error.localizedDescription
                )
            )
            ZMLog.warning("SDK", "Connection refresh after resume failed")
        }
    }

    /// Complete direct-chat inbound topic snapshot. Topic derivation remains in JS.
    public func getPushTopicSnapshot() async throws -> ZMPushTopicSnapshot {
        let response = try await ipcBridge.sendRequest(type: "push.topics")
        guard let rawConversations = response["conversations"] as? [[String: Any]] else {
            throw ZMError.invalidData("Push topic snapshot is missing conversations")
        }
        let conversations = try rawConversations.map { value -> ZMPushConversationTopics in
            guard let conversationId = value["conversationId"] as? String,
                  let lifecycle = value["lifecycle"] as? String,
                  let rawTopics = value["inboundTopics"] as? [[String: Any]] else {
                throw ZMError.invalidData("Malformed push topic conversation")
            }
            let topics = try rawTopics.map { topic -> ZMPushTopic in
                guard let name = topic["topic"] as? String,
                      let writer = topic["writerPublicKey"] as? String else {
                    throw ZMError.invalidData("Malformed push topic entry")
                }
                return ZMPushTopic(topic: name, writerPublicKey: writer)
            }
            return ZMPushConversationTopics(
                conversationId: conversationId,
                lifecycle: lifecycle,
                inboundTopics: topics
            )
        }
        return ZMPushTopicSnapshot(
            version: zmInt(response["version"]) ?? 1,
            hydrated: response["hydrated"] as? Bool ?? false,
            supportsGroups: response["supportsGroups"] as? Bool ?? false,
            conversations: conversations
        )
    }
    
    // MARK: - Event Handling
    
    private func setupEventHandler() async {
        await ipcBridge.setEventHandler { [weak self] eventType, payload in
            Task { @MainActor [weak self] in
                guard let self = self else { return }
                self.handleEvent(eventType, payload: payload)
            }
        }
    }
    
    private func handleEvent(_ eventType: String, payload: [String: Any]) {
        switch eventType {
        case "message.received":
            guard let conversationId = payload["conversationId"] as? String,
                  let messageData = payload["message"] as? [String: Any],
                  let message = ZMParse.message(from: messageData) else {
                return
            }
            messageReceived.send((conversationId: conversationId, message: message))
            // Update conversation list in background
            refreshConversationsAfterEvent(.messageReceived)

        case "push.topics_changed":
            pushTopicsChanged.send(())

        case "push.notification_failed":
            let message = payload["error"] as? String ?? "Blind-push request failed"
            operationalFailure.send(
                ZMOperationalFailure(operation: .pushNotification, code: .pushFailed, message: message)
            )
            ZMLog.warning("SDK", "Advisory blind-push request failed")
            
        case "conversation.invite_received":
            guard let conversationData = payload["conversation"] as? [String: Any],
                  let conversation = ZMParse.conversation(from: conversationData) else {
                return
            }
            inviteReceived.send(conversation)
            refreshConversationsAfterEvent(.inviteReceived)
            
        case "conversation.member_left":
            if let conversationId = payload["conversationId"] as? String,
               let leaverKey = payload["leaverKey"] as? String ?? payload["publicKey"] as? String {
                memberLeft.send((conversationId: conversationId, leaverKey: leaverKey))
                refreshConversationsAfterEvent(.memberLeft)
            }
            
        case "conversation.group_deleted":
            if let conversationId = payload["conversationId"] as? String {
                groupDeleted.send(conversationId)
                refreshConversationsAfterEvent(.groupDeleted)
            }
            
        case "conversation.group_renamed":
            if let conversationId = payload["conversationId"] as? String,
               let newName = payload["newName"] as? String {
                groupRenamed.send((conversationId: conversationId, newName: newName))
                refreshConversationsAfterEvent(.groupRenamed)
            }
            
        case "conversation.member_added":
            if let conversationId = payload["conversationId"] as? String,
               let newMemberKey = payload["newMemberKey"] as? String {
                let newMemberName = payload["newMemberName"] as? String ?? String(newMemberKey.prefix(8))
                memberAdded.send((conversationId: conversationId, memberKey: newMemberKey, memberName: newMemberName))
                refreshConversationsAfterEvent(.memberAdded)
            }
            
        case "message.status":
            if let messageId = payload["messageId"] as? String,
               let conversationId = payload["conversationId"] as? String,
               let status = payload["status"] as? String {
                messageStatus.send((messageId: messageId, conversationId: conversationId, status: status))
            }
            
        case "connection.status":
            if let online = payload["online"] as? Bool {
                isOnline = online
            }
            if let peers = zmInt(payload["peerCount"]) {
                peerCount = peers
            }

        case "connection.dht_health":
            if let status = payload["status"] as? String {
                dhtHealth = status
            }

        case "connection.peer_status":
            if let conversationId = payload["conversationId"] as? String,
               let peerId = payload["peerId"] as? String,
               let status = payload["status"] as? String {
                peerStatus.send((conversationId: conversationId, peerId: peerId, status: status))
            }

        // The core emits `media.transfer_complete` for BOTH directions and
        // distinguishes them by `mediaLocalPath`: present means a download landed.
        // There is no `media.download_complete` event — listening for one is why
        // received media never surfaced.
        case "media.transfer_complete":
            guard let mediaId = payload["mediaId"] as? String else { break }
            mediaTransferComplete.send(mediaId)
            if let localPath = payload["mediaLocalPath"] as? String {
                mediaDownloadComplete.send((mediaId: mediaId, filePath: localPath))
            }

        case "media.transfer_progress":
            if let mediaId = payload["mediaId"] as? String,
               let progress = zmDouble(payload["progress"]) {
                mediaTransferProgress.send((mediaId: mediaId, progress: progress))
                mediaDownloadProgress.send((mediaId: mediaId, progress: progress))
            }

        case "platform.http_request":
            Task { [weak self] in
                await self?.handlePlatformHTTPRequest(payload)
            }

        case "message.persist_failed":
            let message = "Message \(payload["messageId"] as? String ?? "?") in "
                + "\(payload["conversationId"] as? String ?? "?"): "
                + "\(payload["error"] as? String ?? "unknown")"
            operationalFailure.send(
                ZMOperationalFailure(operation: .messagePersist, code: .persistFailed, message: message)
            )
            ZMLog.error("SDK", "Message persistence failed")

        case "ipc.error":
            let code = payload["code"] as? String ?? "IPC_ERROR"
            let message = payload["message"] as? String ?? "Unknown IPC error"
            operationalFailure.send(
                ZMOperationalFailure(
                    operation: .ipcEvent,
                    code: .ipc(ZMErrorCode(rawValue: code)),
                    message: message
                )
            )
            ZMLog.error("SDK", "Worklet reported an IPC error")

        default:
            ZMLog.debug("SDK", "Unhandled event type received")
        }
    }

    private func handlePlatformHTTPRequest(_ payload: [String: Any]) async {
        guard let requestID = payload["requestId"] as? String,
              let urlString = payload["url"] as? String,
              let url = URL(string: urlString),
              url.scheme?.lowercased() == "https",
              let body = payload["body"] as? [String: Any] else {
            return
        }

        // The worklet names the destination, so this app decides where it is
        // allowed to point: the configured mailbox and nothing else.
        let base = (config.inviteMailboxURL ?? "").trimmingCharacters(in: CharacterSet(charactersIn: "/"))
        guard !base.isEmpty, urlString.hasPrefix(base + "/") else {
            await returnPlatformHTTPFailure(requestID, "Platform HTTPS URL is outside the configured invite mailbox")
            return
        }

        var responsePayload: [String: Any] = ["requestId": requestID]
        do {
            var request = URLRequest(url: url)
            request.httpMethod = "POST"
            request.timeoutInterval = 15
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            request.httpBody = try JSONSerialization.data(withJSONObject: body)

            let (data, response) = try await URLSession.shared.data(for: request)
            guard let http = response as? HTTPURLResponse,
                  (200...299).contains(http.statusCode) else {
                throw ZMError.invalidData("Platform HTTPS request failed")
            }
            guard data.count <= 256 * 1024 else {
                throw ZMError.invalidData("Platform HTTPS response too large")
            }
            let decoded: [String: Any]
            if data.isEmpty {
                decoded = [:]
            } else {
                guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
                    throw ZMError.invalidData("Invalid platform HTTPS response")
                }
                decoded = object
            }
            responsePayload["success"] = true
            responsePayload["response"] = decoded
        } catch {
            responsePayload["success"] = false
            responsePayload["error"] = error.localizedDescription
        }

        await returnPlatformHTTPResponse(responsePayload)
    }

    private func returnPlatformHTTPFailure(_ requestID: String, _ reason: String) async {
        await returnPlatformHTTPResponse(["requestId": requestID, "success": false, "error": reason])
    }

    /// The worklet is blocked on this reply, so a failure to deliver it is worth
    /// a breadcrumb even though there is nothing left to retry.
    private func returnPlatformHTTPResponse(_ payload: [String: Any]) async {
        do {
            _ = try await ipcBridge.sendRequest(
                type: "platform.http_response",
                payload: payload,
                timeout: 5
            )
        } catch {
            ZMLog.warning("SDK", "Platform HTTPS response delivery failed")
        }
    }

    /// Event handlers cannot throw back through the IPC callback, but refresh
    /// failures still need a durable breadcrumb instead of disappearing.
    private func refreshConversationsAfterMutation(_ trigger: ZMOperationalFailure.ConversationRefreshTrigger) async {
        do {
            try await refreshConversations()
        } catch {
            reportOperationalFailure(
                operation: .conversationRefresh(trigger),
                code: .refreshFailed,
                error: error
            )
        }
    }

    private func refreshConversationsAfterEvent(_ trigger: ZMOperationalFailure.ConversationRefreshTrigger) {
        // A catch-up burst can deliver hundreds of messages in one turn. One list request observes
        // all messages already persisted by the core; queueing another Task and IPC request for
        // every event only increases memory and delays the event stream.
        guard conversationRefreshTask == nil else {
            conversationRefreshNeedsFollowup = true
            return
        }

        conversationRefreshTask = Task { @MainActor [weak self] in
            guard let self else { return }
            defer { self.conversationRefreshTask = nil }

            repeat {
                self.conversationRefreshNeedsFollowup = false

                do {
                    try await self.refreshConversations()
                } catch {
                    guard !Task.isCancelled else { return }

                    self.operationalFailure.send(
                        ZMOperationalFailure(
                            operation: .conversationRefresh(trigger),
                            code: .refreshFailed,
                            message: error.localizedDescription
                        )
                    )
                    ZMLog.warning("SDK", "Conversation refresh failed")
                    return
                }
            } while self.conversationRefreshNeedsFollowup && !Task.isCancelled
        }
    }

    private func reportOperationalFailure(
        operation: ZMOperationalFailure.Operation,
        code: ZMOperationalFailure.Code,
        error: Error
    ) {
        operationalFailure.send(
            ZMOperationalFailure(operation: operation, code: code, message: error.localizedDescription)
        )
    }
}

// MARK: - Payment Message Types

public enum PaymentMessageType {
    case transaction
    case paymentRequest
    case walletAddress
    
    var ipcType: String {
        switch self {
        case .transaction: return "message.send_transaction"
        case .paymentRequest: return "message.send_payment_request"
        case .walletAddress: return "message.send_wallet_address"
        }
    }
}
