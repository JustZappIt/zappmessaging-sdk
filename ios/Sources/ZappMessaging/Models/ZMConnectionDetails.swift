//
//  ZMConnectionDetails.swift
//  ZappMessaging
//

import Foundation

/// Point-in-time transport diagnostics returned by `connection.details`.
///
/// This mirrors Android's `ZappMessagingSDK.ConnectionDetails` so host apps can
/// explain queued messages instead of reducing every failure mode to online/offline.
public struct ZMConnectionDetails: Equatable, Sendable {
    public let online: Bool
    public let peerCount: Int
    public let globalConnections: Int
    public let pendingQueues: Int
    public let pendingMessageCount: Int
    public let pendingInvites: Int
    public let dhtHealth: String
    public let dhtLastCheck: String?
    public let consecutiveFailures: Int
    public let directConversations: Int
    public let groupConversations: Int
    public let dhtBootstrapped: Bool
    public let dhtFirewalled: Bool?
    public let dhtRandomized: Bool?
    public let rtNodes: Int
    public let relayEnabled: Bool
    public let relaysConnected: Int
    public let relaysTotal: Int

    public init(
        online: Bool,
        peerCount: Int,
        globalConnections: Int,
        pendingQueues: Int,
        pendingMessageCount: Int,
        pendingInvites: Int,
        dhtHealth: String,
        dhtLastCheck: String?,
        consecutiveFailures: Int,
        directConversations: Int,
        groupConversations: Int,
        dhtBootstrapped: Bool,
        dhtFirewalled: Bool?,
        dhtRandomized: Bool?,
        rtNodes: Int,
        relayEnabled: Bool,
        relaysConnected: Int,
        relaysTotal: Int
    ) {
        self.online = online
        self.peerCount = peerCount
        self.globalConnections = globalConnections
        self.pendingQueues = pendingQueues
        self.pendingMessageCount = pendingMessageCount
        self.pendingInvites = pendingInvites
        self.dhtHealth = dhtHealth
        self.dhtLastCheck = dhtLastCheck
        self.consecutiveFailures = consecutiveFailures
        self.directConversations = directConversations
        self.groupConversations = groupConversations
        self.dhtBootstrapped = dhtBootstrapped
        self.dhtFirewalled = dhtFirewalled
        self.dhtRandomized = dhtRandomized
        self.rtNodes = rtNodes
        self.relayEnabled = relayEnabled
        self.relaysConnected = relaysConnected
        self.relaysTotal = relaysTotal
    }
}
