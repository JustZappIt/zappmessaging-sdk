//
//  ZMGroupLink.swift
//  ZappMessaging
//
//  Group invite links and member removal. Mirrors Kotlin ZMGroupLink.kt.
//
//  `ZMGroupLinkInfo.link` is a bearer secret: anyone holding it can ask to
//  join. Never log it, never keep it in navigation or TCA state, and hand it
//  only to copy, share and QR code actions.
//

import Foundation

/// Who admits people who use the link.
public enum ZMGroupLinkApproval: String, Equatable, Sendable {
    /// Admitted as soon as the owner's app sees the request.
    case auto
    /// Each request waits for the owner to approve or decline it.
    case owner

    init(raw: String?) { self = raw.flatMap(ZMGroupLinkApproval.init(rawValue:)) ?? .auto }
}

public enum ZMGroupLinkState: String, Equatable, Sendable {
    case none
    case active
    case off
    case unknown

    init(raw: String?) { self = raw.flatMap(ZMGroupLinkState.init(rawValue:)) ?? .unknown }
}

/// The owner's view of a group's invite link.
public struct ZMGroupLinkInfo: Equatable, Sendable, CustomStringConvertible, CustomDebugStringConvertible {
    public let conversationId: String
    public let state: ZMGroupLinkState
    /// The shareable link. Nil when there is no link. A bearer secret.
    public let link: String?
    public let linkId: String?
    public let createdAt: Date?
    public let expiresAt: Date?
    /// Joins allowed through this link, or nil for no limit.
    public let maxJoins: Int?
    public let joins: Int
    public let includeName: Bool
    public let approval: ZMGroupLinkApproval
    /// "rate" when the link switched itself to owner approval.
    public let approvalReason: String?
    public let pendingRequests: Int

    public var description: String { "ZMGroupLinkInfo(conversationId: \(conversationId), state: \(state), link: <redacted>)" }
    public var debugDescription: String { description }
}

/// Options for enabling or updating a link. Only set fields are sent;
/// `clearExpiry` and `clearMaxJoins` remove a limit.
public struct ZMGroupLinkOptions: Equatable, Sendable {
    public var expiresAt: Date?
    public var clearExpiry: Bool
    public var maxJoins: Int?
    public var clearMaxJoins: Bool
    public var includeName: Bool?
    public var approval: ZMGroupLinkApproval?

    public init(
        expiresAt: Date? = nil,
        clearExpiry: Bool = false,
        maxJoins: Int? = nil,
        clearMaxJoins: Bool = false,
        includeName: Bool? = nil,
        approval: ZMGroupLinkApproval? = nil
    ) {
        self.expiresAt = expiresAt
        self.clearExpiry = clearExpiry
        self.maxJoins = maxJoins
        self.clearMaxJoins = clearMaxJoins
        self.includeName = includeName
        self.approval = approval
    }
}

public enum ZMGroupLinkInspectStatus: String, Equatable, Sendable {
    case ok
    case expired
    case malformed
    case unsupportedVersion = "unsupported_version"
    case newerFormat = "newer_format"

    init(raw: String?) { self = raw.flatMap(ZMGroupLinkInspectStatus.init(rawValue:)) ?? .malformed }
}

/// What a link says about itself, read without contacting anyone.
public struct ZMGroupLinkInspection: Equatable, Sendable {
    public let status: ZMGroupLinkInspectStatus
    public let nameHint: String?
    /// A hint only; the owner enforces the real expiry.
    public let expiresAt: Date?
    /// Opaque and safe to keep: derived from the link, not the secret itself.
    public let linkId: String?
}

public enum ZMGroupJoinRequestStatus: String, Equatable, Sendable {
    case requested
    case alreadyRequested = "already_requested"
    case alreadyMember = "already_member"
    case expired
    case malformed
    case unsupportedVersion = "unsupported_version"
    case newerFormat = "newer_format"

    init(raw: String?) { self = raw.flatMap(ZMGroupJoinRequestStatus.init(rawValue:)) ?? .malformed }
}

/// The answer to asking to join.
public struct ZMGroupJoinResult: Equatable, Sendable {
    public let status: ZMGroupJoinRequestStatus
    public let linkId: String?
    /// Set when already a member.
    public let conversationId: String?
}

/// Where a request this device made stands.
public enum ZMGroupJoinStatus: String, Equatable, Sendable {
    case waiting
    case pendingApproval = "pending_approval"
    case joined
    case inactive
    case expired
    case full
    case declined
    case cancelled
    case unknown

    public var isWaiting: Bool { self == .waiting || self == .pendingApproval }

    init(raw: String?) { self = raw.flatMap(ZMGroupJoinStatus.init(rawValue:)) ?? .unknown }
}

public struct ZMGroupJoinUpdate: Equatable, Sendable {
    public let linkId: String
    public let status: ZMGroupJoinStatus
    public let conversationId: String?
    public let nameHint: String?
}

/// A request waiting for the owner, in approval mode.
public struct ZMGroupJoinApprovalRequest: Equatable, Sendable, Identifiable {
    public var id: String { joinerKey }
    public let conversationId: String
    public let joinerKey: String
    public let joinerName: String
    public let requestedAt: Date?
    /// The owner removed this person from the group before.
    public let previouslyRemoved: Bool
}

public struct ZMRemoveMemberResult: Equatable, Sendable {
    public let participants: [String]
    /// Members whose app does not understand removal yet. They see new
    /// messages once they update.
    public let olderMemberCount: Int
}

extension ZMParse {
    static func groupLink(from data: [String: Any]) -> ZMGroupLinkInfo {
        ZMGroupLinkInfo(
            conversationId: data["conversationId"] as? String ?? "",
            state: ZMGroupLinkState(raw: data["state"] as? String),
            link: data["link"] as? String,
            linkId: data["linkId"] as? String,
            createdAt: zmDate(data["createdAt"]),
            expiresAt: zmDate(data["expiresAt"]),
            maxJoins: zmInt(data["maxJoins"]),
            joins: zmInt(data["joins"]) ?? 0,
            includeName: data["includeName"] as? Bool ?? true,
            approval: ZMGroupLinkApproval(raw: data["approval"] as? String),
            approvalReason: data["approvalReason"] as? String,
            pendingRequests: zmInt(data["pendingRequests"]) ?? 0
        )
    }

    static func groupLinkPayload(conversationId: String, options: ZMGroupLinkOptions) -> [String: Any] {
        var payload: [String: Any] = ["conversationId": conversationId]
        if options.clearExpiry {
            payload["expiresAt"] = NSNull()
        } else if let expiresAt = options.expiresAt {
            payload["expiresAt"] = Int64(expiresAt.timeIntervalSince1970 * 1000)
        }
        if options.clearMaxJoins {
            payload["maxJoins"] = NSNull()
        } else if let maxJoins = options.maxJoins {
            payload["maxJoins"] = maxJoins
        }
        if let includeName = options.includeName { payload["includeName"] = includeName }
        if let approval = options.approval { payload["approval"] = approval.rawValue }
        return payload
    }

    static func inspection(from data: [String: Any]) -> ZMGroupLinkInspection {
        ZMGroupLinkInspection(
            status: ZMGroupLinkInspectStatus(raw: data["status"] as? String),
            nameHint: data["nameHint"] as? String,
            // The link carries Unix seconds.
            expiresAt: zmInt(data["expiresAt"]).map { Date(timeIntervalSince1970: TimeInterval($0)) },
            linkId: data["linkId"] as? String
        )
    }

    static func joinResult(from data: [String: Any]) -> ZMGroupJoinResult {
        ZMGroupJoinResult(
            status: ZMGroupJoinRequestStatus(raw: data["status"] as? String),
            linkId: data["linkId"] as? String,
            conversationId: data["conversationId"] as? String
        )
    }

    static func joinUpdate(from data: [String: Any]) -> ZMGroupJoinUpdate? {
        guard let linkId = data["linkId"] as? String else { return nil }
        return ZMGroupJoinUpdate(
            linkId: linkId,
            status: ZMGroupJoinStatus(raw: data["status"] as? String),
            conversationId: data["conversationId"] as? String,
            nameHint: data["nameHint"] as? String
        )
    }

    static func approvalRequest(conversationId: String, from data: [String: Any]) -> ZMGroupJoinApprovalRequest? {
        guard let joinerKey = data["joinerKey"] as? String else { return nil }
        return ZMGroupJoinApprovalRequest(
            conversationId: conversationId,
            joinerKey: joinerKey,
            joinerName: data["joinerName"] as? String ?? String(joinerKey.prefix(8)),
            requestedAt: zmDate(data["requestedAt"]),
            previouslyRemoved: data["previouslyRemoved"] as? Bool ?? false
        )
    }
}
