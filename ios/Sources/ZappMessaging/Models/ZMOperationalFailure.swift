//
//  ZMOperationalFailure.swift
//  ZappMessaging
//

import Foundation

/// A non-atomic follow-up failure that must remain observable without changing
/// the success result of an operation that has already committed.
public struct ZMOperationalFailure: Equatable, Sendable {
    public enum Operation: Equatable, Sendable {
        case connectionResume
        case pushNotification
        case messagePersist
        case ipcEvent
        case workletRecovery
        case conversationConnect
        case conversationRefresh(ConversationRefreshTrigger)

        public var identifier: String {
            switch self {
            case .connectionResume: return "connection.resume"
            case .pushNotification: return "push.notification"
            case .messagePersist: return "message.persist"
            case .ipcEvent: return "ipc.event"
            case .workletRecovery: return "worklet.recovery"
            case .conversationConnect: return "conversation.connect"
            case .conversationRefresh(let trigger): return "conversation.refresh.\(trigger.rawValue)"
            }
        }
    }

    public enum ConversationRefreshTrigger: String, Equatable, Sendable {
        case create
        case leave
        case delete
        case remove
        case rename
        case addMember = "add_member"
        case messageReceived = "message.received"
        case inviteReceived = "conversation.invite_received"
        case memberLeft = "conversation.member_left"
        case groupDeleted = "conversation.group_deleted"
        case groupRenamed = "conversation.group_renamed"
        case memberAdded = "conversation.member_added"
        case removeMember = "remove_member"
        case approveJoin = "group_link.approve"
        case memberRemoved = "conversation.member_removed"
        case removedFromGroup = "conversation.removed_from_group"
        case linkMemberJoined = "group_link.member_joined"
    }

    public enum Code: Equatable, Sendable {
        case statusFailed
        case pushFailed
        case persistFailed
        case connectFailed
        case refreshFailed
        case ipc(ZMErrorCode)
        case workletRestarted
        case workletRestartExhausted

        public var identifier: String {
            switch self {
            case .statusFailed: return "STATUS_FAILED"
            case .pushFailed: return "PUSH_FAILED"
            case .persistFailed: return "PERSIST_FAILED"
            case .connectFailed: return "CONNECT_FAILED"
            case .refreshFailed: return "REFRESH_FAILED"
            case .ipc(let code): return code.rawValue
            case .workletRestarted: return "WORKLET_RESTARTED"
            case .workletRestartExhausted: return "WORKLET_RESTART_EXHAUSTED"
            }
        }
    }

    public let operation: Operation
    public let code: Code
    public let message: String

    public init(operation: Operation, code: Code, message: String) {
        self.operation = operation
        self.code = code
        self.message = message
    }
}
