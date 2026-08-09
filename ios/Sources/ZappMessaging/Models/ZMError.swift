//
//  ZMError.swift
//  ZappMessaging
//
//  Error types for ZappMessaging SDK
//

import Foundation

/// Errors that can occur in ZappMessaging SDK
public enum ZMErrorCode: Equatable, Sendable {
    case ownPublicKey
    case missingParticipant
    case invalidParticipants
    case invalidPublicKey
    case conversationNotFound
    case unknown(String)

    public init(rawValue: String) {
        switch rawValue {
        case "OWN_PUBLIC_KEY": self = .ownPublicKey
        case "MISSING_PARTICIPANT": self = .missingParticipant
        case "INVALID_PARTICIPANTS": self = .invalidParticipants
        case "INVALID_PUBLIC_KEY": self = .invalidPublicKey
        case "CONVERSATION_NOT_FOUND": self = .conversationNotFound
        default: self = .unknown(rawValue)
        }
    }

    public var rawValue: String {
        switch self {
        case .ownPublicKey: return "OWN_PUBLIC_KEY"
        case .missingParticipant: return "MISSING_PARTICIPANT"
        case .invalidParticipants: return "INVALID_PARTICIPANTS"
        case .invalidPublicKey: return "INVALID_PUBLIC_KEY"
        case .conversationNotFound: return "CONVERSATION_NOT_FOUND"
        case .unknown(let value): return value
        }
    }
}

public enum ZMError: Error, LocalizedError {
    case notInitialized
    case identityNotFound
    case conversationNotFound
    case contactNotFound
    case connectionFailed(String)
    case sendFailed(String)
    case invalidData(String)
    case workletError(String)
    case ipcTimeout
    case ipcError(code: ZMErrorCode, message: String)
    case keychainError(String)
    case invalidSeedPhrase
    case mediaError(String)
    
    public var errorDescription: String? {
        switch self {
        case .notInitialized:
            return "ZappMessaging SDK is not initialized"
        case .identityNotFound:
            return "No identity found. Please create or restore an identity first."
        case .conversationNotFound:
            return "Conversation not found"
        case .contactNotFound:
            return "Contact not found"
        case .connectionFailed(let message):
            return "Connection failed: \(message)"
        case .sendFailed(let message):
            return "Failed to send message: \(message)"
        case .invalidData(let message):
            return "Invalid data: \(message)"
        case .workletError(let message):
            return "JavaScript worklet error: \(message)"
        case .ipcTimeout:
            return "IPC request timed out"
        case .ipcError(_, let message):
            return "IPC error: \(message)"
        case .keychainError(let message):
            return "Keychain error: \(message)"
        case .invalidSeedPhrase:
            return "Invalid seed phrase"
        case .mediaError(let message):
            return "Media error: \(message)"
        }
    }
}
