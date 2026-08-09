//
//  ZMIdentity.swift
//  ZappMessaging
//
//  Represents a user's cryptographic identity
//

import Foundation

/// User's cryptographic identity
public struct ZMIdentity: Codable, Identifiable, Equatable, Sendable {
    /// Unique identifier (same as publicKey)
    public var id: String { publicKey }
    
    /// Ed25519 public key (hex encoded)
    public let publicKey: String
    
    /// User's display name
    public let displayName: String
    
    /// When the identity was created
    public let createdAt: Date
    
    public init(publicKey: String, displayName: String, createdAt: Date = Date()) {
        self.publicKey = publicKey
        self.displayName = displayName
        self.createdAt = createdAt
    }
}
