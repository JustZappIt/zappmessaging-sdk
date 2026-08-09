//
//  ZMContact.swift
//  ZappMessaging
//
//  Represents a contact
//

import Foundation

/// A contact entry
public struct ZMContact: Codable, Identifiable, Equatable, Sendable {
    /// Unique identifier (same as publicKey)
    public var id: String { publicKey }
    
    /// Contact's public key (hex encoded)
    public let publicKey: String
    
    /// Contact's display name
    public var name: String
    
    /// When the contact was added
    public let addedAt: Date
    
    /// ZEC wallet address (if shared)
    public var walletAddress: String?
    
    /// Wallet address type (unified, sapling, transparent)
    public var addressType: String?
    
    public init(publicKey: String, name: String, addedAt: Date = Date(), walletAddress: String? = nil, addressType: String? = nil) {
        self.publicKey = publicKey
        self.name = name
        self.addedAt = addedAt
        self.walletAddress = walletAddress
        self.addressType = addressType
    }
}
