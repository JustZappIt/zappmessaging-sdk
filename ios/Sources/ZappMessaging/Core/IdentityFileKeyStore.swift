//
//  IdentityFileKeyStore.swift
//  ZappMessaging
//
//  Keychain-held key for encrypting identity.json (wallet entropy) at rest.
//

import Foundation
import Security

/// Supplies the 32-byte key the worklet uses to encrypt identity.json (the
/// BIP-39 wallet entropy) at rest — passed as `--identity-file-key`.
///
/// The key is random, minted once, and lives in the iOS Keychain as a
/// non-syncing generic-password item (`AfterFirstUnlockThisDeviceOnly`):
/// hardware-encrypted at rest and absent from iCloud/device backups. If the
/// item is ever lost (keychain reset), an already-encrypted identity becomes
/// unreadable and the worklet surfaces "no identity", so the user restores
/// from the 24-word seed phrase — the standard wallet recovery path.
enum IdentityFileKeyStore {
    private static let service = "xyz.justzappit.zappmessaging"
    private static let account = "identity-file-key"
    private static let keyBytes = 32

    /// The key as 64 lowercase hex chars, or nil when the Keychain is
    /// unusable (the worklet then keeps the legacy plaintext identity file
    /// rather than losing messaging).
    static func getOrCreateKeyHex() -> String? {
        if let existing = read() { return hex(existing) }

        var fresh = Data(count: keyBytes)
        let randomStatus = fresh.withUnsafeMutableBytes {
            SecRandomCopyBytes(kSecRandomDefault, keyBytes, $0.baseAddress!)
        }
        guard randomStatus == errSecSuccess else { return nil }

        let attrs: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
            kSecAttrAccessible as String: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly,
            kSecValueData as String: fresh
        ]
        let addStatus = SecItemAdd(attrs as CFDictionary, nil)
        if addStatus == errSecSuccess { return hex(fresh) }
        if addStatus == errSecDuplicateItem, let existing = read() {
            // Raced with a concurrent caller; the stored item wins.
            return hex(existing)
        }
        print("[IdentityFileKeyStore] Keychain add failed (\(addStatus)); identity file stays plaintext")
        return nil
    }

    private static func read() -> Data? {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne
        ]
        var result: AnyObject?
        let status = SecItemCopyMatching(query as CFDictionary, &result)
        guard status == errSecSuccess, let data = result as? Data, data.count == keyBytes else {
            return nil
        }
        return data
    }

    private static func hex(_ data: Data) -> String {
        data.map { String(format: "%02x", $0) }.joined()
    }
}
