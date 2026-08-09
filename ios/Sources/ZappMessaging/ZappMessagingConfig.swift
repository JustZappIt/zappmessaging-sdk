//
//  ZappMessagingConfig.swift
//  ZappMessaging
//
//  Startup configuration passed to the Bare worklet as argv.
//
//  This mirrors the Android side (BareWorkletManager.kt). The worklet reads
//  these flags in core/lib/storage.js and core/index.js. Getting them wrong is
//  not a crash — it is a silently degraded client:
//
//    - no --data-dir       -> JS falls back to ~/Documents/zappmessaging, which
//                             on iOS is the iCloud-backed app sandbox. The
//                             corestore/rocksdb tree bloats the user's backup
//                             and can be mangled on restore-to-new-device.
//    - no --blind-peer-keys-> no offline delivery. Messages only arrive while
//                             both peers are online simultaneously.
//    - no --bootstrap-nodes-> the DHT has no seed nodes to join through.
//

import Foundation

/// Startup configuration for the messaging worklet.
public struct ZappMessagingConfig: Sendable {
    /// Where the worklet keeps identity.json, the corestore and the rocksdb tree.
    public var dataDir: URL

    /// Comma-separated blind-peer public keys. **Required for offline delivery.**
    public var blindPeerKeys: String?

    /// Comma-separated DHT bootstrap nodes.
    public var bootstrapNodes: String?

    /// Stable public blind-peer address used for a direct first connection.
    /// Normal DHT discovery remains the fallback if it is unavailable.
    public var blindPeerAddress: String?

    /// LAN gateway IP, so the DHT can seed its routing table from the gateway
    /// node. Matters on the hotspot path, where the AP's DHT can relay queries
    /// out even though this device is behind double-NAT.
    public var localGateway: String?

    /// Base HTTPS URL of the bootstrap invite mailbox. Leave nil to use only the
    /// DHT mailbox; set it to keep invites flowing on networks that drop UDP.
    /// Doubles as the allow-list for host-carried requests, so the worklet
    /// cannot ask this app to POST anywhere else.
    public var inviteMailboxURL: String?

    /// Worklet log level. Leave nil in release: `debug` dumps keypairs.
    public var logLevel: String?

    /// Maximum media payload accepted by the JavaScript core. This is deliberately lower
    /// than the runtime heap because media reassembly briefly holds chunks and their joined
    /// buffer at the same time.
    public var mediaMaxBytes: Int?

    public init(
        dataDir: URL,
        blindPeerKeys: String? = nil,
        bootstrapNodes: String? = nil,
        blindPeerAddress: String? = nil,
        localGateway: String? = nil,
        inviteMailboxURL: String? = nil,
        logLevel: String? = nil,
        mediaMaxBytes: Int? = nil
    ) {
        self.dataDir = dataDir
        self.blindPeerKeys = blindPeerKeys
        self.bootstrapNodes = bootstrapNodes
        self.blindPeerAddress = blindPeerAddress
        self.localGateway = localGateway
        self.inviteMailboxURL = inviteMailboxURL
        self.logLevel = logLevel
        self.mediaMaxBytes = mediaMaxBytes
    }

    /// The argv handed to `BareWorklet.start`. Order and spelling must match
    /// BareWorkletManager.kt — the JS parses these by exact flag name.
    var argv: [String] {
        var args = ["--data-dir=\(dataDir.path)"]

        if let blindPeerKeys, !blindPeerKeys.isEmpty {
            args.append("--blind-peer-keys=\(blindPeerKeys)")
        }
        if let bootstrapNodes, !bootstrapNodes.isEmpty {
            args.append("--bootstrap-nodes=\(bootstrapNodes)")
        }
        if let blindPeerAddress, !blindPeerAddress.isEmpty {
            args.append("--blind-peer-address=\(blindPeerAddress)")
        }
        if let localGateway, !localGateway.isEmpty {
            args.append("--local-gateway=\(localGateway)")
        }
        if let inviteMailboxURL, !inviteMailboxURL.isEmpty {
            args.append("--invite-mailbox-url=\(inviteMailboxURL)")
        }
        if let logLevel, !logLevel.isEmpty {
            args.append("--log-level=\(logLevel)")
        }
        if let mediaMaxBytes, mediaMaxBytes > 0 {
            args.append("--media-max-bytes=\(mediaMaxBytes)")
        }

        return args
    }
}

public extension ZappMessagingConfig {
    /// The name the JS core appends to `--data-dir`. See `getDataDir()` in
    /// core/lib/storage.js — the flag names the *container*, and the store lives
    /// in a `zappmessaging` subdirectory of it. Android passes `context.filesDir`
    /// for exactly this reason.
    static let storeDirectoryName = "zappmessaging"

    /// The container the worklet stores into: `<app>/Library/Application Support`.
    ///
    /// Deliberately not `Documents`: that is the iCloud-backed, user-visible
    /// container. The store is a rocksdb/corestore tree — large, and a stale copy
    /// restored onto a second device can fork an append-only log. The identity is
    /// re-derivable from the wallet seed, so excluding it from backup costs the
    /// user nothing they cannot recover.
    ///
    /// Returns the *container*, not the store: pass this to `--data-dir` and the
    /// JS appends `zappmessaging` itself. Passing the store path instead yields
    /// `…/zappmessaging/zappmessaging`.
    ///
    /// Pre-creates the store subdirectory so the no-backup flag lands on it and
    /// only it, rather than on all of Application Support.
    static func defaultDataDir(fileManager: FileManager = .default) throws -> URL {
        let container = try fileManager.url(
            for: .applicationSupportDirectory,
            in: .userDomainMask,
            appropriateFor: nil,
            create: true
        )

        var store = container.appendingPathComponent(storeDirectoryName, isDirectory: true)

        if !fileManager.fileExists(atPath: store.path) {
            try fileManager.createDirectory(at: store, withIntermediateDirectories: true)
        }

        var values = URLResourceValues()
        values.isExcludedFromBackup = true
        try store.setResourceValues(values)

        return container
    }

    /// Where the worklet's files actually land, given a `--data-dir` container.
    /// Delete this to wipe chat state (identity.json, corestore, rocksdb).
    static func storeDirectory(inContainer container: URL) -> URL {
        container.appendingPathComponent(storeDirectoryName, isDirectory: true)
    }
}
