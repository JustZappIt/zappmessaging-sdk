# ZappMessaging iOS SDK

Swift wrapper for the ZappMessaging P2P chat SDK.

## Overview

ZappMessaging is a peer-to-peer messaging SDK that uses Hyperswarm for network connectivity and runs JavaScript core logic in a BareKit worklet.

## Features

- Ed25519 cryptographic identity
- Deterministic identity restoration from the host wallet's BIP39 seed
- P2P messaging via Hyperswarm
- 2 chat types: Direct and Group
- Content-addressed media storage
- Chunked media transfer
- Identity entropy encrypted at rest under a Keychain-held key
- SwiftUI-friendly with Combine publishers

## Requirements

- iOS 16.0+
- Swift 5.9+
- Node.js 20+ (builds the worklet bundle and links the native addons)
- XcodeGen, `brew install xcodegen` (only for the standalone Xcode project)
- BareKit (for JavaScript worklet execution)

## Installation

The package manifest currently lives in the `ios/` subdirectory and its
generated Bare addons are intentionally gitignored, so remote Swift Package
Manager installation is not yet supported. Prepare a checkout first:

```bash
git clone https://github.com/JustZappIt/zappmessaging-sdk.git
cd zappmessaging-sdk
npm run setup
```

`npm run setup` installs the npm dependencies, links the addon xcframeworks into
`ios/Addons/`, and builds `ios/Resources/worklet.bundle`. `Package.swift` refuses
to resolve until those artifacts exist.

Then add the checkout's `ios/` directory to Xcode as a local package.

To work on the framework on its own, generate the standalone Xcode project:

```bash
cd ios
xcodegen generate
```

`ios/project.yml` is the source of truth for that project. The generated
`ios/ZappMessaging.xcodeproj` is gitignored and must never be committed;
regenerate it after every change to `project.yml`.

`npm run setup` also writes `Resources/worklet-manifest.json`. Hosts should
verify its `sourceCommit` against their dependency pin and its `bundleSHA256`
against `worklet.bundle` at build time. This prevents Xcode from silently
packaging generated artifacts left behind by another SDK checkout. The bundle
and its addons remain one version-locked unit; never run `npm run link` without
`npm run build`.

## Usage

### Initialize SDK

```swift
import ZappMessaging

let sdk = ZappMessagingSDK(
    config: ZappMessagingConfig(dataDir: appSupportDirectory)
)

// Initialize
try await sdk.initialize()
```

Native and JavaScript diagnostics are disabled by default. A host may set
`logLevel: "debug"` in `ZappMessagingConfig` for short-lived local
troubleshooting. Native diagnostics contain only lifecycle and structural
events: raw IPC frames, identifiers, addresses, paths, argv, keys, and error
descriptions are never logged. Do not enable debug diagnostics in production.

### Create Identity

```swift
// Restore the same deterministic identity used by the host wallet.
let identity = try await sdk.restoreFromSeedPhrase(
    "word1 word2 ... word24",
    displayName: "Alice"
)

// The worklet persists the BIP-39 entropy behind this phrase (see
// "Identity storage" below); the phrase itself is not kept in any other form.
```

### Identity storage

`restoreFromSeedPhrase` derives the chat keypair inside the JavaScript worklet
and persists the 32 bytes of BIP-39 entropy — enough to reconstruct the whole
mnemonic — in `identity.json` under the SDK's data dir, so the identity can be
reloaded on the next launch and the phrase re-exported. That entropy is
equivalent to the wallet seed, so the data dir deserves the same care as the
wallet's own storage.

At rest the entropy is secretbox-encrypted (XSalsa20-Poly1305) under a random
32-byte key that `IdentityFileKeyStore` mints once and keeps in the iOS
Keychain as a non-syncing, this-device-only item, excluded from iCloud and
device backups. The key is handed to the worklet as `--identity-file-key` at
start. Two fallbacks matter:

- If the Keychain is unusable when the worklet starts, no key is supplied and
  the core keeps `identity.json` in its legacy plaintext format rather than
  losing messaging.
- If the Keychain item is lost later, the encrypted file can no longer be read,
  the SDK reports no identity, and the user restores from the 24-word phrase.

### Manage Contacts

```swift
// Add contact
try await sdk.addContact(publicKey: "abc123...", name: "Bob")

// List contacts
let contacts = try await sdk.getContacts()

// Update contact
try await sdk.updateContact(publicKey: "abc123...", name: "Bob Smith")

// Delete contact
try await sdk.deleteContact(publicKey: "abc123...")
```

### Create Conversations

```swift
// Direct chat
let conversation = try await sdk.createConversation(
    type: .direct,
    participants: ["bob_public_key"],
    displayName: "Bob"
)

// Group chat
let conversation = try await sdk.createConversation(
    type: .group,
    participants: ["bob_key", "charlie_key"],
    displayName: "Team Chat"
)
```

### Send Messages

```swift
// Send text message
let message = try await sdk.sendMessage(
    conversationId: conversation.id,
    content: "Hello, world!"
)

// Send media message
let message = try await sdk.sendMediaMessage(
    conversationId: conversation.id,
    mediaPath: "/path/to/image.jpg",
    contentType: "image/jpeg",
    caption: "Check this out!"
)

// Get messages
let messages = try await sdk.getMessages(
    conversationId: conversation.id,
    limit: 50
)
```

### Listen for Events

```swift
// Incoming messages
sdk.messageReceived
    .sink { conversationId, message in
        print("New message in \(conversationId): \(message.content)")
    }
    .store(in: &cancellables)

// Conversation invites
sdk.inviteReceived
    .sink { conversation in
        print("Invited to: \(conversation.displayName)")
    }
    .store(in: &cancellables)

// Media download progress
sdk.mediaDownloadProgress
    .sink { mediaId, progress in
        print("Download \(mediaId): \(progress * 100)%")
    }
    .store(in: &cancellables)

// Media download complete
sdk.mediaDownloadComplete
    .sink { mediaId, filePath in
        print("Downloaded \(mediaId) to \(filePath)")
    }
    .store(in: &cancellables)
```

### Observe State Changes

```swift
// Use @Published properties in SwiftUI
struct ChatView: View {
    @ObservedObject var sdk: ZappMessagingSDK
    
    var body: some View {
        List(sdk.conversations) { conversation in
            Text(conversation.displayName)
        }
        .task {
            try? await sdk.refreshConversations()
        }
    }
}
```

### App Lifecycle

```swift
// Suspend when app backgrounds
func sceneDidEnterBackground() {
    Task {
        await sdk.suspend()
    }
}

// Resume when app foregrounds
func sceneWillEnterForeground() {
    Task {
        await sdk.resume()
    }
}

// Shutdown when app terminates
func sceneDidDisconnect() {
    Task {
        await sdk.shutdown()
    }
}
```

## Architecture

```
┌─────────────────────────────────────┐
│         SwiftUI App                 │
│  (Uses ZappMessagingSDK)            │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│     ZappMessagingSDK.swift          │
│  - Public API                       │
│  - Combine Publishers               │
│  - State Management                 │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│        IPCBridge.swift              │
│  - NDJSON Protocol                  │
│  - Request/Response Handling        │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│    BareWorkletManager.swift         │
│  - JavaScript Worklet Lifecycle     │
│  - BareKit Integration              │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│      JavaScript Core                │
│  - Hyperswarm P2P                   │
│  - Message Routing                  │
│  - Media Transfer                   │
└─────────────────────────────────────┘
```

## Models

### ZMIdentity
User's cryptographic identity with Ed25519 public key.

### ZMConversation
Direct or group chat conversation.

### ZMMessage
Individual message with optional media attachments.

### ZMContact
Contact entry with public key and display name.

### ZMError
SDK error types with localized descriptions.

## Security

- **Ed25519 Keypairs**: Industry-standard elliptic curve cryptography
- **BIP39 Seed Phrases**: 24-word mnemonic for backup/restore
- **Identity entropy at rest**: the BIP-39 entropy behind the phrase is stored encrypted under a Keychain-held key, with a plaintext fallback when the Keychain is unavailable — see "Identity storage" above
- **P2P Encryption**: All messages encrypted via Hyperswarm's Noise protocol

## Current limitation

The iOS wrapper does not yet register an APNs/blind-push notification
capability and cannot wake a suspended app for background chat delivery. The
host app resumes the worklet and synchronizes authoritative encrypted messages
when it returns to the foreground. APNs support requires a separate blind-push
transport integration; the legacy Android ntfy/Web-Push endpoint flow should
not be copied into iOS.

## Testing

The tests are an iOS unit-test bundle (`Tests/ZappMessagingTests`) declared in
`project.yml`, so they run through the generated Xcode project. `Package.swift`
declares no test target, and both the package and its binary targets are
iOS-only, so `swift test` has nothing to run and a host `swift build` is not a
supported path.

```bash
# from the repository root, if npm run setup has not been run yet
npm run setup

cd ios
xcodegen generate
xcodebuild test \
  -project ZappMessaging.xcodeproj \
  -scheme ZappMessaging \
  -destination 'platform=iOS Simulator,name=iPhone 16'
```

Use a device name that `xcrun simctl list devices available` reports.

## License

Apache 2.0

## Support

For issues and questions, please open an issue on GitHub.
