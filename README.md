# ZappMessaging - P2P Messaging SDK

Open-source peer-to-peer messaging SDK for Zapp applications, built on the Holepunch/Pear stack (Hyperswarm + Bare).

## Overview

ZappMessaging provides a comprehensive P2P messaging solution with no central server:
- **Direct Messages**: Private peer-to-peer conversations addressed by public key, with no accept step in the SDK
- **Group Chats**: Multi-person chats with invite system and permissions
- **Media Transfer**: Chunked image and GIF transfer with content-addressed storage
- **No Accounts**: Peer discovery over the Hyperswarm DHT with Noise-encrypted transport
- **Privacy-First**: Keypair-based identity, no phone numbers or tracking
- **Resilient Delivery**: An optional blind peer mirrors encrypted cores and relays connections, so chat keeps working behind restrictive networks without seeing message contents

## Packages

- `core/` - Platform-agnostic JavaScript modules (CommonJS, Bare runtime)
- `ios/` - Swift framework for iOS apps with XCFramework
- `android/` - Kotlin library for Android apps with AAR
- `server/` - Blind-peer relay and invite mailbox for offline delivery

## Quick Start (iOS)

```swift
import ZappMessaging

let sdk = ZappMessagingSDK(
    config: ZappMessagingConfig(dataDir: appSupportDirectory)
)

try await sdk.initialize()

// Chat identity is derived from the host wallet's BIP39 seed.
let identity = try await sdk.restoreFromSeedPhrase(seedPhrase, displayName: "Alice")
print("Chat public key: \(identity.publicKey)")

let conversation = try await sdk.createConversation(
    type: .direct,
    participants: [recipientPublicKey],
    displayName: "Bob"
)

let message = try await sdk.sendMessage(
    conversationId: conversation.id,
    content: "Hello!"
)
print("Sent \(message.id)")
```

See [ios/README.md](ios/README.md) for setup, configuration, and the full Swift surface.

## Chat Types Supported

### Direct Messages
- **Discovery**: Order-independent topic derived from both public keys
- **Use Case**: Private conversations, direct deals, customer support
- **Access**: An inbound invite auto-joins the pairwise topic, replies with the reciprocal core key, and then raises `conversation.invite_received`. Accept and block policy is the host application's responsibility

### Group Chats
- **Discovery**: Random group ID with invite system
- **Use Case**: Trading groups, project teams, private communities
- **Access**: Invite-based with member management and permissions

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   iOS/Android   │    │  Native Wrapper  │    │  JS Worklet     │
│   App           │◄──►│  (Public API)    │◄──►│ (Core Logic)    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                       │
                                              ┌─────────────────┐
                                              │  Hyperswarm     │
                                              │  (P2P Network)  │
                                              └─────────────────┘
```

## Documentation

- [Core API Reference](core/API.md) - IPC protocol between the native wrappers and the worklet
- [iOS SDK](ios/README.md) - Swift setup and usage
- [Android SDK](android/README.md) - Kotlin setup and usage
- [Server](server/README.md) - Blind-peer relay and invite mailbox deployment

## Technology Stack

### Core P2P Infrastructure
- **P2P Runtime**: BareKit worklet (JavaScript in Bare kernel)
- **Networking**: Hyperswarm (DHT-based peer discovery with Noise protocol)
- **Crypto**: Hypercore-Crypto (Ed25519 keypairs, Blake2b hashing)
- **Storage**: Corestore (RocksDB-backed atomic storage)
- **Offline Delivery**: Blind-peer mirroring via `blind-peering` and `protomux-rpc`
- **Compatibility**: B4A (Buffer/Uint8Array cross-platform)

### Mobile Integration
- **iOS**: BareKit XCFramework with Swift Package Manager
- **Android**: BareKit AAR with Gradle support
- **IPC**: NDJSON (newline-delimited JSON) over BareKit IPC
- **Architecture**: Single JavaScript core with platform-native wrappers

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, commit message and pull request expectations, and the Developer's Certificate of Origin sign-off.

## Security

Report vulnerabilities through the private [security advisory flow](https://github.com/JustZappIt/zappmessaging-sdk/security/advisories/new). Do not file them as public issues.

## License

Apache License 2.0 - see [LICENSE](LICENSE) file for details.

## Related Projects

- [JustZappIt](https://github.com/JustZappIt/JustZappIt) - Landing site
- [zapp-android](https://github.com/JustZappIt/zapp-android) - Android app
