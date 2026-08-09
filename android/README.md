# ZappMessaging Android SDK

Kotlin wrapper for the ZappMessaging P2P chat SDK, using BareKit Android for JavaScript worklet execution.

## Overview

ZappMessaging is a peer-to-peer messaging SDK that uses Hyperswarm for network connectivity and runs JavaScript core logic in a BareKit worklet.

## Features

- Ed25519 cryptographic identity, derived from 32 bytes of BIP-39 entropy
- 24-word BIP-39 recovery phrase, returned once by `createIdentity`
- P2P messaging via Hyperswarm
- Direct and group chat support
- Content-addressed media storage
- Chunked media transfer
- Identity entropy encrypted at rest under an Android Keystore wrapped key
- Coroutines + StateFlow for reactive state

## Identity storage

The Kotlin module holds no key material of its own. `IdentityFileKeyProvider` mints a
random 32-byte data key on first run and persists it only as AES-256-GCM ciphertext in
the `zappmessaging_keys` SharedPreferences file, wrapped by a key that never leaves the
Android Keystore (StrongBox where the device supports it, TEE otherwise). The plaintext
data key is passed to the worklet as `--identity-file-key`, and the JavaScript core
secretbox-encrypts (XSalsa20-Poly1305) the BIP-39 entropy in its `identity.json` with it.

If the Keystore is unusable on the device, no key is supplied and the core keeps
`identity.json` in the legacy plaintext format. If the wrapped blob no longer unwraps
(the OS reset the keystore), a fresh data key is minted, the existing encrypted identity
file can no longer be read, and the user recovers from the 24-word phrase.

## Requirements

- Android 10+ (API 29)
- Kotlin 1.9+
- A local checkout of BareKit (https://github.com/holepunchto/bare-kit). The module
  declares `api(project(":bare-kit"))`, so the consuming build must include that
  subproject. `to.holepunch.bare.kit` is the Java package the Kotlin sources import,
  not a Maven coordinate.

## Setup

### Build worklet bundle and native addons

From the repository root:

```bash
npm run setup:android
```

This runs `npm install`, then `scripts/link-addons.js` (bare-link) and `bare-pack` to
produce:
- `android/src/main/assets/worklet.bundle`, the bundled JavaScript core
- `android/src/main/jniLibs/{abi}/*.so`, native addons for all Android ABIs

Both outputs are committed, so consumers do not need to run this unless they change
`core/` or bump a native dependency.

### Add as dependency

Clone BareKit next to this repository, then in your app's `settings.gradle.kts`:
```kotlin
include(":zappmessaging")
project(":zappmessaging").projectDir = file("../zappmessaging-sdk/android")
include(":bare-kit")
project(":bare-kit").projectDir = file("../bare-kit/android")
```

Both paths are relative to your app's root project. Substitute your own checkout
locations. Omitting `:bare-kit` fails with "Project with path ':bare-kit' could not be
found".

In your app's `build.gradle.kts`:
```kotlin
dependencies {
    implementation(project(":zappmessaging"))
}
```

## Usage

Every call below except `suspend()`, `resume()` and `shutdown()` is a suspend function.

### Initialize SDK

```kotlin
val sdk = ZappMessagingSDK()

// From a coroutine
sdk.initialize(applicationContext)
```

### Create Identity

Returns the identity and its 24-word recovery phrase in one round-trip. Surface the
phrase to the user for backup before discarding it. The call wipes existing
conversations and contacts before minting the new identity.

```kotlin
val (identity, seedPhrase) = sdk.createIdentity("Alice")
```

### Restore from Seed Phrase

```kotlin
val identity = sdk.restoreFromSeedPhrase(
    "word1 word2 ... word24",
    displayName = "Alice"
)
```

### Create Conversations

```kotlin
// Direct chat
val conversation = sdk.createConversation(
    type = ConversationType.DIRECT,
    participants = listOf("bob_public_key"),
    displayName = "Bob"
)

// Group chat
val conversation = sdk.createConversation(
    type = ConversationType.GROUP,
    participants = listOf("bob_key", "charlie_key"),
    displayName = "Team Chat"
)
```

### Send Messages

```kotlin
val message = sdk.sendMessage(
    conversationId = conversation.id,
    content = "Hello, world!"
)
```

### Observe State

```kotlin
// Collect in a ViewModel or Composable
sdk.identity.collect { identity -> /* update UI */ }
sdk.conversations.collect { conversations -> /* update UI */ }
sdk.messageReceived.collect { (conversationId, message) -> /* handle incoming */ }
```

### App Lifecycle

```kotlin
// In Activity or Application
override fun onPause() {
    super.onPause()
    sdk.suspend()
}

override fun onResume() {
    super.onResume()
    sdk.resume()
}

override fun onDestroy() {
    super.onDestroy()
    sdk.shutdown()
}
```

## Architecture

```
┌─────────────────────────────────────┐
│       Android App (Compose)         │
│  (Uses ZappMessagingSDK)            │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│     ZappMessagingSDK.kt             │
│  - Public API                       │
│  - StateFlow / SharedFlow           │
│  - State Management                 │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│        IPCBridge.kt                 │
│  - NDJSON Protocol                  │
│  - Request/Response Handling        │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│    BareWorkletManager.kt            │
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

## License

Apache 2.0
