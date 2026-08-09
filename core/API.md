# ZappMessaging Core API Documentation

## Overview

The ZappMessaging Core is a JavaScript library that provides peer-to-peer messaging functionality using Hyperswarm for network connectivity. It runs in the Bare runtime and communicates with Swift/Kotlin wrappers via IPC.

## Architecture

```
┌─────────────────────────────────────────┐
│         Swift/Kotlin UI Layer           │
└──────────────┬──────────────────────────┘
               │ IPC (NDJSON)
┌──────────────▼──────────────────────────┐
│         IPC Handler                      │
├──────────────────────────────────────────┤
│  Identity  │  ChatStore  │  ContactStore │
│  P2PManager│  MediaStore │  MediaTransfer│
└──────────────┬──────────────────────────┘
               │ Hyperswarm
               ▼
         P2P Network
```

## IPC Protocol

### Message Format

All IPC messages use newline-delimited JSON (NDJSON):

**Request:**
```json
{
  "id": "unique-request-id",
  "type": "category.action",
  "payload": { /* action-specific data */ }
}
```

**Response:**
```json
{
  "id": "unique-request-id",
  "success": true,
  "data": { /* response data */ },
  "error": { "code": "ERROR_CODE", "message": "..." }
}
```

**Event (Push from Core):**
```json
{
  "type": "event.type",
  "payload": { /* event data */ },
  "timestamp": 1234567890
}
```

A single NDJSON line may not exceed 1 MB in either direction. An oversized
inbound line is discarded and reported as an `ipc.error` event; the core keeps
list responses under the cap by stripping inline thumbnails (see
`message.list`).

## API Categories

### 0. Protocol Handshake

#### `protocol.init`
Negotiate the IPC protocol version. Send this before any other command: the
core does not enforce it, but a wrapper that skips it cannot tell a
version-incompatible worklet from a working one, and will misread later
responses instead of failing cleanly.

**Request:**
```json
{
  "type": "protocol.init",
  "payload": {
    "protocolVersion": "1.0"
  }
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "protocolVersion": "1.0",
    "features": ["messaging", "groups", "media", "blind_peer", "contacts", "payments", "read_receipts"],
    "compatible": true
  }
}
```

`compatible` is true when the major versions match. A missing
`protocolVersion` in the request is treated as `"0.0"`.

### 1. Identity Management

#### `identity.create`
Create a new cryptographic identity.

**Destructive.** Before the new identity is issued the core stops the P2P
manager, clears the chat store and the contact store, and resets the read
receipt preference to its default. Any prior conversations, messages and
contacts on this device are gone. It restarts P2P and re-arms the blind-peer
mirror on the new keypair before responding.

**Request:**
```json
{
  "type": "identity.create",
  "payload": {
    "displayName": "Alice"
  }
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "publicKey": "a1b2c3...",
    "displayName": "Alice",
    "seedPhrase": "word1 word2 word3 ... word24"
  }
}
```

The response carries the user's 24-word recovery phrase, returned inline so a
wrapper does not need a second `migration.get_seed_phrase` call that could fail
independently of identity creation. A wrapper must never log this response,
persist it, or send it anywhere. Show the phrase to the user and drop it.

#### `identity.get`
Get current identity information. Returns an empty object when no identity
exists yet.

**Request:**
```json
{
  "type": "identity.get",
  "payload": {}
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "publicKey": "a1b2c3...",
    "displayName": "Alice"
  }
}
```

#### `identity.update`
Change the display name. Returns the same shape as `identity.get`.

**Request:**
```json
{
  "type": "identity.update",
  "payload": {
    "displayName": "Alice B"
  }
}
```

#### `identity.export`
Return only the public key.

**Response:**
```json
{
  "success": true,
  "data": {
    "publicKey": "a1b2c3..."
  }
}
```

#### `identity.restore`
Alias for `migration.restore_from_seed_phrase`, kept for backward
compatibility. Same payload and same response.

### 2. Contacts Management

#### `contacts.add`
Add a new contact.

**Request:**
```json
{
  "type": "contacts.add",
  "payload": {
    "publicKey": "def456...",
    "name": "Bob"
  }
}
```

#### `contacts.list`
List all contacts, sorted by name.

**Response:**
```json
{
  "success": true,
  "data": {
    "contacts": [
      {
        "publicKey": "def456...",
        "name": "Bob",
        "addedAt": 1234567890
      }
    ]
  }
}
```

#### `contacts.update`
Update contact information.

**Request:**
```json
{
  "type": "contacts.update",
  "payload": {
    "publicKey": "def456...",
    "updates": {
      "name": "Bob Smith"
    }
  }
}
```

#### `contacts.remove`
Remove a contact.

**Request:**
```json
{
  "type": "contacts.remove",
  "payload": {
    "publicKey": "def456..."
  }
}
```

#### `contacts.updateWalletAddress`
Set the wallet address on an existing contact. Returns
`{ "success": true, "contact": { /* updated contact */ } }`.

**Request:**
```json
{
  "type": "contacts.updateWalletAddress",
  "payload": {
    "publicKey": "def456...",
    "walletAddress": "u1..."
  }
}
```

### 3. Conversation Management

#### `conversation.create`
Create a new conversation.

**Request (Direct Chat):**
```json
{
  "type": "conversation.create",
  "payload": {
    "type": "direct",
    "participants": ["def456..."],
    "displayName": "Bob"
  }
}
```

**Request (Group Chat):**
```json
{
  "type": "conversation.create",
  "payload": {
    "type": "group",
    "participants": ["def456...", "ghi789..."],
    "displayName": "Team Chat"
  }
}
```

The handler also accepts `"type": "store"` with a `storeId` and `"type": "city"`
with a `citySlug`, but neither is implemented: only `direct` and `group` join a
swarm topic, so a store or city conversation is persisted as a local-only record
that never sends or receives. Do not build against them.

A direct conversation takes exactly one participant, and its id is derived
deterministically from both public keys, so both sides land in the same room.
Creating one that already exists reuses the existing record and re-sends the
invite.

**Response:**
```json
{
  "success": true,
  "data": {
    "conversation": {
      "id": "conv-abc123",
      "type": "direct",
      "participantIds": ["def456..."],
      "displayName": "Bob",
      "lastMessage": null,
      "lastMessageTimestamp": null,
      "createdAt": 1234567890
    }
  }
}
```

Group conversations carry three additional fields: `groupId`, `creatorKey`, and
`isOwner` (true when `creatorKey` is this identity's public key). Direct
conversations carry none of them.

#### `conversation.list`
List all conversations, most recent first. Conversations the user has left are
excluded.

**Response:**
```json
{
  "success": true,
  "data": {
    "conversations": [
      {
        "id": "conv-abc123",
        "type": "direct",
        "participantIds": ["def456..."],
        "displayName": "Bob",
        "lastMessage": "Hello!",
        "lastMessageTimestamp": 1234567890,
        "createdAt": 1234567800
      }
    ]
  }
}
```

#### `conversation.get`
Get a specific conversation. `conversation` is null when the id is unknown.

**Request:**
```json
{
  "type": "conversation.get",
  "payload": {
    "conversationId": "conv-abc123"
  }
}
```

#### `conversation.direct_status`
Resolve the deterministic conversation id for a peer without creating anything,
and report whether the user has left it.

**Request:**
```json
{
  "type": "conversation.direct_status",
  "payload": {
    "participants": ["def456..."]
  }
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "conversationId": "conv-abc123",
    "isLeft": false
  }
}
```

#### `conversation.leave`
Leave a group conversation. Notifies the other members, disconnects, and deletes
the local record.

**Request:**
```json
{
  "type": "conversation.leave",
  "payload": {
    "conversationId": "conv-abc123"
  }
}
```

#### `conversation.delete`
Delete a group conversation (owner only).

**Request:**
```json
{
  "type": "conversation.delete",
  "payload": {
    "conversationId": "conv-abc123"
  }
}
```

#### `conversation.remove`
Remove any conversation locally, direct or group. The conversation is
tombstoned first, so a later inbound message or invite cannot resurrect it. A
group also notifies its other members.

**Request:**
```json
{
  "type": "conversation.remove",
  "payload": {
    "conversationId": "conv-abc123"
  }
}
```

#### `conversation.rename`
Rename a conversation. For a group the new name is also broadcast to members.

**Request:**
```json
{
  "type": "conversation.rename",
  "payload": {
    "conversationId": "conv-abc123",
    "name": "Team Chat v2"
  }
}
```

#### `conversation.add_member`
Add a member to a group. Owner only. Sends the new member an invite and
announces them to the existing members.

**Request:**
```json
{
  "type": "conversation.add_member",
  "payload": {
    "conversationId": "conv-abc123",
    "publicKey": "ghi789...",
    "displayName": "Carol"
  }
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "success": true,
    "participants": ["def456...", "ghi789..."]
  }
}
```

An already-present member yields `{ "success": true, "alreadyMember": true }`.

#### `conversation.get_messages`
Paginated message history. Unlike `message.list` this reports totals, so it is
the one to use for scrollback.

**Request:**
```json
{
  "type": "conversation.get_messages",
  "payload": {
    "conversationId": "conv-abc123",
    "offset": 0,
    "limit": 50
  }
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "messages": [ /* message objects, thumbnails stripped */ ],
    "total": 320,
    "hasMore": true
  }
}
```

### 4. Message Management

#### `message.send`
Send a text message.

**Request:**
```json
{
  "type": "message.send",
  "payload": {
    "conversationId": "conv-abc123",
    "content": "Hello, world!",
    "contentType": "text/plain",
    "replyToId": null,
    "replyToSenderName": null,
    "replyToContent": null
  }
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "message": {
      "id": "msg-xyz789",
      "conversationId": "conv-abc123",
      "senderId": "a1b2c3...",
      "senderName": "Alice",
      "content": "Hello, world!",
      "contentType": "text/plain",
      "timestamp": 1234567890,
      "isFromMe": true,
      "status": "sent",
      "mediaId": null,
      "mediaSize": null,
      "mediaWidth": null,
      "mediaHeight": null,
      "thumbnailData": null,
      "mediaLocalPath": null,
      "mediaTransferState": null,
      "replyToId": null,
      "replyToSenderName": null,
      "replyToContent": null
    },
    "sent": true,
    "durability": {
      "sent": true,
      "appended": true,
      "relay": "request_acknowledged"
    }
  }
}
```

`sent` reports live socket delivery only. Durability is the stronger signal:
the response is issued only after the message is appended to the conversation's
Hypercore, and a send that could not be appended fails with
`MESSAGE_NOT_DURABLE` rather than returning. `durability.relay` is one of
`request_acknowledged` (the blind peer accepted the doorbell), `pending` (the
doorbell failed, the block is still durable), `not_requested`, or `unavailable`.

#### `message.send_transaction` / `message.send_payment_request` / `message.send_wallet_address`
Send a structured message. The whole message body is supplied by the caller
under `message`; the core stamps sender fields and sends it the same way
`message.send` does, with the same response shape.

**Request:**
```json
{
  "type": "message.send_transaction",
  "payload": {
    "conversationId": "conv-abc123",
    "message": { /* caller-defined message fields */ }
  }
}
```

#### `message.list`
Get recent messages for a conversation.

**Request:**
```json
{
  "type": "message.list",
  "payload": {
    "conversationId": "conv-abc123",
    "limit": 50
  }
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "messages": [
      {
        "id": "msg-xyz789",
        "conversationId": "conv-abc123",
        "senderId": "a1b2c3...",
        "content": "Hello!",
        "timestamp": 1234567890,
        "isFromMe": true
      }
    ]
  }
}
```

Inline base64 thumbnails are stripped from every message in the response and
replaced with `"hasThumbnail": true`; 50 of them can push one NDJSON line past
the 1 MB receive cap. Hydrate each one lazily with `message.get_thumbnail`.
Listing a conversation also sends delivery receipts for its newest inbound
messages.

#### `message.get_thumbnail`
Fetch a single message's inline thumbnail. Returns
`{ "thumbnailData": null }` when there is none.

**Request:**
```json
{
  "type": "message.get_thumbnail",
  "payload": {
    "conversationId": "conv-abc123",
    "messageId": "msg-xyz789"
  }
}
```

#### `message.mark_read`
Mark the conversation read up to its newest inbound message and send a read
receipt. Direct conversations only, and a no-op when read receipts are
disabled.

**Request:**
```json
{
  "type": "message.mark_read",
  "payload": {
    "conversationId": "conv-abc123"
  }
}
```

#### `message.set_read_receipts`
Turn read receipts on or off. Symmetric: off stops both sending our receipts and
surfacing incoming ones. Defaults to on, and resets to on after
`identity.create` or a restore that changes identity, so push the user's real
setting on startup.

**Request:**
```json
{
  "type": "message.set_read_receipts",
  "payload": {
    "enabled": true
  }
}
```

#### `message.set_presence_visible`
Turn peer presence broadcasting on or off. Returns
`{ "success": true, "enabled": true }`.

### 5. Media Management

#### `media.prepare_send`
Prepare media for sending (hash and store). The wrapper writes a compressed file
to a temp path; the core reads, hashes and stores it. Fails if the file is empty
or larger than the media cap (32 MB by default, `--media-max-bytes`).

**Request:**
```json
{
  "type": "media.prepare_send",
  "payload": {
    "filePath": "/tmp/image.jpg",
    "extension": "jpg"
  }
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "mediaId": "hash-abc123...",
    "mediaSize": 123456,
    "mediaLocalPath": "/path/to/stored/media.jpg"
  }
}
```

#### `media.send_message`
Send a media message. Returns `{ "message": { ... }, "durability": { ... } }`
with the same `durability` shape as `message.send`. Note there is no top-level
`sent` field here.

**Request:**
```json
{
  "type": "media.send_message",
  "payload": {
    "conversationId": "conv-abc123",
    "contentType": "image/jpeg",
    "mediaId": "hash-abc123...",
    "mediaSize": 123456,
    "mediaWidth": 1920,
    "mediaHeight": 1080,
    "thumbnailData": "base64-encoded-thumbnail",
    "mediaLocalPath": "/path/to/media.jpg"
  }
}
```

The message JSON carries only the metadata and thumbnail. The bytes are
transferred separately in chunks, pushed to peers connected at send time and
pulled by any recipient that sees a `mediaId` it does not have on disk.

#### `media.get_path`
Get local path for media by hash.

**Request:**
```json
{
  "type": "media.get_path",
  "payload": {
    "mediaId": "hash-abc123..."
  }
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "path": "/path/to/media.jpg",
    "exists": true
  }
}
```

### 6. Connection Management

#### `connection.connect`
Connect to a conversation's P2P topics, flush its queued outgoing messages, and
send delivery receipts for its newest inbound messages.

**Request:**
```json
{
  "type": "connection.connect",
  "payload": {
    "conversationId": "conv-abc123"
  }
}
```

#### `connection.status`
Get P2P connection status.

**Response:**
```json
{
  "success": true,
  "data": {
    "online": true,
    "peerCount": 5
  }
}
```

#### `connection.details`
Full connectivity diagnostics. Intended for a debug screen, not for UI state.

**Response:**
```json
{
  "success": true,
  "data": {
    "online": true,
    "peerCount": 5,
    "globalConnections": 12,
    "pendingQueues": 1,
    "pendingMessageCount": 3,
    "pendingInvites": 0,
    "dhtHealth": "healthy",
    "dhtLastCheck": 1234567890,
    "consecutiveFailures": 0,
    "directConversations": 4,
    "groupConversations": 1,
    "dhtBootstrapped": true,
    "dhtFirewalled": false,
    "dhtRandomized": false,
    "rtNodes": 42,
    "relayEnabled": true,
    "relaysConnected": 1,
    "relaysTotal": 1
  }
}
```

### 7. Migration & Backup

Named `migration.*` for historical reasons. The only durable form is BIP-39
entropy and the 24-word mnemonic derived from it.

#### `migration.get_seed_phrase`
Get the 24-word recovery phrase for backup. Never log or persist this response.

**Response:**
```json
{
  "success": true,
  "data": {
    "seedPhrase": "word1 word2 word3 ... word24"
  }
}
```

#### `migration.restore_from_seed_phrase`
Restore identity from a seed phrase.

**Request:**
```json
{
  "type": "migration.restore_from_seed_phrase",
  "payload": {
    "seedPhrase": "word1 word2 word3 ... word24",
    "displayName": "Alice"
  }
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "publicKey": "a1b2c3...",
    "displayName": "Alice"
  }
}
```

Destructive only when the incoming seed derives a different public key than the
one already loaded: in that case P2P stops, the chat and contact stores are
cleared, and the read receipt preference resets to its default before the new
identity is installed. Re-publishing the same seed is idempotent and leaves
local history, swarm state and the stored display name untouched, so a wrapper
may safely retry it.

### 8. Blind-Peer Relay

Returns `{ "enabled": false, "reason": "not_initialized" }` for every action
until the blind mirror comes up.

#### `blind_peer.status`

**Response:**
```json
{
  "success": true,
  "data": {
    "enabled": true,
    "blindPeerCount": 1,
    "registeredConversations": 4,
    "relaysConnected": 1,
    "relaysTotal": 1,
    "keys": ["a1b2c3d4e5f6..."]
  }
}
```

`keys` are truncated for logging and are not usable as full public keys.

#### `blind_peer.set_keys`
Replace the blind-peer key set. Returns `{ "ok": true, "keyCount": 1 }`.

**Request:**
```json
{
  "type": "blind_peer.set_keys",
  "payload": {
    "keys": ["a1b2c3..."]
  }
}
```

### 9. Push Notifications

#### `push.topics`
List the inbound push topics the wrapper should subscribe to. Direct
conversations only. Re-read it whenever a `push.topics_changed` event arrives.

**Response:**
```json
{
  "success": true,
  "data": {
    "version": 1,
    "hydrated": true,
    "supportsGroups": false,
    "conversations": [
      {
        "conversationId": "conv-abc123",
        "lifecycle": "ready",
        "inboundTopics": ["topic-hex..."]
      }
    ]
  }
}
```

`lifecycle` is `ready` once at least one inbound topic exists, otherwise
`awaiting_remote_core`.

#### `push.register_endpoint`
Register a Web Push endpoint with the relay. Returns `{ "ok": true }`.

**Request:**
```json
{
  "type": "push.register_endpoint",
  "payload": {
    "endpoint": "https://...",
    "p256dh": "base64...",
    "auth": "base64..."
  }
}
```

### 10. Host-Carried HTTPS Bridge

The worklet has no HTTPS stack of its own. When the invite mailbox has to be
reached over HTTPS (the fallback for networks that drop UDP, so the DHT mailbox
is unusable), the core asks the host app to perform the request and waits for
the reply. A wrapper that does not implement this loses invite delivery on those
networks with no other symptom.

The worklet keeps the mailbox cryptography, so the body it hands over is already
opaque. The worklet names the destination, so the host decides whether it is
allowed: both shipping wrappers require HTTPS and require the URL to sit under
the configured `--invite-mailbox-url`, and reject anything else.

**Event pushed by the core:**
```json
{
  "type": "platform.http_request",
  "payload": {
    "requestId": "abc-1",
    "url": "https://mailbox.example/...",
    "body": { /* opaque JSON to POST */ },
    "timeoutMs": 15000
  },
  "timestamp": 1234567890
}
```

**Reply the wrapper sends back:**
```json
{
  "type": "platform.http_response",
  "payload": {
    "requestId": "abc-1",
    "success": true,
    "response": { /* parsed JSON response body */ }
  }
}
```

On failure send `{ "requestId": "abc-1", "success": false, "error": "..." }`.
The response to `platform.http_response` is `{ "accepted": true }`, or
`{ "accepted": false }` when no request is pending under that `requestId`
(already timed out, or an unknown id). `requestId` is required; omitting it is
an error.

## Push Events

Events pushed from Core to UI:

### `conversation.invite_received`
Triggered when a direct or group invite is received.

```json
{
  "type": "conversation.invite_received",
  "payload": {
    "conversation": { /* conversation object */ }
  },
  "timestamp": 1234567890
}
```

### `conversation.member_left`
Triggered when a member leaves a group.

```json
{
  "type": "conversation.member_left",
  "payload": {
    "conversationId": "conv-abc123",
    "leaverKey": "def456..."
  },
  "timestamp": 1234567890
}
```

### `conversation.member_added`
Triggered when the group owner adds a member.

```json
{
  "type": "conversation.member_added",
  "payload": {
    "conversationId": "conv-abc123",
    "newMemberKey": "ghi789...",
    "newMemberName": "Carol"
  },
  "timestamp": 1234567890
}
```

### `conversation.group_renamed`
Triggered when a group is renamed by another member.

```json
{
  "type": "conversation.group_renamed",
  "payload": {
    "conversationId": "conv-abc123",
    "newName": "Team Chat v2"
  },
  "timestamp": 1234567890
}
```

### `conversation.group_deleted`
Triggered when a group is deleted by owner.

```json
{
  "type": "conversation.group_deleted",
  "payload": {
    "conversationId": "conv-abc123",
    "displayName": "Team Chat"
  },
  "timestamp": 1234567890
}
```

### `message.received`
Triggered when a new message is received. Duplicates are suppressed: a message
that arrives over both the live socket and Hypercore replication raises this
once.

```json
{
  "type": "message.received",
  "payload": {
    "conversationId": "conv-abc123",
    "message": { /* message object */ }
  },
  "timestamp": 1234567890
}
```

### `message.status`
Triggered when an outgoing message changes state. `status` is one of `queued`,
`sent`, `delivered`, `read`.

```json
{
  "type": "message.status",
  "payload": {
    "messageId": "msg-xyz789",
    "conversationId": "conv-abc123",
    "status": "delivered"
  },
  "timestamp": 1234567890
}
```

### `message.persist_failed`
Triggered when an outgoing message could not be appended to Hypercore. The send
also fails with `MESSAGE_NOT_DURABLE`.

```json
{
  "type": "message.persist_failed",
  "payload": {
    "conversationId": "conv-abc123",
    "messageId": "msg-xyz789",
    "error": "..."
  },
  "timestamp": 1234567890
}
```

### `media.transfer_complete`
Triggered when an inbound chunked media transfer finishes and its content hash
verifies. The core emits nothing on the outbound side, so this is always a
completed download, and `mediaLocalPath` is always present. Both shipping
wrappers key on `mediaLocalPath` to identify one.

```json
{
  "type": "media.transfer_complete",
  "payload": {
    "mediaId": "hash-abc123...",
    "mediaLocalPath": "/path/to/media.jpg",
    "mediaSize": 123456
  },
  "timestamp": 1234567890
}
```

### `media.transfer_progress`
Triggered as inbound chunks arrive. `progress` is the fraction of chunks
received, 0 to 1. Outbound transfers report no progress.

```json
{
  "type": "media.transfer_progress",
  "payload": {
    "mediaId": "hash-abc123...",
    "progress": 0.75
  },
  "timestamp": 1234567890
}
```

### `connection.status`
Triggered when P2P connectivity changes.

```json
{
  "type": "connection.status",
  "payload": {
    "online": true,
    "peerCount": 5
  },
  "timestamp": 1234567890
}
```

### `connection.peer_status`
Triggered when a peer in a conversation comes online or goes offline. `peerId`
is truncated to 12 characters and is not a usable public key.

```json
{
  "type": "connection.peer_status",
  "payload": {
    "conversationId": "conv-abc123",
    "peerId": "def456789abc",
    "status": "online"
  },
  "timestamp": 1234567890
}
```

### `connection.dht_health`
Triggered when DHT health changes. `status` is `healthy`, `degraded`, or
`critical`.

```json
{
  "type": "connection.dht_health",
  "payload": {
    "status": "degraded",
    "message": "..."
  },
  "timestamp": 1234567890
}
```

### `push.topics_changed`
Triggered when the inbound push topic set changes. Re-read `push.topics`.

```json
{
  "type": "push.topics_changed",
  "payload": { "version": 1 },
  "timestamp": 1234567890
}
```

### `push.notification_failed`
Triggered when the blind-peer doorbell request for an outgoing message failed.
Advisory: the message is still durable and still replicates.

```json
{
  "type": "push.notification_failed",
  "payload": {
    "conversationId": "conv-abc123",
    "messageId": "msg-xyz789",
    "error": "..."
  },
  "timestamp": 1234567890
}
```

### `platform.http_request`
Host-carried HTTPS request. See "Host-Carried HTTPS Bridge" above.

### `ipc.error`
Triggered when an inbound NDJSON line exceeded the 1 MB cap and was discarded.

```json
{
  "type": "ipc.error",
  "payload": {
    "code": "BUFFER_OVERFLOW",
    "message": "IPC frame exceeded limit and was discarded"
  },
  "timestamp": 1234567890
}
```

## Chat Types

### 1. Direct Chat
- **Topic Derivation**: Order-independent hash of both public keys
- **Discovery**: Both peers join the same pairwise topic
- **Invites**: Sent via recipient's personal topic

### 2. Group Chat
- **Topic Derivation**: Hash of group ID
- **Discovery**: All members join the shared group topic
- **Invites**: Sent via each participant's personal topic
- **Permissions**: Only creator can delete the group

### 3. Personal Topic
- **Topic Derivation**: Hash of user's public key
- **Discovery**: User joins their own personal topic
- **Use Case**: Receiving invites and direct messages

## Error Codes

A response carries `error.code` only when the handler threw an error that
declares one. Everything else, including unknown categories and actions, all
input validation, and store failures, surfaces as the generic `ERROR` with the
thrown message.

- `ERROR`: any handler that threw a plain `Error`
- `CONVERSATION_NOT_FOUND`: send into an unknown `conversationId`
- `MISSING_PARTICIPANT`: direct chat created with no participant, or a stored direct conversation that has no remote participant
- `INVALID_PARTICIPANTS`: direct chat created with more than one participant
- `INVALID_PUBLIC_KEY`: direct chat participant key is not 64 hex characters
- `OWN_PUBLIC_KEY`: direct chat addressed to this identity's own key
- `MESSAGE_NOT_DURABLE`: the outgoing message was not appended to Hypercore, so nothing was sent

`BUFFER_OVERFLOW` is not a response code. It appears as the `code` field of the
`ipc.error` push event.

## Storage Locations

Everything lives under `<dataDir>/zappmessaging/`, where `<dataDir>` is the
value of the `--data-dir` argv flag the wrapper passes at worklet start. Both
shipping wrappers always pass it:

- **iOS**: the app's `Library/Application Support` container, marked excluded from backup (`ZappMessagingConfig.defaultDataDir`)
- **Android**: `context.filesDir`, i.e. `/data/data/<package>/files` (`BareWorkletManager`)

The core appends `zappmessaging` itself, so pass the container and not the store
directory. Without `--data-dir` it falls back to `<homedir>/Documents/zappmessaging`,
which on iOS is the iCloud-backed sandbox and is why the flag is not optional in
practice.

- **Identity**: `<dataDir>/zappmessaging/identity.json`
- **Contacts**: `<dataDir>/zappmessaging/contacts.json`
- **Conversations**: `<dataDir>/zappmessaging/chats/index.json`
- **Messages**: `<dataDir>/zappmessaging/chats/{conversationId}.json`
- **Media**: `<dataDir>/zappmessaging/media/{hash}.{ext}`
- **Corestore**: `<dataDir>/zappmessaging/corestore/`
- **Remote core index**: `<dataDir>/zappmessaging/corekeys.json`
- **Logs**: `<dataDir>/zappmessaging/diag.log`, plus an IPC log in the OS temp directory

Deleting `<dataDir>/zappmessaging/` wipes all chat state.

## Dependencies

See `dependencies` in the root `package.json` for the authoritative list. The
core builds on Hyperswarm for DHT peer discovery and Noise transport,
hypercore-crypto and sodium-universal for keys and hashing, Corestore for
append-only storage, blind-peering and protomux-rpc for offline relay, and the
`bare-*` modules for runtime primitives.

## Testing

Run the test suite:
```bash
npm test
```

It covers storage operations, identity management, mnemonic encoding and
decoding, socket and IPC framing, topic derivation, conversation key derivation,
chat store operations, media store, transfer and fetch, contact management,
Hypercore authorization and cursors, blind mirror, invite mailbox, push topics,
and P2P manager basics.
