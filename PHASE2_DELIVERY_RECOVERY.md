# Phase 2: delivery and recovery

Implemented on `fix/delivery-recovery`, based on freshly fetched `origin/main`
(`0385044`). The initial working tree was clean. Main contained the same phase 1
file tree as the previous `fix/messaging-integrity` checkout. No phase 1 fixes
were reverted.

## Changes and rationale

- **Identity lifecycle:** `P2PManager.stop()` closes network activity but retains
  application listeners; `dispose()` removes listeners for final shutdown.
  Explicit task scopes stop admission and await old work before identity changes.
  Socket callbacks and startup continuations are scoped to their original
  network generation. Receipt appends, IPC/control handlers, core ingestion and
  media work cannot finish under a replacement identity.
- **Core and mirror recovery:** different-identity replacement closes the old
  mirror and cores, removes old named writable feeds and the core-key index,
  clears chat/contact history, and reconnects with the new keys. Mirror setup
  cannot publish a stale swarm reference even when restart overtakes its initial
  restore. Media transfers and maintenance/startup timers are cancelled.
  Cleanup errors prevent proceeding with replacement instead of being ignored.
- **Restoration:** the mnemonic and display name are validated before cleanup,
  including when no identity is loaded. Same-seed restoration preserves history,
  preferences, core handles and the active swarm. If stopped, it restarts and
  rejoins saved conversations and pending messages without appending them again.
- **Notifications:** successful Hypercore append remains the send durability
  boundary. The direct socket path and IPC response no longer wait for the
  doorbell RPC. Notification work permits two concurrent operations and 64 queued
  jobs, with the existing three attempts (initial, then 1s and 3s delays) and a
  15s limit per attempt. A timed-out underlying request retains its concurrency
  slot until it settles; timeouts cannot multiply outstanding RPCs. Suspension,
  shutdown and identity replacement cancel queued work and retry timers.
- **Mailbox recovery:** deposits still fall back until one store accepts. Reads
  drain every independent store with concurrency three. HTTPS and DHT can be
  alternatives for the same store only when explicitly mapped. A successful
  empty response is local to that store. Requests have a 15s timeout and a 30s
  network-operation deadline per transport; drains allow at most eight pages,
  100 entries per page and 256 KiB per decoded page.
- **Invitation application:** concurrent duplicate deliveries share application;
  distinct versions for a conversation are serialized. Successful normalized
  payload fingerprints use the existing persisted `appliedControls` field so
  lost acknowledgements and restarts do not repeat successful application.
  Changed names, membership, core keys or invite IDs remain distinct. Each
  source store acknowledges only applied or permanently rejected entries.
  Transient application failures remain available for retry. The validated
  `bootstrapReply` flag survives normalization, preventing reciprocal reply loops.
- **Global expiration:** startup builds an in-memory recipient index. Maintenance
  rotates through at most 16 recipients per second, with another bounded pass
  when a deposit meets global capacity. Expired mail is reclaimed without its
  recipient returning. Counts change only after successful writes/deletes.
  Corrupt/unreadable storage fails closed, and cleanup errors retain capacity
  and accepted files. Shutdown clears the maintenance timer.

Phase 1 authorization, normalized records, conversation key derivation,
replication cursor ordering and media authorization remain in force.

## API and stored-data compatibility

The durable send result retains its shape:

| Field | Meaning |
| --- | --- |
| `appended: true` | The local Hypercore append succeeded. This is the durability evidence. |
| `sent` | A live socket write was attempted successfully; it does not prove recipient delivery. |
| `relay: 'pending'` | Advisory notification work was scheduled independently; its eventual outcome is not represented in this response. It can fail, be dropped at the queue bound, or be cancelled. |
| `relay: 'not_requested'` | A mirror exists but this record is not eligible for a notification. |
| `relay: 'unavailable'` | No mirror collaborator is available. |

`request_acknowledged` is no longer returned by the durable send. A doorbell
acknowledgement does not prove block replication or recipient delivery. New
messages normally stay `queued` until authenticated delivery/read receipts
arrive. Existing `sent`, `delivered` and `read` evidence is preserved; notification
outcomes never update delivery status. Notification failure remains an advisory
`push.notification_failed` event.

Kotlin and Swift already parse the returned message and consume status events;
they do not interpret the `durability` object. Their return types and IPC fields
are unchanged, and API comments now describe local acceptance accurately.

Existing mailbox JSON/encryption/authentication and pagination formats remain
compatible. There is no general storage migration. Invite fingerprints occupy
the existing bounded control-deduplication history. Different-identity cleanup
intentionally deletes the previous account's local feeds and history; it does
not affect same-seed recovery.

Optional worklet argument `--invite-mailbox-peer-key=<configured-key>` explicitly
associates `--invite-mailbox-url` with that exact configured blind-peer key.
Without the mapping, the URL and each distinct configured peer key are probed
independently; duplicate payloads are still deduplicated. Embedders can also
supply `P2PManager({ mailboxStores: [{ url, key, address }] })` to express stores
with their alternative transports. No host configuration or pins were changed.

## Verification

- Baseline `npm test`: **348 passed**.
- Final `npm test`: **367 passed**, including 19 new recovery regressions.
- `npm run test:identity`: Bare golden vectors and mnemonic checks passed.
- `npm run build:android`: passed; regenerated the tracked Android bundle.
- `npm run build`: passed; rebuilt the ignored iOS bundle and artifact manifest.
- Parsed both bundles with `bare-bundle`: every packed core file matches its
  source. Android's changed payloads are the twelve edited core files and two
  new lifecycle/notification helpers; no dependency payload changed or vanished.
- `git diff --check`: passed.

Regression coverage uses real Corestore, identity and chat storage with simulated
network I/O. It exercises production IPC/peer/media handlers and separately runs
`core/index.js`'s production worklet wiring through mirror initialization races,
identity replacement, invitations, messages and receipts. Tests also cover
notification concurrency/timeouts/cancellation, empty-primary fallback recovery,
transient application failure, persisted duplicate suppression, inactive-recipient
expiration, restart accounting, acknowledgement failure and maintenance shutdown.

## Remaining delivery risks and launch checks

- Doorbells remain best-effort. Queue overflow, timeout or suspension can discard
  a wakeup attempt; no persistent notification-job queue was introduced. The
  message remains in Hypercore for direct delivery/replication, but receipt can
  wait until the recipient reconnects or polls. Relay acknowledgement never
  proves remote durable storage.
- Bounded mailbox maintenance can conservatively reject a deposit until a later
  pass reclaims expired capacity in a large store. Transient disk/application
  failures deliberately defer acknowledgement. A crash between application and
  recording its fingerprint can replay the idempotent handler; conversation and
  message storage still deduplicate visible records.
- Replacing an identity still spans multiple files rather than one atomic
  transaction. Destructive account changes require successful local storage;
  crash-atomic account switching/storage migration is outside this phase.
- Before launch, validate Android/iOS background/resume, process death and account
  switching on devices, and the real primary/fallback mailbox deployment under
  outages. Verify any HTTPS-to-peer mapping against actual independent stores.
  These device and deployed-server checks have **not** been performed here.
- The server expiration fix must eventually reach running mailbox services, and
  hosts must eventually consume the updated SDK/bundle. Nothing was deployed,
  published or pushed in this task.

**Android's separate consumed checkout at `../zappMessaging` has not been
updated.** The Android app, BareKit reference checkout and dependency pins were
not modified. All implementation changes are in this public SDK checkout.
