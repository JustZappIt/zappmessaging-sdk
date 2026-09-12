# Messaging audit: batch one

Implemented in the public `zappmessaging-sdk` checkout. The Android app's
separately consumed `../zappMessaging` checkout and `.zapp-deps` are unchanged.

## Authorization and routing

Existing group invites must come from the stored owner, who must still be a
stored member. Claimed ownership cannot replace established ownership. Owner
updates merge additions into stored membership before joining the topic; repeat
invites cannot expand a separate runtime roster. Group text, controls, pending
messages, core announcements and outgoing media check stored authorization.
A group runtime cache alone grants no access.

Media requests require an active, authorized conversation with a persisted,
locally authorized reference to the hash. Merely sending a message containing a
known hash cannot grant the sender access to unrelated cached bytes. New local
outgoing shares grant their conversation access; incoming references gain access
only after full hash-verified bytes arrive from authorized members of the fixed
requested conversation. A pending transfer cannot change conversation when a
second peer references the same hash. Other conversations receive no grant from
that transfer. All authorized conversations are considered after restart, and
chunk writes recheck authorization after membership changes.

Group `__core_keys` announcements use the existing group topic as the key in
`cores`. Outgoing group records carry `groupTopicHex` and use the topic in the
wire `conversationId`. Neither change discloses the secret `groupId`. Local
conversation IDs and core storage names do not change. Topics resolve from
persisted conversations even before swarm joins finish.

Both live frames and replicated records use `peer-record.js` and the awaitable
`peer-receiver.js`. Replication's registered core determines its conversation
and sender. An explicit mismatched group topic is rejected. Group invites,
member additions, leaves, deletions and renames run the same IPC handlers on
both paths. Additions/deletions are owner-only; leaving removes the authenticated
sender; rename retains the existing member-authorized policy. Receipts are
conversation-scoped. Existing group read receipts remain unsupported; delivery
receipts retain their existing cumulative behavior.

Presence and core-key exchange are connection setup controls, not replayable
history operations. Replicated copies are ignored. Other unknown record types
are also ignored, never converted into chat messages.

## Validation and persistence

Incoming chat records require an object, a nonempty message ID, and correctly
typed fields. IDs use ASCII letters, digits, underscores and hyphens, at most
128 bytes. Identity keys and media hashes are 32-byte hex strings (normalized
to lowercase). Content and reply content are limited to 256 KiB each, names to
1 KiB, MIME types to 256 bytes, thumbnails to 1 MiB, and the complete JSON
record to 1.5 MiB. Participant lists and core-key announcements allow at most
256 entries. Existing transport frame limits still apply.

MIME types remain extensible; payment/application payloads remain opaque strings.
Numeric media metadata keeps the existing common-platform integer clamping,
and timestamps retain the existing skew normalization. Incoming numeric fields
must be finite numbers. The authenticated sender replaces any claimed sender.
Remote status, `isFromMe`, local media paths, transfer state and `mediaAuthorized`
sharing provenance are ignored.
Native message events use the normalized persisted row. No native API change
is required.

ChatStore validates before generating an ID or writing anything. Peer records
without IDs are permanently invalid rather than generating a new row on every
retry. `INVALID_PEER_RECORD` and explicit Hypercore decode failures can be
skipped. Other errors remain retryable. A replication batch commits its cursor
only after messages/controls and its coalesced durable receipt complete. Cursor
write failures roll back the in-memory cursor. A duplicate message retries its
preview/index write, covering failures after the message file committed.

Live delivery uses disk deduplication, so a failed live write cannot poison an
in-memory dedup cache. New group controls have IDs; the last 5,000 authenticated
control ID fingerprints per conversation are persisted in `appliedControls` and
excluded from native conversation responses. Unauthorized controls never write
this journal. This bounds duplicate suppression; very old controls outside that
window can be reapplied. Control application and its fingerprint use separate
writes: interruption between them can replay an idempotent operation or repeat
a UI event, but must not create a chat row. Writer revocation from a control sink
cancels the drain without awaiting itself.

Legacy controls without IDs use state-idempotent handlers: repeated renames to
the current name or already-applied additions do not repeat their UI events.
A legitimate A → B → A rename or owner re-add after a member leaves remains
possible. Without an operation ID, a delayed old control cannot be distinguished
from a deliberate repeated operation after intervening changes; legacy controls
therefore do not claim full cross-transport replay protection.

## Existing data and client compatibility

History reads validate each row before deduplication and preview use. Malformed
rows are removed from the active JSON array; valid rows are preserved, deduplicated
and sorted. Existing media integers, thumbnails and timestamps are normalized.
Before the first repair, the original array is saved alongside it as
`<conversation>.json.pre-validation.bak`. Backup/repair write failures prevent
message ingestion from acknowledging the record. A wholly unreadable or non-array
history file is left intact and ingestion remains retryable; it requires recovery
from backup or explicit operator repair. Backups contain private chat data and
inherit the app sandbox's storage protection.

Already-persisted blank rows produced by old controls have lost their original
`type`, so they cannot safely be distinguished from legitimate empty messages.
They are retained. Already-advanced cursors are not reset automatically: replaying
old membership operations could resurrect departed members. Previously accepted
spoofed metadata or ownership that is indistinguishable from legitimate stored
data also requires explicit review; this upgrade does not guess a new owner.

No conversation IDs, encryption keys, dependency versions or pins are migrated.
Existing stored messages with valid fields remain readable. Current clients'
message IDs and supported MIME content interoperate within the limits above.
Legacy replicated records can use sender-local conversation IDs because their
registered core supplies the local scope. Legacy live records with a known local
ID remain accepted after authorization. Unknown explicit receipt routes fail
closed instead of falling back into a DM.

Existing media rows lack the new local `mediaAuthorized` provenance and do not
automatically authorize serving, even when marked outgoing or downloaded: older
peer metadata could forge those flags. Existing cached bytes remain viewable
locally. A new explicit outgoing share or a complete verified incoming download
grants access in its conversation. Opening incoming media history attempts
verification downloads even for cached hashes without provenance. Reauthorization
requires an available peer with the bytes; historical peers that cannot serve
them leave those references unavailable for retransmission. This is a deliberate
fail-closed migration, without inferring security authority from legacy flags.

Old peers do not understand topic-keyed core announcements or the corrected
receipt routing. Full group key/receipt interoperability requires both peers to
upgrade. Existing live group chat/media and owner invites retain their formats.
Old peers also retain their original replication-control bug until upgraded.

## Verification and release limits

The pre-change suite passed 322 tests. The updated suite adds production-store
and handler regressions in `tests/messaging-integrity.test.js`; transport sockets
and remote core reads are deterministic test doubles. Tests cover forged repeat
invites, authorized owner updates, media access/revocation/multiple references,
invalid records followed by valid ones, message/index/cursor failures and retries,
poison repair, differing local group IDs, startup topic resolution, replicated
controls/receipts, and duplicate live/replicated delivery.

Run `npm test` and `npm run build:android`. Android's generated bundle is committed;
iOS's bundle remains host-generated and ignored, per CONTRIBUTING.md.

This is SDK verification, not verification of the Android app's consumed checkout.
No device, live DHT/blind-peer deployment, or mixed-version mobile rollout is
certified by these tests. Updating the consumed checkout and validating mobile
integration remain release prerequisites.

This batch does not provide cryptographic revocation of secrets/history already
shared with a former member, nor a signed/causally ordered membership log. A member
who already possesses the group secret and writer keys may still decrypt available
replicated history; a fresh membership/key-distribution protocol is needed for
strong post-departure secrecy. Member-authored historical records received only
after that member leaves are rejected under current membership. These limitations
must be assessed before claiming full group revocation security at launch.
Notification latency, mailbox failover/expiration and identity restart lifecycle
were outside this batch.
