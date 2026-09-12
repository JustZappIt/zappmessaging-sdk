# Phase 3: media performance and recovery

This change includes phase 3 media improvements and the prerequisite phase 2
delivery/recovery implementation, which was previously local and uncommitted.
The initial SDK suite passed 367 tests.
Phase 1 authorization, normalized records, content hashes, durable appends,
replication cursors and phase 2 identity/task barriers remain in place.
The separate Android-consumed `zappMessaging` checkout and dependency pins
were not changed. Android UI changes require integrating this SDK first.

## Confirmed causes and measurements

The duplicate-upload reproduction remains valid in the phase 2 starting tree:
proactive send and receiver request used separate schedulers, and deduplication
was per socket rather than authenticated recipient. An actual 1 MiB synthetic
buffer through those production send handlers yielded **2,097,152 payload bytes
and two file reads**. The corrected handlers yield **1,048,576 bytes and one read**,
including when the recipient has multiple sockets.

Additional confirmed code-path problems:

- Native media send called `connection.connect` before storing/sending media.
  That operation awaited DHT discovery and an invitation/mailbox attempt.
  Phase 2 correctly detached notification delivery *after* append, but this
  separate preflight could still block before that boundary. Media preflight now
  prepares the local core/topic without waiting for discovery, and schedules
  bootstrap invitations through bounded, lifecycle-scoped background work.
- `writeChunk` discarded the raw stream's backpressure result. The old sender
  buffered entire files and could emit success after failed writes. Uploads now
  stop after `write(false)`, resume on `drain`, and reject closure, write errors,
  cancellation and a 60-second drain stall. The blocked frame is not resent.
- Each chunk's authorization guard reread/normalized entire conversation files.
  A stat-validated history cache removes that repeated work without removing
  membership checks, atomic history writes or cursor persistence.
- Android prepared images on `Dispatchers.IO` already. The visible outgoing row
  only appeared after SDK acceptance; that delay looked like an unresponsive
  send. It now appears immediately, including during preparation. Remote preview
  decoding and local-file inspection also moved off the Compose thread.
- Download retries had no deadline until a first chunk arrived. A request that
  got no bytes could remain stuck. Metadata and following chunks also raced
  asynchronous metadata persistence within a single socket read.

Synthetic measurements on this development Mac (Node, local filesystem; no
phone codec, radio, real peer network, relay or mobile filesystem):

| Scenario | Before/control | After |
| --- | ---: | ---: |
| 1 MiB proactive send + simultaneous request | 2 MiB payload / 2 reads | 1 MiB payload / 1 read |
| 160 media authorization checks against 2,000 rows | 293.5 ms | 12.9 ms |
| Append 200 catch-up records to 2,000 rows | 991.3 ms | 622.4 ms |
| Rows retained after that catch-up | 2,200 | 2,200 |
| Bare streamx, 1 MiB upload, 1 ms synthetic write completion | previously ignored backpressure | max 65,581 outstanding application bytes |

History figures are medians of three paired runs, alternating cache-enabled and
cache-disabled production handlers in the same build. Every append still writes
history and the conversation index. The benchmark does not include Hypercore
cursor writes or real offline replication. Initial single-run measurements were
316/17 ms for authorization and 1,464/862 ms for catch-up; a run contending with
Gradle reached 1,681 ms with the cache. This variability is why paired medians,
not those isolated timings, are the comparison above.

The unthrottled mock upload loop took 4.78 ms before and approximately 4–6 ms
after. That loop is not a network-speed measurement; its useful evidence is the
halved payload and read counts. No compression quality or resolution settings
were changed. The physical Wi-Fi samples below supplement these synthetic results; cellular,
relay and GPU presentation timing remain unverified.

Reproduce from the public SDK root:

```sh
npm test
node --require ./core/tests/helpers/node-compat.js scripts/media-transfer-benchmark.js
node --require ./core/tests/helpers/node-compat.js scripts/media-history-benchmark.js
bare test/media-backpressure.js
```

## Transfer and persistence behavior

- Proactive uploads and requests enter one scheduler keyed by authenticated Noise
  recipient and media hash. Active requests share the existing promise. There is
  no fixed-period suppression of legitimate requests after failure/completion.
  Physical testing caught a small-file request arriving after the proactive job
  had already completed. Successful proactive jobs now retain at most 512 bounded
  single-use first-request credits, scoped to authenticated recipient/hash and
  the still-open sending socket. An optional request attempt field lets the
  initial request consume that work; explicit retries bypass it. Close, cancel
  and identity changes erase credits. Legacy requests remain fully serviceable.
  A later unrelated first request can consume an unused credit, but its timed
  explicit retry recovers; no permanent or time-window suppression is used.
- Two uploads may run, at most one per recipient, with 64 queued jobs. Queued jobs
  hold metadata only. Active recipients sharing a hash share the file read.
  At the configured 32 MiB file ceiling, at most two distinct full upload buffers
  are held; each backpressured socket gets at most one additional 64 KiB frame.
  Receive chunks retain their existing 32 MiB aggregate budget and four-transfer
  limit; final concatenation temporarily needs another file-sized buffer.
- A slow recipient does not occupy both upload slots. Queue saturation and a
  fully stalled set of active recipients can still make other transfers wait;
  each stalled write has a deadline and queue rejection produces failure state.
- Receiver requests have a 10-second first-byte/inactivity deadline, 1-second
  retry spacing, 12 attempts, four active requests and 512 retained conversation/hash scopes.
  Only one conversation at a time may use a hash's reassembler. A scope with
  no reachable peer releases its active slot while retaining its retry timer;
  another conversation can request the same image immediately. Completion
  removes only the verified scope, leaving other conversations to prove their
  own possession. Pending limits count scopes, not just distinct hashes.
  A fresh author connection immediately wakes a request that previously found
  no peer, rather than waiting out its first-byte deadline.
  Subsequent attempts rotate reachable candidate sockets instead of repeatedly
  selecting a reachable peer that may no longer hold the file. Exhaustion is
  visible. Manual retry or a fresh author connection can restart failed work.
- Download retry cancellation cannot cancel an upload of the same hash.
  Disconnects reject queued/active socket work; shutdown/account replacement
  cancel request timers and await cancelled uploads before replacing identity.
- Framing pauses the underlying readable stream while asynchronous metadata
  handling completes. Following chunks in the same read are handled only after
  the authorized expected transfer is registered. Retained read data is capped
  at 2 MiB; excess closes the socket instead of growing an unbounded queue.
- Completed downloads still pass the content hash before acceptance. Verified
  bytes atomically replace stale/partial files. Authorization and the local path
  are persisted together for the requesting conversation, before UI completion.
  Other conversations do not gain authorization merely by naming the same hash.
- The history cache holds at most four files / 8 MiB of serialized history
  (object overhead is additional). It caches only successful reads/writes,
  checks inode/size/mtime/ctime, returns copies and clears on identity replacement.
  External edits and failed mutations cannot silently authorize stale rows.
- Live rate limiting now throttles the serialized receive handler rather than
  dropping excess messages. Existing replicated-block cursor tests still pass.
  The pre-existing 5,000-message per-conversation retention cap remains.

The installed transport is `@hyperswarm/secret-stream` 6.9.1 over `streamx` 2.25.0.
Its source and the [streamx upstream API](https://github.com/mafintosh/streamx)
were checked before using `write(false)`/`drain`; the
[Hyperswarm API](https://github.com/holepunchto/hyperswarm) establishes the Noise
connection context. Tests exercise streamx under both Node and Bare.

## Native APIs and honest UI evidence

Both Kotlin and Swift support `clientMessageId` on media send, `retryMedia` and
`cancelMedia`. Stable client IDs are persisted as the SDK message ID, so an
ambiguous response can be retried without creating duplicate visible rows.
Retries also reconcile against the durable Hypercore log, under a per-writer
append lock. An existing committed message is reused without another block or
notification; a locally saved row whose append failed is appended before retry
reports success or sends bytes. This lookup survives reopening the store and
ambiguous append results. Retry lookup scans backward through the local log
(linear in history in the worst case); ordinary first sends do not scan it.
Offline retry queues the durable metadata and emits `waiting_peer`; the receiver
requests bytes on reconnect. An outgoing socket write never advances delivery
status to delivered/read.

`media.transfer_state` carries `mediaId`, `direction` and `state`. Native state
snapshots use **`upload:<hash>` / `download:<hash>`** keys; receiving a copy of a
file cannot falsely complete an upload of that same hash. The bounded snapshot
survives events that arrive before the native send response. Upload state is
aggregated across pending recipients so a fast peer cannot hide a slow/failing
peer. `queued_socket` means local socket acceptance, **not remote verification**.
`media.transfer_complete` with a local path means a verified, persisted download.
There is no new remote media-verification acknowledgement protocol in this phase;
outgoing images therefore retain explicit unconfirmed-receipt wording.

Android room and support chat share media preparation/retry handling, with two
concurrent preparations and eight retained pending sources. Images appear before
compression starts; preparing, queued, sending, waiting-for-peer and failed/retry
states are localized in English and Spanish. Support chat now renders media
rather than an empty caption bubble. Temporary files are cleaned on completion
or failure, and preparation/decoding/file operations stay off the main thread.
Receipt evidence survives row reconciliation. Receiving all chunks no longer
marks a download final before validation/persistence.

## Opt-in diagnostics and device measurements

Diagnostics have a dedicated allowlisted schema: temporary random correlation
ID, stage, duration/elapsed milliseconds and byte count. They contain no content,
thumbnail/image bytes, message IDs, media hashes, peer/wallet identifiers or paths.
No measurements are retained in a global diagnostics history. They are separate
from general SDK debug logging.

For an Android build containing these SDK/app changes:

```sh
adb -s DEVICE_SERIAL shell setprop log.tag.ZappMediaTiming DEBUG
# Restart the app/worklet normally after enabling the property. Do not clear wallet data.
adb -s DEVICE_SERIAL logcat -v monotonic -s ZappMediaTiming:D '*:S'
# Disable after measuring; restart the app/worklet again:
adb -s DEVICE_SERIAL shell setprop log.tag.ZappMediaTiming INFO
```

The Android flag adds `--media-diagnostics` to the worklet. iOS hosts enable
`ZappMessagingConfig(mediaDiagnostics: true)` and collect the typed
`mediaDiagnostics` publisher. Stage IDs are temporary and local to an operation
and device; they are not a cross-device identity or message fingerprint.

Stages cover selection/preparation (app), connection preparation, media hashing/
storage, local+Hypercore append, file read, first/last byte queued, receiver hash
validation, receiver persistence, UI completion event and image-loader
availability. Download queue/request/no-peer/first-chunk/retry timings separate
receiver scheduling from waiting on a peer. Correlate isolated sends by event order; do not subtract wall
clocks across devices. The receiver UI metric measures image-loader availability
from UI model creation, not a GPU presentation timestamp.

For a real baseline, keep the currently installed builds first. Record elapsed
selection-to-preview and sender/receiver visible availability with a stopwatch;
those builds do not contain the new numeric instrumentation. Then use builds
known to contain this public SDK and repeat the same image. Test both directions,
Android/Android and Android/iOS where available, on Wi-Fi, cellular and a known
relayed path; repeat with the recipient offline/reconnecting. Record file byte
size, timing stages and connection category only. Verify the relay path rather
than inferring relaying solely from cellular use.

Two physical Android phones subsequently connected: Android 16 and Android 15,
both on Wi-Fi. A matching mainnet FOSS debug APK built with the temporary public
SDK override was installed and launched on both without clearing app data.
The second phone had no Zapp installation, so there is no paired pre-fix device
baseline. Numeric-only capture recorded image transfers in both directions, with receiver
hash validation, persistence and UI availability. The user reported the result
felt substantially better. No reachable iOS device is currently available. Both active transports remained
Wi-Fi at the final check. Capture was stopped and both diagnostic tag properties
restored to INFO; the worklet startup opt-in is off on its next normal restart.

Initial physical samples (first phase-3 device build, before the late-request fix):

| Payload bytes | Sender Android | Preparation | Media connection preflight | Durable append | Local acceptance from selection | First-to-last socket write | Receiver hash validation / persistence |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 73,179 | 16 | 58 ms | 2 ms | 4 ms | 92 ms | 2 ms | <1 / 2 ms |
| 1,173,431 | 15 | 66 ms | 1 ms | 15 ms | 231 ms | 210 ms | 5 / 5 ms |
| 2,602,846 | 15 | 37 ms | 3 ms | 14 ms | 174 ms | 570 ms | 11 / 5 ms |

The larger samples waited approximately 3–4 seconds between local append and
first write while peer/network readiness was established. This is a remaining
connectivity delay, separate from preparation and append; these timing-only logs
do not establish the exact discovery/relay cause. Receiver image-loader samples
were 80–242 ms from UI model creation, not from selection or network arrival.
Device clocks were not subtracted. These are individual samples, not a controlled
before/after device benchmark; Wi-Fi path relaying was not established.

The 73 KB sample also exposed two complete writes about 57 ms apart: its request
arrived after proactive completion. This led to the additional attempt-aware
single-use coordination above and three additional regressions. The final matching
APK was installed and launched on both phones. A new 269,709-byte image then
produced exactly one upload in the captured sender trace: 127 ms preparation,
1 ms connection preflight, 4 ms append, 175 ms local acceptance, 4 ms from first
to last socket write, and receiver validation/persistence of 1/3 ms. This confirms
one-upload behavior for that physical sample; it is not a full traffic capture.

A separate final-build 2,797,132-byte image was locally accepted in 167 ms but
waited about 11 seconds from append to first write; actual socket writing took
418 ms and receiver validation/persistence took 15/10 ms. The initial opt-in trace does
not disambiguate receiver retry/queue timing from peer readiness during that
wait. Follow-up code inspection found and fixed a no-peer request waiting out
its deadline even after a fresh author connection. A regression verifies that
wake-up behavior. The latest build adds download queue/request/first-chunk/retry
stages to isolate any remaining wait. These two additions postdate the physical
samples above. The latest build subsequently recorded recovery of an upload that
started around the receiver app update: 893,971 bytes were requested after
reconnection, verified and persisted. The receiver measured 0 ms queue wait and
62 ms request-to-first-chunk; the retried sender write took 56 ms, with receiver
validation/persistence of 2/3 ms. A second 73,179-byte file also recovered. This
was an incidental app-update interruption, not a controlled network-loss test.
No no-peer-wake event was captured, so that specific fix remains device-unmeasured.
The 11-second wait
must not be reported as conclusively solved or blamed on compression.

The physical test APK predates the PR rebase onto the newer native logging-privacy
fix. PR preparation preserves that fix and adds a runtime opt-in check to numeric
Android media logging. Core sources and worklet bytes are unchanged by the rebase.

Installed test APK SHA-256:
`692e61ea9d6b774e41b87b66fc0d75eec95968c46102b5fa20a54ef2fa10adf4`.
Its packaged worklet was byte-identical to the then-current public SDK Android bundle:
`855ef4a1652a491fdd26e1563a95c64fb39009bee2f797632dc0cf4201f58157`.

## Verification and integration status

- SDK baseline: 367 passing tests; initial PR: 385; review follow-up: **394 passing tests**.
- Review follow-up adds concurrent/sequential stable-ID retry, failed/ambiguous
  append recovery, store-reopen deduplication, per-conversation same-hash queue
  bounds/fairness, and actual worklet authorization/completion regressions.
  Both worklet bundles are rebuilt for the follow-up. The physical-device and
  Android app results below are from the earlier build, not a new device run.
- Production worklet regression covers metadata plus image chunks in one read,
  authorization, persistence and the resulting UI event.
- Other new regressions cover stalled discovery/invitation/notification, duplicate
  uploads, multiple sockets, backpressure, independent recipient progress,
  cancellation/identity change, no-first-chunk retries, cache invalidation and
  live catch-up above the rate limit. Existing corruption, authorization,
  mid-transfer timeout, durable-store and replication-cursor regressions pass.
- Bare identity vectors and the Bare streamx smoke test pass.
- `npm run build:android` and `npm run build` pass. Android bundle rebuilt;
  ignored iOS bundle and manifest rebuilt. Both bundles' 31 packed core JS files
  match their source files.
- Kotlin app compilation, 26 chat model/list tests, ktlint and detekt pass with
  JDK 17 selected and a **temporary Gradle init-script override to the public SDK**.
- The mainnet FOSS debug APK assembly also passes with that override and JDK 17.
- Ordinary app compilation against unchanged `../zappMessaging` cannot resolve
  the new state/retry/client-ID APIs. This is an integration prerequisite, not
  end-to-end verification of the consumed SDK.
- `checkProperties` fails under existing local signing/API property overrides.
  Inspected checked-in credential defaults remain blank; no local configuration
  was changed. Swift syntax parsing passes; a full iOS host/device build was not run.

Required Android integration steps:

1. Review and merge this public SDK change, including the phase 2 prerequisites
   and generated Android bundle. Use the resulting reviewed commit for integration;
   phase 1 security and the native logging-privacy changes from `main` are retained.
2. Import those reviewed SDK changes into the app's **existing** `zappMessaging`
   sibling, reconciling its feature work. Include core sources, Kotlin wrappers,
   media state enum, worklet startup flag and rebuilt Android bundle together.
   Do not simply reset that sibling or substitute only the bundle.
3. Check out the resulting reviewed consumed-SDK commit and deliberately update
   its `.zapp-deps` pin in the coordinated integration change. Keep the app's
   dependency path unchanged. None of these steps was performed in this phase.
4. Run the normal JDK-17 app compile/install and real-device matrix with the
   integrated checkout. Verify both installed endpoints' SDK/bundle provenance.

Remaining public-launch checks: SDK integration; the observed 3–11 second
pre-transfer waits (request/queue versus connectivity attribution); real Android/iOS sends and
background/resume/process-death/account-switch behavior; Wi-Fi/cellular/relay
validation; and the phase 2 deployment/recovery checks in its report. Media
cannot download without a reachable authorized peer that holds the bytes. Retry
budgets and queues are finite, partial downloads restart from byte zero, and the
sender has no remote media-verification acknowledgement. No VPS deployment,
server configuration, encrypted offline-media feature, dependency-pin change,
deployment or package publication was performed. PR preparation subsequently
created an isolated branch against current `main`; the original working checkouts
and the separately consumed SDK remain unchanged.
