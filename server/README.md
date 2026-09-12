# Zapp blind peer: mirror, connection relay, invite mailbox, media retention

This is the only authored copy of Zapp's blind-peer server code. The Android
repository vendors a generated bundle of it for its own deployment; regenerate
that bundle rather than editing it, or the two will drift.

Stock `blind-peer-cli` mirrors encrypted Hypercores but does not serve the
`blind-relay` Protomux protocol, so pointing Hyperswarm's `relayThrough` option
at a stock blind peer can never relay a peer connection. It also has nowhere to
put a first invite, which is what the mailbox is for. Image bytes ride the
mirror in per-conversation media cores; `media-retention.js` clears those a
week after their last new block, so the mirror holds images only long enough
to deliver them while message history stays until storage pressure evicts it.

```bash
cd server
npm install --omit=dev
BLIND_PEER_STORAGE=/opt/blind-peer/data \
BLIND_PEER_PORT=49737 \
BLIND_PEER_MAX_STORAGE=10gb \
npm start
```

Keep the existing storage directory to preserve the configured blind-peer public
key. Allow UDP and TCP 49737. All three services run under that one key.

## Environment

| Variable | Default | Purpose |
| --- | --- | --- |
| `BLIND_PEER_STORAGE` | `./blind-peer-data` | Corestore path; also the mailbox's parent |
| `BLIND_PEER_PORT` | `49737` | HyperDHT UDP/TCP port |
| `BLIND_PEER_MAX_STORAGE` | `10gb` | Mirror storage ceiling |
| `BLIND_PEER_TRUSTED_KEYS` | none | Comma-separated z32 keys allowed to announce |
| `INVITE_MAILBOX_DIR` | `$BLIND_PEER_STORAGE/invite-mailbox` | One JSON file per recipient |
| `INVITE_MAILBOX_HTTP_PORT` | `49739` | Mailbox HTTP listener |
| `INVITE_MAILBOX_HTTP_HOST` | `127.0.0.1` | Keep on loopback; terminate TLS in front |
| `INVITE_MAILBOX_MAX_RPM` | `600` | Aggregate HTTP request ceiling |
| `INVITE_MAILBOX_MAX_BYTES_PER_RECIPIENT` | `262144` | Byte budget per mailbox |
| `MEDIA_RETENTION_MAX_AGE_MS` | 7 days | Clear a media core this long after its last new block |
| `MEDIA_RETENTION_MIN_INTERVAL_MS` | 1 hour | Least time between two retention passes |

## Publishing the mailbox over HTTPS

Clients that can reach the DHT use the Protomux mailbox on the existing Noise
connection and need nothing here. The HTTPS listener exists for networks that
drop UDP outright, and it must stay on loopback with a TLS terminator in front.

**The reverse proxy has to strip the path prefix.** The client posts to
`<INVITE_MAILBOX_URL>/put`, `/list` and `/ack`, and the server matches the
request path against exactly those three names. Forwarding `/zapp-invite/put`
unstripped makes every request 404, the client silently falls back to the DHT
path, and firewalled peers fail exactly as they did before this server existed.

```caddyfile
handle_path /zapp-invite/* {
    reverse_proxy 127.0.0.1:49739
}
```

`handle_path` strips the matched prefix; plain `handle` does not. Verify with:

```bash
curl -sS -o /dev/null -w '%{http_code}\n' \
  -X POST -H 'content-type: application/json' -d '{}' \
  https://<host>/zapp-invite/put
```

400 means the route works and the empty body was rejected on its merits. 404
means the prefix is still being forwarded.

## What the server can and cannot see

Envelope bodies are encrypted to the recipient and signed by the sender; the
server verifies the signature and the recipient binding, then stores opaque
ciphertext. It does learn which identity is writing to which identity, which is
the routing metadata the mailbox cannot avoid.

`put` is open by necessity: a sender has no way to authenticate to a mailbox
belonging to someone who has never been online. Deposits are bounded per
recipient, per sender, in bytes and in total. Envelopes are capped at 16 KB,
which has to hold a group invite carrying every participant's key, so the
per-recipient budget bounds bytes rather than entries: one large invite costs
what several small ones would. Listings are paged so a flooded mailbox stays
drainable rather than becoming permanently unreadable.

`list` and `ack` are restricted to the mailbox owner: over Protomux by the Noise
connection key, over HTTPS by an Ed25519 signature with a nonce and a
five-minute freshness window. An acknowledgement is a delete, so the client
applies an invite before sending one.

The relay forwards opaque encrypted UDX traffic for anyone who asks. Apply host
bandwidth limits and monitor usage before treating this as production
infrastructure.
