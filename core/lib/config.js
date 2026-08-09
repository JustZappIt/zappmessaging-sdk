/**
 * Config - Centralized runtime configuration
 *
 * All tunable constants in one place. Values can be overridden via
 * Bare.argv flags (--key=value) parsed at module load time.
 *
 * CLI arg format: --blind-peer-keys=key1,key2  --bootstrap-nodes=host:port,host:port
 */

const _argv = (typeof Bare !== 'undefined' ? Bare.argv : [])

function getArg (name) {
  const prefix = '--' + name + '='
  const arg = _argv.find(a => a.startsWith(prefix))
  return arg ? arg.substring(prefix.length) : null
}

function getArgInt (name, fallback) {
  const raw = getArg(name)
  if (raw === null) return fallback
  const n = parseInt(raw, 10)
  return Number.isNaN(n) ? fallback : n
}

// --- Blind peer configuration ---
const blindPeerKeysRaw = getArg('blind-peer-keys')
const BLIND_PEER_KEYS = blindPeerKeysRaw
  ? blindPeerKeysRaw.split(',').map(k => k.trim()).filter(Boolean)
  : []   // Empty by default — must be injected for production

// --- Blind peer dial address (optional) ---
// When set, the blind-peer public key is dialed with this host:port embedded
// (via hyperdht-address) so HyperDHT pre-connects straight to the VPS instead
// of running a full findPeer DHT walk on every cold start (~1.9s measured).
// Falls back to the normal walk automatically if the embedded address is stale.
// Format: --blind-peer-address=host:port  (port defaults to 49737)
const blindPeerAddrRaw = getArg('blind-peer-address')
const BLIND_PEER_ADDRESS = (() => {
  if (!blindPeerAddrRaw) return null
  const trimmed = blindPeerAddrRaw.trim()
  const idx = trimmed.lastIndexOf(':')
  const host = idx === -1 ? trimmed : trimmed.slice(0, idx)
  const port = idx === -1 ? 49737 : parseInt(trimmed.slice(idx + 1) || '49737', 10)
  return host ? { host, port: Number.isNaN(port) ? 49737 : port } : null
})()

// --- Invite mailbox HTTPS endpoint (optional) ---
// UDP/HyperDHT is not universally usable on mobile networks. This endpoint
// carries the same end-to-end encrypted invite envelopes over ordinary HTTPS,
// with identity-signed list/ack requests. It is a control-plane fallback only;
// message blocks remain in Hypercore/blind-peering.
const INVITE_MAILBOX_URL = getArg('invite-mailbox-url') || null

// --- Identity file key (secret; injected by the native layer) ---
// 64 hex chars = 32 bytes. When present, identity.json (the BIP-39 wallet
// entropy) is encrypted at rest with this key — see identity.js. The native
// side keeps the key wrapped by the platform keystore (Android Keystore /
// iOS Keychain) and hands it to the in-process worklet via argv. Never log
// this value or any argv array that carries it.
const identityFileKeyRaw = getArg('identity-file-key')
const IDENTITY_FILE_KEY = (() => {
  if (!identityFileKeyRaw) return null
  const trimmed = identityFileKeyRaw.trim().toLowerCase()
  return /^[0-9a-f]{64}$/.test(trimmed) ? trimmed : null
})()

// --- Local gateway IP (passed by Android when on Wi-Fi/hotspot) ---
// Used to seed phone2's DHT routing table from phone1's node on the local
// LAN when phone2 is behind double-NAT (e.g. on phone1's tethered hotspot).
// The gateway is typically the AP's LAN IP (e.g. 10.215.90.1 for Android
// hotspot). Phone1's DHT listens on all interfaces including the hotspot
// interface, so phone2 can reach it at gateway:49737 to bootstrap DHT.
const LOCAL_GATEWAY_IP = getArg('local-gateway') || null

// --- Bootstrap node configuration ---
const DEFAULT_BOOTSTRAP_NODES = [
  { host: 'node1.hyperdht.org', port: 49737 },
  { host: 'node2.hyperdht.org', port: 49737 },
  { host: 'node3.hyperdht.org', port: 49737 }
]

const customBootstrapRaw = getArg('bootstrap-nodes')
const CUSTOM_BOOTSTRAP_NODES = customBootstrapRaw
  ? customBootstrapRaw.split(',').map(entry => {
    const [host, portStr] = entry.trim().split(':')
    return { host, port: parseInt(portStr || '49737', 10) }
  }).filter(n => n.host)
  : []

// --- Heartbeat / P2P tuning ---
const HEARTBEAT_INTERVAL_MS = getArgInt('heartbeat-interval', 45000)
const HEARTBEAT_TIMEOUT_MS = getArgInt('heartbeat-timeout', 90000)
const MAX_DISCOVERED_NODES = getArgInt('max-discovered-nodes', 10)

// Cap simultaneous saved-conversation joins during startup. Each join can open
// a Hypercore and start DHT discovery, so an unbounded fan-out competes with the
// blind-peer connection on accounts with a large conversation history.
const STARTUP_JOIN_CONCURRENCY = Math.max(1, getArgInt('startup-join-concurrency', 4))

// Number of known-good DHT nodes to persist across restarts so a cold start can
// seed its routing table instead of paying a full bootstrap every launch.
const DHT_NODE_CACHE_MAX = getArgInt('dht-node-cache-max', 20)

// --- Message limits ---
const MAX_MESSAGES_PER_CONVERSATION = getArgInt('max-messages-per-conv', 5000)

// --- Buffer limits ---
const IPC_MAX_RECV_BUF_SIZE = getArgInt('ipc-max-buf', 1048576)       // 1 MB
const SOCKET_LEGACY_BUF_LIMIT = getArgInt('socket-legacy-buf', 10 * 1024 * 1024)  // 10 MB
const SOCKET_FRAME_MAX_LEN = getArgInt('socket-frame-max', 50 * 1024 * 1024)      // 50 MB
const MEDIA_CHUNK_SIZE = getArgInt('media-chunk-size', 64 * 1024)      // 64 KB
const MEDIA_MAX_BYTES = getArgInt('media-max-bytes', 32 * 1024 * 1024) // 32 MB

// --- DHT health ---
const DHT_CHECK_INTERVAL_MS = getArgInt('dht-check-interval', 120000)  // 2 min
const DHT_CHECK_TIMEOUT_MS = getArgInt('dht-check-timeout', 8000)      // 8 s

// --- Rate limiting ---
const PEER_RATE_LIMIT_PER_MIN = getArgInt('peer-rate-limit', 120)
const PEER_RATE_LIMIT_WINDOW_MS = 60000

// --- Logging ---
const LOG_LEVEL = getArg('log-level') || 'info'  // 'debug' | 'info' | 'warn' | 'error' | 'off'

module.exports = {
  BLIND_PEER_KEYS,
  BLIND_PEER_ADDRESS,
  INVITE_MAILBOX_URL,
  IDENTITY_FILE_KEY,
  LOCAL_GATEWAY_IP,
  DEFAULT_BOOTSTRAP_NODES,
  CUSTOM_BOOTSTRAP_NODES,
  HEARTBEAT_INTERVAL_MS,
  HEARTBEAT_TIMEOUT_MS,
  MAX_DISCOVERED_NODES,
  STARTUP_JOIN_CONCURRENCY,
  DHT_NODE_CACHE_MAX,
  MAX_MESSAGES_PER_CONVERSATION,
  IPC_MAX_RECV_BUF_SIZE,
  SOCKET_LEGACY_BUF_LIMIT,
  SOCKET_FRAME_MAX_LEN,
  MEDIA_CHUNK_SIZE,
  MEDIA_MAX_BYTES,
  DHT_CHECK_INTERVAL_MS,
  DHT_CHECK_TIMEOUT_MS,
  PEER_RATE_LIMIT_PER_MIN,
  PEER_RATE_LIMIT_WINDOW_MS,
  LOG_LEVEL
}
