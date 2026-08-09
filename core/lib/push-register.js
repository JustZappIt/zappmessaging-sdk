'use strict'

/**
 * push-register - client side of the blind-peer push-endpoint registration RPC.
 *
 * Opens a dedicated, short-lived DHT connection to the blind peer (authenticated
 * with our Ed25519 identity keyPair, so the server reads conn.remotePublicKey =
 * our identity = the `referrer` it stores against) and calls the
 * 'register-push-endpoint' method to register our UnifiedPush / Web-Push
 * subscription {endpoint, p256dh, auth}.
 *
 * The wire contract MUST stay byte-identical to the server's
 * blind-peer-push/push/register-channel.js:
 *   protocol = 'zapp-push-register', channel id = ASCII 'zp',
 *   method = 'register-push-endpoint', request = RegisterEncoding (3 compact
 *   strings: endpoint, p256dh, auth), response = c.none (empty reply = ack).
 *
 * A dedicated connection (not blind-mirror's replication connection) keeps
 * registration off the replication/suspend/backoff path and matches the server,
 * which attaches the register responder to every incoming connection.
 */

const ProtomuxRPC = require('protomux-rpc')
const c = require('compact-encoding')
const b4a = require('b4a')
const HypercoreId = require('hypercore-id-encoding')

const PUSH_PROTOCOL = 'zapp-push-register'
const PUSH_CHANNEL_ID = b4a.from('zp')
const METHOD = 'register-push-endpoint'
const DEFAULT_TIMEOUT_MS = 15000

const RegisterEncoding = {
  preencode (state, m) {
    c.string.preencode(state, m.endpoint || '')
    c.string.preencode(state, m.p256dh || '')
    c.string.preencode(state, m.auth || '')
  },
  encode (state, m) {
    c.string.encode(state, m.endpoint || '')
    c.string.encode(state, m.p256dh || '')
    c.string.encode(state, m.auth || '')
  },
  decode (state) {
    return {
      endpoint: c.string.decode(state),
      p256dh: c.string.decode(state),
      auth: c.string.decode(state)
    }
  }
}

/**
 * Register a push subscription with one blind peer.
 *
 * @param {HyperDHT} dht           - swarm.dht
 * @param {object} keyPair         - our identity Ed25519 keyPair {publicKey, secretKey}
 * @param {string|Buffer} blindPeerKey - the blind peer pubkey (hex/z32/buffer)
 * @param {{endpoint:string, p256dh:string, auth:string}} sub
 * @param {{timeout?:number}} [opts]
 * @returns {Promise<boolean>} resolves true on ack, throws on failure/timeout
 */
async function registerPushEndpoint (dht, keyPair, blindPeerKey, sub, opts = {}) {
  if (!dht) throw new Error('registerPushEndpoint: no dht')
  if (!keyPair) throw new Error('registerPushEndpoint: no identity keyPair')
  if (!sub || !sub.endpoint || !sub.p256dh || !sub.auth) {
    throw new Error('registerPushEndpoint: endpoint, p256dh and auth are all required')
  }

  const target = HypercoreId.decode(blindPeerKey)
  const timeout = opts.timeout || DEFAULT_TIMEOUT_MS

  const conn = dht.connect(target, { keyPair })
  let rpc = null
  let timer = null

  try {
    const result = new Promise((resolve, reject) => {
      timer = setTimeout(() => reject(new Error('register-push-endpoint timed out')), timeout)
      if (timer && timer.unref) timer.unref()
      conn.on('error', reject)
      rpc = new ProtomuxRPC(conn, {
        protocol: PUSH_PROTOCOL,
        id: PUSH_CHANNEL_ID,
        valueEncoding: c.none
      })
      rpc.request(METHOD, sub, { requestEncoding: RegisterEncoding, responseEncoding: c.none })
        .then(() => resolve(true), reject)
    })
    return await result
  } finally {
    if (timer) clearTimeout(timer)
    try { if (rpc) rpc.destroy() } catch (_) {}
    try { conn.destroy() } catch (_) {}
  }
}

module.exports = {
  registerPushEndpoint,
  PUSH_PROTOCOL,
  PUSH_CHANNEL_ID,
  METHOD,
  RegisterEncoding
}
