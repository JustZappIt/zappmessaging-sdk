#!/usr/bin/env node

/**
 * The blind peer as Zapp deploys it: a Hypercore mirror, HyperDHT's
 * `blind-relay` connection protocol, the bootstrap invite mailbox and a
 * retention pass that forgets images a week after they arrive, all under one
 * persistent key.
 *
 * Stock `blind-peer-cli` provides only the mirror, which is why firewalled
 * peers pointed at it could never obtain a socket.
 */

const path = require('path')
const BlindPeer = require('blind-peer')
const HypercoreId = require('hypercore-id-encoding')
const { attachBlindRelay } = require('./blind-relay')
const { attachInviteMailbox, DEFAULTS } = require('./invite-mailbox')
const { attachMediaRetention, DEFAULTS: RETENTION } = require('./media-retention')

const SCALE = { b: 1, kb: 1000, mb: 1000 ** 2, gb: 1000 ** 3, tb: 1000 ** 4 }
const DEFAULT_DHT_PORT = 49737

function parseBytes (value, fallback) {
  if (!value) return fallback
  const match = /^(\d+)\s*(b|kb|mb|gb|tb)?$/i.exec(String(value).trim())
  if (!match) throw new Error('Invalid byte size: ' + value)
  return Number(match[1]) * SCALE[(match[2] || 'b').toLowerCase()]
}

function decodeKeys (value) {
  if (!value) return []
  return value.split(',').map(key => key.trim()).filter(Boolean).map(key => HypercoreId.decode(key))
}

function integer (value, fallback) {
  const parsed = Number.parseInt(value, 10)
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback
}

async function main () {
  const storage = process.env.BLIND_PEER_STORAGE || './blind-peer-data'
  const port = integer(process.env.BLIND_PEER_PORT, DEFAULT_DHT_PORT)
  const maxBytes = parseBytes(process.env.BLIND_PEER_MAX_STORAGE, 10 * SCALE.gb)
  const trustedPubKeys = decodeKeys(process.env.BLIND_PEER_TRUSTED_KEYS)
  const httpPort = integer(process.env.INVITE_MAILBOX_HTTP_PORT, DEFAULTS.httpPort)
  const httpHost = process.env.INVITE_MAILBOX_HTTP_HOST || DEFAULTS.httpHost

  const blindPeer = new BlindPeer(storage, { port, maxBytes, trustedPubKeys })
  blindPeer.on('flush-error', error => console.error('Blind peer flush error:', error))
  blindPeer.on('notification-error', error => console.error('Blind peer notification error:', error))

  await blindPeer.ready()

  const relay = attachBlindRelay(blindPeer, { log: message => console.error(message) })
  const mailbox = attachInviteMailbox(blindPeer, {
    directory: process.env.INVITE_MAILBOX_DIR || path.resolve(storage, 'invite-mailbox'),
    maxHttpRequestsPerMinute: integer(process.env.INVITE_MAILBOX_MAX_RPM, DEFAULTS.maxHttpRequestsPerMinute),
    maxBytesPerRecipient: integer(
      process.env.INVITE_MAILBOX_MAX_BYTES_PER_RECIPIENT, DEFAULTS.maxBytesPerRecipient
    ),
    log: message => console.error(message)
  })
  await mailbox.listenHttp(httpPort, httpHost)
  const retention = await attachMediaRetention(blindPeer, {
    maxAgeMs: integer(process.env.MEDIA_RETENTION_MAX_AGE_MS, RETENTION.maxAgeMs),
    minIntervalMs: integer(process.env.MEDIA_RETENTION_MIN_INTERVAL_MS, RETENTION.minIntervalMs),
    log: message => console.error(message)
  })

  await blindPeer.listen()

  console.log('Blind peer + connection relay + invite mailbox + media retention listening')
  console.log('Public key: ' + HypercoreId.encode(blindPeer.publicKey))
  console.log('Local address: ' + JSON.stringify(blindPeer.swarm.dht.localAddress()))
  const bound = mailbox.httpAddress()
  console.log('Invite mailbox HTTP: http://' + httpHost + ':' + (bound ? bound.port : httpPort))

  let closing = false
  const close = async () => {
    if (closing) return
    closing = true
    await retention.close()
    await mailbox.close()
    await relay.close()
    await blindPeer.close()
  }
  process.once('SIGINT', () => close().catch(console.error))
  process.once('SIGTERM', () => close().catch(console.error))
}

if (require.main === module) {
  main().catch(error => {
    console.error(error)
    process.exitCode = 1
  })
}

module.exports = { parseBytes, decodeKeys }
