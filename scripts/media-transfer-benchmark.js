// Synthetic Node benchmark. Run with --require ./core/tests/helpers/node-compat.js.
// No device, image codec, real network, relaying or mobile filesystem is measured.
const { performance } = require('node:perf_hooks')
const { MediaTransfer } = require(__dirname + '/../core/lib/media-transfer')
const data = Buffer.alloc(1024 * 1024, 7), hash = 'ab'.repeat(32)
let reads = 0, bytes = 0
const transfer = new MediaTransfer({ getMedia: () => { reads++; return data }, hasMedia: () => true })
const socket = { peerId: 'cd'.repeat(32), writeChunk: (h, i, n, chunk) => { bytes += chunk.length } }
;(async () => {
const start = performance.now()
await Promise.all([transfer.sendMediaToAll([socket], hash), transfer.handleRequest(Buffer.from(hash, 'hex'), socket)])
console.log(JSON.stringify({ scenario: 'proactive plus simultaneous request, 1 MiB', ms: performance.now()-start, payloadBytes: bytes, fileReads: reads }))
})()
