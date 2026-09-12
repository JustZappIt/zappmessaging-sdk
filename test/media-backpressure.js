// Bare-runtime smoke test for the installed streamx write/drain contract.
const { Writable } = require('streamx')
const b4a = require('b4a')
const crypto = require('hypercore-crypto')
const { FramedSocket } = require('../core/lib/socket-framing')
const { MediaTransfer, CHUNK_SIZE } = require('../core/lib/media-transfer')
;(async () => {
  const data = b4a.alloc(1024 * 1024, 7)
  const hash = b4a.toString(crypto.data(data), 'hex')
  let bytes = 0, maximumBuffered = 0, inFlight = 0
  const raw = new Writable({ highWaterMark: 16384, write (frame, callback) {
    bytes += frame.length - 45
    inFlight += frame.length
    maximumBuffered = Math.max(maximumBuffered, raw._writableState.buffered)
    setTimeout(() => { inFlight -= frame.length; callback() }, 1)
  } })
  const write = raw.write.bind(raw)
  raw.write = frame => {
    const ready = write(frame)
    maximumBuffered = Math.max(maximumBuffered, raw._writableState.buffered + inFlight)
    return ready
  }
  const transfer = new MediaTransfer({ getMedia: () => data })
  await transfer.sendMedia(new FramedSocket(raw), hash)
  if (bytes !== data.length || maximumBuffered > CHUNK_SIZE + 45) throw new Error('Backpressure contract failed')
  raw.destroy()
  console.log(JSON.stringify({ runtime: 'Bare', bytes, maximumBuffered }))
})().catch(error => { console.error(error.message); Bare.exit(1) })
