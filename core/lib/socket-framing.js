/**
 * Socket Framing - Length-prefixed binary framing for Hyperswarm sockets
 *
 * Wraps raw TCP sockets to handle message boundaries correctly.
 * Protocol: [4 bytes: uint32 BE length][1 byte: type][payload]
 *
 * Types:
 *   0x01 - JSON message (chat messages, control messages)
 *   0x02 - Media chunk [32B hash][4B chunkIdx][4B totalChunks][chunk data]
 *   0x03 - Media request [32B hash]
 *   0x04 - Ping (heartbeat)
 *   0x05 - Pong (heartbeat response)
 *
 * Backward compatible: detects legacy raw JSON (first byte '{') and falls back.
 */

const b4a = require('b4a')
const EventEmitter = require('bare-events')

const MSG_TYPE_JSON = 0x01
const MSG_TYPE_CHUNK = 0x02
const MSG_TYPE_REQUEST = 0x03
const MSG_TYPE_PING = 0x04
const MSG_TYPE_PONG = 0x05

class FramedSocket {
  constructor(rawSocket) {
    this.socket = rawSocket
    this._recvBuf = b4a.alloc(0)
    this._destroyed = false
    this._legacyMode = false
    this._legacyBuf = ''

    // Callbacks
    this.onMessage = null // (parsedJSON) => {}
    this.onChunk = null // (hashBuf, chunkIndex, totalChunks, data) => {}
    this.onRequest = null // (hashBuf) => {}
    this.onError = null // (error) => {}
    this.onPong = null // () => {} — called when remote responds to our ping

    // Heartbeat tracking
    this.lastPongTime = Date.now()

    rawSocket.on('data', (data) => this._onData(data))
  }

  _onData(data) {
    if (this._destroyed) return

    // Backward compatibility: detect legacy raw JSON
    if (this._recvBuf.length === 0 && !this._legacyMode) {
      const firstByte = data[0]
      if (firstByte === 0x7b) { // '{' character
        this._legacyMode = true
      }
    }

    if (this._legacyMode) {
      this._handleLegacy(data)
      return
    }

    this._recvBuf = b4a.concat([this._recvBuf, data])
    this._drainFrames()
  }

  _handleLegacy(data) {
    // Legacy mode: NDJSON-style parsing for backward compatibility
    this._legacyBuf += b4a.toString(data)

    // Process all complete newline-delimited messages
    let newlineIdx
    while ((newlineIdx = this._legacyBuf.indexOf('\n')) !== -1) {
      const line = this._legacyBuf.substring(0, newlineIdx).trim()
      this._legacyBuf = this._legacyBuf.substring(newlineIdx + 1)
      if (line.length === 0) continue
      try {
        const message = JSON.parse(line)
        if (this.onMessage) this.onMessage(message)
      } catch (e) {
        if (this.onError) this.onError(e)
      }
    }

    // Try parsing remaining buffer as a complete JSON object (no trailing newline)
    if (this._legacyBuf.length > 0) {
      try {
        const message = JSON.parse(this._legacyBuf)
        this._legacyBuf = ''
        if (this.onMessage) this.onMessage(message)
      } catch (_) {
        // Incomplete — keep buffering
      }
    }

    // Safety: discard if buffer grows too large
    if (this._legacyBuf.length > 10 * 1024 * 1024) {
      this._legacyBuf = ''
    }
  }

  _drainFrames() {
    while (this._recvBuf.length >= 4) {
      const len = this._recvBuf.readUInt32BE(0)

      // Safety: reject absurdly large frames (>50MB)
      if (len > 50 * 1024 * 1024) {
        if (this.onError) this.onError(new Error('Frame too large: ' + len))
        this._recvBuf = b4a.alloc(0)
        return
      }

      if (this._recvBuf.length < 4 + len) break // Incomplete frame

      const payload = this._recvBuf.subarray(4, 4 + len)
      this._recvBuf = this._recvBuf.subarray(4 + len)
      this._processFrame(payload)
    }
  }

  _processFrame(payload) {
    if (payload.length < 1) return

    const type = payload[0]
    const body = payload.subarray(1)

    switch (type) {
      case MSG_TYPE_JSON: {
        try {
          const msg = JSON.parse(b4a.toString(body))
          if (this.onMessage) this.onMessage(msg)
        } catch (e) {
          if (this.onError) this.onError(e)
        }
        break
      }

      case MSG_TYPE_CHUNK: {
        if (body.length < 40) return // 32 hash + 4 idx + 4 total = minimum
        const hash = body.subarray(0, 32)
        const chunkIndex = body.readUInt32BE(32)
        const totalChunks = body.readUInt32BE(36)
        const chunkData = body.subarray(40)
        if (this.onChunk) this.onChunk(hash, chunkIndex, totalChunks, chunkData)
        break
      }

      case MSG_TYPE_REQUEST: {
        if (body.length < 32) return
        const hash = body.subarray(0, 32)
        if (this.onRequest) this.onRequest(hash)
        break
      }

      case MSG_TYPE_PING: {
        // Auto-reply with pong
        this.writePong()
        break
      }

      case MSG_TYPE_PONG: {
        this.lastPongTime = Date.now()
        if (this.onPong) this.onPong()
        break
      }

      default:
        if (this.onError) this.onError(new Error('Unknown frame type: ' + type))
    }
  }

  /**
   * Send a JSON message (type 0x01)
   */
  writeJSON(obj) {
    if (this._destroyed) return false
    const json = b4a.from(JSON.stringify(obj))
    const frame = b4a.alloc(4 + 1 + json.length)
    frame.writeUInt32BE(1 + json.length, 0)
    frame[4] = MSG_TYPE_JSON
    json.copy(frame, 5)
    return this._write(frame)
  }

  /**
   * Reports whether the frame was handed to a socket that could still take it.
   *
   * `_destroyed` only tracks our own teardown, so it cannot see a socket the OS
   * killed without a close event. Asking the socket, and treating a throw as a
   * failure, is what stops a send to a dead peer being reported as delivered.
   * A `false` return from `write` is backpressure, not failure.
   * @private
   */
  _write(frame) {
    if (this.socket.destroyed || this.socket.writable === false) return false
    try {
      this.socket.write(frame)
    } catch (e) {
      return false
    }
    return true
  }

  /**
   * Send a media chunk (type 0x02)
   */
  writeChunk(hashBuf, chunkIndex, totalChunks, data) {
    if (this._destroyed) return false
    const payloadLen = 1 + 32 + 4 + 4 + data.length
    const frame = b4a.alloc(4 + payloadLen)
    frame.writeUInt32BE(payloadLen, 0)
    frame[4] = MSG_TYPE_CHUNK
    hashBuf.copy(frame, 5)
    frame.writeUInt32BE(chunkIndex, 37)
    frame.writeUInt32BE(totalChunks, 41)
    data.copy(frame, 45)
    this.socket.write(frame)
    return true
  }

  /**
   * Send a media request (type 0x03)
   */
  writeRequest(hashBuf) {
    if (this._destroyed) return false
    const frame = b4a.alloc(4 + 1 + 32)
    frame.writeUInt32BE(33, 0)
    frame[4] = MSG_TYPE_REQUEST
    hashBuf.copy(frame, 5)
    this.socket.write(frame)
    return true
  }

  /**
   * Send a ping frame (type 0x04) — remote will auto-reply with pong.
   */
  writePing() {
    if (this._destroyed) return false
    const frame = b4a.alloc(4 + 1)
    frame.writeUInt32BE(1, 0) // length = 1 (type byte only)
    frame[4] = MSG_TYPE_PING
    this.socket.write(frame)
    return true
  }

  /**
   * Send a pong frame (type 0x05) — response to a ping.
   */
  writePong() {
    if (this._destroyed) return false
    const frame = b4a.alloc(4 + 1)
    frame.writeUInt32BE(1, 0)
    frame[4] = MSG_TYPE_PONG
    this.socket.write(frame)
    return true
  }

  destroy() {
    this._destroyed = true
    this._recvBuf = b4a.alloc(0)
  }
}

module.exports = { FramedSocket, MSG_TYPE_JSON, MSG_TYPE_CHUNK, MSG_TYPE_REQUEST, MSG_TYPE_PING, MSG_TYPE_PONG }
