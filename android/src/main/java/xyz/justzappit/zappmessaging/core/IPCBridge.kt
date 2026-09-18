package xyz.justzappit.zappmessaging.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.*
import xyz.justzappit.zappmessaging.ZMLog
import xyz.justzappit.zappmessaging.models.*
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap

/** The byte sink a request is written to; [BareWorkletManager] in production. */
internal interface IPCTransport {
    suspend fun sendData(data: ByteArray)
}

/**
 * Handles NDJSON IPC protocol communication with the JavaScript worklet.
 * Equivalent to iOS IPCBridge.swift.
 *
 * Manages request/response correlation, NDJSON framing (buffering partial
 * reads, splitting on newlines), and event dispatch to registered listeners.
 * Framing state belongs to one transport session: see [beginSession].
 */
internal class IPCBridge {

    private val json = Json {
        ignoreUnknownKeys = true
        isLenient = true
    }

    private val pendingRequests = ConcurrentHashMap<String, CompletableDeferred<JsonObject>>()
    private val eventHandlers = mutableListOf<(String, JsonObject) -> Unit>()
    // Raw bytes, decoded once a complete line is available, so a UTF-8 code
    // point may span two pipe reads.
    private var receiveBuffer = ByteArray(0)
    // True while dropping the remainder of a single over-cap frame, up to its
    // next newline, so one huge frame can't take the whole stream down.
    private var skippingOversized = false
    // Identifies the byte stream the framing state belongs to. Bytes presented
    // with an older token come from a reader that was stopped, and are dropped.
    private var sessionToken = 0
    private var transport: IPCTransport? = null

    private val bufferLock = Any()

    /**
     * Per-frame ceiling. Any larger NDJSON line is skipped while the rest of
     * the stream is preserved. Generous because legitimate frames are small
     * now that message.list no longer inlines base64 thumbnails.
     */
    private val maxLineBytes = 16 * 1024 * 1024

    /**
     * Set the transport requests are written to.
     */
    fun setTransport(transport: IPCTransport) {
        this.transport = transport
    }

    /** Requests still waiting for a response, timeout, or cancellation. */
    val pendingRequestCount: Int
        get() = pendingRequests.size

    // ── Request / Response ──────────────────────────────────────────────

    /**
     * Send an IPC request and suspend until the response arrives or timeout.
     *
     * @param type IPC message type (e.g. "identity.get")
     * @param payload Request payload
     * @param timeoutMs Timeout in milliseconds (default 30s)
     * @return Response data as JsonObject
     * @throws ZMError.IpcTimeout if the request times out
     * @throws ZMError.IpcError if the worklet returns an error
     */
    suspend fun sendRequest(
        type: String,
        payload: JsonObject = JsonObject(emptyMap()),
        timeoutMs: Long = DEFAULT_TIMEOUT_MS
    ): JsonObject {
        val request = IPCRequest(type = type, payload = payload)
        val deferred = CompletableDeferred<JsonObject>()

        pendingRequests[request.id] = deferred

        // Serialize and send as NDJSON (append newline)
        val requestJson = json.encodeToString(request) + "\n"
        val requestBytes = requestJson.toByteArray(StandardCharsets.UTF_8)

        try {
            transport?.sendData(requestBytes)
                ?: throw ZMError.NotInitialized()
        } catch (e: Exception) {
            pendingRequests.remove(request.id)
            throw e
        }

        // The entry is removed on every exit, including the caller's own
        // cancellation, so an abandoned request cannot linger until shutdown.
        return try {
            withTimeout(timeoutMs) {
                deferred.await()
            }
        } catch (e: kotlinx.coroutines.TimeoutCancellationException) {
            throw ZMError.IpcTimeout()
        } finally {
            pendingRequests.remove(request.id, deferred)
        }
    }

    // ── Incoming Data Handling ───────────────────────────────────────────

    /**
     * Start a new transport session and return its token.
     *
     * Every in-flight request fails, and any partial frame left over from the
     * previous byte stream is dropped, so a restarted worklet's first line is
     * never glued onto the tail of the old one. [handleIncomingData] ignores
     * bytes carrying an older token, which is what keeps a late callback from
     * the stopped reader from re-introducing stale bytes.
     */
    fun beginSession(): Int {
        val token = synchronized(bufferLock) {
            receiveBuffer = ByteArray(0)
            skippingOversized = false
            ++sessionToken
        }
        cancelAllPendingRequests()
        return token
    }

    /**
     * Handle incoming raw bytes from the worklet.
     * Called by [BareWorkletManager] on the IPC readable callback with the
     * token its session was started with.
     *
     * Buffers partial data and processes complete NDJSON lines.
     */
    fun handleIncomingData(bytes: ByteArray, session: Int) {
        synchronized(bufferLock) {
            if (session != sessionToken) return
            receiveBuffer += bytes

            // Process every complete newline-delimited frame. Each line's bytes
            // are decoded as a whole, so a multi-byte UTF-8 sequence straddling
            // two pipe reads survives intact.
            while (true) {
                val newlineIndex = receiveBuffer.indexOf(NEWLINE)
                if (newlineIndex == -1) break

                if (skippingOversized) {
                    // These bytes are the tail of an over-cap frame we already
                    // gave up on; drop them and resume at the next frame.
                    receiveBuffer = receiveBuffer.copyOfRange(newlineIndex + 1, receiveBuffer.size)
                    skippingOversized = false
                    continue
                }

                // Check before copying or decoding the line. A complete huge
                // frame may arrive in one callback, so the unterminated-tail
                // check below is not sufficient on its own.
                if (newlineIndex > maxLineBytes) {
                    ZMLog.warning(TAG) { "Oversized IPC frame skipped" }
                    receiveBuffer = receiveBuffer.copyOfRange(newlineIndex + 1, receiveBuffer.size)
                    continue
                }

                val lineBytes = receiveBuffer.copyOfRange(0, newlineIndex)
                receiveBuffer = receiveBuffer.copyOfRange(newlineIndex + 1, receiveBuffer.size)
                if (lineBytes.isNotEmpty()) {
                    val line = String(lineBytes, StandardCharsets.UTF_8)
                    if (line.isNotBlank()) processLine(line)
                }
            }

            // An un-terminated frame past the cap: drop just this frame and skip
            // to its next newline. Clearing the whole buffer instead would drop
            // every in-flight response behind it.
            if (receiveBuffer.size > maxLineBytes) {
                ZMLog.warning(TAG) { "Unterminated oversized IPC frame skipped" }
                skippingOversized = true
                receiveBuffer = ByteArray(0)
            }
        }
    }

    private fun processLine(line: String) {
        try {
            val element = json.parseToJsonElement(line)
            if (element !is JsonObject) {
                ZMLog.warning(TAG) { "Non-object IPC frame skipped" }
                return
            }

            // Try as response first (has "id" and "success" fields)
            if (element.containsKey("id") && element.containsKey("success")) {
                handleResponse(element)
                return
            }

            // Try as event (has "type" but no "id")
            if (element.containsKey("type") && !element.containsKey("id")) {
                handleEvent(element)
                return
            }

            ZMLog.warning(TAG) { "Unrecognized IPC frame skipped" }
        } catch (_: Exception) {
            ZMLog.warning(TAG) { "Malformed IPC frame skipped" }
        }
    }

    private fun handleResponse(obj: JsonObject) {
        val id = obj["id"]?.jsonPrimitive?.contentOrNull ?: return
        val success = obj["success"]?.jsonPrimitive?.booleanOrNull ?: false

        val deferred = pendingRequests.remove(id) ?: return

        if (success) {
            val data = obj["data"]?.jsonObject ?: JsonObject(emptyMap())
            deferred.complete(data)
        } else {
            val error = obj["error"]?.jsonObject
            val errorCode = ZMErrorCode.fromRaw(
                error?.get("code")?.jsonPrimitive?.contentOrNull ?: "ERROR"
            )
            val errorMsg = error?.get("message")?.jsonPrimitive?.contentOrNull ?: "Unknown error"
            deferred.completeExceptionally(ZMError.IpcError(errorCode, errorMsg))
        }
    }

    private fun handleEvent(obj: JsonObject) {
        val type = obj["type"]?.jsonPrimitive?.contentOrNull ?: return
        val payload = obj["payload"]?.jsonObject ?: JsonObject(emptyMap())

        synchronized(eventHandlers) {
            for (handler in eventHandlers) {
                try {
                    handler(type, payload)
                } catch (_: Exception) {
                    ZMLog.error(TAG) { "IPC event handler failed" }
                }
            }
        }
    }

    // ── Event Handling ──────────────────────────────────────────────────

    /**
     * Register a listener for push events from the worklet.
     */
    fun onEvent(handler: (type: String, payload: JsonObject) -> Unit) {
        synchronized(eventHandlers) {
            eventHandlers.add(handler)
        }
    }

    /**
     * Remove all event handlers.
     */
    fun clearEventHandlers() {
        synchronized(eventHandlers) {
            eventHandlers.clear()
        }
    }

    /**
     * Cancel all pending requests (e.g. during shutdown).
     * Completes each deferred exceptionally so callers don't hang forever.
     */
    fun cancelAllPendingRequests() {
        val error = ZMError.NotInitialized()
        for ((_, deferred) in pendingRequests) {
            deferred.completeExceptionally(error)
        }
        pendingRequests.clear()
    }

    // ── Convenience Parsing Methods ─────────────────────────────────────

    /**
     * Parse a ZMIdentity from an IPC response data object.
     */
    fun parseIdentity(data: JsonObject): ZMIdentity? {
        val publicKey = data["publicKey"]?.jsonPrimitive?.contentOrNull ?: return null
        val displayName = data["displayName"]?.jsonPrimitive?.contentOrNull ?: return null
        val createdAt = data["createdAt"]?.jsonPrimitive?.longOrNull ?: System.currentTimeMillis()
        return ZMIdentity(publicKey = publicKey, displayName = displayName, createdAt = createdAt)
    }

    /**
     * Parse a ZMConversation from an IPC response data object.
     */
    fun parseConversation(data: JsonObject): ZMConversation? {
        val id = data["id"]?.jsonPrimitive?.contentOrNull ?: return null
        val typeStr = data["type"]?.jsonPrimitive?.contentOrNull ?: return null
        val type = ConversationType.fromRaw(typeStr) ?: return null
        val participantIds = data["participantIds"]?.jsonArray
            ?.mapNotNull { it.jsonPrimitive.contentOrNull }
            ?: emptyList()
        val displayName = data["displayName"]?.jsonPrimitive?.contentOrNull ?: ""

        return ZMConversation(
            id = id,
            type = type,
            participantIds = participantIds,
            groupId = data["groupId"]?.jsonPrimitive?.contentOrNull,
            creatorKey = data["creatorKey"]?.jsonPrimitive?.contentOrNull,
            displayName = displayName,
            lastMessage = data["lastMessage"]?.jsonPrimitive?.contentOrNull,
            lastMessageTimestamp = data["lastMessageTimestamp"]?.jsonPrimitive?.longOrNull,
            createdAt = data["createdAt"]?.jsonPrimitive?.longOrNull ?: System.currentTimeMillis(),
            isOwner = data["isOwner"]?.jsonPrimitive?.booleanOrNull
        )
    }

    /**
     * Parse a ZMMessage from an IPC response data object.
     */
    fun parseMessage(data: JsonObject): ZMMessage? {
        val id = data["id"]?.jsonPrimitive?.contentOrNull ?: return null
        val conversationId = data["conversationId"]?.jsonPrimitive?.contentOrNull ?: return null
        val senderId = data["senderId"]?.jsonPrimitive?.contentOrNull ?: return null
        val content = data["content"]?.jsonPrimitive?.contentOrNull ?: ""
        val contentType = data["contentType"]?.jsonPrimitive?.contentOrNull ?: "text/plain"
        val isFromMe = data["isFromMe"]?.jsonPrimitive?.booleanOrNull ?: false

        return ZMMessage(
            id = id,
            conversationId = conversationId,
            senderId = senderId,
            senderName = data["senderName"]?.jsonPrimitive?.contentOrNull,
            content = content,
            contentType = contentType,
            timestamp = data["timestamp"]?.jsonPrimitive?.longOrNull ?: System.currentTimeMillis(),
            isFromMe = isFromMe,
            mediaId = data["mediaId"]?.jsonPrimitive?.contentOrNull,
            mediaSize = data["mediaSize"]?.jsonPrimitive?.intOrNull,
            mediaWidth = data["mediaWidth"]?.jsonPrimitive?.intOrNull,
            mediaHeight = data["mediaHeight"]?.jsonPrimitive?.intOrNull,
            thumbnailData = data["thumbnailData"]?.jsonPrimitive?.contentOrNull,
            mediaLocalPath = data["mediaLocalPath"]?.jsonPrimitive?.contentOrNull,
            mediaTransferState = data["mediaTransferState"]?.jsonPrimitive?.contentOrNull
                ?.let { MediaTransferState.fromRaw(it) },
            replyToId = data["replyToId"]?.jsonPrimitive?.contentOrNull,
            replyToSenderName = data["replyToSenderName"]?.jsonPrimitive?.contentOrNull,
            replyToContent = data["replyToContent"]?.jsonPrimitive?.contentOrNull,
            replyToContentType = data["replyToContentType"]?.jsonPrimitive?.contentOrNull,
            status = data["status"]?.jsonPrimitive?.contentOrNull
        )
    }

    /**
     * Parse a ZMContact from an IPC response data object.
     */
    fun parseContact(data: JsonObject): ZMContact? {
        val publicKey = data["publicKey"]?.jsonPrimitive?.contentOrNull ?: return null
        val name = data["name"]?.jsonPrimitive?.contentOrNull ?: return null
        val addedAt = data["addedAt"]?.jsonPrimitive?.longOrNull ?: System.currentTimeMillis()
        return ZMContact(
            publicKey = publicKey,
            name = name,
            addedAt = addedAt,
            walletAddress = data["walletAddress"]?.jsonPrimitive?.contentOrNull,
            addressType = data["addressType"]?.jsonPrimitive?.contentOrNull
        )
    }

    /**
     * Negotiate protocol version with the JS worklet.
     * Should be called immediately after the worklet starts.
     * @throws ZMError.IpcError if the protocol is incompatible
     */
    suspend fun negotiateProtocol(): JsonObject {
        val payload = buildJsonObject {
            put("protocolVersion", CLIENT_PROTOCOL_VERSION)
        }
        val result = sendRequest("protocol.init", payload, timeoutMs = 5_000L)
        val compatible = result["compatible"]?.jsonPrimitive?.booleanOrNull ?: false
        if (!compatible) {
            val serverVersion = result["protocolVersion"]?.jsonPrimitive?.contentOrNull ?: "unknown"
            throw ZMError.IpcError(
                ZMErrorCode.Unknown("INCOMPATIBLE_PROTOCOL"),
                "Protocol version mismatch: client=$CLIENT_PROTOCOL_VERSION server=$serverVersion"
            )
        }
        ZMLog.debug(TAG) { "IPC protocol negotiated" }
        return result
    }

    companion object {
        private const val TAG = "IPCBridge"
        private const val DEFAULT_TIMEOUT_MS = 30_000L
        const val CLIENT_PROTOCOL_VERSION = "1.0"
        private const val NEWLINE = '\n'.code.toByte()
    }
}
