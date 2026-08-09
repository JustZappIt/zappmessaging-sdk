package xyz.justzappit.zappmessaging.models

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject

/**
 * IPC request message sent to the JavaScript worklet.
 */
@Serializable
data class IPCRequest(
    val id: String = java.util.UUID.randomUUID().toString(),
    val type: String,
    val payload: JsonObject = JsonObject(emptyMap())
)

/**
 * IPC response message received from the JavaScript worklet.
 */
@Serializable
data class IPCResponse(
    val id: String,
    val success: Boolean,
    val data: JsonObject? = null,
    val error: IPCErrorData? = null
)

/**
 * IPC error data embedded in a response.
 */
@Serializable
data class IPCErrorData(
    val code: String = "ERROR",
    val message: String = "Unknown error"
)

/**
 * IPC event message pushed from the JavaScript worklet.
 */
@Serializable
data class IPCEvent(
    val type: String,
    val payload: JsonObject = JsonObject(emptyMap()),
    val timestamp: Long = 0L
)
