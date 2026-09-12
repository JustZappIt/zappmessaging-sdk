package xyz.justzappit.zappmessaging.models

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * State of a media transfer operation.
 * Mirrors iOS MediaTransferState for cross-platform consistency.
 */
@Serializable
enum class MediaTransferState {
    @SerialName("queued") QUEUED,
    @SerialName("queued_socket") QUEUED_SOCKET,
    @SerialName("downloading") DOWNLOADING,
    @SerialName("sending") SENDING,
    @SerialName("sent") SENT,
    @SerialName("receiving") RECEIVING,
    @SerialName("complete") COMPLETE,
    @SerialName("failed") FAILED;

    companion object {
        fun fromRaw(value: String): MediaTransferState? = when (value) {
            "queued" -> QUEUED
            "queued_socket" -> QUEUED_SOCKET
            "downloading" -> DOWNLOADING
            "sending" -> SENDING
            "sent" -> SENT
            "receiving" -> RECEIVING
            "complete" -> COMPLETE
            "failed" -> FAILED
            else -> null
        }
    }
}
