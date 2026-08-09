package xyz.justzappit.zappmessaging.models

import kotlinx.serialization.Serializable

/**
 * A chat message with optional media attachments.
 * Mirrors iOS ZMMessage for cross-platform consistency.
 */
@Serializable
data class ZMMessage(
    /** Unique message identifier */
    val id: String,
    /** Conversation this message belongs to */
    val conversationId: String,
    /** Sender's public key */
    val senderId: String,
    /** Sender's display name */
    val senderName: String? = null,
    /** Message content (text or caption) */
    val content: String,
    /** Content type (MIME type) */
    val contentType: String = "text/plain",
    /** When the message was sent (epoch millis) */
    val timestamp: Long = System.currentTimeMillis(),
    /** Whether this message was sent by the current user */
    val isFromMe: Boolean,
    /** Media hash ID (for media messages) */
    val mediaId: String? = null,
    /** Media file size in bytes */
    val mediaSize: Int? = null,
    /** Media width in pixels */
    val mediaWidth: Int? = null,
    /** Media height in pixels */
    val mediaHeight: Int? = null,
    /** Base64-encoded thumbnail data */
    val thumbnailData: String? = null,
    /** Local file path to media */
    val mediaLocalPath: String? = null,
    /** Media transfer state */
    val mediaTransferState: MediaTransferState? = null,
    /** ID of the message being replied to */
    val replyToId: String? = null,
    /** Display name of the sender of the replied-to message */
    val replyToSenderName: String? = null,
    /** Content preview of the replied-to message */
    val replyToContent: String? = null,
    /** Persisted outgoing delivery state ("queued", "sent", "delivered", or "read") */
    val status: String? = null
)
