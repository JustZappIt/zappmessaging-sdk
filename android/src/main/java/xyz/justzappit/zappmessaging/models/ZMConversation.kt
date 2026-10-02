package xyz.justzappit.zappmessaging.models

import kotlinx.serialization.Serializable

/**
 * A chat conversation (direct or group).
 * Mirrors iOS ZMConversation for cross-platform consistency.
 */
@Serializable
data class ZMConversation(
    /** Unique conversation identifier */
    val id: String,
    /** Type of conversation */
    val type: ConversationType,
    /** Participant public keys (excluding self for groups) */
    val participantIds: List<String>,
    /** Group ID (for group chats only) */
    val groupId: String? = null,
    /** Creator's public key (for groups only) */
    val creatorKey: String? = null,
    /** Display name for the conversation */
    val displayName: String,
    /** Preview of last message */
    val lastMessage: String? = null,
    /** Timestamp of last message (epoch millis) */
    val lastMessageTimestamp: Long? = null,
    /** When the conversation was created (epoch millis) */
    val createdAt: Long = System.currentTimeMillis(),
    /** Whether current user is the group owner */
    val isOwner: Boolean? = null,
    /** Number of unread messages */
    val unreadCount: Int? = null,
    /** Set when the group owner removed us: history stays, nothing more flows. Epoch millis. */
    val removedAt: Long? = null
)
