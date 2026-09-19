package xyz.justzappit.zappmessaging.models

/**
 * Group invite links and member removal. Mirrors iOS ZMGroupLink.swift.
 *
 * [ZMGroupLinkInfo.link] is a bearer secret: anyone holding it can ask to
 * join. Never log it, never put it in navigation state, and hand it only to
 * copy, share and QR code actions.
 */

/** Who admits people who use the link. */
enum class ZMGroupLinkApproval(val rawValue: String) {
    /** Admitted as soon as the owner's app sees the request. */
    AUTO("auto"),

    /** Each request waits for the owner to approve or decline it. */
    OWNER("owner");

    companion object {
        fun fromRaw(raw: String?): ZMGroupLinkApproval = entries.firstOrNull { it.rawValue == raw } ?: AUTO
    }
}

enum class ZMGroupLinkState(val rawValue: String) {
    NONE("none"),
    ACTIVE("active"),
    OFF("off"),
    UNKNOWN("unknown");

    companion object {
        fun fromRaw(raw: String?): ZMGroupLinkState = entries.firstOrNull { it.rawValue == raw } ?: UNKNOWN
    }
}

/** The owner's view of a group's invite link. */
data class ZMGroupLinkInfo(
    val conversationId: String,
    val state: ZMGroupLinkState,
    /** The shareable link. Null when there is no link. A bearer secret. */
    val link: String? = null,
    val linkId: String? = null,
    val createdAt: Long? = null,
    /** Epoch millis, or null for no expiry. */
    val expiresAt: Long? = null,
    /** Joins allowed through this link, or null for no limit. */
    val maxJoins: Int? = null,
    val joins: Int = 0,
    val includeName: Boolean = true,
    val approval: ZMGroupLinkApproval = ZMGroupLinkApproval.AUTO,
    /** "rate" when the link switched itself to owner approval. */
    val approvalReason: String? = null,
    val pendingRequests: Int = 0,
) {
    override fun toString(): String = "ZMGroupLinkInfo(conversationId=$conversationId, state=$state, link=<redacted>)"
}

/**
 * Options for enabling or updating a link. Only non-null fields are sent;
 * [clearExpiry] and [clearMaxJoins] remove a limit.
 */
data class ZMGroupLinkOptions(
    val expiresAt: Long? = null,
    val clearExpiry: Boolean = false,
    val maxJoins: Int? = null,
    val clearMaxJoins: Boolean = false,
    val includeName: Boolean? = null,
    val approval: ZMGroupLinkApproval? = null,
)

enum class ZMGroupLinkInspectStatus(val rawValue: String) {
    OK("ok"),
    EXPIRED("expired"),
    MALFORMED("malformed"),
    UNSUPPORTED_VERSION("unsupported_version"),
    NEWER_FORMAT("newer_format");

    companion object {
        fun fromRaw(raw: String?): ZMGroupLinkInspectStatus = entries.firstOrNull { it.rawValue == raw } ?: MALFORMED
    }
}

/** What a link says about itself, read without contacting anyone. */
data class ZMGroupLinkInspection(
    val status: ZMGroupLinkInspectStatus,
    val nameHint: String? = null,
    /** Unix seconds, a hint only; the owner enforces the real expiry. */
    val expiresAt: Long? = null,
    /** Opaque, safe to keep and log. Derived from the link, not the secret itself. */
    val linkId: String? = null,
)

enum class ZMGroupJoinRequestStatus(val rawValue: String) {
    REQUESTED("requested"),
    ALREADY_REQUESTED("already_requested"),
    ALREADY_MEMBER("already_member"),
    EXPIRED("expired"),
    MALFORMED("malformed"),
    UNSUPPORTED_VERSION("unsupported_version"),
    NEWER_FORMAT("newer_format");

    companion object {
        fun fromRaw(raw: String?): ZMGroupJoinRequestStatus = entries.firstOrNull { it.rawValue == raw } ?: MALFORMED
    }
}

/** The answer to asking to join. */
data class ZMGroupJoinResult(
    val status: ZMGroupJoinRequestStatus,
    val linkId: String? = null,
    /** Set when already a member. */
    val conversationId: String? = null,
)

/** Where a request this device made stands. */
enum class ZMGroupJoinStatus(val rawValue: String) {
    WAITING("waiting"),
    PENDING_APPROVAL("pending_approval"),
    JOINED("joined"),
    INACTIVE("inactive"),
    EXPIRED("expired"),
    FULL("full"),
    DECLINED("declined"),
    CANCELLED("cancelled"),
    UNKNOWN("unknown");

    val isWaiting: Boolean get() = this == WAITING || this == PENDING_APPROVAL

    companion object {
        fun fromRaw(raw: String?): ZMGroupJoinStatus = entries.firstOrNull { it.rawValue == raw } ?: UNKNOWN
    }
}

data class ZMGroupJoinUpdate(
    val linkId: String,
    val status: ZMGroupJoinStatus,
    val conversationId: String? = null,
    val nameHint: String? = null,
)

/** A request waiting for the owner, in approval mode. */
data class ZMGroupJoinApprovalRequest(
    val conversationId: String,
    val joinerKey: String,
    val joinerName: String,
    val requestedAt: Long? = null,
    /** The owner removed this person from the group before. */
    val previouslyRemoved: Boolean = false,
)

data class ZMRemoveMemberResult(
    val participants: List<String>,
    /**
     * Members whose app does not understand removal yet. They see new messages
     * once they update.
     */
    val olderMemberCount: Int,
)
