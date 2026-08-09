package xyz.justzappit.zappmessaging.models

import kotlinx.serialization.Serializable

/**
 * A contact entry.
 * Mirrors iOS ZMContact for cross-platform consistency.
 */
@Serializable
data class ZMContact(
    /** Contact's public key (hex encoded) */
    val publicKey: String,
    /** Contact's display name */
    val name: String,
    /** When the contact was added (epoch millis) */
    val addedAt: Long = System.currentTimeMillis(),
    /** ZEC wallet address (if shared) */
    val walletAddress: String? = null,
    /** Wallet address type (unified, sapling, transparent) */
    val addressType: String? = null
)
