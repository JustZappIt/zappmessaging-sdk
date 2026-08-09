package xyz.justzappit.zappmessaging.models

import kotlinx.serialization.Serializable

/**
 * User's cryptographic identity.
 * Mirrors iOS ZMIdentity for cross-platform consistency.
 */
@Serializable
data class ZMIdentity(
    /** Ed25519 public key (hex encoded) */
    val publicKey: String,
    /** User's display name */
    val displayName: String,
    /** When the identity was created (epoch millis) */
    val createdAt: Long = System.currentTimeMillis()
)
