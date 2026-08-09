package xyz.justzappit.zappmessaging.models

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Type of conversation supported by the messaging core.
 */
@Serializable
enum class ConversationType {
    @SerialName("direct") DIRECT,
    @SerialName("group") GROUP;

    val rawValue: String
        get() = when (this) {
            DIRECT -> "direct"
            GROUP -> "group"
        }

    companion object {
        fun fromRaw(value: String): ConversationType? = when (value) {
            "direct" -> DIRECT
            "group" -> GROUP
            else -> null
        }
    }
}
