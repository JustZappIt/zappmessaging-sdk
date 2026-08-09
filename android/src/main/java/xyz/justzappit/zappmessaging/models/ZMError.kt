package xyz.justzappit.zappmessaging.models

sealed class ZMErrorCode(val rawValue: String) {
    data object OwnPublicKey : ZMErrorCode("OWN_PUBLIC_KEY")
    data object MissingParticipant : ZMErrorCode("MISSING_PARTICIPANT")
    data object InvalidParticipants : ZMErrorCode("INVALID_PARTICIPANTS")
    data object InvalidPublicKey : ZMErrorCode("INVALID_PUBLIC_KEY")
    data object ConversationNotFound : ZMErrorCode("CONVERSATION_NOT_FOUND")
    data class Unknown(private val value: String) : ZMErrorCode(value)

    companion object {
        fun fromRaw(rawValue: String): ZMErrorCode = when (rawValue) {
            OwnPublicKey.rawValue -> OwnPublicKey
            MissingParticipant.rawValue -> MissingParticipant
            InvalidParticipants.rawValue -> InvalidParticipants
            InvalidPublicKey.rawValue -> InvalidPublicKey
            ConversationNotFound.rawValue -> ConversationNotFound
            else -> Unknown(rawValue)
        }
    }
}

/**
 * Errors that can occur in ZappMessaging SDK.
 * Mirrors iOS ZMError for cross-platform consistency.
 */
sealed class ZMError(override val message: String) : Exception(message) {
    class NotInitialized : ZMError("ZappMessaging SDK is not initialized")
    class IdentityNotFound : ZMError("No identity found. Please create or restore an identity first.")
    class ConversationNotFound : ZMError("Conversation not found")
    class ContactNotFound : ZMError("Contact not found")
    class ConnectionFailed(detail: String) : ZMError("Connection failed: $detail")
    class SendFailed(detail: String) : ZMError("Failed to send message: $detail")
    class InvalidData(detail: String) : ZMError("Invalid data: $detail")
    class WorkletError(detail: String) : ZMError("JavaScript worklet error: $detail")
    class IpcTimeout : ZMError("IPC request timed out")
    class IpcError(val code: ZMErrorCode, detail: String) : ZMError("IPC error: $detail")
    class SecureStorageError(detail: String) : ZMError("Secure storage error: $detail")
    class InvalidSeedPhrase : ZMError("Invalid seed phrase")
    class MediaError(detail: String) : ZMError("Media error: $detail")
}
