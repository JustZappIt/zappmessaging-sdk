package xyz.justzappit.zappmessaging

import android.util.Log

/**
 * Privacy-safe native diagnostics.
 *
 * Native logging follows the same explicit debug opt-in as the JavaScript
 * worklet. Production builds are silent by default, and callers must only pass
 * structural metadata: never payloads, identifiers, addresses, filesystem
 * paths, argv, keys, or exception messages.
 */
internal object ZMLog {
    private val isEnabled: Boolean
        get() = BuildConfig.ZAPP_MESSAGING_LOG_LEVEL.equals("debug", ignoreCase = true)

    fun debug(tag: String, message: () -> String) {
        if (isEnabled) Log.d(tag, message())
    }

    fun warning(tag: String, message: () -> String) {
        if (isEnabled) Log.w(tag, message())
    }

    fun error(tag: String, message: () -> String) {
        if (isEnabled) Log.e(tag, message())
    }
}
