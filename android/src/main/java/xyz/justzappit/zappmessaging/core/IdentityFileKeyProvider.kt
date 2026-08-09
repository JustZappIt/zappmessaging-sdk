package xyz.justzappit.zappmessaging.core

import android.content.Context
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import android.util.Base64
import android.util.Log
import java.security.KeyStore
import java.security.SecureRandom
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec

/**
 * Supplies the 32-byte key the worklet uses to encrypt identity.json (the
 * BIP-39 wallet entropy) at rest — passed as `--identity-file-key`.
 *
 * The data key is random, minted once per install, and persisted only as
 * ciphertext wrapped by an AES-256-GCM key that never leaves the Android
 * Keystore (StrongBox when the device has it). The plaintext key exists in
 * memory only long enough to hand to the in-process worklet.
 *
 * If the wrapped blob fails to unwrap (keystore reset by the OS), the old
 * data key is unrecoverable and a fresh one is minted: the worklet then
 * cannot read an already-encrypted identity and surfaces "no identity", so
 * the user restores from the 24-word seed phrase — the standard wallet
 * recovery path. Messaging is never blocked on key plumbing.
 */
object IdentityFileKeyProvider {

    private const val TAG = "IdentityFileKey"
    private const val ANDROID_KEYSTORE = "AndroidKeyStore"
    private const val KEYSTORE_ALIAS = "zappmessaging.identity_file_wrap"
    private const val PREFS_NAME = "zappmessaging_keys"
    private const val PREF_WRAPPED = "identity_file_key_wrapped"
    private const val TRANSFORM = "AES/GCM/NoPadding"
    private const val DATA_KEY_BYTES = 32
    private const val GCM_IV_BYTES = 12
    private const val GCM_TAG_BITS = 128

    /**
     * Returns the identity-file key as 64 lowercase hex chars, or null when
     * the Keystore is unusable on this device (the worklet then keeps the
     * legacy plaintext identity file rather than losing messaging).
     */
    fun getOrCreateKeyHex(context: Context): String? {
        return try {
            val prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
            val wrapKey = getOrCreateWrapKey()

            prefs.getString(PREF_WRAPPED, null)?.let { stored ->
                unwrap(stored, wrapKey)?.let { return it.toHex() }
                Log.w(TAG, "Stored identity key failed to unwrap; rotating (seed-phrase restore required)")
            }

            val dataKey = ByteArray(DATA_KEY_BYTES).also { SecureRandom().nextBytes(it) }
            prefs.edit().putString(PREF_WRAPPED, wrap(dataKey, wrapKey)).apply()
            dataKey.toHex()
        } catch (e: Exception) {
            Log.e(TAG, "Keystore unavailable; identity file stays plaintext: ${e.message}")
            null
        }
    }

    private fun getOrCreateWrapKey(): SecretKey {
        val ks = KeyStore.getInstance(ANDROID_KEYSTORE).apply { load(null) }
        (ks.getKey(KEYSTORE_ALIAS, null) as? SecretKey)?.let { return it }

        val spec = KeyGenParameterSpec.Builder(
            KEYSTORE_ALIAS,
            KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT
        )
            .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
            .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
            .setKeySize(256)

        val generator = KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, ANDROID_KEYSTORE)
        return try {
            generator.init(spec.setIsStrongBoxBacked(true).build())
            generator.generateKey()
        } catch (e: Exception) {
            // StrongBoxUnavailableException on most devices — fall back to the TEE.
            generator.init(spec.setIsStrongBoxBacked(false).build())
            generator.generateKey()
        }
    }

    private fun wrap(dataKey: ByteArray, wrapKey: SecretKey): String {
        val cipher = Cipher.getInstance(TRANSFORM)
        cipher.init(Cipher.ENCRYPT_MODE, wrapKey)
        val sealed = cipher.iv + cipher.doFinal(dataKey)
        return Base64.encodeToString(sealed, Base64.NO_WRAP)
    }

    private fun unwrap(stored: String, wrapKey: SecretKey): ByteArray? {
        return try {
            val sealed = Base64.decode(stored, Base64.NO_WRAP)
            if (sealed.size <= GCM_IV_BYTES) return null
            val cipher = Cipher.getInstance(TRANSFORM)
            cipher.init(Cipher.DECRYPT_MODE, wrapKey, GCMParameterSpec(GCM_TAG_BITS, sealed, 0, GCM_IV_BYTES))
            val key = cipher.doFinal(sealed, GCM_IV_BYTES, sealed.size - GCM_IV_BYTES)
            if (key.size == DATA_KEY_BYTES) key else null
        } catch (e: Exception) {
            null
        }
    }

    private fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it) }
}
