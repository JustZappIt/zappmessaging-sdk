package xyz.justzappit.zappmessaging.core

import android.content.Context
import android.net.ConnectivityManager
import android.util.Log
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import to.holepunch.bare.kit.IPC
import to.holepunch.bare.kit.Worklet
import xyz.justzappit.zappmessaging.BuildConfig
import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

/**
 * Manages the BareKit JavaScript worklet lifecycle on Android.
 * Equivalent to iOS BareWorkletManager.swift.
 *
 * Handles starting, stopping, suspending, and resuming the worklet,
 * and wires IPC data flow to [IPCBridge].
 */
class BareWorkletManager {

    private var worklet: Worklet? = null
    private var ipc: IPC? = null
    private var ipcBridge: IPCBridge? = null

    private val shuttingDown = AtomicBoolean(false)
    private val lifecycleLock = Any()
    private val writeMutex = Mutex()

    @Volatile
    var isRunning: Boolean = false
        private set

    /**
     * Start the JavaScript worklet and wire IPC to the bridge.
     *
     * @param context Android context for loading assets
     * @param ipcBridge The IPC bridge to receive incoming data
     */
    fun start(context: Context, ipcBridge: IPCBridge) {
        synchronized(lifecycleLock) {
            if (isRunning) {
                Log.w(TAG, "Worklet already running")
                return
            }

            shuttingDown.set(false)
            this.ipcBridge = ipcBridge

            // Extract worklet bundle from APK assets to internal storage.
            // Bare runtime needs a filesystem path to load the bundle format
            // (bare-pack bundles are not raw JS — they have a binary header).
            val bundleDir = File(context.filesDir, "bare")
            bundleDir.mkdirs()
            val bundleFile = File(bundleDir, "worklet.bundle")
            if (!bundleFile.exists() || shouldUpdateBundle(context, bundleFile)) {
                context.assets.open("worklet.bundle").use { input ->
                    bundleFile.outputStream().use { output ->
                        input.copyTo(output)
                    }
                }
                Log.i(TAG, "Extracted worklet.bundle to ${bundleFile.absolutePath}")
            }

            // Create worklet with 64MB memory limit, assets pointing to bundle directory
            val options = Worklet.Options()
                .memoryLimit(64 * 1024 * 1024)
                .assets(bundleDir.absolutePath)

            val w = Worklet(options)
            worklet = w

            // IMPORTANT: Start the worklet BEFORE creating IPC.
            // bare_worklet_start() sets up the pipe FDs via a barrier.
            // If IPC callbacks are registered before start(), native assertions fail.
            val argv = mutableListOf("--data-dir=${context.filesDir.absolutePath}")
            if (BuildConfig.BLIND_PEER_KEYS.isNotEmpty()) {
                argv += "--blind-peer-keys=${BuildConfig.BLIND_PEER_KEYS}"
            }
            if (BuildConfig.BLIND_PEER_BOOTSTRAP.isNotEmpty()) {
                argv += "--bootstrap-nodes=${BuildConfig.BLIND_PEER_BOOTSTRAP}"
            }
            if (BuildConfig.BLIND_PEER_ADDRESS.isNotEmpty()) {
                argv += "--blind-peer-address=${BuildConfig.BLIND_PEER_ADDRESS}"
            }
            if (BuildConfig.INVITE_MAILBOX_URL.isNotEmpty()) {
                argv += "--invite-mailbox-url=${BuildConfig.INVITE_MAILBOX_URL}"
            }
            // Pass the current network gateway to the worklet so the JS DHT
            // layer can seed its routing table from the gateway node on LAN.
            // Critical for the hotspot scenario: when this phone is connected
            // to another phone's hotspot, the gateway IP is the AP phone's
            // LAN address. The AP phone's DHT (port 49737) is reachable at
            // that address and can relay DHT queries to the internet even
            // though this phone is behind double-NAT at the IP level.
            val gatewayIp = getGatewayIp(context)
            if (gatewayIp != null) {
                argv += "--local-gateway=$gatewayIp"
                Log.i(TAG, "Local gateway detected: $gatewayIp")
            }
            if (BuildConfig.ZAPP_MESSAGING_LOG_LEVEL.isNotEmpty()) {
                argv += "--log-level=${BuildConfig.ZAPP_MESSAGING_LOG_LEVEL}"
            }
            // Keystore-wrapped key for encrypting identity.json (wallet
            // entropy) at rest. Secret: argv must never be logged once added.
            IdentityFileKeyProvider.getOrCreateKeyHex(context)?.let { keyHex ->
                argv += "--identity-file-key=$keyHex"
            }
            w.start(bundleFile.absolutePath, argv.toTypedArray())

            // Create IPC channel after worklet is started
            val ipcChannel = IPC(w)
            ipc = ipcChannel

            // Set up readable callback for incoming data from worklet.
            // Guard against reads after shutdown: the callback runs on the BareKit
            // IPC thread and may overlap with stop() on the main thread.
            ipcChannel.readable {
                if (shuttingDown.get()) return@readable
                val bridge = ipcBridge ?: return@readable
                try {
                    var hasData = false
                    while (!shuttingDown.get()) {
                        val data: ByteBuffer? = ipcChannel.read()
                        if (data == null || data.remaining() == 0) break
                        hasData = true
                        val bytes = ByteArray(data.remaining())
                        data.get(bytes)
                        Log.d(TAG, "IPC data received from worklet: ${bytes.size} bytes")
                        bridge.handleIncomingData(bytes)
                    }
                    if (!hasData) {
                        Log.d(TAG, "IPC readable callback fired but no data available")
                    }
                } catch (e: Exception) {
                    if (!shuttingDown.get()) {
                        Log.e(TAG, "IPC read failed", e)
                    }
                }
                Unit
            }

            isRunning = true
            Log.i(TAG, "Worklet started successfully")
        } // synchronized(lifecycleLock)
    }

    /**
     * Stop the JavaScript worklet and clean up resources.
     */
    fun stop() {
        synchronized(lifecycleLock) {
            if (!isRunning) return

            // Signal shutdown before touching handles so that callbacks
            // running on the BareKit IPC thread skip further reads/writes.
            shuttingDown.set(true)

            try {
                ipc?.close()
            } catch (e: Exception) {
                Log.w(TAG, "IPC close error (ignored)", e)
            }
            ipc = null
            ipcBridge = null

            // Give the native IPC callback thread time to observe the closed
            // pipe and return before we free the worklet's native memory.
            // Without this, terminate() races with the IPC thread — the
            // thread can SIGSEGV in pthread_key_clean_all accessing freed TLS.
            try {
                Thread.sleep(SHUTDOWN_DRAIN_MS)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
            }

            try {
                worklet?.terminate()
            } catch (e: Exception) {
                Log.w(TAG, "Worklet terminate error (ignored)", e)
            }
            worklet = null

            isRunning = false
            Log.i(TAG, "Worklet stopped")
        } // synchronized(lifecycleLock)
    }

    /**
     * Suspend the worklet (for app backgrounding).
     */
    fun suspend() {
        if (!isRunning || shuttingDown.get()) return
        try {
            worklet?.suspend()
            Log.i(TAG, "Worklet suspended")
        } catch (e: Exception) {
            Log.w(TAG, "Worklet suspend failed", e)
        }
    }

    /**
     * Suspend the worklet with a linger duration.
     *
     * @param lingerMs How long to keep the process alive before full exit
     */
    fun suspend(lingerMs: Int) {
        if (!isRunning || shuttingDown.get()) return
        try {
            worklet?.suspend(lingerMs)
            Log.i(TAG, "Worklet suspended with linger: ${lingerMs}ms")
        } catch (e: Exception) {
            Log.w(TAG, "Worklet suspend(linger) failed", e)
        }
    }

    /**
     * Resume a suspended worklet.
     */
    fun resume() {
        if (!isRunning || shuttingDown.get()) return
        try {
            worklet?.resume()
            Log.i(TAG, "Worklet resumed")
        } catch (e: Exception) {
            Log.w(TAG, "Worklet resume failed", e)
        }
    }

    /**
     * Send data to the worklet via IPC.
     * Handles partial writes by retrying until all bytes are written.
     *
     * @param data The bytes to send
     * @throws IllegalStateException if the worklet is not running
     */
    suspend fun sendData(data: ByteArray) = writeMutex.withLock {
        if (shuttingDown.get()) {
            throw IllegalStateException("IPC unavailable — worklet is shutting down")
        }
        val ipcChannel = ipc ?: throw IllegalStateException("IPC not available — worklet not started")
        val buffer = ByteBuffer.wrap(data)

        Log.d(TAG, "Sending ${data.size} bytes to worklet via IPC")
        
        try {
            while (buffer.hasRemaining()) {
                val written = ipcChannel.write(buffer)
                if (written <= 0) {
                    // Non-blocking write returned 0 — use async fallback
                    val remaining = ByteArray(buffer.remaining())
                    buffer.get(remaining)
                    Log.d(TAG, "Using async write for remaining ${remaining.size} bytes")
                    suspendCoroutine<Unit> { continuation ->
                        try {
                            ipcChannel.write(ByteBuffer.wrap(remaining)) { exception ->
                                if (exception != null) {
                                    continuation.resumeWithException(exception)
                                } else {
                                    continuation.resume(Unit)
                                }
                            }
                        } catch (error: Throwable) {
                            continuation.resumeWithException(error)
                        }
                    }
                    Log.d(TAG, "Async write completed successfully")
                    return@withLock
                }
            }
            Log.d(TAG, "Synchronous write completed successfully")
        } catch (e: Exception) {
            if (shuttingDown.get()) {
                throw IllegalStateException("IPC closed during write — worklet is shutting down", e)
            }
            throw e
        }
    }

    /**
     * Return the default-route gateway IP for the active network, or null.
     *
     * When this phone is connected to another phone's hotspot, the gateway is
     * the AP phone's LAN address (e.g. 10.215.90.1 for Android hotspot). The
     * AP phone's HyperDHT node listens on all interfaces including the hotspot
     * interface at port 49737, so the JS worklet can seed its DHT routing
     * table from that address without going through the carrier NAT.
     *
     * Returns null if the active network has no IPv4 gateway (e.g. VPN-only,
     * cellular without a detectable gateway, or API failure).
     */
    private fun getGatewayIp(context: Context): String? {
        return try {
            val cm = context.getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
            val activeNetwork = cm.activeNetwork ?: return null
            val linkProps = cm.getLinkProperties(activeNetwork) ?: return null
            linkProps.routes
                .filter { it.isDefaultRoute }
                .mapNotNull { it.gateway?.hostAddress }
                // IPv4 only: skip IPv6 link-local (fe80::) and loopback
                .firstOrNull { addr -> addr.contains('.') && !addr.startsWith("169.254") }
        } catch (e: Exception) {
            Log.d(TAG, "Gateway detection failed (non-fatal): ${e.message}")
            null
        }
    }

    /**
     * Check if the extracted bundle is outdated (app was updated).
     */
    private fun shouldUpdateBundle(context: Context, bundleFile: File): Boolean {
        return try {
            val appInfo = context.packageManager.getPackageInfo(context.packageName, 0)
            bundleFile.lastModified() < appInfo.lastUpdateTime
        } catch (e: Exception) {
            true
        }
    }

    companion object {
        private const val TAG = "BareWorkletManager"
        /** Drain time after IPC close to let the native callback thread exit before terminate(). */
        private const val SHUTDOWN_DRAIN_MS = 150L
    }
}
