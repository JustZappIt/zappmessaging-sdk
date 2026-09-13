package xyz.justzappit.zappmessaging

import android.content.Context
import android.net.ConnectivityManager
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.plus
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.serialization.json.*
import xyz.justzappit.zappmessaging.core.BareWorkletManager
import xyz.justzappit.zappmessaging.core.IPCBridge
import xyz.justzappit.zappmessaging.models.*
import java.io.ByteArrayOutputStream
import java.net.HttpURLConnection
import java.net.URL
import java.util.concurrent.atomic.AtomicLong

/**
 * Data returned by [ZappMessagingSDK.createIdentity]. Contains both the identity and the
 * recovery mnemonic in one shot — callers must surface [seedPhrase] to the user for backup
 * before discarding it.
 */
data class ZMIdentityWithSeed(
    val identity: ZMIdentity,
    val seedPhrase: String,
)

/**
 * Main public API for the ZappMessaging Android SDK.
 * Equivalent to iOS ZappMessagingSDK.swift.
 *
 * Provides identity management, conversation CRUD, messaging, contacts,
 * media transfer, and connection status — all backed by the zappMessaging
 * JavaScript core running in a BareKit worklet.
 *
 * Usage:
 * ```kotlin
 * val sdk = ZappMessagingSDK()
 * sdk.initialize(context)
 * val identity = sdk.createIdentity("Alice")
 * ```
 */
class ZappMessagingSDK internal constructor(
    private val ipcBridge: IPCBridge,
    private val workletManager: BareWorkletManager,
) {

    constructor() : this(IPCBridge(), BareWorkletManager())

    // ── Published State ─────────────────────────────────────────────────

    private val _identity = MutableStateFlow<ZMIdentity?>(null)
    /** Current user identity */
    val identity: StateFlow<ZMIdentity?> = _identity.asStateFlow()

    private val _conversations = MutableStateFlow<List<ZMConversation>>(emptyList())
    /** All conversations */
    val conversations: StateFlow<List<ZMConversation>> = _conversations.asStateFlow()

    private val _contacts = MutableStateFlow<List<ZMContact>>(emptyList())
    /** All contacts */
    val contacts: StateFlow<List<ZMContact>> = _contacts.asStateFlow()

    private val _isOnline = MutableStateFlow(false)
    /** Connection status */
    val isOnline: StateFlow<Boolean> = _isOnline.asStateFlow()

    private val _peerCount = MutableStateFlow(0)
    /** Peer count */
    val peerCount: StateFlow<Int> = _peerCount.asStateFlow()

    private val _dhtHealth = MutableStateFlow(DHT_HEALTH_DEFAULT)
    /** DHT health status: "healthy", "degraded", or "critical" */
    val dhtHealth: StateFlow<String> = _dhtHealth.asStateFlow()

    // ── Event Flows ─────────────────────────────────────────────────────

    private val _peerStatus = MutableSharedFlow<Triple<String, String, String>>(extraBufferCapacity = 64)
    /** Flow of (conversationId, peerId, "online"|"offline") for per-conversation peer events */
    val peerStatus: SharedFlow<Triple<String, String, String>> = _peerStatus.asSharedFlow()

    private val _messageReceived = MutableSharedFlow<Pair<String, ZMMessage>>(extraBufferCapacity = 256)
    /** Flow of (conversationId, message) for incoming messages */
    val messageReceived: SharedFlow<Pair<String, ZMMessage>> = _messageReceived.asSharedFlow()

    private val _inviteReceived = MutableSharedFlow<ZMConversation>(extraBufferCapacity = 64)
    /** Flow of conversation invites */
    val inviteReceived: SharedFlow<ZMConversation> = _inviteReceived.asSharedFlow()

    private val _mediaDownloadProgress = MutableSharedFlow<Pair<String, Double>>(extraBufferCapacity = 64)
    /** Flow of (mediaId, progress 0..1) as chunks of an inbound media transfer arrive. */
    val mediaDownloadProgress: SharedFlow<Pair<String, Double>> = _mediaDownloadProgress.asSharedFlow()

    private val _mediaDownloadComplete = MutableSharedFlow<Pair<String, String>>(extraBufferCapacity = 16)
    /** Flow of (mediaId, filePath) once an inbound media transfer is stored and hash-verified. */
    val mediaDownloadComplete: SharedFlow<Pair<String, String>> = _mediaDownloadComplete.asSharedFlow()

    private val _memberLeft = MutableSharedFlow<Pair<String, String>>(extraBufferCapacity = 16)
    /** Flow of (conversationId, leaverKey) when a member leaves */
    val memberLeft: SharedFlow<Pair<String, String>> = _memberLeft.asSharedFlow()

    private val _groupDeleted = MutableSharedFlow<String>(extraBufferCapacity = 16)
    /** Flow of conversationId when a group is deleted */
    val groupDeleted: SharedFlow<String> = _groupDeleted.asSharedFlow()

    private val _groupRenamed = MutableSharedFlow<Pair<String, String>>(extraBufferCapacity = 16)
    /** Flow of (conversationId, newName) when a group is renamed */
    val groupRenamed: SharedFlow<Pair<String, String>> = _groupRenamed.asSharedFlow()

    private val _memberAdded = MutableSharedFlow<Triple<String, String, String>>(extraBufferCapacity = 16)
    /** Flow of (conversationId, memberKey, memberName) when a member is added */
    val memberAdded: SharedFlow<Triple<String, String, String>> = _memberAdded.asSharedFlow()

    private val _messageStatus = MutableSharedFlow<Triple<String, String, String>>(extraBufferCapacity = 64)
    /** Flow of (messageId, conversationId, status) for delivery status */
    val messageStatus: SharedFlow<Triple<String, String, String>> = _messageStatus.asSharedFlow()

    private val _mediaTransferProgress = MutableSharedFlow<Pair<String, Double>>(extraBufferCapacity = 64)
    /**
     * Same events as [mediaDownloadProgress]: the core reports progress only
     * for inbound transfers and nothing for sends. Kept for hosts that
     * subscribed before the direction was named; prefer [mediaDownloadProgress].
     */
    val mediaTransferProgress: SharedFlow<Pair<String, Double>> = _mediaTransferProgress.asSharedFlow()

    private val _mediaTransferComplete = MutableSharedFlow<String>(extraBufferCapacity = 16)
    /**
     * The mediaId of each completed inbound transfer, i.e. [mediaDownloadComplete]
     * without the path. This never signals that an outbound send finished;
     * prefer [mediaDownloadComplete].
     */
    val mediaTransferComplete: SharedFlow<String> = _mediaTransferComplete.asSharedFlow()

    private val _pushTopicsChanged = MutableSharedFlow<Unit>(extraBufferCapacity = 16)
    /** Emitted whenever direct-chat inbound push capabilities change. */
    val pushTopicsChanged: SharedFlow<Unit> = _pushTopicsChanged.asSharedFlow()

    // ── Private Components ──────────────────────────────────────────────

    private var applicationContext: Context? = null

    /**
     * Scope for work the worklet asks the host to perform. Recreated on each
     * [initialize] and cancelled by [shutdown], so an in-flight request cannot
     * outlive the SDK it belongs to.
     */
    private var hostScope: CoroutineScope? = null

    @Volatile
    private var isInitialized = false
    private val initializationMutex = Mutex()

    /**
     * Advances whenever the published identity changes or the SDK shuts down.
     * List refreshes capture it before their round-trip and discard a result
     * that belongs to an older account, so a refresh started under the previous
     * identity can never publish that account's data under the new one.
     */
    private val accountGeneration = AtomicLong(0)

    // ── Initialization ──────────────────────────────────────────────────

    /**
     * Initialize the SDK. Must be called before any other method.
     *
     * @param context Android application context
     */
    suspend fun initialize(context: Context) = initializationMutex.withLock {
        if (isInitialized) return@withLock

        try {
            applicationContext = context.applicationContext
            hostScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
            ipcBridge.setTransport(workletManager)
            workletManager.start(context.applicationContext, ipcBridge)
            setupEventHandlers()

            // Verify worklet started and negotiate protocol version.
            // Retries with backoff instead of a blind delay.
            var workletReady = false
            for (attempt in 1..WORKLET_STARTUP_MAX_RETRIES) {
                try {
                    delay(WORKLET_STARTUP_PROBE_DELAY_MS * attempt)
                    ipcBridge.negotiateProtocol()
                    workletReady = true
                    break
                } catch (_: Exception) {
                    ZMLog.warning(TAG) { "Worklet readiness probe $attempt failed" }
                }
            }
            if (!workletReady) {
                ZMLog.error(TAG) { "Worklet readiness probes exhausted" }
                throw ZMError.NotInitialized()
            }

            // Load identity if it exists
            try {
                val response = ipcBridge.sendRequest("identity.get")
                publishIdentity(ipcBridge.parseIdentity(response))
            } catch (_: Exception) {
                ZMLog.debug(TAG) { "No existing identity available during initialization" }
            }

            // Load initial data if identity exists
            if (_identity.value != null) {
                try {
                    refreshConversations()
                    refreshContacts()
                    getConnectionStatus()
                } catch (_: Exception) {
                    ZMLog.warning(TAG) { "Initial data refresh failed" }
                }
            }

            isInitialized = true
            ZMLog.debug(TAG) { "SDK initialized" }
        } catch (error: Throwable) {
            teardown()
            throw error
        }
    }

    /**
     * Shutdown the SDK and release all resources.
     *
     * Every published value returns to its initial state: no identity, empty
     * conversation and contact lists, offline with zero peers. Nothing observed
     * from a stopped worklet is authoritative, and a later [initialize] loads
     * whatever the core holds at that point.
     */
    fun shutdown() {
        teardown()
        ZMLog.debug(TAG) { "SDK shut down" }
    }

    private fun teardown() {
        // Stopping the worklet ends the bridge session, which fails every
        // in-flight request and drops any partial frame from the old stream.
        workletManager.stop()
        ipcBridge.clearEventHandlers()
        hostScope?.cancel()
        hostScope = null
        applicationContext = null
        isInitialized = false
        publishIdentity(null)
        _isOnline.value = false
        _peerCount.value = 0
        _dhtHealth.value = DHT_HEALTH_DEFAULT
    }

    /**
     * Make [newIdentity] the published identity. When it differs from the
     * current one, the account-scoped caches are emptied first and the
     * generation advances, so nothing from the previous account is visible
     * under the new key even if a refresh later fails.
     */
    private fun publishIdentity(newIdentity: ZMIdentity?) {
        if (_identity.value?.publicKey != newIdentity?.publicKey) {
            accountGeneration.incrementAndGet()
            _conversations.value = emptyList()
            _contacts.value = emptyList()
        }
        _identity.value = newIdentity
    }

    // ── Identity Management ─────────────────────────────────────────────

    /**
     * Create a new cryptographic identity. Returns the new identity AND its 24-word
     * recovery mnemonic in a single IPC round-trip — there's no separate export step
     * that can fail independently. Caller is responsible for prompting the user to
     * back the phrase up before discarding the returned value.
     *
     * @param displayName User's display name
     */
    suspend fun createIdentity(displayName: String): ZMIdentityWithSeed {
        val payload = buildJsonObject { put("displayName", displayName) }
        val response = ipcBridge.sendRequest("identity.create", payload)
        val newIdentity = ipcBridge.parseIdentity(response)
            ?: throw ZMError.InvalidData("Invalid create identity response")
        val seedPhrase = response["seedPhrase"]?.jsonPrimitive?.contentOrNull
            ?: throw ZMError.InvalidData("identity.create response missing seedPhrase")

        publishIdentity(newIdentity)
        return ZMIdentityWithSeed(identity = newIdentity, seedPhrase = seedPhrase)
    }

    /**
     * Get current identity (from cached state).
     */
    fun getIdentity(): ZMIdentity? = _identity.value

    /**
     * Update the display name of the current identity.
     *
     * @param displayName New display name
     */
    suspend fun updateDisplayName(displayName: String) {
        requireIdentity()
        val payload = buildJsonObject { put("displayName", displayName) }
        val response = ipcBridge.sendRequest("identity.update", payload)
        val persistedDisplayName = response["displayName"]?.jsonPrimitive?.contentOrNull
            ?: throw ZMError.InvalidData("identity.update response missing displayName")
        _identity.value = _identity.value?.copy(displayName = persistedDisplayName)
    }

    /** Returns the BIP-39 mnemonic for the current identity. Throws if there is no identity. */
    suspend fun exportSeedPhrase(): String {
        if (_identity.value == null) throw ZMError.IdentityNotFound()

        val response = ipcBridge.sendRequest("migration.get_seed_phrase")
        return response["seedPhrase"]?.jsonPrimitive?.contentOrNull
            ?: throw ZMError.InvalidData("No seed phrase in response")
    }

    /**
     * Restore identity from a BIP39 seed phrase.
     *
     * @param seedPhrase The 24-word seed phrase
     * @param displayName The display name for the restored identity
     * @return The restored identity
     */
    suspend fun restoreFromSeedPhrase(seedPhrase: String, displayName: String): ZMIdentity {
        val payload = buildJsonObject {
            put("seedPhrase", seedPhrase)
            put("displayName", displayName)
        }

        val response = ipcBridge.sendRequest("migration.restore_from_seed_phrase", payload)
        val publicKey = response["publicKey"]?.jsonPrimitive?.contentOrNull
            ?: throw ZMError.InvalidData("Invalid restore response")
        val restoredName = response["displayName"]?.jsonPrimitive?.contentOrNull ?: displayName

        val restoredIdentity = ZMIdentity(publicKey = publicKey, displayName = restoredName)
        publishIdentity(restoredIdentity)

        // The restore itself is durable at this point; a transient list failure
        // must not make the host report that it failed. The caches were already
        // emptied above, so the failure cannot leave old data published.
        try {
            refreshConversations()
            refreshContacts()
        } catch (_: Exception) {
            ZMLog.warning(TAG) { "Post-restore data refresh failed" }
        }

        return restoredIdentity
    }

    // ── Conversation Management ─────────────────────────────────────────

    /**
     * Returns whether the deterministic direct conversation with [publicKey] was explicitly
     * removed on this device. Hosts can use this to confirm before an intentional rejoin.
     */
    suspend fun hasLeftDirectConversation(publicKey: String): Boolean {
        requireIdentity()
        val payload = buildJsonObject {
            put("participants", JsonArray(listOf(JsonPrimitive(publicKey))))
        }
        val response = ipcBridge.sendRequest("conversation.direct_status", payload)
        return response["isLeft"]?.jsonPrimitive?.booleanOrNull ?: false
    }

    /**
     * Create a new conversation.
     *
     * @param type Conversation type
     * @param participants List of participant public keys
     * @param displayName Optional display name
     * @return The created conversation
     */
    suspend fun createConversation(
        type: ConversationType,
        participants: List<String>,
        displayName: String? = null
    ): ZMConversation {
        requireIdentity()

        val payload = buildJsonObject {
            put("type", type.rawValue)
            put("participants", JsonArray(participants.map { JsonPrimitive(it) }))
            displayName?.let { put("displayName", it) }
        }

        val response = ipcBridge.sendRequest("conversation.create", payload)
        val convData = response["conversation"]?.jsonObject
            ?: throw ZMError.InvalidData("Missing conversation in response")
        val conversation = ipcBridge.parseConversation(convData)
            ?: throw ZMError.InvalidData("Invalid conversation data")

        refreshConversations()
        ensureConversationConnected(conversation.id)
        return conversation
    }

    /**
     * Refresh conversations from core.
     */
    suspend fun refreshConversations() {
        val generation = accountGeneration.get()
        val response = ipcBridge.sendRequest("conversation.list")
        val convArray = response["conversations"]?.jsonArray ?: return
        val conversations = convArray.mapNotNull { element ->
            element.jsonObject.let { ipcBridge.parseConversation(it) }
        }
        if (accountGeneration.get() == generation) _conversations.value = conversations
    }

    /**
     * Leave a group conversation.
     */
    suspend fun leaveConversation(conversationId: String) {
        val payload = buildJsonObject { put("conversationId", conversationId) }
        ipcBridge.sendRequest("conversation.leave", payload)
        refreshConversations()
    }

    /**
     * Delete a group conversation (owner only).
     */
    suspend fun deleteConversation(conversationId: String) {
        val payload = buildJsonObject { put("conversationId", conversationId) }
        ipcBridge.sendRequest("conversation.delete", payload)
        refreshConversations()
    }

    /**
     * Remove a conversation locally (works for both direct and group).
     */
    suspend fun removeConversation(conversationId: String) {
        val payload = buildJsonObject { put("conversationId", conversationId) }
        ipcBridge.sendRequest("conversation.remove", payload)
        refreshConversations()
    }

    /**
     * Rename a group conversation.
     */
    suspend fun renameGroup(conversationId: String, name: String) {
        val payload = buildJsonObject {
            put("conversationId", conversationId)
            put("name", name)
        }
        ipcBridge.sendRequest("conversation.rename", payload)
        refreshConversations()
    }

    /**
     * Add a member to a group conversation.
     */
    suspend fun addMember(conversationId: String, publicKey: String, displayName: String? = null) {
        val payload = buildJsonObject {
            put("conversationId", conversationId)
            put("publicKey", publicKey)
            displayName?.let { put("displayName", it) }
        }
        ipcBridge.sendRequest("conversation.add_member", payload)
        refreshConversations()
    }

    // ── Message Management ──────────────────────────────────────────────

    /**
     * Send a text message.
     *
     * @param conversationId Target conversation
     * @param content Message text
     * @param contentType MIME type (default "text/plain")
     * @return The sent message
     */
    suspend fun sendMessage(
        conversationId: String,
        content: String,
        contentType: String = "text/plain",
        replyToId: String? = null,
        replyToSenderName: String? = null,
        replyToContent: String? = null
    ): ZMMessage {
        requireIdentity()
        ensureConversationConnected(conversationId)

        val payload = buildJsonObject {
            put("conversationId", conversationId)
            put("content", content)
            put("contentType", contentType)
            replyToId?.let { put("replyToId", it) }
            replyToSenderName?.let { put("replyToSenderName", it) }
            replyToContent?.let { put("replyToContent", it) }
        }

        val response = ipcBridge.sendRequest("message.send", payload)
        return parsePersistedMessage(response)
    }

    /**
     * Mark a conversation as read and send a read receipt to the other party,
     * so our outgoing messages get a read tick on their device. No-op when the
     * peer has nothing newer to acknowledge or read receipts are disabled.
     */
    suspend fun markRead(conversationId: String) {
        val payload = buildJsonObject { put("conversationId", conversationId) }
        ipcBridge.sendRequest("message.mark_read", payload)
    }

    /**
     * Enable or disable read receipts. Symmetric: when off, we neither send our
     * own read acks nor surface other people's.
     */
    suspend fun setReadReceiptsEnabled(enabled: Boolean) {
        val payload = buildJsonObject { put("enabled", enabled) }
        ipcBridge.sendRequest("message.set_read_receipts", payload)
    }

    /**
     * Show or hide our online/connected status to peers. When hidden, peers stop
     * seeing us as online — we ask them to retract the dot our live socket lights
     * up. Reciprocity (not seeing others while hidden) is enforced app-side.
     */
    suspend fun setPresenceVisible(visible: Boolean) {
        val payload = buildJsonObject { put("enabled", visible) }
        ipcBridge.sendRequest("message.set_presence_visible", payload)
    }

    /**
     * Get messages for a conversation.
     *
     * @param conversationId Target conversation
     * @param limit Maximum number of messages to return
     * @return List of messages
     */
    suspend fun getMessages(conversationId: String, limit: Int = 50): List<ZMMessage> {
        val payload = buildJsonObject {
            put("conversationId", conversationId)
            put("limit", limit)
        }

        val response = ipcBridge.sendRequest("message.list", payload)
        val msgArray = response["messages"]?.jsonArray ?: return emptyList()
        val messages = msgArray.mapNotNull { ipcBridge.parseMessage(it.jsonObject) }
        return hydrateThumbnails(conversationId, messages, msgArray)
    }

    /**
     * message.list omits inline base64 thumbnails (50 of them can push the
     * response past the IPC receive cap). Re-fetch each stripped thumbnail via
     * message.get_thumbnail with bounded concurrency, so the returned model is
     * identical to the pre-strip behaviour and callers need no changes.
     */
    private suspend fun hydrateThumbnails(
        conversationId: String,
        messages: List<ZMMessage>,
        rawArray: JsonArray
    ): List<ZMMessage> {
        val needsThumbnail = rawArray.mapNotNull { element ->
            val obj = element as? JsonObject ?: return@mapNotNull null
            val hasThumb = obj["hasThumbnail"]?.jsonPrimitive?.booleanOrNull ?: false
            val id = obj["id"]?.jsonPrimitive?.contentOrNull
            if (hasThumb && id != null) id else null
        }.toSet()
        if (needsThumbnail.isEmpty()) return messages

        val fetched = coroutineScope {
            val gate = Semaphore(THUMBNAIL_FETCH_CONCURRENCY)
            needsThumbnail.map { id ->
                async { id to gate.withPermit { fetchThumbnail(conversationId, id) } }
            }.awaitAll()
        }.toMap()

        return messages.map { message ->
            if (message.id in needsThumbnail) {
                message.copy(thumbnailData = fetched[message.id])
            } else {
                message
            }
        }
    }

    private suspend fun fetchThumbnail(conversationId: String, messageId: String): String? {
        return try {
            val payload = buildJsonObject {
                put("conversationId", conversationId)
                put("messageId", messageId)
            }
            val response = ipcBridge.sendRequest("message.get_thumbnail", payload)
            response["thumbnailData"]?.jsonPrimitive?.contentOrNull
        } catch (_: Exception) {
            ZMLog.warning(TAG) { "Thumbnail fetch failed" }
            null
        }
    }

    /**
     * Send a media message (photo, GIF, etc.).
     *
     * @param conversationId Target conversation
     * @param mediaPath Local file path to the media
     * @param contentType MIME type of the media
     * @param caption Optional caption text
     * @return The sent message
     */
    suspend fun sendMediaMessage(
        conversationId: String,
        mediaPath: String,
        contentType: String,
        caption: String = "",
        thumbnailData: String? = null,
        replyToId: String? = null,
        replyToSenderName: String? = null,
        replyToContent: String? = null
    ): ZMMessage {
        requireIdentity()
        ensureConversationConnected(conversationId)

        // Step 1: Prepare media (hash and store)
        val extension = contentType.substringAfterLast("/", "jpg")
        val preparePayload = buildJsonObject {
            put("filePath", mediaPath)
            put("extension", extension)
        }
        val prepareResponse = ipcBridge.sendRequest("media.prepare_send", preparePayload)

        val mediaId = prepareResponse["mediaId"]?.jsonPrimitive?.contentOrNull
            ?: throw ZMError.MediaError("Failed to prepare media — missing mediaId")
        val mediaSize = prepareResponse["mediaSize"]?.jsonPrimitive?.intOrNull
            ?: throw ZMError.MediaError("Failed to prepare media — missing mediaSize")
        val mediaLocalPath = prepareResponse["mediaLocalPath"]?.jsonPrimitive?.contentOrNull
            ?: throw ZMError.MediaError("Failed to prepare media — missing mediaLocalPath")

        // Step 2: Send media message
        val sendPayload = buildJsonObject {
            put("conversationId", conversationId)
            put("content", caption)
            put("contentType", contentType)
            put("mediaId", mediaId)
            put("mediaSize", mediaSize)
            put("mediaLocalPath", mediaLocalPath)
            thumbnailData?.let { put("thumbnailData", it) }
            replyToId?.let { put("replyToId", it) }
            replyToSenderName?.let { put("replyToSenderName", it) }
            replyToContent?.let { put("replyToContent", it) }
        }

        val response = ipcBridge.sendRequest("media.send_message", sendPayload)
        return parsePersistedMessage(response)
    }

    /**
     * The message the core persisted is the only one whose id later receipts,
     * replies and history refer to. A successful response without one is a
     * contract violation, not something to paper over with a local stand-in.
     */
    private fun parsePersistedMessage(response: JsonObject): ZMMessage {
        val msgData = response["message"]?.jsonObject
            ?: throw ZMError.InvalidData("Missing message in response")
        return ipcBridge.parseMessage(msgData)
            ?: throw ZMError.InvalidData("Invalid message data")
    }

    // ── Contact Management ──────────────────────────────────────────────

    /**
     * Add a contact.
     */
    suspend fun addContact(publicKey: String, name: String) {
        requireIdentity()
        val payload = buildJsonObject {
            put("publicKey", publicKey)
            put("name", name)
        }
        ipcBridge.sendRequest("contacts.add", payload)
        refreshContacts()
    }

    /**
     * Refresh contacts from core.
     */
    suspend fun refreshContacts() {
        val generation = accountGeneration.get()
        val response = ipcBridge.sendRequest("contacts.list")
        val contactArray = response["contacts"]?.jsonArray ?: return
        val contacts = contactArray.mapNotNull { ipcBridge.parseContact(it.jsonObject) }
        if (accountGeneration.get() == generation) _contacts.value = contacts
    }

    /**
     * Update a contact's name.
     */
    suspend fun updateContact(publicKey: String, name: String) {
        val payload = buildJsonObject {
            put("publicKey", publicKey)
            put("updates", buildJsonObject { put("name", name) })
        }
        ipcBridge.sendRequest("contacts.update", payload)
        refreshContacts()
    }

    /**
     * Update a contact's wallet address.
     */
    suspend fun updateContactWalletAddress(publicKey: String, walletAddress: String) {
        val payload = buildJsonObject {
            put("publicKey", publicKey)
            put("walletAddress", walletAddress)
        }
        ipcBridge.sendRequest("contacts.updateWalletAddress", payload)
        refreshContacts()
    }

    /**
     * Delete a contact.
     */
    suspend fun deleteContact(publicKey: String) {
        val payload = buildJsonObject { put("publicKey", publicKey) }
        ipcBridge.sendRequest("contacts.remove", payload)
        refreshContacts()
    }

    // ── Payment Messages ────────────────────────────────────────────────

    /**
     * Send a payment-related message (transaction, request, or wallet address).
     *
     * @param conversationId Target conversation
     * @param message The structured message payload
     * @param type The payment message type
     * @return The sent message
     */
    suspend fun sendPaymentMessage(
        conversationId: String,
        message: JsonObject,
        type: PaymentMessageType
    ): ZMMessage {
        requireIdentity()
        ensureConversationConnected(conversationId)

        val payload = buildJsonObject {
            put("conversationId", conversationId)
            put("message", message)
        }

        val response = ipcBridge.sendRequest(type.ipcType, payload)
        return parsePersistedMessage(response)
    }

    // ── Connection Management ───────────────────────────────────────────

    /**
     * Get current connection status.
     *
     * @return Pair of (isOnline, peerCount)
     */
    suspend fun getConnectionStatus(): Pair<Boolean, Int> {
        val response = ipcBridge.sendRequest("connection.status")
        val online = response["online"]?.jsonPrimitive?.booleanOrNull ?: false
        val peers = response["peerCount"]?.jsonPrimitive?.intOrNull ?: 0

        _isOnline.value = online
        _peerCount.value = peers

        return Pair(online, peers)
    }

    /**
     * Get detailed connection info for diagnostics display.
     */
    suspend fun getConnectionDetails(): ConnectionDetails {
        val response = ipcBridge.sendRequest("connection.details")
        return ConnectionDetails(
            online = response["online"]?.jsonPrimitive?.booleanOrNull ?: false,
            peerCount = response["peerCount"]?.jsonPrimitive?.intOrNull ?: 0,
            globalConnections = response["globalConnections"]?.jsonPrimitive?.intOrNull ?: 0,
            pendingQueues = response["pendingQueues"]?.jsonPrimitive?.intOrNull ?: 0,
            pendingMessageCount = response["pendingMessageCount"]?.jsonPrimitive?.intOrNull ?: 0,
            pendingInvites = response["pendingInvites"]?.jsonPrimitive?.intOrNull ?: 0,
            dhtHealth = response["dhtHealth"]?.jsonPrimitive?.contentOrNull ?: "unknown",
            dhtLastCheck = response["dhtLastCheck"]?.jsonPrimitive?.contentOrNull,
            consecutiveFailures = response["consecutiveFailures"]?.jsonPrimitive?.intOrNull ?: 0,
            directConversations = response["directConversations"]?.jsonPrimitive?.intOrNull ?: 0,
            groupConversations = response["groupConversations"]?.jsonPrimitive?.intOrNull ?: 0,
            dhtBootstrapped = response["dhtBootstrapped"]?.jsonPrimitive?.booleanOrNull ?: false,
            dhtFirewalled = response["dhtFirewalled"]?.jsonPrimitive?.booleanOrNull,
            dhtRandomized = response["dhtRandomized"]?.jsonPrimitive?.booleanOrNull,
            rtNodes = response["rtNodes"]?.jsonPrimitive?.intOrNull ?: 0,
            relayEnabled = response["relayEnabled"]?.jsonPrimitive?.booleanOrNull ?: false,
            relaysConnected = response["relaysConnected"]?.jsonPrimitive?.intOrNull ?: 0,
            relaysTotal = response["relaysTotal"]?.jsonPrimitive?.intOrNull ?: 0
        )
    }

    data class ConnectionDetails(
        val online: Boolean,
        val peerCount: Int,
        val globalConnections: Int,
        val pendingQueues: Int,
        val pendingMessageCount: Int,
        val pendingInvites: Int,
        val dhtHealth: String,
        val dhtLastCheck: String?,
        val consecutiveFailures: Int,
        val directConversations: Int,
        val groupConversations: Int,
        val dhtBootstrapped: Boolean,
        val dhtFirewalled: Boolean?,
        val dhtRandomized: Boolean?,
        val rtNodes: Int,
        val relayEnabled: Boolean,
        val relaysConnected: Int,
        val relaysTotal: Int
    )

    private suspend fun ensureConversationConnected(conversationId: String) {
        val payload = buildJsonObject { put("conversationId", conversationId) }
        ipcBridge.sendRequest("connection.connect", payload)
    }

    /**
     * Register a Web-Push subscription with the blind peer so it can
     * doorbell-wake this device when a new message arrives for our identity.
     * The blind peer stores it keyed by our identity (the referrer).
     *
     * @return true if at least one blind peer accepted the registration
     */
    suspend fun registerPushEndpoint(endpoint: String, p256dh: String, auth: String): Boolean {
        val payload = buildJsonObject {
            put("endpoint", endpoint)
            put("p256dh", p256dh)
            put("auth", auth)
        }
        val response = ipcBridge.sendRequest("push.register_endpoint", payload)
        return response["ok"]?.jsonPrimitive?.booleanOrNull ?: false
    }

    /**
     * Snapshot of direct-chat inbound FCM topics. The JavaScript core is the
     * sole authority for discovery-key derivation; callers must reconcile this
     * complete snapshot and unsubscribe entries that disappear.
     */
    suspend fun getPushTopicSnapshot(): ZMPushTopicSnapshot {
        val response = ipcBridge.sendRequest("push.topics")
        val conversations =
            try {
                response["conversations"]?.jsonArray?.map { element ->
                    val value = element.jsonObject
                    val conversationId =
                        value["conversationId"]?.jsonPrimitive?.contentOrNull
                            ?: throw ZMError.InvalidData("push topic conversation is missing conversationId")
                    val lifecycle =
                        value["lifecycle"]?.jsonPrimitive?.contentOrNull
                            ?: throw ZMError.InvalidData("push topic conversation is missing lifecycle")
                    val topics =
                        value["inboundTopics"]?.jsonArray?.map { topicElement ->
                            val topic = topicElement.jsonObject
                            val name =
                                topic["topic"]?.jsonPrimitive?.contentOrNull
                                    ?: throw ZMError.InvalidData("push topic entry is missing topic")
                            val writer =
                                topic["writerPublicKey"]?.jsonPrimitive?.contentOrNull
                                    ?: throw ZMError.InvalidData("push topic entry is missing writerPublicKey")
                            ZMPushTopic(topic = name, writerPublicKey = writer)
                        } ?: throw ZMError.InvalidData("push topic conversation is missing inboundTopics")
                    ZMPushConversationTopics(
                        conversationId = conversationId,
                        lifecycle = lifecycle,
                        inboundTopics = topics,
                    )
                } ?: throw ZMError.InvalidData("push topic snapshot is missing conversations")
            } catch (error: ZMError.InvalidData) {
                throw error
            } catch (error: IllegalArgumentException) {
                throw ZMError.InvalidData("malformed push topic snapshot")
            }
        return ZMPushTopicSnapshot(
            version = response["version"]?.jsonPrimitive?.intOrNull ?: 1,
            hydrated = response["hydrated"]?.jsonPrimitive?.booleanOrNull ?: false,
            supportsGroups = response["supportsGroups"]?.jsonPrimitive?.booleanOrNull ?: false,
            conversations = conversations,
        )
    }

    // ── Lifecycle ───────────────────────────────────────────────────────

    /**
     * Suspend SDK (for app backgrounding).
     */
    fun suspend() {
        workletManager.suspend()
    }

    /**
     * Resume SDK (from app backgrounding).
     */
    fun resume() {
        workletManager.resume()
        hostScope?.launch {
            try {
                getConnectionStatus()
            } catch (_: Exception) {
                ZMLog.warning(TAG) { "Connection refresh after resume failed" }
            }
        }
    }

    // ── Event Handling ──────────────────────────────────────────────────

    internal fun setupEventHandlers() {
        ipcBridge.onEvent { eventType, payload ->
            when (eventType) {
                "message.received" -> {
                    val eventConversationId = payload["conversationId"]?.jsonPrimitive?.contentOrNull
                    val msgData = payload["message"]?.jsonObject
                    if (eventConversationId != null && msgData != null) {
                        val message = ipcBridge.parseMessage(msgData)
                        if (message != null) {
                            _messageReceived.tryEmit(Pair(eventConversationId, message))
                        }
                    }
                }

                "push.topics_changed" -> {
                    _pushTopicsChanged.tryEmit(Unit)
                }

                "push.notification_failed" -> {
                    ZMLog.warning(TAG) { "Advisory blind-push request failed" }
                }

                "conversation.invite_received" -> {
                    val convData = payload["conversation"]?.jsonObject
                    if (convData != null) {
                        val conversation = ipcBridge.parseConversation(convData)
                        if (conversation != null) {
                            _inviteReceived.tryEmit(conversation)
                        }
                    }
                }

                "conversation.member_left" -> {
                    val conversationId = payload["conversationId"]?.jsonPrimitive?.contentOrNull
                    val leaverKey = payload["leaverKey"]?.jsonPrimitive?.contentOrNull
                        ?: payload["publicKey"]?.jsonPrimitive?.contentOrNull
                    if (conversationId != null && leaverKey != null) {
                        _memberLeft.tryEmit(Pair(conversationId, leaverKey))
                    }
                }

                "conversation.group_deleted" -> {
                    val conversationId = payload["conversationId"]?.jsonPrimitive?.contentOrNull
                    if (conversationId != null) {
                        _groupDeleted.tryEmit(conversationId)
                    }
                }

                "conversation.group_renamed" -> {
                    val conversationId = payload["conversationId"]?.jsonPrimitive?.contentOrNull
                    val newName = payload["newName"]?.jsonPrimitive?.contentOrNull
                    if (conversationId != null && newName != null) {
                        _groupRenamed.tryEmit(Pair(conversationId, newName))
                    }
                }

                "conversation.member_added" -> {
                    val conversationId = payload["conversationId"]?.jsonPrimitive?.contentOrNull
                    val newMemberKey = payload["newMemberKey"]?.jsonPrimitive?.contentOrNull
                    val newMemberName = payload["newMemberName"]?.jsonPrimitive?.contentOrNull
                        ?: newMemberKey?.take(8) ?: ""
                    if (conversationId != null && newMemberKey != null) {
                        _memberAdded.tryEmit(Triple(conversationId, newMemberKey, newMemberName))
                    }
                }

                "message.status" -> {
                    val messageId = payload["messageId"]?.jsonPrimitive?.contentOrNull
                    val conversationId = payload["conversationId"]?.jsonPrimitive?.contentOrNull
                    val status = payload["status"]?.jsonPrimitive?.contentOrNull
                    if (messageId != null && conversationId != null && status != null) {
                        _messageStatus.tryEmit(Triple(messageId, conversationId, status))
                    }
                }

                "connection.status" -> {
                    val online = payload["online"]?.jsonPrimitive?.booleanOrNull
                    val peers = payload["peerCount"]?.jsonPrimitive?.intOrNull
                    if (online != null) _isOnline.value = online
                    if (peers != null) _peerCount.value = peers
                }

                "connection.dht_health" -> {
                    val status = payload["status"]?.jsonPrimitive?.contentOrNull
                    if (status != null) _dhtHealth.value = status
                }

                "connection.peer_status" -> {
                    val conversationId = payload["conversationId"]?.jsonPrimitive?.contentOrNull
                    val peerId = payload["peerId"]?.jsonPrimitive?.contentOrNull
                    val status = payload["status"]?.jsonPrimitive?.contentOrNull
                    if (conversationId != null && peerId != null && status != null) {
                        _peerStatus.tryEmit(Triple(conversationId, peerId, status))
                    }
                }

                // Both media events are inbound only: the core emits them as
                // received chunks land and once the stored file's hash verifies
                // (see core/API.md). Nothing is emitted for outbound sends.
                "media.transfer_complete" -> {
                    val mediaId = payload["mediaId"]?.jsonPrimitive?.contentOrNull
                    val mediaLocalPath = payload["mediaLocalPath"]?.jsonPrimitive?.contentOrNull
                    if (mediaId != null && mediaLocalPath != null) {
                        _mediaDownloadComplete.tryEmit(Pair(mediaId, mediaLocalPath))
                        _mediaTransferComplete.tryEmit(mediaId)
                    }
                }

                "media.transfer_progress" -> {
                    val mediaId = payload["mediaId"]?.jsonPrimitive?.contentOrNull
                    val progress = payload["progress"]?.jsonPrimitive?.doubleOrNull
                    if (mediaId != null && progress != null) {
                        _mediaDownloadProgress.tryEmit(Pair(mediaId, progress))
                        _mediaTransferProgress.tryEmit(Pair(mediaId, progress))
                    }
                }

                "platform.http_request" -> {
                    handlePlatformHttpRequest(payload)
                }

                "ipc.error" -> {
                    ZMLog.error(TAG) { "Worklet reported an IPC error" }
                }

                else -> {
                    ZMLog.debug(TAG) { "Unhandled event type received" }
                }
            }
        }
    }

    private fun handlePlatformHttpRequest(payload: JsonObject) {
        val requestId = payload["requestId"]?.jsonPrimitive?.contentOrNull ?: return
        val url = payload["url"]?.jsonPrimitive?.contentOrNull ?: return
        val body = payload["body"]?.jsonObject ?: return
        val scope = hostScope ?: return

        scope.launch {
            val result = runCatching { postPlatformJson(url, body) }
            val responsePayload = buildJsonObject {
                put("requestId", requestId)
                put("success", result.isSuccess)
                result.getOrNull()?.let { put("response", it) }
                result.exceptionOrNull()?.let { error ->
                    val detail = generateSequence(error) { it.cause }
                        .joinToString(": ") { it.message ?: it::class.java.simpleName }
                    put("error", detail)
                }
            }
            runCatching {
                ipcBridge.sendRequest(
                    type = "platform.http_response",
                    payload = responsePayload,
                    timeoutMs = PLATFORM_HTTP_RESPONSE_TIMEOUT_MS,
                )
            }.onFailure {
                ZMLog.warning(TAG) { "Platform HTTPS response delivery failed" }
            }
        }
    }

    private fun postPlatformJson(urlString: String, body: JsonObject): JsonObject {
        val configuredBase = BuildConfig.INVITE_MAILBOX_URL.trimEnd('/')
        require(configuredBase.isNotEmpty() && urlString.startsWith("$configuredBase/")) {
            "Platform HTTPS URL is outside the configured invite mailbox"
        }
        val url = URL(urlString)
        require(url.protocol == "https") { "Platform mailbox transport requires HTTPS" }
        val bytes = body.toString().toByteArray(Charsets.UTF_8)
        val connectivityManager = applicationContext?.getSystemService(ConnectivityManager::class.java)
        val connection = (
            connectivityManager?.activeNetwork?.openConnection(url)
                ?: url.openConnection()
            ) as HttpURLConnection
        connection.apply {
            requestMethod = "POST"
            connectTimeout = PLATFORM_HTTP_TIMEOUT_MS
            readTimeout = PLATFORM_HTTP_TIMEOUT_MS
            doOutput = true
            setRequestProperty("Content-Type", "application/json")
            setFixedLengthStreamingMode(bytes.size)
        }

        return try {
            connection.outputStream.use { it.write(bytes) }
            val status = connection.responseCode
            val stream = if (status in 200..299) connection.inputStream else connection.errorStream
            val responseBytes = stream?.use { input ->
                val output = ByteArrayOutputStream()
                val buffer = ByteArray(8192)
                while (true) {
                    val count = input.read(buffer)
                    if (count == -1) break
                    output.write(buffer, 0, count)
                    require(output.size() <= PLATFORM_HTTP_MAX_RESPONSE_BYTES) {
                        "Platform HTTPS response too large"
                    }
                }
                output.toByteArray()
            } ?: ByteArray(0)
            require(status in 200..299) { "Platform HTTPS status $status" }
            if (responseBytes.isEmpty()) {
                JsonObject(emptyMap())
            } else {
                Json.parseToJsonElement(String(responseBytes, Charsets.UTF_8)).jsonObject
            }
        } finally {
            connection.disconnect()
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private fun requireIdentity() {
        if (_identity.value == null) throw ZMError.IdentityNotFound()
    }

    companion object {
        private const val TAG = "ZappMessagingSDK"
        private const val DHT_HEALTH_DEFAULT = "healthy"
        /** Max concurrent message.get_thumbnail fetches while hydrating a list. */
        private const val THUMBNAIL_FETCH_CONCURRENCY = 6
        private const val WORKLET_STARTUP_PROBE_DELAY_MS = 300L
        private const val WORKLET_STARTUP_MAX_RETRIES = 5
        private const val PLATFORM_HTTP_TIMEOUT_MS = 15_000
        private const val PLATFORM_HTTP_RESPONSE_TIMEOUT_MS = 5_000L
        private const val PLATFORM_HTTP_MAX_RESPONSE_BYTES = 256 * 1024
    }
}

/**
 * Payment message types for structured payment messages over chat.
 */
enum class PaymentMessageType(val ipcType: String) {
    TRANSACTION("message.send_transaction"),
    PAYMENT_REQUEST("message.send_payment_request"),
    WALLET_ADDRESS("message.send_wallet_address")
}
