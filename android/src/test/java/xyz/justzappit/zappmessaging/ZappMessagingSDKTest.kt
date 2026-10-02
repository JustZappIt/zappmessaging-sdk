package xyz.justzappit.zappmessaging

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test
import xyz.justzappit.zappmessaging.core.BareWorkletManager
import xyz.justzappit.zappmessaging.core.IPCBridge
import xyz.justzappit.zappmessaging.core.IPCTransport
import xyz.justzappit.zappmessaging.models.ZMError
import xyz.justzappit.zappmessaging.models.ZMGroupJoinApprovalRequest
import xyz.justzappit.zappmessaging.models.ZMGroupJoinRequestStatus
import xyz.justzappit.zappmessaging.models.ZMGroupJoinStatus
import xyz.justzappit.zappmessaging.models.ZMGroupJoinUpdate
import xyz.justzappit.zappmessaging.models.ZMGroupLinkApproval
import xyz.justzappit.zappmessaging.models.ZMGroupLinkOptions
import xyz.justzappit.zappmessaging.models.ZMGroupLinkState
import java.nio.charset.StandardCharsets

/**
 * Drives the facade through a scripted worklet: each request type maps to a
 * response, and unmapped requests are parked so a test can answer them late.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class ZappMessagingSDKTest {

    private class ScriptedWorklet(private val bridge: IPCBridge) : IPCTransport {
        val session = bridge.beginSession()
        val responses = mutableMapOf<String, () -> JsonObject>()
        val failures = mutableSetOf<String>()
        val parked = mutableListOf<Pair<String, String>>() // (id, type)
        val sent = mutableListOf<Pair<String, JsonObject>>() // (type, payload)

        override suspend fun sendData(data: ByteArray) {
            val request = Json.parseToJsonElement(String(data, StandardCharsets.UTF_8).trimEnd('\n')).jsonObject
            val id = request["id"]!!.jsonPrimitive.content
            val type = request["type"]!!.jsonPrimitive.content
            sent += type to (request["payload"]?.jsonObject ?: JsonObject(emptyMap()))
            when {
                type in failures -> deliver("""{"id":"$id","success":false,"error":{"code":"ERROR","message":"scripted failure"}}""")
                type in responses -> deliver("""{"id":"$id","success":true,"data":${responses.getValue(type)()}}""")
                else -> parked += id to type
            }
        }

        fun answer(id: String, data: JsonObject) = deliver("""{"id":"$id","success":true,"data":$data}""")

        fun event(type: String, payload: JsonObject) =
            deliver("""{"type":"$type","payload":$payload,"timestamp":1}""")

        private fun deliver(line: String) =
            bridge.handleIncomingData((line + "\n").toByteArray(StandardCharsets.UTF_8), session)
    }

    private fun sdkWithScript(): Pair<ZappMessagingSDK, ScriptedWorklet> {
        val bridge = IPCBridge()
        val worklet = ScriptedWorklet(bridge)
        bridge.setTransport(worklet)
        val sdk = ZappMessagingSDK(bridge, BareWorkletManager())
        sdk.setupEventHandlers()
        return sdk to worklet
    }

    private fun identity(key: String) = buildJsonObject { put("publicKey", key); put("displayName", "Name") }

    private fun conversationList(vararg ids: String) = buildJsonObject {
        put("conversations", buildJsonArray {
            for (id in ids) add(buildJsonObject {
                put("id", id); put("type", "direct"); put("displayName", id)
                put("participantIds", buildJsonArray { add(kotlinx.serialization.json.JsonPrimitive("peer")) })
            })
        })
    }

    private fun contactList(vararg names: String) = buildJsonObject {
        put("contacts", buildJsonArray {
            for (name in names) add(buildJsonObject { put("publicKey", "ab".repeat(32)); put("name", name) })
        })
    }

    private fun <T> collectInto(scope: kotlinx.coroutines.CoroutineScope, flow: Flow<T>, into: MutableList<T>): Job =
        scope.launch(UnconfinedTestDispatcher()) { flow.collect { into += it } }

    // ── Identity boundary (finding 2) ────────────────────────────────────

    @Test
    fun createIdentityClearsThePreviousAccountsLists() = runTest {
        val (sdk, worklet) = sdkWithScript()
        worklet.responses["migration.restore_from_seed_phrase"] = { identity("aa") }
        worklet.responses["conversation.list"] = { conversationList("conv-a") }
        worklet.responses["contacts.list"] = { contactList("Alice") }
        sdk.restoreFromSeedPhrase("seed", "Name")
        assertEquals(listOf("conv-a"), sdk.conversations.value.map { it.id })
        assertEquals(listOf("Alice"), sdk.contacts.value.map { it.name })

        worklet.responses["identity.create"] = { buildJsonObject { put("publicKey", "bb"); put("displayName", "New"); put("seedPhrase", "w") } }
        val created = sdk.createIdentity("New")

        assertEquals("bb", created.identity.publicKey)
        assertEquals("bb", sdk.identity.value?.publicKey)
        assertTrue(sdk.conversations.value.isEmpty())
        assertTrue(sdk.contacts.value.isEmpty())
    }

    @Test
    fun restoreWithFailingRefreshPublishesNoStaleData() = runTest {
        val (sdk, worklet) = sdkWithScript()
        worklet.responses["migration.restore_from_seed_phrase"] = { identity("aa") }
        worklet.responses["conversation.list"] = { conversationList("conv-a") }
        worklet.responses["contacts.list"] = { contactList("Alice") }
        sdk.restoreFromSeedPhrase("seed-a", "Name")
        assertEquals(1, sdk.conversations.value.size)

        worklet.responses["migration.restore_from_seed_phrase"] = { identity("bb") }
        worklet.failures += "conversation.list"
        worklet.failures += "contacts.list"
        val restored = sdk.restoreFromSeedPhrase("seed-b", "Name")

        assertEquals("bb", restored.publicKey)
        assertTrue(sdk.conversations.value.isEmpty())
        assertTrue(sdk.contacts.value.isEmpty())
    }

    @Test
    fun sameIdentityRestoreKeepsTheLists() = runTest {
        val (sdk, worklet) = sdkWithScript()
        worklet.responses["migration.restore_from_seed_phrase"] = { identity("aa") }
        worklet.responses["conversation.list"] = { conversationList("conv-a") }
        worklet.responses["contacts.list"] = { contactList("Alice") }
        sdk.restoreFromSeedPhrase("seed", "Name")

        worklet.failures += "conversation.list"
        worklet.failures += "contacts.list"
        sdk.restoreFromSeedPhrase("seed", "Name")
        assertEquals(listOf("conv-a"), sdk.conversations.value.map { it.id })
        assertEquals(listOf("Alice"), sdk.contacts.value.map { it.name })
    }

    @Test
    fun aRefreshStartedUnderThePreviousIdentityCannotPublishAfterTheSwitch() = runTest {
        val (sdk, worklet) = sdkWithScript()
        worklet.responses["migration.restore_from_seed_phrase"] = { identity("aa") }
        worklet.responses["contacts.list"] = { contactList() }
        worklet.responses["conversation.list"] = { conversationList() }
        sdk.restoreFromSeedPhrase("seed-a", "Name")

        worklet.responses.remove("conversation.list")
        val staleRefresh = launch { sdk.refreshConversations() }
        runCurrent()
        val (staleId, staleType) = worklet.parked.single()
        assertEquals("conversation.list", staleType)

        worklet.responses["migration.restore_from_seed_phrase"] = { identity("bb") }
        worklet.responses["conversation.list"] = { conversationList() }
        sdk.restoreFromSeedPhrase("seed-b", "Name")

        worklet.answer(staleId, conversationList("old-account-conversation"))
        staleRefresh.join()
        assertTrue(sdk.conversations.value.isEmpty())
    }

    @Test
    fun shutdownReturnsEveryPublishedValueToItsInitialState() = runTest {
        val (sdk, worklet) = sdkWithScript()
        worklet.responses["migration.restore_from_seed_phrase"] = { identity("aa") }
        worklet.responses["conversation.list"] = { conversationList("conv-a") }
        worklet.responses["contacts.list"] = { contactList("Alice") }
        sdk.restoreFromSeedPhrase("seed", "Name")
        worklet.event("connection.status", buildJsonObject { put("online", true); put("peerCount", 3) })
        worklet.event("connection.dht_health", buildJsonObject { put("status", "critical") })
        assertTrue(sdk.isOnline.value)

        sdk.shutdown()

        assertNull(sdk.identity.value)
        assertTrue(sdk.conversations.value.isEmpty())
        assertTrue(sdk.contacts.value.isEmpty())
        assertEquals(false, sdk.isOnline.value)
        assertEquals(0, sdk.peerCount.value)
        assertEquals("healthy", sdk.dhtHealth.value)
    }

    // ── Persisted-message contract (finding 5) ───────────────────────────

    @Test
    fun mediaAndPaymentSendsRejectResponsesWithoutAPersistedMessage() = runTest {
        val (sdk, worklet) = sdkWithScript()
        worklet.responses["migration.restore_from_seed_phrase"] = { identity("aa") }
        worklet.responses["conversation.list"] = { conversationList() }
        worklet.responses["contacts.list"] = { contactList() }
        sdk.restoreFromSeedPhrase("seed", "Name")
        worklet.responses["connection.connect"] = { buildJsonObject {} }
        worklet.responses["media.prepare_send"] = {
            buildJsonObject { put("mediaId", "hash"); put("mediaSize", 10); put("mediaLocalPath", "/m/hash.jpg") }
        }
        worklet.responses["media.send_message"] = { buildJsonObject {} }
        worklet.responses["message.send_transaction"] = { buildJsonObject { put("message", buildJsonObject { put("id", "x") }) } }

        try {
            sdk.sendMediaMessage("conv", "/tmp/in.jpg", "image/jpeg")
            fail("expected InvalidData")
        } catch (error: ZMError.InvalidData) {
            assertTrue(error.message.contains("Missing message"))
        }
        try {
            sdk.sendPaymentMessage("conv", buildJsonObject { put("amount", "1") }, PaymentMessageType.TRANSACTION)
            fail("expected InvalidData")
        } catch (error: ZMError.InvalidData) {
            assertTrue(error.message.contains("Invalid message data"))
        }
    }

    // ── Media event direction (finding 6) ────────────────────────────────

    @Test
    fun inboundMediaEventsReachTheDownloadFlowsAndNothingElseIsFabricated() = runTest {
        val (sdk, worklet) = sdkWithScript()
        val downloadProgress = mutableListOf<Pair<String, Double>>()
        val transferProgress = mutableListOf<Pair<String, Double>>()
        val downloadComplete = mutableListOf<Pair<String, String>>()
        val transferComplete = mutableListOf<String>()
        val jobs = listOf(
            collectInto(this, sdk.mediaDownloadProgress, downloadProgress),
            collectInto(this, sdk.mediaTransferProgress, transferProgress),
            collectInto(this, sdk.mediaDownloadComplete, downloadComplete),
            collectInto(this, sdk.mediaTransferComplete, transferComplete),
        )

        worklet.event("media.transfer_progress", buildJsonObject { put("mediaId", "h1"); put("progress", 0.5) })
        worklet.event("media.transfer_complete", buildJsonObject {
            put("mediaId", "h1"); put("mediaLocalPath", "/m/h1.jpg"); put("mediaSize", 10)
        })
        // Names the core never emits must not conjure downloads.
        worklet.event("media.download_progress", buildJsonObject { put("mediaId", "h2"); put("progress", 0.9) })
        worklet.event("media.download_complete", buildJsonObject { put("mediaId", "h2"); put("filePath", "/m/h2.jpg") })
        // Completion without a stored path is not a completed download.
        worklet.event("media.transfer_complete", buildJsonObject { put("mediaId", "h3") })
        runCurrent()

        assertEquals(listOf("h1" to 0.5), downloadProgress)
        assertEquals(listOf("h1" to 0.5), transferProgress)
        assertEquals(listOf("h1" to "/m/h1.jpg"), downloadComplete)
        assertEquals(listOf("h1"), transferComplete)
        jobs.forEach { it.cancel() }
    }

    // ── Group invite links and removal ────────────────────────────────────

    private fun signedIn(): Pair<ZappMessagingSDK, ScriptedWorklet> {
        val (sdk, worklet) = sdkWithScript()
        worklet.responses["migration.restore_from_seed_phrase"] = { identity("aa") }
        worklet.responses["conversation.list"] = { conversationList() }
        worklet.responses["contacts.list"] = { contactList() }
        return sdk to worklet
    }

    @Test
    fun theOwnersLinkIsParsedAndNeverPrinted() = runTest {
        val (sdk, worklet) = signedIn()
        worklet.responses["group_link.enable"] = {
            buildJsonObject {
                put("conversationId", "g1"); put("state", "active"); put("link", "https://join.justzappit.xyz/g/v1#SECRET")
                put("linkId", "11"); put("joins", 2); put("maxJoins", 5); put("includeName", false)
                put("approval", "owner"); put("approvalReason", "rate"); put("pendingRequests", 3)
            }
        }
        val info = sdk.enableGroupLink("g1", ZMGroupLinkOptions(maxJoins = 5, approval = ZMGroupLinkApproval.OWNER, clearExpiry = true))

        assertEquals(ZMGroupLinkState.ACTIVE, info.state)
        assertEquals(ZMGroupLinkApproval.OWNER, info.approval)
        assertEquals(5, info.maxJoins)
        assertEquals(3, info.pendingRequests)
        assertTrue(!info.toString().contains("SECRET"))
        val payload = worklet.sent.last { it.first == "group_link.enable" }.second
        assertEquals("owner", payload["approval"]!!.jsonPrimitive.content)
        assertEquals("5", payload["maxJoins"]!!.jsonPrimitive.content)
        assertTrue(payload["expiresAt"] is kotlinx.serialization.json.JsonNull)
        assertNull(payload["includeName"])
    }

    @Test
    fun joiningReportsStatusAndNeedsAnIdentity() = runTest {
        val (sdk, worklet) = signedIn()
        try {
            sdk.joinGroupViaLink("https://join.justzappit.xyz/g/v1#x")
            fail("joining without an identity must fail")
        } catch (_: ZMError) {
        }
        sdk.restoreFromSeedPhrase("seed", "Name")
        worklet.responses["group_link.join"] = { buildJsonObject { put("status", "already_member"); put("linkId", "l1"); put("conversationId", "g1") } }
        val result = sdk.joinGroupViaLink("https://join.justzappit.xyz/g/v1#x", "Ana")
        assertEquals(ZMGroupJoinRequestStatus.ALREADY_MEMBER, result.status)
        assertEquals("g1", result.conversationId)
        assertEquals("Ana", worklet.sent.last().second["joinerName"]!!.jsonPrimitive.content)

        worklet.responses["group_link.join"] = { buildJsonObject { put("status", "something_new") } }
        assertEquals(ZMGroupJoinRequestStatus.MALFORMED, sdk.joinGroupViaLink("x").status)
    }

    @Test
    fun groupLinkAndRemovalEventsReachTheirFlows() = runTest {
        val (sdk, worklet) = signedIn()
        val updates = mutableListOf<ZMGroupJoinUpdate>()
        val requests = mutableListOf<ZMGroupJoinApprovalRequest>()
        val withdrawn = mutableListOf<Pair<String, String>>()
        val removed = mutableListOf<String>()
        val memberRemoved = mutableListOf<Pair<String, String>>()
        val jobs = listOf(
            collectInto(this, sdk.groupJoinUpdated, updates),
            collectInto(this, sdk.groupJoinRequestReceived, requests),
            collectInto(this, sdk.groupJoinRequestWithdrawn, withdrawn),
            collectInto(this, sdk.removedFromGroup, removed),
            collectInto(this, sdk.memberRemoved, memberRemoved),
        )
        worklet.event("group_link.join_updated", buildJsonObject { put("linkId", "l1"); put("status", "pending_approval") })
        worklet.event("group_link.join_updated", buildJsonObject { put("linkId", "l1"); put("status", "joined"); put("conversationId", "g1") })
        worklet.event("group_link.request_received", buildJsonObject { put("conversationId", "g1"); put("joinerKey", "cd".repeat(32)); put("joinerName", "Ben"); put("previouslyRemoved", true) })
        worklet.event("group_link.request_withdrawn", buildJsonObject { put("conversationId", "g1"); put("joinerKey", "cd") })
        worklet.event("conversation.removed_from_group", buildJsonObject { put("conversationId", "g2") })
        worklet.event("conversation.member_removed", buildJsonObject { put("conversationId", "g1"); put("removedKey", "ef") })

        assertEquals(listOf(ZMGroupJoinStatus.PENDING_APPROVAL, ZMGroupJoinStatus.JOINED), updates.map { it.status })
        assertTrue(updates.first().status.isWaiting)
        assertEquals("g1", updates.last().conversationId)
        assertEquals("Ben", requests.single().joinerName)
        assertTrue(requests.single().previouslyRemoved)
        assertEquals(listOf("g1" to "cd"), withdrawn)
        assertEquals(listOf("g2"), removed)
        assertEquals(listOf("g1" to "ef"), memberRemoved)
        jobs.forEach { it.cancel() }
    }

    @Test
    fun removingAMemberSendsTheKeyAndReportsOlderMembers() = runTest {
        val (sdk, worklet) = signedIn()
        worklet.responses["conversation.remove_member"] = {
            buildJsonObject {
                put("success", true); put("olderMemberCount", 2)
                put("participants", buildJsonArray { add(kotlinx.serialization.json.JsonPrimitive("aa")) })
            }
        }
        val result = sdk.removeMember("g1", "ef".repeat(32), resetLink = false)
        assertEquals(2, result.olderMemberCount)
        assertEquals(listOf("aa"), result.participants)
        val payload = worklet.sent.last { it.first == "conversation.remove_member" }.second
        assertEquals("false", payload["resetLink"]!!.jsonPrimitive.content)
        assertTrue("the list refreshes after a removal", worklet.sent.any { it.first == "conversation.list" })
    }

    @Test
    fun aRemovedGroupCarriesItsRemovalTime() = runTest {
        val (sdk, worklet) = signedIn()
        worklet.responses["conversation.list"] = {
            buildJsonObject {
                put("conversations", buildJsonArray {
                    add(buildJsonObject {
                        put("id", "g1"); put("type", "group"); put("displayName", "Crew"); put("removedAt", 1234)
                        put("participantIds", buildJsonArray { })
                    })
                })
            }
        }
        sdk.refreshConversations()
        assertEquals(1234L, sdk.conversations.value.single().removedAt)
    }
}
