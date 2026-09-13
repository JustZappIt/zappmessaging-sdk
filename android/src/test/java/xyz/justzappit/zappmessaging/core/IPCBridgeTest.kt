package xyz.justzappit.zappmessaging.core

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import xyz.justzappit.zappmessaging.models.ZMError
import java.nio.charset.StandardCharsets

@OptIn(ExperimentalCoroutinesApi::class)
class IPCBridgeTest {

    /** Records every request line so a test can answer it. */
    private class RecordingTransport : IPCTransport {
        val requests = mutableListOf<JsonObject>()
        override suspend fun sendData(data: ByteArray) {
            val line = String(data, StandardCharsets.UTF_8).trimEnd('\n')
            requests += kotlinx.serialization.json.Json.parseToJsonElement(line).jsonObject
        }
    }

    private fun bytes(text: String) = text.toByteArray(StandardCharsets.UTF_8)

    // ── Contact parsing (finding 4) ──────────────────────────────────────

    @Test
    fun parseContactKeepsWalletFields() {
        val bridge = IPCBridge()
        // The same fixture the Swift parser test uses.
        val contact = bridge.parseContact(
            buildJsonObject {
                put("publicKey", "ab".repeat(32))
                put("name", "Alice")
                put("addedAt", 1_700_000_000_000L)
                put("walletAddress", "u1" + "q".repeat(40))
                put("addressType", "unified")
            }
        )
        assertNotNull(contact)
        assertEquals("u1" + "q".repeat(40), contact!!.walletAddress)
        assertEquals("unified", contact.addressType)
        assertEquals(1_700_000_000_000L, contact.addedAt)
    }

    @Test
    fun parseContactWithoutWalletFieldsLeavesThemNull() {
        val contact = IPCBridge().parseContact(
            buildJsonObject {
                put("publicKey", "ab".repeat(32))
                put("name", "Alice")
            }
        )
        assertNotNull(contact)
        assertNull(contact!!.walletAddress)
        assertNull(contact.addressType)
        assertNull(IPCBridge().parseContact(buildJsonObject { put("publicKey", "x") }))
    }

    // ── Request lifecycle (finding 7) ────────────────────────────────────

    @Test
    fun cancelledRequestLeavesNoPendingEntry() = runTest {
        val bridge = IPCBridge()
        bridge.setTransport(RecordingTransport())
        bridge.beginSession()

        val job = launch { bridge.sendRequest("identity.get", timeoutMs = 30_000) }
        runCurrent()
        assertEquals(1, bridge.pendingRequestCount)

        job.cancel()
        job.join()
        assertEquals(0, bridge.pendingRequestCount)
    }

    @Test
    fun timedOutRequestFailsWithIpcTimeoutAndLeavesNoPendingEntry() = runTest {
        val bridge = IPCBridge()
        bridge.setTransport(RecordingTransport())
        bridge.beginSession()

        val request = async { runCatching { bridge.sendRequest("identity.get", timeoutMs = 200) } }
        runCurrent()
        assertEquals(1, bridge.pendingRequestCount)

        advanceTimeBy(250)
        runCurrent()
        assertTrue(request.await().exceptionOrNull() is ZMError.IpcTimeout)
        assertEquals(0, bridge.pendingRequestCount)
    }

    @Test
    fun responseCompletesTheMatchingRequest() = runTest {
        val bridge = IPCBridge()
        val transport = RecordingTransport()
        bridge.setTransport(transport)
        val session = bridge.beginSession()

        val request = async { bridge.sendRequest("identity.get") }
        runCurrent()
        val id = transport.requests.single()["id"]!!.jsonPrimitive.content

        bridge.handleIncomingData(bytes("""{"id":"$id","success":true,"data":{"publicKey":"pk"}}""" + "\n"), session)
        assertEquals("pk", request.await()["publicKey"]!!.jsonPrimitive.content)
        assertEquals(0, bridge.pendingRequestCount)
    }

    // ── Transport sessions (finding 8) ───────────────────────────────────

    @Test
    fun newSessionDropsPartialFrameFromPreviousStream() {
        val bridge = IPCBridge()
        val received = mutableListOf<String>()
        bridge.onEvent { type, _ -> received += type }

        val first = bridge.beginSession()
        bridge.handleIncomingData(bytes("""{"type":"""), first)

        val second = bridge.beginSession()
        val event = bytes("""{"type":"test.event","payload":{},"timestamp":1}""" + "\n")
        bridge.handleIncomingData(event, second)
        assertEquals(listOf("test.event"), received)
    }

    @Test
    fun bytesFromAStoppedReaderAreIgnored() {
        val bridge = IPCBridge()
        val received = mutableListOf<String>()
        bridge.onEvent { type, _ -> received += type }

        val stale = bridge.beginSession()
        val live = bridge.beginSession()
        val event = """{"type":"test.event","payload":{},"timestamp":1}""" + "\n"

        bridge.handleIncomingData(bytes(event), stale)
        assertTrue(received.isEmpty())
        // A stale partial prefix must not poison the live stream either.
        bridge.handleIncomingData(bytes("""{"type":"""), stale)
        bridge.handleIncomingData(bytes(event), live)
        assertEquals(listOf("test.event"), received)
    }

    @Test
    fun beginSessionFailsInFlightRequests() = runTest {
        val bridge = IPCBridge()
        bridge.setTransport(RecordingTransport())
        bridge.beginSession()

        val request = async { runCatching { bridge.sendRequest("identity.get") } }
        runCurrent()
        bridge.beginSession()
        assertTrue(request.await().exceptionOrNull() is ZMError.NotInitialized)
        assertEquals(0, bridge.pendingRequestCount)
    }
}
