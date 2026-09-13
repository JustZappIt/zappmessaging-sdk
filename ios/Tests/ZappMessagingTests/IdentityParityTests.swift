//
//  IdentityParityTests.swift
//  ZappMessagingTests
//
//  THE cross-platform parity test.
//
//  Chat identity is derived from the BIP-39 wallet seed *inside the JS worklet*
//  (core/lib/identity.js), never in Swift or Kotlin. Both platforms pack the
//  same core/index.js, so the derived Ed25519 key should be identical. That is
//  an argument, not a proof. This is the proof.
//
//  The expected pubkeys below are produced by the JS oracle:
//      cd .. && npm run test:identity        (test/identity-golden-vectors.js)
//
//  If this test fails, the bug is in this platform's bundle/addon linkage —
//  NOT in the protocol. Do not "fix" it by changing the expected values.
//
//  Deliberately drives IPCBridge/BareWorkletManager directly rather than
//  ZappMessagingSDK, to keep the derivation assertion free of the facade's
//  conversation/contact refreshes. SDKContractTests covers the facade path.
//

import XCTest
@testable import ZappMessaging

final class IdentityParityTests: XCTestCase {

    private struct Vector {
        let name: String
        let phrase: String
        let publicKey: String
    }

    /// Standard BIP-39 (Trezor) 24-word vectors. Public, hold no funds.
    /// Zapp is 24-word only — 256-bit entropy, matching the Zcash seed.
    private static let vectors: [Vector] = [
        Vector(
            name: "all-zero entropy (0x00 * 32)",
            phrase: "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon "
                  + "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon art",
            publicKey: "7afa7190d9f5daeaa45d9650ed3ce7c0973bb0e35f7361bf858389a8cf1c3f3c"
        ),
        Vector(
            name: "all-ones entropy (0xff * 32)",
            phrase: "zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo vote",
            publicKey: "f3103bc0ea9cfb6fb1d0c9871ce3582384c4c533257f60a7f641403a66126997"
        ),
        Vector(
            name: "legal winner (0x7f * 32)",
            phrase: "legal winner thank year wave sausage worth useful legal winner thank year "
                  + "wave sausage worth useful legal winner thank year wave sausage worth title",
            publicKey: "244ec91ea2ae03216b6d7dd2a94ebaa9e152634b9e3a67e342b70111be031941"
        ),
        Vector(
            name: "letter advice (0x80 * 32)",
            phrase: "letter advice cage absurd amount doctor acoustic avoid letter advice cage absurd "
                  + "amount doctor acoustic avoid letter advice cage absurd amount doctor acoustic bless",
            publicKey: "b4a28893346e0b5399c55d8127982fa13818c8bbeb25c8490feeeef42a091951"
        )
    ]

    // MARK: - Harness

    /// Boot a worklet against a throwaway data dir, restore the phrase, return
    /// the derived hex pubkey, tear the worklet down.
    private func derivePublicKey(from phrase: String) async throws -> String {
        let dataDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("zm-parity-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dataDir, withIntermediateDirectories: true)

        let bridge = IPCBridge()
        let manager = BareWorkletManager()
        await bridge.setTransport(manager)

        // No blind-peer keys / bootstrap nodes: derivation is offline and must
        // not depend on the network. If it ever does, that is a bug worth failing on.
        let config = ZappMessagingConfig(dataDir: dataDir)
        try await manager.start(config: config, ipcBridge: bridge)

        defer {
            Task { await manager.stop() }
            try? FileManager.default.removeItem(at: dataDir)
        }

        // Let the JS side finish booting before the first request.
        try await Task.sleep(nanoseconds: 1_500_000_000)

        let response = try await bridge.sendRequest(
            type: "migration.restore_from_seed_phrase",
            payload: ["seedPhrase": phrase, "displayName": "parity-test"]
        )

        guard let publicKey = response["publicKey"] as? String else {
            XCTFail("worklet returned no publicKey; response = \(response)")
            return ""
        }
        return publicKey
    }

    // MARK: - Tests

    /// The one that matters. iOS must reproduce the JS oracle's keys exactly.
    func testWorkletReproducesGoldenVectors() async throws {
        for vector in Self.vectors {
            let derived = try await derivePublicKey(from: vector.phrase)

            XCTAssertEqual(
                derived,
                vector.publicKey,
                """
                CROSS-PLATFORM IDENTITY DIVERGENCE — \(vector.name)
                  expected (JS oracle / Android): \(vector.publicKey)
                  got      (iOS worklet):         \(derived)
                The iOS bundle or its addon linkage is wrong. Do not edit the expectation.
                """
            )
        }
    }

    /// A wrong-but-plausible derivation usually still returns *a* 32-byte key.
    /// Check the shape too, so a silent truncation can't pass as a mismatch we
    /// might be tempted to explain away.
    func testDerivedKeyIsWellFormed() async throws {
        let derived = try await derivePublicKey(from: Self.vectors[0].phrase)

        XCTAssertEqual(derived.count, 64, "Ed25519 pubkey must be 32 bytes = 64 hex chars")
        XCTAssertTrue(
            derived.allSatisfy { $0.isHexDigit },
            "pubkey must be hex, got: \(derived)"
        )
    }

    /// Derivation must be a pure function of the phrase. If a fresh install
    /// derives a different key, the user is unreachable at their old address.
    func testDerivationIsDeterministicAcrossFreshInstalls() async throws {
        let first = try await derivePublicKey(from: Self.vectors[0].phrase)
        let second = try await derivePublicKey(from: Self.vectors[0].phrase)

        XCTAssertEqual(first, second, "same phrase, fresh data dir, different key — derivation is not deterministic")
    }
}
