//
//  BareWorklet.swift
//  ZappMessaging
//
//  Real BareKit implementation for JavaScript worklet
//  Provides P2P messaging via Hyperswarm and Bare runtime
//
//  Uses the Obj-C BareKit.xcframework directly (BareWorklet, BareIPC classes)
//  with Swift async/await wrappers.
//

import Foundation
import BareKit

enum BareWorkletError: Error {
    case notStarted
    case alreadyStarted
    case workletNotInitialized
    case bundleNotFound
    case startupFailed(String)
    case ipcNotAvailable
    case writeFailed(Error)
    case ipcEnded
    case ipcBufferOverflow
    case ipcMessageTooLarge(Int)
}

/// BareKit's synchronous `read()` returns nil for EAGAIN/would-block and an
/// empty `NSData` for a successful zero-byte read (EOF). This mirrors the
/// upstream Objective-C implementation and its `worklet-ipc-eof` test.
enum BareIPCReadResult: Equatable {
    case wouldBlock
    case data(Data)
    case endOfStream

    static func classify(_ data: Data?) -> Self {
        guard let data else { return .wouldBlock }
        return data.isEmpty ? .endOfStream : .data(data)
    }
}

/// Worklet initialization state machine
enum WorkletState {
    case notStarted
    case starting
    case running
    case suspended
    case failed(Error)
}

/// Manages a Bare runtime worklet and its IPC channel.
/// Wraps the Obj-C BareWorklet/BareIPC classes with async Swift APIs.
class ZMBareWorklet {
    private var worklet: BareWorklet?
    private var ipc: BareIPC?
    private var isStarted = false
    private var listenerTask: Task<Void, Never>?
    private let stateLock = NSLock()
    private var isStopping = false

    /// Current worklet state (state machine)
    private var state: WorkletState = .notStarted

    var onIPC: (@Sendable (Data) async -> Void)?
    var onTerminalFailure: ((Error) -> Void)?

    /// Public state getter for external monitoring
    var currentState: WorkletState {
        stateLock.withLock { state }
    }
    
    init() {
        let config = BareWorkletConfiguration.default()
        config?.memoryLimit = 128 * 1024 * 1024
        self.worklet = BareWorklet(configuration: config)
        ZMLog.debug("BareWorklet", "Worklet initialized")
    }

    /// Verify bundle exists and is valid
    private func verifyBundle(at path: String) throws -> Bool {
        guard FileManager.default.fileExists(atPath: path) else {
            ZMLog.error("BareWorklet", "Worklet bundle not found")
            return false
        }

        // Verify bundle is not empty
        let attributes = try FileManager.default.attributesOfItem(atPath: path)
        guard let fileSize = attributes[.size] as? UInt64, fileSize > 0 else {
            ZMLog.error("BareWorklet", "Worklet bundle is empty")
            return false
        }

        ZMLog.debug("BareWorklet", "Worklet bundle verified")
        return true
    }

    /// Start the worklet.
    ///
    /// - Parameter arguments: argv for the JS side (see `ZappMessagingConfig`).
    ///   Passing an empty array makes the worklet fall back to its own defaults,
    ///   which on iOS means an iCloud-backed data dir and no offline delivery.
    func start(arguments: [String]) throws {
        // State validation: Check current state before starting
        let startingState = currentState
        guard case .notStarted = startingState else {
            if case .running = startingState {
                throw BareWorkletError.alreadyStarted
            } else if case .failed(let error) = startingState {
                throw BareWorkletError.startupFailed("Previous start failed: \(error.localizedDescription)")
            } else if case .starting = startingState {
                throw BareWorkletError.startupFailed("Worklet is already starting")
            }
            throw BareWorkletError.alreadyStarted
        }

        guard let worklet = worklet else {
            throw BareWorkletError.workletNotInitialized
        }

        // Find the worklet.bundle - it could be in:
        // 1. The ZappMessaging framework bundle (when used as a framework)
        // 2. The main app bundle (when running tests or standalone)
        let bundleName = "worklet"
        let bundleExt = "bundle"

        // When built by SPM the sources are linked statically into the app, so
        // `Bundle(for:)` resolves to `Bundle.main` and misses the nested
        // `ZappMessaging_ZappMessaging.bundle` that SPM puts resources in. Only
        // `Bundle.module` finds it. The XcodeGen framework build does not define
        // SWIFT_PACKAGE, so it keeps the original lookup.
        #if SWIFT_PACKAGE
        let frameworkBundle = Bundle.module
        #else
        let frameworkBundle = Bundle(for: ZMBareWorklet.self)
        #endif
        var targetBundle: Bundle = frameworkBundle
        var bundlePath: String?

        // Search for bundle
        if let path = frameworkBundle.path(forResource: bundleName, ofType: bundleExt) {
            bundlePath = path
            targetBundle = frameworkBundle
        } else if let path = Bundle.main.path(forResource: bundleName, ofType: bundleExt) {
            bundlePath = path
            targetBundle = Bundle.main
        }

        guard let finalBundlePath = bundlePath else {
            stateLock.withLock { state = .failed(BareWorkletError.bundleNotFound) }
            ZMLog.error("BareWorklet", "Worklet bundle unavailable")
            throw BareWorkletError.bundleNotFound
        }

        // Verify bundle is valid
        do {
            let isValid = try verifyBundle(at: finalBundlePath)
            guard isValid else {
                stateLock.withLock { state = .failed(BareWorkletError.bundleNotFound) }
                throw BareWorkletError.bundleNotFound
            }
        } catch {
            stateLock.withLock { state = .failed(error) }
            throw BareWorkletError.startupFailed("Bundle verification failed: \(error.localizedDescription)")
        }

        // Arguments include paths, network configuration and the identity-file
        // key. Never log argv, even with individual fields redacted.
        ZMLog.debug("BareWorklet", "Starting worklet")

        // PHASE 1: Transition to .starting state
        stateLock.withLock { state = .starting }
        ZMLog.debug("BareWorklet", "State changed to starting")

        // PHASE 2: Start the worklet BEFORE creating IPC.
        // bare_worklet_start() sets up the pipe FDs via a barrier.
        // If IPC is created before start(), dup(-1) fails and reads crash.
        worklet.start(bundleName, ofType: bundleExt, in: targetBundle, arguments: arguments)

        // Add small delay to ensure native barrier completes
        Thread.sleep(forTimeInterval: 0.1)

        // PHASE 3: Create IPC (now FDs are valid)
        guard let ipcChannel = BareIPC(worklet: worklet) else {
            stateLock.withLock { state = .failed(BareWorkletError.ipcNotAvailable) }
            ZMLog.error("BareWorklet", "IPC channel creation failed")
            throw BareWorkletError.ipcNotAvailable
        }
        stateLock.withLock { self.ipc = ipcChannel }
        ZMLog.debug("BareWorklet", "IPC channel created")

        // PHASE 4: Set up IPC listener
        // A peer can produce IPC faster than Swift parses it. Keep the queue bounded so
        // sustained traffic cannot grow into a jetsam crash. Dropping bytes would corrupt
        // framing, so overflow is terminal and goes through the existing clean restart path.
        let (stream, continuation) = AsyncStream<Data>.makeStream(
            bufferingPolicy: .bufferingOldest(16)
        )

        listenerTask = Task { [weak self] in
            defer {
                ZMLog.debug("BareWorklet", "IPC listener ended")
            }

            for await data in stream {
                guard !Task.isCancelled else { break }

                // An oversized chunk means framing or the core violated the IPC contract.
                // Continuing would parse the remaining stream at the wrong boundary.
                guard data.count <= 1_048_576 else {
                    ZMLog.warning("BareWorklet", "Oversized IPC message rejected")
                    self?.handleTerminalFailure(BareWorkletError.ipcMessageTooLarge(data.count))
                    break
                }

                if let onIPC = self?.onIPC {
                    await onIPC(data)
                }
            }
            ZMLog.debug("BareWorklet", "IPC listener stream ended")
        }

        // PHASE 5: Mark running before arming readability. BareKit may invoke
        // the callback immediately, including for EOF, and that failure must
        // not be lost in the startup window.
        stateLock.withLock {
            state = .running
            isStarted = true
        }
        ipcChannel.readable = { [weak self, weak ipcChannel] _ in
            guard let self, let ipc = ipcChannel else { return }
            while true {
                switch BareIPCReadResult.classify(ipc.read()) {
                case .wouldBlock:
                    return
                case .data(let data):
                    if case .dropped = continuation.yield(data) {
                        ipc.readable = nil
                        continuation.finish()
                        self.handleTerminalFailure(BareWorkletError.ipcBufferOverflow)
                        return
                    }
                case .endOfStream:
                    ipc.readable = nil
                    continuation.finish()
                    self.handleTerminalFailure(BareWorkletError.ipcEnded)
                    return
                }
            }
        }
        ZMLog.debug("BareWorklet", "Worklet running with IPC listener active")
    }
    
    /// Let the IPC thread unwind before terminating. `terminate()` races it, and
    /// the thread can SIGSEGV in `pthread_key_clean_all` touching freed TLS.
    /// Kotlin's BareWorkletManager sleeps the same 150ms for the same reason.
    private static let shutdownDrainMs: TimeInterval = 0.150

    func stop() {
        stateLock.lock()
        guard isStarted else {
            stateLock.unlock()
            return
        }
        isStopping = true
        stateLock.unlock()

        listenerTask?.cancel()
        listenerTask = nil

        let ipcToClose = stateLock.withLock { () -> BareIPC? in
            let value = ipc
            ipc = nil
            return value
        }
        ipcToClose?.readable = nil
        ipcToClose?.close()

        Thread.sleep(forTimeInterval: Self.shutdownDrainMs)

        worklet?.terminate()
        stateLock.withLock {
            isStarted = false
            isStopping = false
            state = .notStarted
        }

        ZMLog.debug("BareWorklet", "Worklet stopped")
    }
    
    func suspend() {
        let priorState = stateLock.withLock { () -> WorkletState in
            let value = state
            if case .running = value { state = .suspended }
            return value
        }
        guard case .running = priorState else {
            ZMLog.warning("BareWorklet", "Suspend ignored in current state")
            return
        }
        worklet?.suspend()
        ZMLog.debug("BareWorklet", "Worklet suspended")
    }

    func resume() {
        let priorState = stateLock.withLock { () -> WorkletState in
            let value = state
            if case .suspended = value { state = .running }
            return value
        }
        guard case .suspended = priorState else {
            ZMLog.warning("BareWorklet", "Resume ignored in current state")
            return
        }
        worklet?.resume()
        ZMLog.debug("BareWorklet", "Worklet resumed")
    }
    
    func sendIPC(_ data: Data) async throws {
        let activeIPC = stateLock.withLock { isStarted ? ipc : nil }
        guard let activeIPC else {
            throw BareWorkletError.ipcNotAvailable
        }
        
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            activeIPC.write(data) { error in
                if let error = error {
                    continuation.resume(throwing: BareWorkletError.writeFailed(error))
                } else {
                    continuation.resume()
                }
            }
        }
    }

    private func handleTerminalFailure(_ error: Error) {
        let shouldReport = stateLock.withLock { () -> Bool in
            guard isStarted, !isStopping else { return false }
            if case .failed = state { return false }
            state = .failed(error)
            return true
        }
        guard shouldReport else { return }
        ZMLog.error("BareWorklet", "Terminal IPC failure")
        onTerminalFailure?(error)
    }
}

private extension NSLock {
    func withLock<T>(_ body: () throws -> T) rethrows -> T {
        lock()
        defer { unlock() }
        return try body()
    }
}
