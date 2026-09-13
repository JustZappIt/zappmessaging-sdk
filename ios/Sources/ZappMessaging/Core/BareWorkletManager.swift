//
//  BareWorkletManager.swift
//  ZappMessaging
//
//  Manages the BareKit JavaScript worklet lifecycle
//

import Foundation

/// Manages the JavaScript worklet lifecycle. Each worklet start opens a new
/// bridge session and each stop ends it, so bytes from a stopped reader can
/// never land in the next worklet's frame buffer.
actor BareWorkletManager: IPCTransport {
    private var worklet: ZMBareWorklet?
    private var isRunning = false
    private var ipcBridge: IPCBridge?
    private var config: ZappMessagingConfig?
    private var isSuspended = false
    private var isRecovering = false
    private var restartAttempts = 0
    private let maxRestartAttempts = 1
    private var failureHandler: (@Sendable (Error, Bool) -> Void)?
    private var lifecycleGeneration: UInt64 = 0
    
    init() {}
    
    // MARK: - Lifecycle
    
    /// Start the JavaScript worklet.
    ///
    /// `config` becomes the worklet's argv. Without it the JS falls back to an
    /// iCloud-backed data dir and runs with no blind-peer keys, which silently
    /// disables offline delivery. See `ZappMessagingConfig`.
    func start(config: ZappMessagingConfig, ipcBridge: IPCBridge) async throws {
        guard !isRunning, !isRecovering else { return }

        ZMLog.configure(level: config.logLevel)
        lifecycleGeneration &+= 1
        self.config = config
        self.ipcBridge = ipcBridge

        try await startWorklet(config: config, ipcBridge: ipcBridge)
        isRunning = true
        restartAttempts = 0
        if isSuspended { worklet?.suspend() }
        ZMLog.debug("BareWorkletManager", "Worklet started")
    }

    private func startWorklet(config: ZappMessagingConfig, ipcBridge: IPCBridge) async throws {
        let session = await ipcBridge.beginSession()

        // Create and start BareKit worklet
        let nextWorklet = ZMBareWorklet()
        worklet = nextWorklet

        // The reader presents the session it was started with; the bridge
        // drops anything from a session that has since ended.
        nextWorklet.onIPC = { [weak self] data in
            await self?.forwardIncomingData(data, session: session)
        }
        nextWorklet.onTerminalFailure = { [weak self] error in
            Task { await self?.recoverFromTerminalFailure(error) }
        }

        // Keychain-held key for encrypting identity.json (wallet entropy) at
        // rest. Secret: argv must never be logged unredacted once added.
        var arguments = config.argv
        if let keyHex = IdentityFileKeyStore.getOrCreateKeyHex() {
            arguments.append("--identity-file-key=\(keyHex)")
        } else {
            ZMLog.error("BareWorkletManager", "Identity encryption key unavailable")
        }

        try nextWorklet.start(arguments: arguments)
    }

    /// Serial actor delivery preserves the byte stream's order and avoids spawning one
    /// unbounded task per IPC read during media transfers.
    private func forwardIncomingData(_ data: Data, session: UInt64) async {
        await ipcBridge?.handleIncomingData(data, session: session)
    }
    
    /// Stop the JavaScript worklet. Ending the bridge session fails every
    /// in-flight request and discards any partial frame from the old stream.
    func stop() async {
        lifecycleGeneration &+= 1
        isRunning = false
        isRecovering = false
        
        worklet?.stop()
        worklet = nil
        
        _ = await ipcBridge?.beginSession()
        ipcBridge = nil
        config = nil
        isSuspended = false
        isRecovering = false
        restartAttempts = 0
        ZMLog.debug("BareWorkletManager", "Worklet stopped")
    }
    
    /// Suspend the worklet (for app backgrounding)
    func suspend() async {
        isSuspended = true
        guard isRunning, !isRecovering else { return }
        
        worklet?.suspend()
        
        ZMLog.debug("BareWorkletManager", "Worklet suspended")
    }
    
    /// Resume the worklet (from app backgrounding)
    func resume() async {
        isSuspended = false
        guard isRunning, !isRecovering else { return }
        
        worklet?.resume()
        
        ZMLog.debug("BareWorkletManager", "Worklet resumed")
    }
    
    // MARK: - State
    
    /// Check if worklet is running
    func getIsRunning() -> Bool {
        return isRunning
    }
    
    /// Send data to the worklet
    func sendData(_ data: Data) async throws {
        guard isRunning, let worklet else {
            throw ZMError.notInitialized
        }
        try await worklet.sendIPC(data)
    }

    func setFailureHandler(_ handler: @escaping @Sendable (Error, Bool) -> Void) {
        failureHandler = handler
    }

    private func recoverFromTerminalFailure(_ error: Error) async {
        guard isRunning, !isRecovering else { return }
        let recoveryGeneration = lifecycleGeneration
        isRecovering = true
        isRunning = false
        await ipcBridge?.cancelAllPendingRequests(error: error)

        guard lifecycleGeneration == recoveryGeneration, isRecovering else { return }

        worklet?.stop()
        worklet = nil

        guard restartAttempts < maxRestartAttempts,
              let config,
              let ipcBridge else {
            isRecovering = false
            failureHandler?(error, false)
            return
        }

        restartAttempts += 1
        ZMLog.warning("BareWorkletManager", "Restarting after terminal IPC failure")
        do {
            try await startWorklet(config: config, ipcBridge: ipcBridge)
            isRunning = true
            if isSuspended { worklet?.suspend() }
            try await ipcBridge.negotiateProtocol()
            guard lifecycleGeneration == recoveryGeneration, isRecovering else { return }
            isRecovering = false
            failureHandler?(error, true)
            ZMLog.debug("BareWorkletManager", "Worklet restart succeeded")
        } catch {
            worklet?.stop()
            worklet = nil
            isRunning = false
            isRecovering = false
            failureHandler?(error, false)
            ZMLog.error("BareWorkletManager", "Worklet restart exhausted")
        }
    }
}
