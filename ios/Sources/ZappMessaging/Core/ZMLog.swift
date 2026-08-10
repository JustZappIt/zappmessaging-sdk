//
//  ZMLog.swift
//  ZappMessaging
//

import Foundation

/// Privacy-safe native diagnostics shared by the Swift wrapper.
///
/// Logging is disabled unless the host explicitly selects the `debug` worklet
/// log level. Callers must log structural metadata only: never payloads,
/// identifiers, addresses, filesystem paths, argv, keys, or error descriptions.
enum ZMLog {
    private static let lock = NSLock()
    private static var enabled = false

    static func configure(level: String?) {
        lock.withLock {
            enabled = level?.lowercased() == "debug"
        }
    }

    static func debug(_ component: String, _ message: @autoclosure () -> String) {
        write(component, message: message)
    }

    static func warning(_ component: String, _ message: @autoclosure () -> String) {
        write(component, message: message)
    }

    static func error(_ component: String, _ message: @autoclosure () -> String) {
        write(component, message: message)
    }

    private static func write(_ component: String, message: () -> String) {
        let shouldWrite = lock.withLock { enabled }
        guard shouldWrite else { return }
        print("[ZappMessaging][\(component)] \(message())")
    }
}
