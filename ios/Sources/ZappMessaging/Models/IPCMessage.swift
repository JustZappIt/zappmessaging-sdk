//
//  IPCMessage.swift
//  ZappMessaging
//
//  IPC message structures for communication with JavaScript worklet
//

import Foundation

/// IPC request message
struct IPCRequest: Codable {
    let id: String
    let type: String
    let payload: [String: AnyCodable]
    
    init(id: String = UUID().uuidString, type: String, payload: [String: Any] = [:]) {
        self.id = id
        self.type = type
        self.payload = payload.mapValues { AnyCodable($0) }
    }
}

/// IPC response message
struct IPCResponse: Codable {
    let id: String
    let success: Bool
    let data: [String: AnyCodable]?
    let error: IPCErrorData?
}

/// IPC error data
struct IPCErrorData: Codable {
    let code: String
    let message: String
}

/// IPC event message (pushed from JavaScript)
struct IPCEvent: Codable {
    let type: String
    let payload: [String: AnyCodable]
    let timestamp: TimeInterval
}

// MARK: - Numeric coercion
//
// `AnyCodable` decodes a JSON number as `Int` when it has no fractional part and
// as `Double` otherwise, so a millisecond timestamp (`Date.now()` in JS) always
// arrives boxed as `Int`. Swift's `as?` does not bridge between `Int` and
// `Double`, so `any as? TimeInterval` on that box returns nil — which is why
// every timestamp used to fall back to `Date()` and the conversation list could
// not sort. The same trap hits `progress` when it lands on exactly 1.
//
// Read numbers out of an IPC payload through these, never through `as?`.

/// A JSON number as a `Double`, whichever way it was boxed.
func zmDouble(_ value: Any?) -> Double? {
    switch value {
    case let double as Double: return double
    case let int as Int: return Double(int)
    case let number as NSNumber: return number.doubleValue
    case let string as String: return Double(string)
    default: return nil
    }
}

/// A JSON number as an `Int`, whichever way it was boxed.
func zmInt(_ value: Any?) -> Int? {
    switch value {
    case let int as Int: return int
    case let double as Double: return Int(exactly: double)
    case let number as NSNumber: return Int(number.stringValue)
    case let string as String: return Int(string)
    default: return nil
    }
}

/// A JS millisecond-epoch timestamp as a `Date`.
func zmDate(_ value: Any?) -> Date? {
    guard let millis = zmDouble(value) else { return nil }
    return Date(timeIntervalSince1970: millis / 1000)
}

/// Type-erased Codable wrapper for heterogeneous dictionaries
struct AnyCodable: Codable {
    let value: Any
    
    init(_ value: Any) {
        self.value = value
    }
    
    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        
        if let bool = try? container.decode(Bool.self) {
            value = bool
        } else if let int = try? container.decode(Int.self) {
            value = int
        } else if let double = try? container.decode(Double.self) {
            value = double
        } else if let string = try? container.decode(String.self) {
            value = string
        } else if let array = try? container.decode([AnyCodable].self) {
            value = array.map { $0.value }
        } else if let dictionary = try? container.decode([String: AnyCodable].self) {
            value = dictionary.mapValues { $0.value }
        } else if container.decodeNil() {
            value = NSNull()
        } else {
            throw DecodingError.dataCorruptedError(
                in: container,
                debugDescription: "AnyCodable value cannot be decoded"
            )
        }
    }
    
    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        
        switch value {
        case let bool as Bool:
            try container.encode(bool)
        case let int as Int:
            try container.encode(int)
        case let double as Double:
            try container.encode(double)
        case let string as String:
            try container.encode(string)
        case let array as [Any]:
            try container.encode(array.map { AnyCodable($0) })
        case let dictionary as [String: Any]:
            try container.encode(dictionary.mapValues { AnyCodable($0) })
        case is NSNull:
            try container.encodeNil()
        default:
            let context = EncodingError.Context(
                codingPath: container.codingPath,
                debugDescription: "AnyCodable value cannot be encoded"
            )
            throw EncodingError.invalidValue(value, context)
        }
    }
}
