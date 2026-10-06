import Foundation

/// A message a device sends to the relay.
///
/// Every connection to the relay starts with one of `listen`, `accept`, or
/// `connect`, framed as described by ``RelayFrame``. Once the relay answers a
/// `connect` or `accept` with ``RelayEvent/connected``, framing ends and the
/// connection carries the two devices' own bytes, which the relay copies
/// without reading.
///
/// A device is named by its endpoint ID: its Ed25519 public key, as 64
/// lowercase hexadecimal digits. A namespace keeps one application's devices
/// apart from another's on a shared relay.
public enum RelayRequest: Codable, Sendable, Equatable {
    /// A host asks to be reachable as `endpointID` within `namespace`. The
    /// relay answers with a ``RelayEvent/challenge(nonce:)``.
    case listen(namespace: String, endpointID: String)
    /// A host's answer to the challenge: an Ed25519 signature, by the key
    /// `endpointID` names, over ``RelayFrame/listenProofPayload(namespace:endpointID:nonce:)``.
    case proof(signature: Data)
    /// A host's answer to the control connection's keepalive ping.
    case pong
    /// A host opens a new connection to take the session the relay announced
    /// with ``RelayEvent/incoming(sessionID:)``, signing
    /// ``RelayFrame/acceptProofPayload(namespace:endpointID:sessionID:)``
    /// with its key so only the host itself can take its sessions.
    case accept(sessionID: String, signature: Data)
    /// A client asks to be joined to the host registered as `endpointID`.
    case connect(namespace: String, endpointID: String)
}

/// A message the relay sends to a device.
public enum RelayEvent: Codable, Sendable, Equatable {
    /// Proof of key ownership the relay needs before registering a host.
    case challenge(nonce: Data)
    /// The host is registered and the control connection is live.
    case registered
    /// Keepalive on a host's control connection; the host answers ``RelayRequest/pong``.
    case ping
    /// A client wants the host; the host should `accept` `sessionID`.
    case incoming(sessionID: String)
    /// The two ends are joined. Everything after this frame is the peer's bytes.
    case connected
    /// The relay turned the request down.
    case refused(RelayRefusal)
}

/// Why the relay turned a request down.
public enum RelayRefusal: String, Codable, Sendable, Equatable {
    /// No host is registered under the requested endpoint ID.
    case hostOffline
    /// The host didn't take the session in time.
    case hostDidNotAnswer
    /// The listen proof didn't verify.
    case invalidProof
    /// The session to accept doesn't exist or already ended.
    case unknownSession
    /// The relay has too many connections or pending sessions.
    case busy
    /// The request was malformed or arrived out of order.
    case invalidRequest
    /// This relay doesn't serve the request's namespace.
    case namespaceNotAllowed
}

/// Framing shared by the relay and its clients: a 4-byte big-endian length,
/// then that many bytes of JSON.
public enum RelayFrame {
    /// Largest payload either side accepts.
    public static let maxPayloadLength = 16 * 1024

    /// Port the relay listens on unless configured otherwise.
    public static let defaultPort = 3340

    /// Bytes a host signs to prove it owns `endpointID`'s key. The nonce is
    /// fresh per connection, and the namespace keeps a proof for one
    /// application from registering it in another.
    public static func listenProofPayload(namespace: String, endpointID: String, nonce: Data) -> Data {
        payload(tag: "rendezvous-relay-listen/1", fields: [Data(namespace.utf8), Data(endpointID.utf8), nonce])
    }

    /// Bytes a host signs to take the session `sessionID`.
    public static func acceptProofPayload(namespace: String, endpointID: String, sessionID: String) -> Data {
        payload(tag: "rendezvous-relay-accept/1", fields: [namespace, endpointID, sessionID].map { Data($0.utf8) })
    }

    /// `tag`, then each field behind a zero byte. No field contains a zero
    /// byte, so no two field lists produce the same bytes.
    private static func payload(tag: String, fields: [Data]) -> Data {
        var payload = Data(tag.utf8)
        for field in fields {
            payload.append(0)
            payload.append(field)
        }
        return payload
    }

    /// Encodes `value` as one frame.
    public static func encode(_ value: some Encodable) throws -> Data {
        let payload = try JSONEncoder().encode(value)
        guard payload.count <= maxPayloadLength else {
            throw RelayFrameError.frameTooLarge(payload.count)
        }
        let length = UInt32(payload.count)
        var frame = Data([
            UInt8(truncatingIfNeeded: length >> 24),
            UInt8(truncatingIfNeeded: length >> 16),
            UInt8(truncatingIfNeeded: length >> 8),
            UInt8(truncatingIfNeeded: length),
        ])
        frame.append(payload)
        return frame
    }

    /// Removes one complete frame from the front of `buffer` and decodes it.
    /// Returns nil, leaving `buffer` untouched, when the frame isn't complete.
    public static func decode<T: Decodable>(_ type: T.Type, from buffer: inout Data) throws -> T? {
        guard buffer.count >= 4 else { return nil }
        let length = buffer.prefix(4).reduce(0) { ($0 << 8) | Int($1) }
        guard length <= maxPayloadLength else {
            throw RelayFrameError.frameTooLarge(length)
        }
        guard buffer.count >= 4 + length else { return nil }
        let payload = buffer.dropFirst(4).prefix(length)
        buffer = Data(buffer.dropFirst(4 + length))
        do {
            return try JSONDecoder().decode(T.self, from: payload)
        } catch {
            throw RelayFrameError.undecodable
        }
    }
}

/// A malformed relay frame.
public enum RelayFrameError: Error, Equatable {
    /// The frame's declared length exceeds ``RelayFrame/maxPayloadLength``.
    case frameTooLarge(Int)
    /// The payload isn't a valid message.
    case undecodable
}

extension RelayFrame {
    /// How many random bytes a session ID carries, before hex encoding.
    package static let sessionIDByteCount = 16

    /// Whether `sessionID` has the shape the relay gives session IDs:
    /// ``sessionIDByteCount`` bytes as lowercase hex.
    package static func isWellFormedSessionID(_ sessionID: String) -> Bool {
        sessionID == sessionID.lowercased() && Data(relayHex: sessionID)?.count == sessionIDByteCount
    }
}

extension Data {
    /// Decodes an even-length string of hexadecimal digits, or returns nil.
    package init?(relayHex string: String) {
        // `UInt8(_:radix:)` alone would also take a sign, as in "+f".
        guard string.count.isMultiple(of: 2), string.allSatisfy({ $0.isASCII && $0.isHexDigit }) else { return nil }
        var bytes = [UInt8]()
        bytes.reserveCapacity(string.count / 2)
        var index = string.startIndex
        while index < string.endIndex {
            let next = string.index(index, offsetBy: 2)
            guard let byte = UInt8(string[index..<next], radix: 16) else { return nil }
            bytes.append(byte)
            index = next
        }
        self.init(bytes)
    }

    /// Lowercase hexadecimal encoding, two digits per byte.
    package var relayHex: String {
        map { String(format: "%02x", $0) }.joined()
    }
}
