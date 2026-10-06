import Foundation

/// Where a relay listens: a host name or IP address and a TCP port.
///
/// The form users type and devices publish: `host`, `host:port`, `[ipv6]`,
/// or `[ipv6]:port`, with ``RelayFrame/defaultPort`` filled in when the port
/// is left out. A bare IPv6 literal is accepted too, since it can't carry a
/// port without brackets.
public struct RelayAddress: Sendable, Hashable, CustomStringConvertible {
    /// Host name or IP address. IPv6 literals are stored without brackets.
    public let host: String
    /// TCP port.
    public let port: Int

    /// Creates an address from its parts.
    public init(host: String, port: Int = RelayFrame.defaultPort) {
        self.host = host
        self.port = port
    }

    /// Parses `string`, or returns nil for anything that isn't one host and
    /// an optional port, such as a URL.
    public init?(parsing string: String, defaultPort: Int = RelayFrame.defaultPort) {
        let trimmed = string.trimmingCharacters(in: .whitespaces)
        guard !trimmed.isEmpty,
              !trimmed.contains(where: { $0 == "/" || $0.isWhitespace || $0 == "@" }),
              let (host, port) = Self.split(trimmed, defaultPort: defaultPort)
        else { return nil }
        self.host = host
        self.port = port
    }

    /// `host:port`, with an IPv6 host in brackets. Parses back to `self`.
    public var description: String {
        host.contains(":") ? "[\(host)]:\(port)" : "\(host):\(port)"
    }

    /// Splits `string` into a host and port, or returns nil if it isn't
    /// one of the accepted forms.
    private static func split(_ string: String, defaultPort: Int) -> (String, Int)? {
        func validPort(_ text: Substring) -> Int? {
            guard let port = Int(text), (1...65535).contains(port) else { return nil }
            return port
        }
        if string.hasPrefix("[") {
            guard let close = string.firstIndex(of: "]") else { return nil }
            let host = String(string[string.index(after: string.startIndex)..<close])
            let rest = string[string.index(after: close)...]
            guard !host.isEmpty else { return nil }
            if rest.isEmpty { return (host, defaultPort) }
            guard rest.hasPrefix(":"), let port = validPort(rest.dropFirst()) else { return nil }
            return (host, port)
        }
        let parts = string.split(separator: ":", omittingEmptySubsequences: false)
        switch parts.count {
        case 1:
            return (string, defaultPort)
        case 2:
            guard !parts[0].isEmpty, let port = validPort(parts[1]) else { return nil }
            return (String(parts[0]), port)
        default:
            // A bare IPv6 literal; it needs brackets to carry a port.
            guard parts.allSatisfy({ $0.allSatisfy(\.isHexDigit) }) else { return nil }
            return (string, defaultPort)
        }
    }
}
