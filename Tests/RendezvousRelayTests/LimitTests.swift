import NIOCore
import Testing
@testable import RendezvousRelayServer

/// How the relay groups clients for its per-address connection limit.
struct ConnectionLimitKeyTests {
    /// Every address in one IPv6 /64 shares a bucket, so a client can't
    /// dodge the limit by rotating through its block, while another /64
    /// gets a bucket of its own.
    @Test func ipv6AddressesShareTheirSlash64() throws {
        let first = RelayServer.limitKey(for: try SocketAddress(ipAddress: "2001:db8:1:2::1", port: 1))
        let second = RelayServer.limitKey(for: try SocketAddress(ipAddress: "2001:db8:1:2:ffff:ffff:ffff:ffff", port: 2))
        let other = RelayServer.limitKey(for: try SocketAddress(ipAddress: "2001:db8:1:3::1", port: 1))
        #expect(first == second)
        #expect(first != other)
    }

    /// An IPv4 client counts by its own address, whether it arrives on an
    /// IPv4 socket or mapped onto a dual-stack IPv6 one.
    @Test func ipv4AddressesCountAlone() throws {
        let plain = RelayServer.limitKey(for: try SocketAddress(ipAddress: "203.0.113.7", port: 1))
        let mapped = RelayServer.limitKey(for: try SocketAddress(ipAddress: "::ffff:203.0.113.7", port: 1))
        #expect(plain == "203.0.113.7")
        #expect(mapped == plain)
        #expect(RelayServer.limitKey(for: try SocketAddress(ipAddress: "203.0.113.8", port: 1)) != plain)
    }
}
