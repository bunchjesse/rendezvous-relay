import Foundation
import NIOCore
import NIOPosix
import Testing
import RendezvousRelayClient
import RendezvousRelayProtocol

/// The relay's own checks, driven by hand-built frames and the client library.
@Suite(.timeLimit(.minutes(1)))
struct RelayServerTests {
    /// A host that can't sign the relay's challenge with the key it claims
    /// is refused, so nobody can take over another device's registration.
    @Test func listenWithoutKeyOwnershipIsRefused() async throws {
        let relay = try await TestRelay.start()
        let claimed = TestIdentity()
        var control = try await RawRelayConnection.open(port: relay.port, sending: .listen(namespace: "test", endpointID: claimed.endpointID))
        guard case .challenge = try await control.next() else {
            Issue.record("Expected a challenge")
            return
        }
        try await control.send(.proof(signature: try TestIdentity().sign(Data("nonce".utf8))))
        #expect(try await control.next() == .refused(.invalidProof))

        control.close()
        await relay.stop()
    }

    /// Asking for a host that isn't registered fails right away as offline
    /// rather than waiting out the session timeout.
    @Test func connectingToAnUnregisteredHostReportsItOffline() async throws {
        let relay = try await TestRelay.start()
        await #expect(throws: RelayClientError.refused(.hostOffline)) {
            _ = try await relay.connect(namespace: "test", endpointID: TestIdentity().endpointID)
        }
        await relay.stop()
    }

    /// Requests naming something other than an Ed25519 key in lowercase
    /// hex, or an empty or NUL-bearing namespace, are refused before the
    /// relay does anything with them.
    @Test(arguments: [
        RelayRequest.listen(namespace: "test", endpointID: "not-a-key"),
        .connect(namespace: "test", endpointID: String(repeating: "ab", count: 31)),
        .connect(namespace: "test", endpointID: String(repeating: "AB", count: 32)),
        .connect(namespace: "test", endpointID: "+f" + String(repeating: "ab", count: 31)),
        .connect(namespace: "", endpointID: TestIdentity().endpointID),
        .connect(namespace: "a\u{0}b", endpointID: TestIdentity().endpointID),
        .pong,
    ])
    func malformedRequestsAreRefused(request: RelayRequest) async throws {
        let relay = try await TestRelay.start()
        var connection = try await RawRelayConnection.open(port: relay.port, sending: request)
        #expect(try await connection.next() == .refused(.invalidRequest))
        connection.close()
        await relay.stop()
    }

    /// A relay limited to some namespaces refuses to register or connect
    /// anything outside them, and serves the ones it lists.
    @Test func onlyAllowedNamespacesAreServed() async throws {
        let relay = try await TestRelay.start(allowedNamespaces: ["com.example.allowed"])
        let host = TestIdentity()

        var listen = try await RawRelayConnection.open(port: relay.port, sending: .listen(namespace: "com.example.other", endpointID: host.endpointID))
        #expect(try await listen.next() == .refused(.namespaceNotAllowed))
        listen.close()

        await #expect(throws: RelayClientError.refused(.namespaceNotAllowed)) {
            _ = try await relay.connect(namespace: "com.example.other", endpointID: host.endpointID)
        }

        var allowed = try await RawRelayConnection.open(port: relay.port, sending: .listen(namespace: "com.example.allowed", endpointID: host.endpointID))
        guard case .challenge = try await allowed.next() else {
            Issue.record("Expected a challenge in an allowed namespace")
            return
        }
        allowed.close()
        await relay.stop()
    }

    /// A session can only be taken with the registered host's signature:
    /// someone who saw the session ID go by is refused, and the host still
    /// gets it afterwards.
    @Test func acceptingASessionRequiresTheHostsSignature() async throws {
        let relay = try await TestRelay.start()
        let host = TestIdentity()

        var control = try await RawRelayConnection.open(port: relay.port, sending: .listen(namespace: "test", endpointID: host.endpointID))
        guard case .challenge(let nonce) = try await control.next() else {
            Issue.record("Expected a challenge")
            return
        }
        try await control.send(.proof(signature: try host.sign(RelayFrame.listenProofPayload(namespace: "test", endpointID: host.endpointID, nonce: nonce))))
        #expect(try await control.next() == .registered)

        let client = Task { try await relay.connect(namespace: "test", endpointID: host.endpointID) }
        guard case .incoming(let sessionID) = try await control.next() else {
            Issue.record("Expected an incoming session")
            return
        }

        let forged = try TestIdentity().sign(Data(sessionID.utf8))
        var thief = try await RawRelayConnection.open(port: relay.port, sending: .accept(sessionID: sessionID, signature: forged))
        #expect(try await thief.next() == .refused(.unknownSession))

        let payload = RelayFrame.acceptProofPayload(namespace: "test", endpointID: host.endpointID, sessionID: sessionID)
        var accepted = try await RawRelayConnection.open(port: relay.port, sending: .accept(sessionID: sessionID, signature: try host.sign(payload)))
        #expect(try await accepted.next() == .connected)
        let joined = try await client.value

        joined.channel.close(promise: nil)
        accepted.close()
        thief.close()
        control.close()
        await relay.stop()
    }
}

/// ``RelayHost`` registering with a real relay and taking sessions through it.
@Suite(.timeLimit(.minutes(1)))
struct RelayHostTests {
    /// Starts a ``RelayHost`` for `identity` on `relay` whose sessions greet
    /// the client with `greeting` and collect what it sends. Returns the
    /// host, its latest registration flag, and the sessions it accepted.
    private func startHost(
        _ identity: TestIdentity,
        on relay: TestRelay,
        namespace: String = "test",
        greeting: Data = Data()
    ) -> (host: RelayHost, isRegistered: Locked<Bool>, sessions: Locked<[Collector]>) {
        let isRegistered = Locked(false)
        let sessions = Locked<[Collector]>([])
        let host = RelayHost(
            namespace: namespace,
            endpointID: identity.endpointID,
            sign: { try identity.sign($0) },
            dial: { try await relay.dial() },
            acceptSession: { channel in
                let collector = try Collector.install(on: channel)
                sessions.mutate { $0.append(collector) }
                if !greeting.isEmpty {
                    channel.writeAndFlush(ByteBuffer(bytes: greeting), promise: nil)
                }
            },
            onStatusChange: { status in
                isRegistered.value = status.isRegistered
            }
        )
        host.start()
        return (host, isRegistered, sessions)
    }

    /// A host registers, a client joins it through the relay, and bytes
    /// flow both ways. The host greets first, as SSH does, so its bytes can
    /// arrive in the same read as the relay's last frame; they still reach
    /// the client's own handler intact, whether the client added the
    /// handshake before its connection was active or after.
    @Test(arguments: [false, true])
    func clientAndHostExchangeBytesThroughTheRelay(fromInitializer: Bool) async throws {
        let relay = try await TestRelay.start()
        let identity = TestIdentity()
        let greeting = Data("SSH-2.0-host\r\n".utf8)
        let (host, isRegistered, sessions) = startHost(identity, on: relay, greeting: greeting)
        try await waitUntil { isRegistered.value }

        let (channel, received) = try await relay.connect(namespace: "test", endpointID: identity.endpointID, fromInitializer: fromInitializer)
        #expect(try await received.first(greeting.count) == greeting)

        let reply = Data("SSH-2.0-client\r\n".utf8)
        try await channel.writeAndFlush(ByteBuffer(bytes: reply)).get()
        try await waitUntil { !sessions.value.isEmpty }
        let session = try #require(sessions.value.first)
        #expect(try await session.first(reply.count) == reply)

        channel.close(promise: nil)
        host.stop()
        await relay.stop()
    }

    /// A host registered in one namespace can't be reached from another,
    /// which keeps applications (and their build flavors) apart on one relay.
    @Test func registrationsAreScopedToTheirNamespace() async throws {
        let relay = try await TestRelay.start()
        let identity = TestIdentity()
        let (host, isRegistered, _) = startHost(identity, on: relay, namespace: "com.example.app")
        try await waitUntil { isRegistered.value }

        await #expect(throws: RelayClientError.refused(.hostOffline)) {
            _ = try await relay.connect(namespace: "com.example.app.debug", endpointID: identity.endpointID)
        }
        host.stop()
        await relay.stop()
    }

    /// A host the relay refuses reports itself disconnected with the
    /// relay's reason, rather than claiming to be registered.
    @Test func refusedRegistrationReportsTheReason() async throws {
        let relay = try await TestRelay.start(allowedNamespaces: ["com.example.allowed"])
        let identity = TestIdentity()
        let reason = Locked<RelayClientError?>(nil)
        let host = RelayHost(
            namespace: "com.example.other",
            endpointID: identity.endpointID,
            sign: { try identity.sign($0) },
            dial: { try await relay.dial() },
            acceptSession: { _ in },
            onStatusChange: { status in
                if case .disconnected(let error) = status {
                    reason.value = error as? RelayClientError
                }
            }
        )
        host.start()
        try await waitUntil { reason.value != nil }
        #expect(reason.value == .refused(.namespaceNotAllowed))
        host.stop()
        await relay.stop()
    }

    /// A host whose relay restarts registers again on its own, so clients
    /// can reach it once the relay is back.
    @Test func hostReregistersAfterTheRelayRestarts() async throws {
        let first = try await TestRelay.start()
        let identity = TestIdentity()
        let relayPort = Locked(first.port)
        let isRegistered = Locked(false)
        let host = RelayHost(
            namespace: "test",
            endpointID: identity.endpointID,
            sign: { try identity.sign($0) },
            dial: {
                try await ClientBootstrap(group: MultiThreadedEventLoopGroup.singleton)
                    .connect(host: "127.0.0.1", port: relayPort.value)
                    .get()
            },
            acceptSession: { _ in },
            onStatusChange: { status in
                isRegistered.value = status.isRegistered
            }
        )
        host.start()
        try await waitUntil { isRegistered.value }

        await first.stop()
        try await waitUntil { !isRegistered.value }
        let second = try await TestRelay.start()
        relayPort.value = second.port
        try await waitUntil(timeout: .seconds(10)) { isRegistered.value }

        host.stop()
        await second.stop()
    }
}

extension RelayHost.Status {
    /// Whether the status is ``registered``.
    var isRegistered: Bool {
        if case .registered = self { true } else { false }
    }
}
