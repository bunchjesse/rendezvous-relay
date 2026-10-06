import Crypto
import Foundation
import NIOCore
import NIOPosix
import NIOConcurrencyHelpers
import RendezvousRelayProtocol

/// Limits and timings for a ``RelayServer``.
public struct RelayServerConfiguration: Sendable {
    /// Address to listen on, or nil for every interface: IPv6 and IPv4
    /// where the system has IPv6 (`::`), IPv4 alone where it doesn't.
    public var host: String?
    /// Port to listen on; 0 picks a free one.
    public var port: Int
    /// The namespaces the relay serves, or nil to serve any. Requests in any
    /// other namespace are refused with ``RelayRefusal/namespaceNotAllowed``.
    public var allowedNamespaces: Set<String>?
    /// Most connections served at once. Later ones are closed on arrival.
    public var maxConnections: Int
    /// Most connections served at once from one IPv4 address or IPv6 /64,
    /// the block one IPv6 subscriber typically holds. Behind NAT or a proxy
    /// that hides clients' addresses, those clients share the limit.
    public var maxConnectionsPerAddress: Int
    /// Most sessions one host may have waiting to be accepted.
    public var maxPendingSessionsPerHost: Int
    /// How long a connection may take to send its request (and a host its proof).
    public var requestTimeout: TimeAmount
    /// How long a host has to accept a session.
    public var sessionTimeout: TimeAmount
    /// How often the relay pings each host's control connection.
    public var pingInterval: TimeAmount
    /// How long a control connection may stay silent before the relay drops it.
    public var controlTimeout: TimeAmount
    /// Receives one line per notable event: listening, hosts registering
    /// and leaving, refused namespaces.
    public var log: @Sendable (String) -> Void

    /// Creates a configuration with production defaults.
    public init(
        host: String? = nil,
        port: Int = RelayFrame.defaultPort,
        allowedNamespaces: Set<String>? = nil,
        maxConnections: Int = 4096,
        maxConnectionsPerAddress: Int = 64,
        maxPendingSessionsPerHost: Int = 32,
        requestTimeout: TimeAmount = .seconds(10),
        sessionTimeout: TimeAmount = .seconds(10),
        pingInterval: TimeAmount = .seconds(20),
        controlTimeout: TimeAmount = .seconds(60),
        log: @escaping @Sendable (String) -> Void = RelayServerConfiguration.standardErrorLog
    ) {
        self.host = host
        self.port = port
        self.allowedNamespaces = allowedNamespaces
        self.maxConnections = maxConnections
        self.maxConnectionsPerAddress = maxConnectionsPerAddress
        self.maxPendingSessionsPerHost = maxPendingSessionsPerHost
        self.requestTimeout = requestTimeout
        self.sessionTimeout = sessionTimeout
        self.pingInterval = pingInterval
        self.controlTimeout = controlTimeout
        self.log = log
    }

    /// Writes each line to standard error behind an ISO 8601 timestamp.
    public static let standardErrorLog: @Sendable (String) -> Void = { message in
        let line = "\(Date().formatted(.iso8601)) \(message)\n"
        FileHandle.standardError.write(Data(line.utf8))
    }
}

/// A rendezvous that joins a client's TCP connection to a host that can't
/// be reached directly.
///
/// Hosts keep a control connection open, registered under their endpoint ID
/// after proving they own its key. A client asks for a host by endpoint ID;
/// the relay tells the host over its control connection, the host opens a
/// second connection to take the session, and the relay copies bytes between
/// the two. The relay never reads those bytes: applications are expected to
/// authenticate and encrypt them end to end, so the relay sees only who talks
/// to whom, and can only refuse service.
public final class RelayServer: Sendable {
    /// An accepted connection, read and written asynchronously.
    private typealias Connection = NIOAsyncChannel<ByteBuffer, ByteBuffer>
    /// The writing half of a ``Connection``.
    private typealias Outbound = NIOAsyncChannelOutboundWriter<ByteBuffer>

    /// Longest namespace the relay accepts, in UTF-8 bytes.
    static let maxNamespaceLength = 256

    /// Limits and timings.
    private let configuration: RelayServerConfiguration
    /// The event loops the relay's connections run on.
    private let group: any EventLoopGroup
    /// Connections, registered hosts, and pending sessions.
    private let state = NIOLockedValueBox(State())

    /// Creates a relay. Call ``run(onBound:)`` to serve.
    public init(configuration: RelayServerConfiguration = RelayServerConfiguration(), group: any EventLoopGroup = MultiThreadedEventLoopGroup.singleton) {
        self.configuration = configuration
        self.group = group
    }

    /// Serves until the calling task is cancelled. `onBound` receives the
    /// bound port once the relay is listening.
    public func run(onBound: @Sendable (Int) -> Void = { _ in }) async throws {
        let server: NIOAsyncChannel<Connection, Never>
        if let host = configuration.host {
            server = try await bind(host: host)
        } else {
            do {
                server = try await bind(host: "::")
            } catch {
                // Docker's default networks have no IPv6.
                server = try await bind(host: "0.0.0.0")
            }
        }
        let port = server.channel.localAddress?.port ?? configuration.port
        onBound(port)
        let namespaces = configuration.allowedNamespaces.map { $0.sorted().joined(separator: ", ") } ?? "any namespace"
        configuration.log("Listening on \(server.channel.localAddress?.ipAddress ?? "?") port \(port), serving \(namespaces)")

        try await withTaskCancellationHandler {
            try await server.executeThenClose { connections in
                await withDiscardingTaskGroup { group in
                    do {
                        for try await connection in connections {
                            let address = Self.limitKey(for: connection.channel.remoteAddress)
                            guard admitConnection(from: address) else {
                                connection.channel.close(promise: nil)
                                continue
                            }
                            group.addTask {
                                await self.serve(connection)
                                self.releaseConnection(from: address)
                            }
                        }
                    } catch where !(error is CancellationError) {
                        configuration.log("Stopped accepting: \(error)")
                    } catch {}
                    group.cancelAll()
                }
            }
        } onCancel: {
            server.channel.close(promise: nil)
        }
    }

    /// Listens on `host` and the configured port.
    private func bind(host: String) async throws -> NIOAsyncChannel<Connection, Never> {
        try await ServerBootstrap(group: group)
            .serverChannelOption(.backlog, value: 256)
            .serverChannelOption(.socketOption(.so_reuseaddr), value: 1)
            .bind(host: host, port: configuration.port) { [configuration] channel in
                channel.eventLoop.makeCompletedFuture {
                    Self.tune(channel, log: configuration.log)
                    return try Connection(
                        wrappingChannelSynchronously: channel,
                        configuration: .init(isOutboundHalfClosureEnabled: true)
                    )
                }
            }
    }

    /// Options the platform refused, each logged once.
    private static let refusedOptions = NIOLockedValueBox<Set<String>>([])

    /// Applies the socket options the relay prefers. They only tune
    /// performance, so one the platform refuses is logged and skipped: as
    /// `childChannelOption`s, a failure would end the whole accept loop.
    private static func tune(_ channel: any Channel, log: @Sendable (String) -> Void) {
        let options = channel.syncOptions
        let settings: [(String, () throws -> Void)] = [
            ("allowRemoteHalfClosure", { try options?.setOption(.allowRemoteHalfClosure, value: true) }),
            ("TCP_NODELAY", { try options?.setOption(.socketOption(.tcp_nodelay), value: 1) }),
            ("SO_KEEPALIVE", { try options?.setOption(.socketOption(.so_keepalive), value: 1) }),
        ]
        for (name, apply) in settings {
            do {
                try apply()
            } catch {
                if refusedOptions.withLockedValue({ $0.insert(name).inserted }) {
                    log("Couldn't set \(name), continuing without it: \(error)")
                }
            }
        }
    }

    // MARK: - Connections

    /// The bucket ``RelayServerConfiguration/maxConnectionsPerAddress``
    /// counts `address` in: its IPv4 address, or its IPv6 /64, so one
    /// subscriber can't take every slot by rotating through its block.
    static func limitKey(for address: SocketAddress?) -> String {
        guard case .v6(let v6)? = address else { return address?.ipAddress ?? "?" }
        let bytes = withUnsafeBytes(of: v6.address.sin6_addr) { Array($0) }
        // An IPv4 client on a dual-stack socket arrives as ::ffff:a.b.c.d.
        if bytes[0..<10].allSatisfy({ $0 == 0 }), bytes[10] == 0xff, bytes[11] == 0xff {
            return bytes[12...].map(String.init).joined(separator: ".")
        }
        return Data(bytes[0..<8]).relayHex + "::/64"
    }

    /// Counts a new connection from `address`, or returns false if it would
    /// pass the overall or per-address limit.
    private func admitConnection(from address: String) -> Bool {
        state.withLockedValue { state in
            let fromAddress = state.connectionsByAddress[address, default: 0]
            guard state.connectionCount < configuration.maxConnections,
                  fromAddress < configuration.maxConnectionsPerAddress
            else { return false }
            state.connectionCount += 1
            state.connectionsByAddress[address] = fromAddress + 1
            return true
        }
    }

    /// Forgets a connection ``admitConnection(from:)`` counted.
    private func releaseConnection(from address: String) {
        state.withLockedValue { state in
            state.connectionCount -= 1
            let remaining = state.connectionsByAddress[address, default: 1] - 1
            state.connectionsByAddress[address] = remaining > 0 ? remaining : nil
        }
    }

    /// Reads a connection's first request and serves it as a host, a
    /// client, or a host taking a session.
    private func serve(_ connection: Connection) async {
        let channel = connection.channel
        do {
            try await connection.executeThenClose { inbound, outbound in
                var reader = FrameReader(inbound.makeAsyncIterator())
                let deadline = channel.eventLoop.scheduleTask(in: configuration.requestTimeout) {
                    channel.close(promise: nil)
                }
                guard let request = try await reader.next(RelayRequest.self) else {
                    deadline.cancel()
                    return
                }
                if let refusal = refusal(for: request) {
                    deadline.cancel()
                    try await Self.send(.refused(refusal), to: outbound)
                    return
                }
                switch request {
                case .listen(let namespace, let endpointID):
                    try await serveHost(namespace: namespace, endpointID: endpointID, channel: channel, reader: &reader, outbound: outbound, deadline: deadline)
                case .connect(let namespace, let endpointID):
                    deadline.cancel()
                    try await serveClient(key: HostKey(namespace: namespace, endpointID: endpointID), channel: channel, reader: &reader, outbound: outbound)
                case .accept(let sessionID, let signature):
                    deadline.cancel()
                    try await serveAccept(sessionID: sessionID, signature: signature, channel: channel, reader: &reader, outbound: outbound)
                case .proof, .pong:
                    deadline.cancel()
                    try await Self.send(.refused(.invalidRequest), to: outbound)
                }
            }
        } catch {
            // Peers disconnect at any point; there's nothing to report back.
        }
    }

    /// Why `request` can't be served at all, before any state is touched:
    /// a malformed namespace or endpoint ID, or a namespace this relay
    /// doesn't serve.
    private func refusal(for request: RelayRequest) -> RelayRefusal? {
        let namespace: String
        let endpointID: String
        switch request {
        case .listen(let ns, let id), .connect(let ns, let id):
            namespace = ns
            endpointID = id
        case .accept, .proof, .pong:
            return nil
        }
        guard Self.isWellFormed(namespace: namespace), Self.publicKey(for: endpointID) != nil else {
            return .invalidRequest
        }
        if let allowed = configuration.allowedNamespaces, !allowed.contains(namespace) {
            configuration.log("Refused namespace \(Self.loggable(namespace))")
            return .namespaceNotAllowed
        }
        return nil
    }

    /// `namespace` quoted and capped for a log line: it's the client's, so
    /// it must not be able to forge or flood lines.
    private static func loggable(_ namespace: String) -> String {
        String(namespace.prefix(64)).debugDescription
    }

    /// Whether `namespace` is one the proof payloads can carry unambiguously.
    private static func isWellFormed(namespace: String) -> Bool {
        let bytes = namespace.utf8
        return !bytes.isEmpty && bytes.count <= maxNamespaceLength && !bytes.contains(0)
    }

    /// The Ed25519 key `endpointID` names, or nil if it names none.
    private static func publicKey(for endpointID: String) -> Curve25519.Signing.PublicKey? {
        guard endpointID == endpointID.lowercased(), let bytes = Data(relayHex: endpointID) else { return nil }
        return try? Curve25519.Signing.PublicKey(rawRepresentation: bytes)
    }

    // MARK: Hosts

    /// Registers a host once it proves it owns `endpointID`'s key, then
    /// keeps its control connection alive until it ends.
    private func serveHost(
        namespace: String,
        endpointID: String,
        channel: any Channel,
        reader: inout FrameReader,
        outbound: Outbound,
        deadline: Scheduled<Void>
    ) async throws {
        guard let publicKey = Self.publicKey(for: endpointID) else {
            deadline.cancel()
            try await Self.send(.refused(.invalidRequest), to: outbound)
            return
        }
        let nonce = Data((0..<32).map { _ in UInt8.random(in: .min ... .max) })
        try await Self.send(.challenge(nonce: nonce), to: outbound)
        guard case .proof(let signature)? = try await reader.next(RelayRequest.self) else {
            deadline.cancel()
            try await Self.send(.refused(.invalidRequest), to: outbound)
            return
        }
        deadline.cancel()
        let payload = RelayFrame.listenProofPayload(namespace: namespace, endpointID: endpointID, nonce: nonce)
        guard publicKey.isValidSignature(signature, for: payload) else {
            try await Self.send(.refused(.invalidProof), to: outbound)
            return
        }

        let key = HostKey(namespace: namespace, endpointID: endpointID)
        let registration = HostRegistration(channel: channel, outbound: outbound)
        let replaced = state.withLockedValue { state in
            defer { state.hosts[key] = registration }
            return state.hosts[key]
        }
        // Leaving for any reason, a failed `registered` included, must
        // unregister, or clients would wait out the session timeout on a
        // host that's gone.
        defer {
            state.withLockedValue { state in
                if state.hosts[key] === registration {
                    state.hosts[key] = nil
                }
            }
            configuration.log("Unregistered \(endpointID.prefix(12)) in \(Self.loggable(namespace))")
        }
        // A host reconnecting after a network change replaces its stale
        // registration; only a valid proof can do that.
        replaced?.channel.close(promise: nil)
        try await Self.send(.registered, to: outbound)
        configuration.log("Registered \(endpointID.prefix(12)) in \(Self.loggable(namespace))")

        let lastHeard = NIOLockedValueBox(NIODeadline.now())
        let pinger = channel.eventLoop.scheduleRepeatedTask(initialDelay: configuration.pingInterval, delay: configuration.pingInterval) { [configuration] task in
            guard NIODeadline.now() - lastHeard.withLockedValue({ $0 }) < configuration.controlTimeout else {
                channel.close(promise: nil)
                task.cancel()
                return
            }
            Task { try? await Self.send(.ping, to: outbound) }
        }
        defer { pinger.cancel() }
        while let message = try await reader.next(RelayRequest.self) {
            guard message == .pong else {
                try await Self.send(.refused(.invalidRequest), to: outbound)
                return
            }
            lastHeard.withLockedValue { $0 = .now() }
        }
    }

    // MARK: Sessions

    /// Asks the host `key` to take a new session for this client, and
    /// splices the two once it does.
    private func serveClient(key: HostKey, channel: any Channel, reader: inout FrameReader, outbound: Outbound) async throws {
        let sessionID = Data((0..<RelayFrame.sessionIDByteCount).map { _ in UInt8.random(in: .min ... .max) }).relayHex
        let session = PendingSession(hostKey: key, clientChannel: channel, clientOutbound: outbound)
        // Either the host to ask, or why there's none to ask.
        enum Lookup {
            case host(HostRegistration)
            case refused(RelayRefusal)
        }
        let lookup = state.withLockedValue { state -> Lookup in
            guard let host = state.hosts[key] else { return .refused(.hostOffline) }
            let waiting = state.sessions.values.count { $0.hostKey == key }
            guard waiting < configuration.maxPendingSessionsPerHost else { return .refused(.busy) }
            state.sessions[sessionID] = session
            return .host(host)
        }
        let host: HostRegistration
        switch lookup {
        case .host(let registered):
            host = registered
        case .refused(let refusal):
            try await Self.send(.refused(refusal), to: outbound)
            return
        }

        let timeout = channel.eventLoop.scheduleTask(in: configuration.sessionTimeout) {
            session.resolve(with: nil)
        }
        try? await Self.send(.incoming(sessionID: sessionID), to: host.outbound)
        let pairing = await session.waitForHost()
        timeout.cancel()
        state.withLockedValue { _ = $0.sessions.removeValue(forKey: sessionID) }

        guard let pairing else {
            try await Self.send(.refused(.hostDidNotAnswer), to: outbound)
            return
        }
        // The accepting side sends `connected` to both ends; this device
        // sends nothing until it arrives.
        await pairing.splice.pump(from: &reader, to: pairing.hostOutbound)
    }

    /// Gives the pending session `sessionID` to the host that signed for
    /// it, and splices the two connections.
    private func serveAccept(
        sessionID: String,
        signature: Data,
        channel: any Channel,
        reader: inout FrameReader,
        outbound: Outbound
    ) async throws {
        // Only the registered host may take its sessions: the session ID
        // crossed the network in the clear, so it isn't proof on its own.
        guard let pending = state.withLockedValue({ $0.sessions[sessionID] }),
              Self.isValidAcceptProof(signature, sessionID: sessionID, hostKey: pending.hostKey),
              let session = state.withLockedValue({ $0.sessions.removeValue(forKey: sessionID) })
        else {
            try await Self.send(.refused(.unknownSession), to: outbound)
            return
        }
        // Each end hears `connected` before the other's bytes, so neither
        // mistakes them for a relay frame. The host's goes out before the
        // session resolves, since resolving starts the client's pump toward
        // it; the client's goes out before this side's pump starts.
        do {
            try await Self.send(.connected, to: outbound)
        } catch {
            session.resolve(with: nil)
            return
        }
        let splice = Splice(channels: [session.clientChannel, channel])
        guard session.resolve(with: Pairing(hostOutbound: outbound, splice: splice)) else {
            channel.close(promise: nil)
            return
        }
        do {
            try await Self.send(.connected, to: session.clientOutbound)
        } catch {
            // Still pump: the client's side is already splicing and waits for
            // this direction to finish, which it does at once on a closed channel.
            session.clientChannel.close(promise: nil)
            channel.close(promise: nil)
        }
        await splice.pump(from: &reader, to: session.clientOutbound)
    }

    /// Whether `signature` is the registered host's over the accept payload.
    private static func isValidAcceptProof(_ signature: Data, sessionID: String, hostKey: HostKey) -> Bool {
        guard let publicKey = publicKey(for: hostKey.endpointID) else { return false }
        let payload = RelayFrame.acceptProofPayload(namespace: hostKey.namespace, endpointID: hostKey.endpointID, sessionID: sessionID)
        return publicKey.isValidSignature(signature, for: payload)
    }

    /// Writes one framed event.
    private static func send(_ event: RelayEvent, to outbound: Outbound) async throws {
        try await outbound.write(ByteBuffer(bytes: try RelayFrame.encode(event)))
    }
}

// MARK: - State

extension RelayServer {
    /// A registered host's identity on the relay.
    private struct HostKey: Hashable, Sendable {
        /// The application's namespace.
        let namespace: String
        /// The host's Ed25519 public key, in hex.
        let endpointID: String
    }

    /// A host's live control connection.
    private final class HostRegistration: Sendable {
        /// The control connection.
        let channel: any Channel
        /// Where the relay writes the host's events.
        let outbound: Outbound

        /// Creates a registration for a control connection.
        init(channel: any Channel, outbound: Outbound) {
            self.channel = channel
            self.outbound = outbound
        }
    }

    /// What the host's accepting connection hands the waiting client.
    private struct Pairing: Sendable {
        /// The writing half of the host's session connection.
        let hostOutbound: Outbound
        /// The splice both connections pump through.
        let splice: Splice
    }

    /// A client waiting for its host to accept.
    private final class PendingSession: Sendable {
        /// The host the client asked for.
        let hostKey: HostKey
        /// The client's connection.
        let clientChannel: any Channel
        /// The writing half of the client's connection.
        let clientOutbound: Outbound
        /// Whether the session settled, its pairing, and the client waiting on it.
        private let outcome = NIOLockedValueBox<(resolved: Bool, pairing: Pairing?, waiter: CheckedContinuation<Pairing?, Never>?)>((false, nil, nil))

        /// Creates a session for a client waiting on `hostKey`.
        init(hostKey: HostKey, clientChannel: any Channel, clientOutbound: Outbound) {
            self.hostKey = hostKey
            self.clientChannel = clientChannel
            self.clientOutbound = clientOutbound
        }

        /// Settles the session once. Returns false if it was already settled.
        @discardableResult
        func resolve(with pairing: Pairing?) -> Bool {
            let (settled, waiter) = outcome.withLockedValue { state -> (Bool, CheckedContinuation<Pairing?, Never>?) in
                guard !state.resolved else { return (false, nil) }
                state.resolved = true
                state.pairing = pairing
                defer { state.waiter = nil }
                return (true, state.waiter)
            }
            waiter?.resume(returning: pairing)
            return settled
        }

        /// The pairing once the session settles, or nil if no host took it.
        func waitForHost() async -> Pairing? {
            await withCheckedContinuation { continuation in
                let settled = outcome.withLockedValue { state -> Pairing?? in
                    guard state.resolved else {
                        state.waiter = continuation
                        return nil
                    }
                    return .some(state.pairing)
                }
                if let settled {
                    continuation.resume(returning: settled)
                }
            }
        }
    }

    /// Everything the relay tracks across connections.
    private struct State {
        /// Connections being served.
        var connectionCount = 0
        /// Connections being served, by remote IP address.
        var connectionsByAddress: [String: Int] = [:]
        /// Each registered host's control connection.
        var hosts: [HostKey: HostRegistration] = [:]
        /// Sessions waiting for their host, by session ID.
        var sessions: [String: PendingSession] = [:]
    }
}

/// Two joined connections. Each side's task copies its own input to the
/// other side's output; neither leaves (which closes its connection) until
/// both directions are done.
private final class Splice: Sendable {
    /// Both connections, closed together if either direction fails.
    private let channels: [any Channel]
    /// Directions still copying, and the pumps waiting for the other.
    private let remaining = NIOLockedValueBox<(count: Int, waiters: [CheckedContinuation<Void, Never>])>((2, []))

    /// Joins `channels`.
    init(channels: [any Channel]) {
        self.channels = channels
    }

    /// Copies `reader`'s remaining input to `peer`, then waits for the
    /// other direction to finish.
    func pump(from reader: inout FrameReader, to peer: NIOAsyncChannelOutboundWriter<ByteBuffer>) async {
        do {
            if let leftover = reader.takeBuffered() {
                try await peer.write(ByteBuffer(bytes: leftover))
            }
            while let chunk = try await reader.nextChunk() {
                try await peer.write(chunk)
            }
            peer.finish()
        } catch {
            for channel in channels {
                channel.close(promise: nil)
            }
        }
        await finishDirection()
    }

    /// Marks one direction done and waits until the other is too.
    private func finishDirection() async {
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            let waiters = remaining.withLockedValue { state -> [CheckedContinuation<Void, Never>] in
                state.count -= 1
                state.waiters.append(continuation)
                guard state.count == 0 else { return [] }
                defer { state.waiters = [] }
                return state.waiters
            }
            waiters.forEach { $0.resume() }
        }
    }
}

/// Reads relay frames, then raw bytes, from a connection's input.
private struct FrameReader {
    /// The connection's input.
    private var iterator: NIOAsyncChannelInboundStream<ByteBuffer>.AsyncIterator
    /// Bytes read but not yet consumed as a frame.
    private var buffer = Data()

    /// Reads from `iterator`.
    init(_ iterator: NIOAsyncChannelInboundStream<ByteBuffer>.AsyncIterator) {
        self.iterator = iterator
    }

    /// The next frame, or nil when the connection ends first.
    mutating func next<T: Decodable>(_ type: T.Type) async throws -> T? {
        while true {
            if let message = try RelayFrame.decode(type, from: &buffer) {
                return message
            }
            guard let chunk = try await iterator.next() else { return nil }
            buffer.append(contentsOf: chunk.readableBytesView)
        }
    }

    /// Bytes read past the last frame, if any.
    mutating func takeBuffered() -> Data? {
        defer { buffer = Data() }
        return buffer.isEmpty ? nil : buffer
    }

    /// The next raw chunk, or nil at end of input.
    mutating func nextChunk() async throws -> ByteBuffer? {
        try await iterator.next()
    }
}
