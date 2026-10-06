import Crypto
import Foundation
import NIOCore
import NIOPosix
import NIOConcurrencyHelpers
@testable import RendezvousRelayClient
import RendezvousRelayProtocol
import RendezvousRelayServer

/// A relay running in-process on 127.0.0.1 with short timeouts.
struct TestRelay {
    /// The port the relay bound.
    let port: Int
    /// The task running the relay.
    private let task: Task<Void, any Error>

    /// Starts a relay serving `allowedNamespaces` (nil for any).
    static func start(allowedNamespaces: Set<String>? = nil) async throws -> TestRelay {
        let server = RelayServer(configuration: RelayServerConfiguration(
            host: "127.0.0.1",
            port: 0,
            allowedNamespaces: allowedNamespaces,
            requestTimeout: .seconds(5),
            sessionTimeout: .seconds(5),
            log: { _ in }
        ))
        let (bound, continuation) = AsyncStream.makeStream(of: Int.self)
        let task = Task { try await server.run { continuation.yield($0) } }
        var ports = bound.makeAsyncIterator()
        guard let port = await ports.next() else { throw RelayClientError.connectionClosed }
        return TestRelay(port: port, task: task)
    }

    /// Stops serving.
    func stop() async {
        task.cancel()
        _ = await task.result
    }

    /// Opens a plain TCP connection to the relay with nothing in its pipeline.
    func dial() async throws -> any Channel {
        try await ClientBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .connect(host: "127.0.0.1", port: port)
            .get()
    }

    /// Joins a new connection to `endpointID` through the relay, collecting
    /// what the host sends in the returned ``Collector``. The handshake goes
    /// in the bootstrap's channel initializer when `fromInitializer` is set,
    /// and onto the already-connected channel otherwise.
    func connect(namespace: String, endpointID: String, fromInitializer: Bool = false) async throws -> (channel: any Channel, received: Collector) {
        guard fromInitializer else {
            let channel = try await dial()
            let collector = try await channel.eventLoop.flatSubmit {
                do {
                    return try RelayClient.connect(on: channel, namespace: namespace, endpointID: endpointID) { channel in
                        try Collector.install(on: channel)
                    }
                } catch {
                    return channel.eventLoop.makeFailedFuture(error)
                }
            }.get()
            return (channel, collector)
        }
        let joining = try await ClientBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .connect(host: "127.0.0.1", port: port) { channel in
                channel.eventLoop.makeCompletedFuture {
                    let collector = try RelayClient.connect(on: channel, namespace: namespace, endpointID: endpointID) { channel in
                        try Collector.install(on: channel)
                    }
                    return (channel, collector)
                }
            }
        return (joining.0, try await joining.1.get())
    }
}

/// An Ed25519 identity as relay clients name it.
struct TestIdentity: Sendable {
    /// The signing key.
    let key = Curve25519.Signing.PrivateKey()

    /// The endpoint ID: the public key in lowercase hex.
    var endpointID: String {
        key.publicKey.rawRepresentation.relayHex
    }

    /// Signs `data` with the key.
    func sign(_ data: Data) throws -> Data {
        try key.signature(for: data)
    }
}

/// Collects the bytes a channel receives after the relay hands it over.
final class Collector: ChannelInboundHandler, Sendable {
    typealias InboundIn = ByteBuffer

    /// Bytes received so far, and the callers waiting for more.
    private struct State {
        /// Everything received.
        var bytes = Data()
        /// Callers waiting for at least `count` bytes.
        var waiters: [(count: Int, continuation: CheckedContinuation<Data, any Error>)] = []
    }

    /// The collector's state.
    private let state = NIOLockedValueBox(State())

    /// Adds a collector to the end of `channel`'s pipeline. Must run on its event loop.
    static func install(on channel: any Channel) throws -> Collector {
        let collector = Collector()
        try channel.pipeline.syncOperations.addHandler(collector)
        return collector
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = unwrapInboundIn(data)
        let (total, ready) = state.withLockedValue { state in
            state.bytes.append(contentsOf: buffer.readableBytesView)
            let total = state.bytes
            let ready = state.waiters.filter { $0.count <= total.count }
            state.waiters.removeAll { $0.count <= total.count }
            return (total, ready)
        }
        ready.forEach { $0.continuation.resume(returning: total.prefix($0.count)) }
    }

    /// Waits for at least `count` bytes and returns the first `count`.
    func first(_ count: Int, timeout: Duration = .seconds(5)) async throws -> Data {
        try await withThrowingTaskGroup(of: Data.self) { group in
            group.addTask {
                try await withCheckedThrowingContinuation { continuation in
                    let ready = self.state.withLockedValue { state -> Data? in
                        guard state.bytes.count < count else { return state.bytes.prefix(count) }
                        state.waiters.append((count, continuation))
                        return nil
                    }
                    if let ready {
                        continuation.resume(returning: ready)
                    }
                }
            }
            group.addTask {
                try await Task.sleep(for: timeout)
                throw RelayClientError.timedOut
            }
            defer { group.cancelAll() }
            return try await group.next()!
        }
    }
}

/// A connection to the relay that speaks frames by hand, for driving the
/// relay through orders and forgeries the client library never produces.
struct RawRelayConnection {
    /// The connection to the relay.
    private let channel: any Channel
    /// The relay's events, decoded.
    private var events: AsyncThrowingStream<RelayEvent, any Error>.Iterator

    /// Connects to the relay on `port` and sends `request`.
    static func open(port: Int, sending request: RelayRequest) async throws -> RawRelayConnection {
        let (stream, continuation) = AsyncThrowingStream.makeStream(of: RelayEvent.self)
        let channel = try await ClientBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(RelayControlHandler(events: continuation))
                }
            }
            .connect(host: "127.0.0.1", port: port)
            .get()
        var connection = RawRelayConnection(channel: channel, events: stream.makeAsyncIterator())
        try await connection.send(request)
        return connection
    }

    /// Wraps a connected channel and its events.
    private init(channel: any Channel, events: AsyncThrowingStream<RelayEvent, any Error>.Iterator) {
        self.channel = channel
        self.events = events
    }

    /// Sends one framed request.
    mutating func send(_ request: RelayRequest) async throws {
        try await channel.writeAndFlush(ByteBuffer(bytes: try RelayFrame.encode(request))).get()
    }

    /// Reads the relay's next event.
    mutating func next() async throws -> RelayEvent {
        guard let event = try await events.next() else { throw RelayClientError.connectionClosed }
        return event
    }

    /// Closes the connection.
    func close() {
        channel.close(promise: nil)
    }
}

/// A value shared across tasks and closures in tests.
final class Locked<Value: Sendable>: Sendable {
    /// The value, behind a lock.
    private let storage: NIOLockedValueBox<Value>

    /// Creates a box holding `value`.
    init(_ value: Value) {
        storage = NIOLockedValueBox(value)
    }

    /// The current value.
    var value: Value {
        get { storage.withLockedValue { $0 } }
        set { storage.withLockedValue { $0 = newValue } }
    }

    /// Changes the value in place.
    func mutate(_ change: (inout Value) -> Void) {
        storage.withLockedValue { change(&$0) }
    }
}

/// Waits until `condition` holds, failing after `timeout`.
func waitUntil(timeout: Duration = .seconds(5), _ condition: @Sendable () -> Bool) async throws {
    let deadline = ContinuousClock.now + timeout
    while !condition() {
        guard ContinuousClock.now < deadline else { throw RelayClientError.timedOut }
        try await Task.sleep(for: .milliseconds(20))
    }
}
