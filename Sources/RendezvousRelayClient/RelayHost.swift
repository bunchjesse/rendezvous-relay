import Foundation
import NIOCore
import NIOConcurrencyHelpers
import RendezvousRelayProtocol

/// A host's registration with a relay: a control connection that stays open
/// so clients can reach the host through the relay, reconnecting with
/// backoff whenever it drops.
///
/// The host proves it owns its endpoint ID by signing the relay's challenge,
/// and signs each session it takes, so nobody else can register as the host
/// or take its sessions. Each session arrives on a connection of its own,
/// which `acceptSession` sets up like any inbound connection.
public final class RelayHost: Sendable {
    /// Where the registration stands.
    public enum Status: Sendable {
        /// Dialing the relay or proving the host's key.
        case connecting
        /// Registered: clients can reach the host through the relay.
        case registered
        /// The last attempt failed with `error`; another follows after a backoff.
        case disconnected(any Error)
    }

    /// Opens a new TCP connection to the relay and returns its channel,
    /// active and with nothing read from it yet.
    public typealias Dial = @Sendable () async throws -> any Channel

    /// How long the relay may take to answer a request.
    public static let replyTimeout: TimeAmount = .seconds(10)
    /// How long the control connection may stay silent. The relay pings
    /// every 20 seconds by default.
    public static let controlTimeout: TimeAmount = .seconds(50)
    /// Registrations that last this long reset the reconnect backoff.
    static let stableRegistration: Duration = .seconds(30)
    /// The longest wait between attempts.
    static let maximumBackoff: Duration = .seconds(30)
    /// Most sessions taken at once. Later announcements are ignored, and
    /// the relay tells their clients the host didn't answer, so a relay
    /// can't make the host open connections without bound.
    static let maxConcurrentAccepts = 32

    /// The application's namespace on the relay.
    private let namespace: String
    /// This host's Ed25519 public key, in hex.
    private let endpointID: String
    /// Signs with the key `endpointID` names.
    private let sign: @Sendable (Data) throws -> Data
    /// Opens connections to the relay.
    private let dial: Dial
    /// Sets up each joined session connection.
    private let acceptSession: @Sendable (any Channel) throws -> Void
    /// Receives every status change.
    private let onStatusChange: @Sendable (Status) -> Void
    /// Receives failures that don't change the status.
    private let log: @Sendable (String) -> Void
    /// The reconnect loop, while started.
    private let task = NIOLockedValueBox<Task<Void, Never>?>(nil)
    /// Sessions being taken right now.
    private let accepting = NIOLockedValueBox(0)

    /// Creates a registration for the host `endpointID` in `namespace`.
    ///
    /// - Parameters:
    ///   - sign: Signs bytes with the Ed25519 private key `endpointID` names.
    ///   - dial: Opens connections to the relay, for the control connection
    ///     and for every session.
    ///   - acceptSession: Sets up a client's joined connection. Runs on the
    ///     connection's event loop; handlers it adds to the end of the
    ///     pipeline receive the client's first bytes.
    ///   - onStatusChange: Receives every change in ``Status``.
    ///   - log: Receives a line for failures that don't change the status,
    ///     such as a session that couldn't be taken.
    public init(
        namespace: String,
        endpointID: String,
        sign: @escaping @Sendable (Data) throws -> Data,
        dial: @escaping Dial,
        acceptSession: @escaping @Sendable (any Channel) throws -> Void,
        onStatusChange: @escaping @Sendable (Status) -> Void = { _ in },
        log: @escaping @Sendable (String) -> Void = { _ in }
    ) {
        self.namespace = namespace
        self.endpointID = endpointID
        self.sign = sign
        self.dial = dial
        self.acceptSession = acceptSession
        self.onStatusChange = onStatusChange
        self.log = log
    }

    /// Starts registering, retrying with backoff until ``stop()``. Does
    /// nothing if already started.
    public func start() {
        task.withLockedValue { task in
            guard task == nil else { return }
            task = Task { await self.run() }
        }
    }

    /// Ends the registration and its reconnect loop, closing the control
    /// connection. Sessions already joined stay open.
    public func stop() {
        task.withLockedValue { task in
            task?.cancel()
            task = nil
        }
    }

    /// Registers again and again, waiting a second after a failure and
    /// doubling the wait up to ``maximumBackoff`` while failures continue.
    /// A registration that lasted ``stableRegistration`` starts over at a second.
    private func run() async {
        var backoff: Duration = .seconds(1)
        while !Task.isCancelled {
            let started = ContinuousClock.now
            onStatusChange(.connecting)
            do {
                try await register()
            } catch {
                guard !Task.isCancelled else { return }
                onStatusChange(.disconnected(error))
            }
            guard !Task.isCancelled else { return }
            if ContinuousClock.now - started >= Self.stableRegistration {
                backoff = .seconds(1)
            }
            try? await Task.sleep(for: backoff)
            backoff = min(backoff * 2, Self.maximumBackoff)
        }
    }

    /// Registers once and serves the control connection until it ends.
    private func register() async throws {
        let channel = try await dial()
        let (events, continuation) = AsyncThrowingStream.makeStream(of: RelayEvent.self)
        do {
            try await channel.eventLoop.submit {
                try channel.pipeline.syncOperations.addHandlers([
                    IdleStateHandler(readTimeout: Self.controlTimeout),
                    RelayControlHandler(events: continuation),
                ])
            }.get()
        } catch {
            channel.close(promise: nil)
            throw error
        }
        try await withTaskCancellationHandler {
            defer { channel.close(promise: nil) }
            var replies = events.makeAsyncIterator()
            try await send(.listen(namespace: namespace, endpointID: endpointID), on: channel)
            let challenge = try await nextEvent(from: &replies, on: channel, within: Self.replyTimeout)
            guard case .challenge(let nonce) = challenge else { throw Self.failure(for: challenge) }
            let proof = try sign(RelayFrame.listenProofPayload(namespace: namespace, endpointID: endpointID, nonce: nonce))
            try await send(.proof(signature: proof), on: channel)
            let reply = try await nextEvent(from: &replies, on: channel, within: Self.replyTimeout)
            guard reply == .registered else { throw Self.failure(for: reply) }

            onStatusChange(.registered)
            while let event = try await replies.next() {
                switch event {
                case .ping:
                    try await send(.pong, on: channel)
                case .incoming(let sessionID):
                    // The host signs whatever ID it's given; sign only IDs
                    // shaped like the relay's own.
                    guard RelayFrame.isWellFormedSessionID(sessionID) else {
                        log("Ignored a relayed session with a malformed ID")
                        continue
                    }
                    let admitted = accepting.withLockedValue { count in
                        guard count < Self.maxConcurrentAccepts else { return false }
                        count += 1
                        return true
                    }
                    guard admitted else {
                        log("Ignored a relayed session: \(Self.maxConcurrentAccepts) are already being taken")
                        continue
                    }
                    Task {
                        await self.accept(sessionID: sessionID)
                        self.accepting.withLockedValue { $0 -= 1 }
                    }
                default:
                    throw RelayClientError.unexpectedEvent(event)
                }
            }
            throw RelayClientError.connectionClosed
        } onCancel: {
            channel.close(promise: nil)
        }
    }

    /// Takes the session `sessionID` on a new connection.
    private func accept(sessionID: String) async {
        do {
            let payload = RelayFrame.acceptProofPayload(namespace: namespace, endpointID: endpointID, sessionID: sessionID)
            let request = RelayRequest.accept(sessionID: sessionID, signature: try sign(payload))
            let channel = try await dial()
            let acceptSession = acceptSession
            try await channel.eventLoop.flatSubmit {
                do {
                    return try RelayClient.join(channel, sending: request, timeout: Self.replyTimeout, install: acceptSession)
                } catch {
                    channel.close(promise: nil)
                    return channel.eventLoop.makeFailedFuture(error)
                }
            }.get()
        } catch {
            log("Couldn't take a relayed session: \(error)")
        }
    }

    /// Writes one framed request.
    private func send(_ request: RelayRequest, on channel: any Channel) async throws {
        try await channel.writeAndFlush(ByteBuffer(bytes: try RelayFrame.encode(request))).get()
    }

    /// The next event, closing the connection if none arrives within `timeout`.
    private func nextEvent(
        from events: inout AsyncThrowingStream<RelayEvent, any Error>.Iterator,
        on channel: any Channel,
        within timeout: TimeAmount
    ) async throws -> RelayEvent {
        let deadline = channel.eventLoop.scheduleTask(in: timeout) {
            channel.pipeline.fireErrorCaught(RelayClientError.timedOut)
        }
        defer { deadline.cancel() }
        guard let event = try await events.next() else { throw RelayClientError.connectionClosed }
        return event
    }

    /// The error a reply that isn't the one expected stands for.
    private static func failure(for event: RelayEvent) -> RelayClientError {
        if case .refused(let refusal) = event { return .refused(refusal) }
        return .unexpectedEvent(event)
    }
}

/// Decodes a control connection's frames into a stream of events. The
/// stream fails when the connection breaks, sends a malformed frame, or goes
/// quiet past the ``IdleStateHandler`` ahead of it.
final class RelayControlHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    /// Where decoded events go.
    private let events: AsyncThrowingStream<RelayEvent, any Error>.Continuation
    /// Bytes not yet decoded.
    private var frames = RelayFrameBuffer()

    /// A handler yielding to `events`.
    init(events: AsyncThrowingStream<RelayEvent, any Error>.Continuation) {
        self.events = events
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var bytes = unwrapInboundIn(data)
        frames.append(&bytes)
        do {
            while let event = try frames.next(RelayEvent.self) {
                events.yield(event)
            }
        } catch {
            fail(RelayClientError.malformedFrame, context: context)
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        events.finish(throwing: RelayClientError.connectionClosed)
        context.fireChannelInactive()
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if event is IdleStateHandler.IdleStateEvent {
            fail(RelayClientError.timedOut, context: context)
        }
        context.fireUserInboundEventTriggered(event)
    }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        fail(error, context: context)
    }

    /// Ends the stream with `error` and closes the connection.
    private func fail(_ error: any Error, context: ChannelHandlerContext) {
        events.finish(throwing: error)
        context.close(promise: nil)
    }
}

@available(*, unavailable)
extension RelayControlHandler: Sendable {}
