import Foundation
import NIOCore
import RendezvousRelayProtocol

/// Why a relay request didn't produce a joined connection or a registration.
public enum RelayClientError: Error, Equatable, Sendable {
    /// The relay turned the request down.
    case refused(RelayRefusal)
    /// The relay sent something the request doesn't allow at this point.
    case unexpectedEvent(RelayEvent)
    /// The relay sent bytes that aren't a valid frame.
    case malformedFrame
    /// The connection closed before the relay answered.
    case connectionClosed
    /// The relay didn't answer in time.
    case timedOut
}

/// Joins connections through a relay.
///
/// The caller opens the TCP connection, with whatever transport and socket
/// options suit it, and hands the channel over before anything else uses it.
/// The relay handshake runs at the front of the pipeline; once the relay
/// joins the two ends, `install` adds the application's own handlers, and
/// the handshake leaves the pipeline, passing on any of the peer's bytes that
/// arrived behind the relay's last frame.
public enum RelayClient {
    /// How long the relay may take to join a connection.
    public static let defaultTimeout: TimeAmount = .seconds(15)

    /// Asks the relay on `channel` to join it to the host `endpointID` in
    /// `namespace`. Must be called on the channel's event loop, before it
    /// becomes active or before anything has been read from it, typically
    /// from a bootstrap's channel initializer.
    ///
    /// The returned future carries whatever `install` returns, and fails
    /// with a ``RelayClientError`` (closing the channel) if the relay refuses,
    /// doesn't answer within `timeout`, or the connection drops first.
    /// `install` runs on the event loop once the relay has joined the two
    /// ends; handlers it adds to the end of the pipeline receive the peer's
    /// first bytes.
    public static func connect<Result: Sendable>(
        on channel: any Channel,
        namespace: String,
        endpointID: String,
        timeout: TimeAmount = defaultTimeout,
        install: @escaping @Sendable (any Channel) throws -> Result
    ) throws -> EventLoopFuture<Result> {
        try join(channel, sending: .connect(namespace: namespace, endpointID: endpointID), timeout: timeout, install: install)
    }

    /// Sends `request`, which the relay answers with ``RelayEvent/connected``
    /// once the two ends are joined, and then runs `install`.
    static func join<Result: Sendable>(
        _ channel: any Channel,
        sending request: RelayRequest,
        timeout: TimeAmount,
        install: @escaping @Sendable (any Channel) throws -> Result
    ) throws -> EventLoopFuture<Result> {
        let promise = channel.eventLoop.makePromise(of: Result.self)
        let handler = RelayHandshakeHandler(request: try RelayFrame.encode(request), timeout: timeout, promise: promise, install: install)
        try channel.pipeline.syncOperations.addHandler(handler)
        return promise.futureResult
    }
}

/// The front of a pipeline until the relay joins the connection: sends the
/// request, reads the relay's answer, installs the application's handlers,
/// and then removes itself, forwarding any bytes that followed the answer.
final class RelayHandshakeHandler<Result: Sendable>: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer
    typealias OutboundOut = ByteBuffer

    /// The framed request to send.
    private let request: Data
    /// How long the relay may take to join the connection.
    private let timeout: TimeAmount
    /// Completed once, with what `install` returned or why the join failed.
    private let promise: EventLoopPromise<Result>
    /// Adds the application's handlers once the relay joins the ends.
    private let install: @Sendable (any Channel) throws -> Result
    /// Bytes from the relay not yet decoded.
    private var frames = RelayFrameBuffer()
    /// Fails the join when `timeout` passes.
    private var deadline: Scheduled<Void>?
    /// Whether the request went out.
    private var hasSentRequest = false
    /// Whether `promise` is complete.
    private var isSettled = false
    /// The peer's bytes that arrived with the relay's answer, forwarded as
    /// the handler leaves the pipeline.
    private var leftover: ByteBuffer?

    /// A handshake sending `request`, completing `promise`.
    init(request: Data, timeout: TimeAmount, promise: EventLoopPromise<Result>, install: @escaping @Sendable (any Channel) throws -> Result) {
        self.request = request
        self.timeout = timeout
        self.promise = promise
        self.install = install
    }

    func handlerAdded(context: ChannelHandlerContext) {
        if context.channel.isActive {
            sendRequest(context: context)
        }
    }

    func handlerRemoved(context: ChannelHandlerContext) {
        settle(.failure(RelayClientError.connectionClosed))
    }

    func channelActive(context: ChannelHandlerContext) {
        sendRequest(context: context)
        context.fireChannelActive()
    }

    func channelInactive(context: ChannelHandlerContext) {
        settle(.failure(RelayClientError.connectionClosed))
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        guard !isSettled else {
            context.fireErrorCaught(error)
            return
        }
        settle(.failure(error))
        context.close(promise: nil)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard !isSettled else {
            context.fireChannelRead(data)
            return
        }
        var bytes = unwrapInboundIn(data)
        frames.append(&bytes)
        let event: RelayEvent?
        do {
            event = try frames.next(RelayEvent.self)
        } catch {
            fail(RelayClientError.malformedFrame, context: context)
            return
        }
        guard let event else { return }
        switch event {
        case .connected:
            joined(context: context)
        case .refused(let refusal):
            fail(RelayClientError.refused(refusal), context: context)
        default:
            fail(RelayClientError.unexpectedEvent(event), context: context)
        }
    }

    func removeHandler(context: ChannelHandlerContext, removalToken: ChannelHandlerContext.RemovalToken) {
        if let leftover, leftover.readableBytes > 0 {
            context.fireChannelRead(wrapInboundOut(leftover))
            context.fireChannelReadComplete()
        }
        leftover = nil
        context.leavePipeline(removalToken: removalToken)
    }

    /// Sends the request once and starts the deadline.
    private func sendRequest(context: ChannelHandlerContext) {
        guard !hasSentRequest else { return }
        hasSentRequest = true
        deadline = context.eventLoop.assumeIsolated().scheduleTask(in: timeout) { [self] in
            fail(RelayClientError.timedOut, context: context)
        }
        context.writeAndFlush(wrapOutboundOut(ByteBuffer(bytes: request)), promise: nil)
    }

    /// Hands the connection to the application's handlers.
    private func joined(context: ChannelHandlerContext) {
        let installed: Result
        do {
            installed = try install(context.channel)
        } catch {
            fail(error, context: context)
            return
        }
        leftover = frames.takeRemaining()
        settle(.success(installed))
        context.pipeline.syncOperations.removeHandler(context: context, promise: nil)
    }

    /// Fails the join with `error` and closes the connection.
    private func fail(_ error: any Error, context: ChannelHandlerContext) {
        guard !isSettled else { return }
        settle(.failure(error))
        context.close(promise: nil)
    }

    /// Completes the join with `result`, once.
    private func settle(_ result: Swift.Result<Result, any Error>) {
        guard !isSettled else { return }
        isSettled = true
        deadline?.cancel()
        deadline = nil
        promise.completeWith(result)
    }
}

@available(*, unavailable)
extension RelayHandshakeHandler: Sendable {}

/// Accumulates bytes from the relay and splits off whole frames.
struct RelayFrameBuffer {
    /// Bytes not yet split into frames.
    private var buffer = Data()

    /// Appends everything readable in `bytes`.
    mutating func append(_ bytes: inout ByteBuffer) {
        if let read = bytes.readBytes(length: bytes.readableBytes) {
            buffer.append(contentsOf: read)
        }
    }

    /// The next whole frame, or nil until one has arrived.
    mutating func next<T: Decodable>(_ type: T.Type) throws -> T? {
        try RelayFrame.decode(type, from: &buffer)
    }

    /// Everything after the frames read so far, emptying the buffer.
    mutating func takeRemaining() -> ByteBuffer? {
        defer { buffer = Data() }
        return buffer.isEmpty ? nil : ByteBuffer(bytes: buffer)
    }
}
