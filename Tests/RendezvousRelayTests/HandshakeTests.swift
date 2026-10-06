import Foundation
import NIOCore
import NIOEmbedded
import Testing
@testable import RendezvousRelayClient
import RendezvousRelayProtocol

/// Drives ``RelayHandshakeHandler`` on an `EmbeddedChannel`, where the
/// relay's bytes arrive exactly as the test splits them.
struct RelayHandshakeTests {
    /// Starts a client join on a fresh, active embedded channel whose
    /// `install` adds a collecting handler, returning the channel, the join's
    /// future, and the bytes the handlers after the handshake receive.
    private func startJoin(timeout: TimeAmount = .seconds(5)) throws -> (channel: EmbeddedChannel, joined: EventLoopFuture<Void>, received: Locked<[ByteBuffer]>) {
        let channel = EmbeddedChannel()
        try channel.connect(to: SocketAddress(ipAddress: "127.0.0.1", port: 1)).wait()
        let received = Locked<[ByteBuffer]>([])
        let joined = try RelayClient.connect(on: channel, namespace: "test", endpointID: "ab", timeout: timeout) { channel in
            try channel.pipeline.syncOperations.addHandler(Recorder(received: received))
        }
        return (channel, joined, received)
    }

    /// The client sends its request as soon as the channel is active, and
    /// the peer's bytes that arrive in the same read as `connected` reach the
    /// application's handler, not the handshake, which leaves the pipeline.
    @Test func bytesBehindConnectedReachTheApplication() throws {
        let (channel, joined, received) = try startJoin()

        let sent = try #require(try channel.readOutbound(as: ByteBuffer.self))
        var sentBytes = Data(sent.readableBytesView)
        #expect(try RelayFrame.decode(RelayRequest.self, from: &sentBytes) == .connect(namespace: "test", endpointID: "ab"))

        var reply = ByteBuffer(bytes: try RelayFrame.encode(RelayEvent.connected))
        reply.writeString("SSH-2.0-peer\r\n")
        try channel.writeInbound(reply)

        try joined.wait()
        #expect(received.value.map { String(buffer: $0) } == ["SSH-2.0-peer\r\n"])
        try channel.writeInbound(ByteBuffer(string: "more"))
        #expect(received.value.map { String(buffer: $0) } == ["SSH-2.0-peer\r\n", "more"])
        _ = try? channel.finish()
    }

    /// A refusal fails the join with the relay's reason and closes the
    /// connection, and the application's handlers are never installed.
    @Test func refusalFailsTheJoin() throws {
        let (channel, joined, received) = try startJoin()
        _ = try channel.readOutbound(as: ByteBuffer.self)

        try channel.writeInbound(ByteBuffer(bytes: try RelayFrame.encode(RelayEvent.refused(.hostOffline))))

        #expect(throws: RelayClientError.refused(.hostOffline)) { try joined.wait() }
        #expect(received.value.isEmpty)
        #expect(!channel.isActive)
    }

    /// A relay that never answers fails the join once the timeout passes,
    /// instead of leaving the caller waiting.
    @Test func silenceTimesOut() throws {
        let (channel, joined, _) = try startJoin(timeout: .seconds(3))
        channel.embeddedEventLoop.advanceTime(by: .seconds(3))
        #expect(throws: RelayClientError.timedOut) { try joined.wait() }
        #expect(!channel.isActive)
    }

    /// A connection that drops before the relay answers fails the join
    /// rather than leaving its future unfulfilled.
    @Test func earlyCloseFailsTheJoin() throws {
        let (channel, joined, _) = try startJoin()
        channel.pipeline.fireChannelInactive()
        #expect(throws: RelayClientError.connectionClosed) { try joined.wait() }
        _ = try? channel.finish()
    }
}

/// Records every buffer that reaches it.
private final class Recorder: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    /// Everything received, in order.
    private let received: Locked<[ByteBuffer]>

    /// A recorder appending to `received`.
    init(received: Locked<[ByteBuffer]>) {
        self.received = received
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = unwrapInboundIn(data)
        received.mutate { $0.append(buffer) }
    }
}
