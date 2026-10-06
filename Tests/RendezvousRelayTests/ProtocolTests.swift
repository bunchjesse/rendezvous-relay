import Foundation
import Testing
import RendezvousRelayProtocol

/// The wire framing and proof payloads.
struct RelayFrameTests {
    /// A request survives a round trip through the framing, and a frame that
    /// hasn't fully arrived reads as nothing, leaving the buffer intact for
    /// the bytes still to come.
    @Test func framesRoundTripAndWaitForTheirTail() throws {
        let request = RelayRequest.connect(namespace: "com.example.app", endpointID: "ab")
        let frame = try RelayFrame.encode(request)

        var partial = frame.prefix(frame.count - 1)
        #expect(try RelayFrame.decode(RelayRequest.self, from: &partial) == nil)
        #expect(partial.count == frame.count - 1)

        var buffer = frame + Data("tail".utf8)
        #expect(try RelayFrame.decode(RelayRequest.self, from: &buffer) == request)
        #expect(buffer == Data("tail".utf8))
    }

    /// A frame declaring a payload past the limit is rejected from its
    /// header alone, so a peer can't make the other side buffer without bound.
    @Test func oversizedFramesAreRejected() {
        let length = UInt32(RelayFrame.maxPayloadLength + 1)
        var buffer = Data([UInt8(length >> 24), UInt8((length >> 16) & 0xff), UInt8((length >> 8) & 0xff), UInt8(length & 0xff)])
        #expect(throws: RelayFrameError.frameTooLarge(Int(length))) {
            _ = try RelayFrame.decode(RelayEvent.self, from: &buffer)
        }
    }

    /// The listen and accept proofs differ for every field, so a signature
    /// for one namespace, host, nonce, or session can't be replayed for another.
    @Test func proofPayloadsBindEveryField() {
        let base = RelayFrame.listenProofPayload(namespace: "a", endpointID: "b", nonce: Data([1]))
        #expect(base != RelayFrame.listenProofPayload(namespace: "x", endpointID: "b", nonce: Data([1])))
        #expect(base != RelayFrame.listenProofPayload(namespace: "a", endpointID: "x", nonce: Data([1])))
        #expect(base != RelayFrame.listenProofPayload(namespace: "a", endpointID: "b", nonce: Data([2])))
        #expect(base != RelayFrame.acceptProofPayload(namespace: "a", endpointID: "b", sessionID: "\u{1}"))
    }
}

/// Parsing and printing relay addresses.
struct RelayAddressTests {
    /// Addresses parse in every form users type, filling in the default
    /// port where none is given.
    @Test(arguments: [
        ("relay.example.com:4000", RelayAddress(host: "relay.example.com", port: 4000)),
        ("relay.example.com", RelayAddress(host: "relay.example.com", port: RelayFrame.defaultPort)),
        (" relay.example.com:4000 ", RelayAddress(host: "relay.example.com", port: 4000)),
        ("10.0.0.2:51000", RelayAddress(host: "10.0.0.2", port: 51000)),
        ("[fd00::1]:51000", RelayAddress(host: "fd00::1", port: 51000)),
        ("[fd00::1]", RelayAddress(host: "fd00::1", port: RelayFrame.defaultPort)),
        ("fd00::1", RelayAddress(host: "fd00::1", port: RelayFrame.defaultPort)),
    ] as [(String, RelayAddress)])
    func parsesValidAddresses(input: String, expected: RelayAddress) {
        #expect(RelayAddress(parsing: input) == expected)
    }

    /// Malformed input — URLs, credentials, out-of-range or non-numeric
    /// ports, spaces, empty strings — is rejected instead of guessed at.
    @Test(arguments: [
        "http://relay.example.com:3340",
        "user@relay.example.com",
        "relay.example.com:0",
        "relay.example.com:70000",
        "relay.example.com:port",
        "relay.example.com:",
        ":3340",
        "",
        "   ",
        "host name:3340",
        "[fd00::1]x",
        "relay:example:com",
    ])
    func rejectsMalformedAddresses(input: String) {
        #expect(RelayAddress(parsing: input) == nil)
    }

    /// The description parses back to the same address, IPv6 included.
    @Test func descriptionRoundTrips() {
        for address in [RelayAddress(host: "fd00::1", port: 51000), RelayAddress(host: "relay.example.com")] {
            #expect(RelayAddress(parsing: address.description) == address)
        }
        #expect(RelayAddress(host: "fd00::1", port: 51000).description == "[fd00::1]:51000")
    }
}
