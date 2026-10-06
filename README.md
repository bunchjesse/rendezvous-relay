# rendezvous-relay

A small TCP relay that joins two devices that can't reach each other directly — a phone on cellular and a Mac behind a home router, say — so they can talk anyway. Devices are addressed by their Ed25519 public keys. The relay copies bytes between them without reading them; the application is expected to authenticate and encrypt its own traffic end to end (an SSH session, TLS with pinned keys, Noise). The relay sees only which devices talk to each other and how much.

It's generic: any application can use one relay, kept apart from the others by a **namespace** (by convention the app's bundle identifier, such as `com.example.app`, with a separate namespace per build flavor if they mustn't see each other).

| Product | Contents | Dependencies |
|---------|----------|--------------|
| `RendezvousRelayProtocol` | Wire format: `RelayRequest`, `RelayEvent`, `RelayFrame`, `RelayAddress` | Foundation |
| `RendezvousRelayClient` | `RelayClient` (join a host) and `RelayHost` (stay registered, take sessions) over any SwiftNIO channel | `NIOCore`, `NIOConcurrencyHelpers` |
| `RendezvousRelayServer` | `RelayServer`, embeddable (tests run it in-process) | `NIOPosix`, `NIOConcurrencyHelpers`, `swift-crypto` |
| `rendezvous-relay` | The server executable | |

## How it works

```mermaid
sequenceDiagram
    participant H as Host
    participant R as Relay
    participant C as Client
    H->>R: listen(namespace, endpointID)
    R->>H: challenge(nonce)
    H->>R: proof(sign(nonce))
    R->>H: registered
    Note over H,R: control connection stays open (ping/pong every 20 s)
    C->>R: connect(namespace, endpointID)
    R->>H: incoming(sessionID)
    H->>R: accept(sessionID, sign(sessionID)) on a new connection
    R->>H: connected
    R->>C: connected
    Note over H,C: the relay splices the two connections; framing ends
```

- **Framing.** Every message is a 4-byte big-endian length followed by that many bytes of JSON, at most 16 KiB. Once the relay sends `connected`, the connection carries the peers' bytes only.
- **Endpoint IDs** are Ed25519 public keys as 64 lowercase hex digits. A host proves it owns its key by signing the relay's nonce (`RelayFrame.listenProofPayload`), and signs each session it takes (`RelayFrame.acceptProofPayload`), so nobody else can register as the host or steal its sessions. A newer registration with a valid proof replaces an older one, which is how a host that changed networks takes its name back.
- **Clients are not authenticated by the relay.** Anyone who knows a host's endpoint ID can ask to be joined to it; the host's own protocol must decide whether to trust the peer, as SSH does with keys.
- **Namespaces.** A host registered in one namespace can't be reached from another. A relay can be limited to a list of namespaces (`--allow-namespace`), which keeps other applications off it. Namespaces aren't secret — they travel in the clear — so the list stops casual and accidental use, not someone who sets out to use your relay.
- **Refusals** (`RelayRefusal`): `hostOffline`, `hostDidNotAnswer`, `invalidProof`, `unknownSession`, `busy`, `invalidRequest`, `namespaceNotAllowed`.

## Using the client library

Add the package, and depend on `RendezvousRelayClient` (or just `RendezvousRelayProtocol` for the wire types and `RelayAddress`):

```swift
.package(url: "https://github.com/bunchjesse/rendezvous-relay.git", from: "0.1.0"),
```

Open the TCP connection yourself, with whatever transport suits the platform (`NIOTransportServices` on Apple platforms, `NIOPosix` elsewhere), then hand the channel to the relay before anything else uses it. Once the relay joins the two ends, your `install` closure adds the application's handlers; any of the peer's bytes that arrived with the relay's last frame are passed on to them.

A client:

```swift
let joining = try await NIOTSConnectionBootstrap(group: NIOTSEventLoopGroup.singleton)
    .connect(host: relay.host, port: relay.port) { channel in
        channel.eventLoop.makeCompletedFuture {
            try RelayClient.connect(on: channel, namespace: "com.example.app", endpointID: hostID) { channel in
                try addMyProtocolHandlers(to: channel)   // runs once the relay has joined the ends
            }
        }
    }
let installed = try await joining.get()   // fails with RelayClientError if the relay refuses
```

A host:

```swift
let registration = RelayHost(
    namespace: "com.example.app",
    endpointID: myEndpointID,
    sign: { try privateKey.signature(for: $0) },
    dial: { try await bootstrap.connect(host: relay.host, port: relay.port).get() },
    acceptSession: { channel in try addMyServerHandlers(to: channel) },
    onStatusChange: { status in /* .connecting, .registered, .disconnected(error) */ }
)
registration.start()   // keeps reconnecting with backoff until stop()
```

## Running the server

```bash
swift run -c release rendezvous-relay --port 3340 --allow-namespace com.example.app
```

| Flag | Environment | Default |
|------|-------------|---------|
| `--host <address>` | `RELAY_HOST` | every interface (`::`, or `0.0.0.0` where there's no IPv6) |
| `--port <port>` | `RELAY_PORT` | `3340` |
| `--allow-namespace <ns>` (repeatable) | `RELAY_ALLOWED_NAMESPACES` (comma-separated) | every namespace |

Flags win over the environment. A malformed `RELAY_PORT`, or a `RELAY_ALLOWED_NAMESPACES` that names no namespace, exits with a usage error instead of serving. The server logs to standard error: when it starts listening, when hosts register and leave, and refused namespaces.

### Docker

```bash
docker compose up -d --build
docker compose logs -f     # "Listening on :: port 3340, serving …"
```

Pushes to `main` publish `ghcr.io/bunchjesse/rendezvous-relay:latest` (and a tag per commit and git tag) after the tests pass on Linux, so a deployment can pull the image instead of building it. A deployment needs only TCP port 3340 open; the relay keeps no state on disk.

## Security notes

- **No TLS on the relay hop.** Everything the relay forwards is already encrypted end to end by the peers, and a host proves it owns its key before it's registered, so TLS would hide only the endpoint IDs, namespaces, and traffic volume from someone on the network.
- **Limits.** By default the relay serves 4096 connections at once, 64 from any one IPv4 address or IPv6 /64 (clients behind one NAT share that), and 32 pending sessions per host; requests must arrive within 10 seconds, and hosts must take a session within 10 seconds. See `RelayServerConfiguration`.

## Development

```bash
swift build
swift test
```

The tests run a relay in-process on 127.0.0.1, so they need no deployment. See [AGENTS.md](AGENTS.md) for conventions.
