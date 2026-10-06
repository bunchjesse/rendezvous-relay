# AGENTS.md

Notes for agents working in this repo: a generic TCP relay that joins two devices addressed by Ed25519 public keys. [README.md](README.md) describes the protocol, the products, and deployment.

## Layout

| Path | Contents |
|------|----------|
| `Sources/RendezvousRelayProtocol` | Wire format and `RelayAddress`. Foundation only — keep it that way so any client can adopt it |
| `Sources/RendezvousRelayClient` | `RelayClient` (`RelayHandshakeHandler`) and `RelayHost` (`RelayControlHandler`). `NIOCore` only: transport and signing are the caller's |
| `Sources/RendezvousRelayServer` | `RelayServer` and `RelayServerConfiguration` (NIOPosix, swift-crypto) |
| `Sources/RendezvousRelayMain` | The `rendezvous-relay` executable: flags and environment, signal handling |
| `Tests/RendezvousRelayTests` | Swift Testing; an in-process relay on 127.0.0.1 (`TestRelay`), `RawRelayConnection` for hand-built frames, `EmbeddedChannel` tests for the handshake |

## Commands

```bash
swift build
swift test
docker compose up --build     # the server in a Linux container
```

## Rules

- **Stay application-agnostic.** Nothing here may know about a particular app. Applications are told apart by namespace only.
- **The relay never reads peer bytes.** After `connected`, it only splices. Anything that needs to look inside belongs in the application.
- **Every wire change is a compatibility break** for deployed relays and shipped clients. Add fields as optionals that older peers can ignore, and never change the proof payload tags (`rendezvous-relay-listen/1`, `rendezvous-relay-accept/1`) without a new version suffix.
- **Bound every wait.** Requests, proofs, session accepts, and control connections all have timeouts; a new state must too.
- **The client library must support both pipeline entry points**: a handshake added in a bootstrap's channel initializer (before the channel is active) and one added to an already-active channel.
- Swift Testing only. Every `@Test` and every declaration gets a `///` doc comment.

## Deployment

The GitHub workflow tests on Linux and publishes `ghcr.io/bunchjesse/rendezvous-relay` on pushes to `main` and on tags. Production runs that image as the `relay` service in the personal-site repo's `docker-compose.prod.yml` on the bunch.dev droplet, with the allowed namespaces set there.
