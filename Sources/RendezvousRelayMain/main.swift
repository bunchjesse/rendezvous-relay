import Foundation
import RendezvousRelayServer

/// `rendezvous-relay [--host <address>] [--port <port>] [--allow-namespace <namespace>]...`
///
/// Serves until interrupted. `RELAY_HOST`, `RELAY_PORT`, and
/// `RELAY_ALLOWED_NAMESPACES` (comma-separated) set the same values from the
/// environment; flags win. With no allowed namespaces, every namespace is
/// served. A malformed value exits with a usage error rather than being
/// ignored, so a typo can't open the relay to every namespace.
var configuration = RelayServerConfiguration()
let environment = ProcessInfo.processInfo.environment
if let host = environment["RELAY_HOST"], !host.isEmpty {
    configuration.host = host
}
if let raw = environment["RELAY_PORT"], !raw.isEmpty {
    guard let port = Int(raw) else { exitWithUsage("RELAY_PORT isn't a port: \(raw)") }
    configuration.port = port
}
if let list = environment["RELAY_ALLOWED_NAMESPACES"], !list.isEmpty {
    let namespaces = list.split(separator: ",").map { $0.trimmingCharacters(in: .whitespaces) }.filter { !$0.isEmpty }
    guard !namespaces.isEmpty else { exitWithUsage("RELAY_ALLOWED_NAMESPACES names no namespace") }
    configuration.allowedNamespaces = Set(namespaces)
}

var flagNamespaces: Set<String> = []
var arguments = CommandLine.arguments.dropFirst()
while let flag = arguments.popFirst() {
    switch flag {
    case "--host":
        guard let value = arguments.popFirst() else { exitWithUsage() }
        configuration.host = value
    case "--port":
        guard let value = arguments.popFirst().flatMap(Int.init) else { exitWithUsage() }
        configuration.port = value
    case "--allow-namespace":
        guard let value = arguments.popFirst(), !value.isEmpty else { exitWithUsage() }
        flagNamespaces.insert(value)
    default:
        exitWithUsage()
    }
}
if !flagNamespaces.isEmpty {
    configuration.allowedNamespaces = flagNamespaces
}

/// Prints `problem`, if any, and the usage line, and exits with `EX_USAGE`.
func exitWithUsage(_ problem: String? = nil) -> Never {
    if let problem {
        FileHandle.standardError.write(Data("rendezvous-relay: \(problem)\n".utf8))
    }
    FileHandle.standardError.write(Data("usage: rendezvous-relay [--host <address>] [--port <port>] [--allow-namespace <namespace>]...\n".utf8))
    exit(64)
}

let server = RelayServer(configuration: configuration)
let serving = Task {
    do {
        try await server.run()
    } catch {
        FileHandle.standardError.write(Data("rendezvous-relay: \(error)\n".utf8))
        exit(1)
    }
    exit(0)
}

// Docker stops a container with SIGTERM; a terminal sends SIGINT.
let signalSources = [SIGTERM, SIGINT].map { number in
    signal(number, SIG_IGN)
    let source = DispatchSource.makeSignalSource(signal: number, queue: .main)
    source.setEventHandler { serving.cancel() }
    source.resume()
    return source
}
withExtendedLifetime(signalSources) {
    dispatchMain()
}
