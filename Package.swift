// swift-tools-version:6.0
import PackageDescription

let package = Package(
    name: "rendezvous-relay",
    platforms: [
        .macOS(.v14),
        .iOS(.v17),
        .visionOS(.v1),
    ],
    products: [
        .library(name: "RendezvousRelayProtocol", targets: ["RendezvousRelayProtocol"]),
        .library(name: "RendezvousRelayClient", targets: ["RendezvousRelayClient"]),
        .library(name: "RendezvousRelayServer", targets: ["RendezvousRelayServer"]),
        .executable(name: "rendezvous-relay", targets: ["RendezvousRelayMain"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.81.0"),
        .package(url: "https://github.com/apple/swift-crypto.git", "3.0.0"..<"5.0.0"),
    ],
    targets: [
        // The wire format. Foundation only, so any client can adopt it.
        .target(name: "RendezvousRelayProtocol"),
        // The device side over SwiftNIO channels, whichever transport opened
        // them. Signing is the caller's, so it needs no crypto library.
        .target(
            name: "RendezvousRelayClient",
            dependencies: [
                "RendezvousRelayProtocol",
                .product(name: "NIOConcurrencyHelpers", package: "swift-nio"),
                .product(name: "NIOCore", package: "swift-nio"),
            ]
        ),
        // The relay, on SwiftNIO's POSIX sockets so it runs in a Linux
        // container as well as in-process in tests.
        .target(
            name: "RendezvousRelayServer",
            dependencies: [
                "RendezvousRelayProtocol",
                .product(name: "NIOConcurrencyHelpers", package: "swift-nio"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "Crypto", package: "swift-crypto"),
            ]
        ),
        .executableTarget(
            name: "RendezvousRelayMain",
            dependencies: ["RendezvousRelayServer"]
        ),
        .testTarget(
            name: "RendezvousRelayTests",
            dependencies: [
                "RendezvousRelayProtocol",
                "RendezvousRelayClient",
                "RendezvousRelayServer",
                .product(name: "NIOConcurrencyHelpers", package: "swift-nio"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOEmbedded", package: "swift-nio"),
                .product(name: "Crypto", package: "swift-crypto"),
            ]
        ),
    ]
)
