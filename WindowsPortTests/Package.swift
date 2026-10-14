// swift-tools-version:5.9
//===----------------------------------------------------------------------===//
//
// Regression harness for the Windows selector backend (`SelectorWSAPoll.swift`)
// and for the winsock error codes NIO reports.
//
// This is a *separate* SwiftPM package rather than a target in the root
// manifest for one measured reason: `Tests/NIOPosixTests` does not compile on
// Windows at all (6761 errors at this checkout — `usleep`, `socketpair`,
// `socklen_t`, `sockaddr_in`, `CInt`-vs-`SOCKET` handles, …). On Windows
// SwiftPM links every test target into one runner executable, so a single
// broken test target makes the whole suite unrunnable — `--filter` does not
// help. Keeping these tests in their own package means they can actually be
// run on the platform they are about.
//
// The tests themselves use only *public* NIO API and are platform-neutral;
// upstream they belong in `Tests/NIOPosixTests` once that module builds for
// Windows.
//
//===----------------------------------------------------------------------===//
import PackageDescription

let package = Package(
    name: "WindowsPortTests",
    platforms: [.macOS(.v13), .iOS(.v16), .tvOS(.v16), .watchOS(.v9)],
    dependencies: [
        .package(path: "..")
    ],
    targets: [
        .testTarget(
            name: "WindowsPortTests",
            dependencies: [
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
            ]
        )
    ]
)
