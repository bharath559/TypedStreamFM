// swift-tools-version: 6.0
import PackageDescription

// NOTE: swift-tools-version 6.0 defaults the package to Swift 6 language mode
// (complete strict concurrency). Verified against the Swift 6.0 release notes;
// if your toolchain disagrees, add `.swiftLanguageMode(.v6)` to each target's
// swiftSettings.

let package = Package(
    name: "TypedStreamFM",
    products: [
        .library(name: "TypedStreamCore", targets: ["TypedStreamCore"]),
        .library(name: "TypedStreamTestKit", targets: ["TypedStreamTestKit"]),
    ],
    targets: [
        .target(name: "TypedStreamCore"),
        .target(
            name: "TypedStreamTestKit",
            dependencies: ["TypedStreamCore"]
        ),
        .testTarget(
            name: "TypedStreamTests",
            dependencies: ["TypedStreamCore", "TypedStreamTestKit"]
        ),
    ]
)
