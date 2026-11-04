import TypedStreamCore

/// Deterministic, zero-network stand-in for a model backend.
/// Script every failure mode the pipeline must survive.
public struct FakeLanguageModel<Partial: Sendable, Output: Sendable>: Sendable {
    public typealias Handler = @Sendable (String) -> AsyncThrowingStream<ProviderEvent<Partial, Output>, Error>

    private let handler: Handler

    public init(handler: @escaping Handler) {
        self.handler = handler
    }

    public func provider() -> StreamProvider<Partial, Output> {
        StreamProvider(events: handler)
    }
}

extension FakeLanguageModel {
    /// Yields `partials` then `final`, synchronously.
    public static func scripted(partials: [PartialSnapshot<Partial>], final: Output) -> Self {
        Self { _ in
            AsyncThrowingStream { continuation in
                for snapshot in partials {
                    continuation.yield(.partial(snapshot))
                }
                continuation.yield(.final(final))
                continuation.finish()
            }
        }
    }

    /// Fails immediately with `error`.
    public static func failing(_ error: Error) -> Self {
        Self { _ in
            AsyncThrowingStream { continuation in
                continuation.finish(throwing: error)
            }
        }
    }

    /// Never yields. Cancellation-cooperative: `onCancel` fires when the
    /// consumer goes away, so tests can assert the producer was cancelled.
    public static func hanging(onCancel: @Sendable @escaping () -> Void = {}) -> Self {
        Self { _ in
            AsyncThrowingStream { continuation in
                let task = Task {
                    while !Task.isCancelled {
                        try? await Task.sleep(for: .seconds(30))
                    }
                    onCancel()
                    continuation.finish()
                }
                continuation.onTermination = { _ in task.cancel() }
            }
        }
    }

    /// Waits `firstEventDelay` before yielding — for timeout tests.
    public static func slow(
        firstEventDelay: Duration,
        partials: [PartialSnapshot<Partial>],
        final: Output
    ) -> Self {
        Self { _ in
            AsyncThrowingStream { continuation in
                let task = Task {
                    do {
                        try await Task.sleep(for: firstEventDelay)
                        try Task.checkCancellation()
                        for snapshot in partials {
                            continuation.yield(.partial(snapshot))
                        }
                        continuation.yield(.final(final))
                        continuation.finish()
                    } catch {
                        continuation.finish(throwing: error)
                    }
                }
                continuation.onTermination = { _ in task.cancel() }
            }
        }
    }
}

/// Test-side flag for asserting producer cancellation.
public actor Probe {
    private var _marked = false

    public init() {}

    public func mark() { _marked = true }
    public var marked: Bool { _marked }
}
