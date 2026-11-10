import TypedStreamCore

/// Deterministic, zero-network stand-in for a model backend.
/// Script every failure mode the pipeline must survive.
public struct FakeLanguageModel<Partial: Sendable, Output: Sendable>: Sendable {
    public typealias Handler = @Sendable (String) -> AsyncThrowingStream<ProviderEvent<Partial, Output>, Error>

    private let defaultRepairStrategy: RepairStrategy
    private let handler: Handler

    public init(
        defaultRepairStrategy: RepairStrategy = .fullPrompt,
        handler: @escaping Handler
    ) {
        self.defaultRepairStrategy = defaultRepairStrategy
        self.handler = handler
    }

    public func provider(repairStrategy: RepairStrategy? = nil) -> StreamProvider<Partial, Output> {
        StreamProvider(
            defaultRepairStrategy: repairStrategy ?? defaultRepairStrategy,
            events: handler
        )
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
                    continuation.finish()
                }
                continuation.onTermination = { _ in
                    task.cancel()
                    onCancel()
                }
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

/// Test-side recorder for inspecting prompts sent across attempts or sessions.
public actor PromptRecorder {
    public private(set) var prompts: [String] = []
    public private(set) var sessionCreations: Int = 0

    public init() {}

    public func record(prompt: String) {
        prompts.append(prompt)
    }

    public func recordSessionCreated() {
        sessionCreations += 1
    }
}
