/// The seam between the pipeline and any model backend.
///
/// v1 ships `FoundationModelsProvider` (native structured partials); tests use
/// `FakeLanguageModel`. Providers must be cancellation-cooperative — check for
/// cancellation per yielded event — so the pipeline's terminal-event guarantee
/// holds.
public struct StreamProvider<Partial: Sendable, Output: Sendable>: Sendable {
    public let events: @Sendable (String) -> AsyncThrowingStream<ProviderEvent<Partial, Output>, Error>

    public init(
        events: @Sendable @escaping (String) -> AsyncThrowingStream<ProviderEvent<Partial, Output>, Error>
    ) {
        self.events = events
    }
}

/// Internal: an attempt exceeded its timeout. Mapped to a repair-triggering
/// `StructuredOutputError` at the attempt boundary — never surfaced directly.
private struct AttemptTimedOut: Error, Sendable {}

/// Internal: what one attempt produced.
private enum AttemptOutcome: Sendable {
    case validated
    case invalid(StructuredOutputError, preview: String)
    case incomplete
}

/// Per-attempt mutable state, isolated to its own actor so the `@Sendable`
/// timeout operation can touch it soundly.
private actor StreamState {
    private var lastCoverage = 0.0

    /// Monotonic-coverage gate: yields only snapshots that don't regress.
    func shouldYield(coverage: Double) -> Bool {
        guard coverage >= lastCoverage else { return false }
        lastCoverage = coverage
        return true
    }
}

/// The `AsyncSequence` returned by ``TypedStreamPipeline/stream(prompt:)``.
///
/// Wraps `AsyncThrowingStream` so that consumer task cancellation throws
/// `CancellationError` after awaiting producer teardown (Swift's standard
/// `AsyncThrowingStream` resolves `next()` with `nil` on task cancellation).
public struct TypedStream<Partial: Sendable, Output: Sendable>: AsyncSequence, Sendable {
    public typealias Element = StreamEvent<Partial, Output>
    public typealias Failure = any Error

    private let rawStream: AsyncThrowingStream<StreamEvent<Partial, Output>, Error>
    private let producerTask: Task<Void, Never>

    init(
        rawStream: AsyncThrowingStream<StreamEvent<Partial, Output>, Error>,
        producerTask: Task<Void, Never>
    ) {
        self.rawStream = rawStream
        self.producerTask = producerTask
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        private var rawIterator: AsyncThrowingStream<StreamEvent<Partial, Output>, Error>.AsyncIterator
        private let producerTask: Task<Void, Never>

        init(
            rawIterator: AsyncThrowingStream<StreamEvent<Partial, Output>, Error>.AsyncIterator,
            producerTask: Task<Void, Never>
        ) {
            self.rawIterator = rawIterator
            self.producerTask = producerTask
        }

        public mutating func next() async throws -> StreamEvent<Partial, Output>? {
            let element = try await rawIterator.next()
            if element == nil, Task.isCancelled {
                producerTask.cancel()
                await producerTask.value
                await Task.yield()
                throw CancellationError()
            }
            return element
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(rawIterator: rawStream.makeAsyncIterator(), producerTask: producerTask)
    }
}

/// Typed, streaming, self-repairing structured output.
///
/// An actor so the provider/session, budget state, and attempt counter are
/// serialized: the underlying model session is one-request-at-a-time and must
/// have exactly one owner.
///
/// One active stream per pipeline at a time — concurrent `stream(prompt:)`
/// calls would violate the session's one-request contract.
public actor TypedStreamPipeline<Partial: Sendable, Output: Sendable> {
    private let provider: StreamProvider<Partial, Output>
    private let validate: Validator<Output>
    private let budget: RepairBudget
    private var isStreaming = false

    public init(
        provider: StreamProvider<Partial, Output>,
        validate: @escaping Validator<Output>,
        budget: RepairBudget = .default
    ) {
        self.provider = provider
        self.validate = validate
        self.budget = budget
    }

    /// Streams events for a prompt.
    ///
    /// Terminal-event guarantee: the stream always ends with exactly one of
    /// `.validated` (yielded), a `StructuredOutputError` (thrown), or
    /// `CancellationError` (thrown) — never a silent hang.
    public func stream(prompt: String) -> TypedStream<Partial, Output> {
        let (rawStream, continuation) = AsyncThrowingStream<StreamEvent<Partial, Output>, Error>.makeStream(
            bufferingPolicy: .unbounded
        )
        let task = Task {
            do {
                try await self.run(prompt: prompt, continuation: continuation)
                continuation.finish()
            } catch {
                continuation.finish(throwing: error)
            }
        }
        // The bridge: AsyncSequence doesn't propagate cancellation, so the
        // stream boundary does it explicitly. Consumer gone => producer
        // cancelled => CancellationError surfaces to the consumer.
        continuation.onTermination = { _ in task.cancel() }
        return TypedStream(rawStream: rawStream, producerTask: task)
    }

    private func run(
        prompt: String,
        continuation: AsyncThrowingStream<StreamEvent<Partial, Output>, Error>.Continuation
    ) async throws {
        guard !isStreaming else {
            throw StructuredOutputError.undecodable(
                preview: "(concurrent stream(prompt:) call on active pipeline)"
            )
        }
        isStreaming = true
        defer { isStreaming = false }

        // Captured by value: the @Sendable operation below must not see
        // actor state or mutable locals.
        let provider = self.provider
        let validate = self.validate

        var currentPrompt = prompt
        var attempt = 0
        let deadline = ContinuousClock.now + budget.totalTimeout

        while true {
            try Task.checkCancellation()
            let remaining = deadline - ContinuousClock.now
            guard remaining > .zero else {
                throw StructuredOutputError.budgetExhausted(attempts: attempt)
            }

            let state = StreamState()
            let attemptPrompt = currentPrompt
            let outcome: AttemptOutcome

            do {
                outcome = try await self.withAttemptTimeout(min(budget.attemptTimeout, remaining)) {
                    for try await event in provider.events(attemptPrompt) {
                        try Task.checkCancellation()
                        switch event {
                        case .partial(let snapshot):
                            guard await state.shouldYield(coverage: snapshot.coverage) else { continue }
                            continuation.yield(.partial(snapshot))
                        case .final(let output):
                            let preview = String(String(describing: output).prefix(500))
                            do {
                                try validate(output)
                                continuation.yield(.validated(output))
                                return .validated
                            } catch let structured as StructuredOutputError {
                                return .invalid(structured, preview: preview)
                            } catch let cancellation as CancellationError {
                                throw cancellation
                            } catch {
                                return .invalid(
                                    .undecodable(preview: "validator threw unexpected error: \(error)"),
                                    preview: preview
                                )
                            }
                        }
                    }
                    return .incomplete
                }
            } catch _ as AttemptTimedOut {
                outcome = .invalid(
                    .undecodable(preview: "(attempt timed out)"),
                    preview: "(no output)"
                )
            } catch let error as CancellationError {
                throw error
            }
            // A provider stream that ends (or is torn down by cancellation)
            // without a final value must surface cancellation, never
            // masquerade as undecodable.
            try Task.checkCancellation()

            switch outcome {
            case .validated:
                return
            case .invalid(let structured, let preview):
                attempt += 1
                guard attempt <= budget.maxAttempts else {
                    throw StructuredOutputError.budgetExhausted(attempts: attempt)
                }
                continuation.yield(.repairing(RepairAttempt(
                    index: attempt,
                    error: structured,
                    failedPreview: preview
                )))
                currentPrompt = repairPrompt(
                    originalPrompt: prompt,
                    invalidPreview: preview,
                    error: structured
                )
            case .incomplete:
                throw StructuredOutputError.undecodable(
                    preview: "(provider stream ended without a final value)"
                )
            }
        }
    }

    /// Runs `operation`, cancelling it if `timeout` elapses first. The loser
    /// is always cancelled — timeouts never orphan work.
    private func withAttemptTimeout<T: Sendable>(
        _ timeout: Duration,
        operation: @Sendable @escaping () async throws -> T
    ) async throws -> T {
        try await withThrowingTaskGroup(of: T.self) { group in
            group.addTask(operation: operation)
            group.addTask {
                try await Task.sleep(for: timeout)
                throw AttemptTimedOut()
            }
            guard let result = try await group.next() else {
                // Group drained with no result: only possible under
                // cancellation.
                throw CancellationError()
            }
            group.cancelAll()
            return result
        }
    }
}
