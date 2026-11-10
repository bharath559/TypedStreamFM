import Observation

/// A `@MainActor` `@Observable` state controller that bridges
/// ``TypedStreamPipeline`` directly into SwiftUI views.
///
/// Tracks the live partial snapshot, monotonic coverage (`0...1`), repair
/// attempts, final validated output, and terminal error/cancellation state,
/// while guaranteeing that overlapping `start(prompt:)` / `run(prompt:)` calls
/// cancel and await teardown of the prior run before starting the next.
///
/// ```swift
/// @State private var controller = TypedStreamController(
///     provider: FoundationModelsProvider.provider(session: session),
///     check: Check.greaterThanOrEqual(\Movie.year, 1900)
///         && Check.notBlank(\Movie.title)
/// )
/// ```
@Observable
@MainActor
public final class TypedStreamController<Partial: Sendable, Output: Sendable> {
    /// Lifecycle state of the controller.
    public enum Phase: Sendable {
        case idle
        case streaming
        case repairing(RepairAttempt)
        case validated(Output)
        case failed(any Error)
        case cancelled
    }

    private let pipeline: TypedStreamPipeline<Partial, Output>
    @ObservationIgnored private var inFlightTask: Task<Void, Never>?
    @ObservationIgnored private var generationID: UInt64 = 0

    /// Latest partial value emitted during the current attempt.
    public private(set) var partial: Partial?

    /// Monotonic field coverage (`0...1`) for the current attempt, or `1.0` once
    /// `.validated` arrives.
    public private(set) var coverage: Double = 0

    /// The final decoded and semantically validated value.
    public private(set) var output: Output?

    /// Repair attempts recorded during the current run.
    public private(set) var repairAttempts: [RepairAttempt] = []

    /// The domain error if the run failed with ``StructuredOutputError``.
    public private(set) var structuredError: StructuredOutputError?

    /// The terminal error if the run failed (excluding `CancellationError`).
    public private(set) var error: (any Error)?

    /// Current lifecycle phase of the controller.
    public private(set) var phase: Phase = .idle

    /// The most recent repair attempt, if any.
    public var currentRepair: RepairAttempt? {
        repairAttempts.last
    }

    /// `true` while the controller is actively streaming or repairing.
    public var isStreaming: Bool {
        switch phase {
        case .streaming, .repairing:
            return true
        case .idle, .validated, .failed, .cancelled:
            return false
        }
    }

    /// `true` when the pipeline is currently executing a repair retry.
    public var isRepairing: Bool {
        if case .repairing = phase { return true }
        return false
    }

    /// Creates a controller backed by an existing ``TypedStreamPipeline``.
    public init(pipeline: TypedStreamPipeline<Partial, Output>) {
        self.pipeline = pipeline
    }

    /// Creates a controller and its underlying ``TypedStreamPipeline``.
    public convenience init(
        provider: StreamProvider<Partial, Output>,
        validate: @escaping Validator<Output> = { _ in },
        budget: RepairBudget = .default,
        repairStrategy: RepairStrategy? = nil
    ) {
        self.init(
            pipeline: TypedStreamPipeline(
                provider: provider,
                validate: validate,
                budget: budget,
                repairStrategy: repairStrategy
            )
        )
    }

    /// Creates a controller validated by a composable ``Check`` rule.
    public convenience init(
        provider: StreamProvider<Partial, Output>,
        check: Check<Output>,
        budget: RepairBudget = .default,
        repairStrategy: RepairStrategy? = nil
    ) {
        self.init(
            pipeline: TypedStreamPipeline(
                provider: provider,
                check: check,
                budget: budget,
                repairStrategy: repairStrategy
            )
        )
    }

    /// Creates a controller validated by multiple ``Check`` rules evaluated in a single pass.
    public convenience init(
        provider: StreamProvider<Partial, Output>,
        checks: [Check<Output>],
        budget: RepairBudget = .default,
        repairStrategy: RepairStrategy? = nil
    ) {
        self.init(
            pipeline: TypedStreamPipeline(
                provider: provider,
                checks: checks,
                budget: budget,
                repairStrategy: repairStrategy
            )
        )
    }

    /// Runs a streaming generation request for `prompt` and suspends until it
    /// reaches a terminal state (`.validated`, `.failed`, or `.cancelled`).
    ///
    /// Designed for SwiftUI `.task` modifiers: cancelling the calling task
    /// automatically cancels the underlying pipeline producer and transitions
    /// ``phase`` to `.cancelled`.
    public func run(prompt: String) async {
        let worker = beginStream(prompt: prompt)
        await withTaskCancellationHandler {
            await worker.value
        } onCancel: {
            worker.cancel()
        }
    }

    /// Starts a streaming generation request in a managed `@MainActor` task and
    /// returns immediately.
    ///
    /// Designed for synchronous UI callbacks such as SwiftUI `Button` actions.
    /// Calling `start(prompt:)` while a request is already in flight cancels
    /// and awaits teardown of the previous request before starting the new one.
    @discardableResult
    public func start(prompt: String) -> Task<Void, Never> {
        beginStream(prompt: prompt)
    }

    /// Cancels any in-flight generation or repair attempt and transitions
    /// ``phase`` to `.cancelled`.
    public func cancel() {
        guard isStreaming else {
            inFlightTask?.cancel()
            return
        }
        generationID &+= 1
        inFlightTask?.cancel()
        phase = .cancelled
    }

    /// Cancels any in-flight generation and resets all observable properties back
    /// to `.idle`.
    public func reset() {
        generationID &+= 1
        inFlightTask?.cancel()
        partial = nil
        coverage = 0
        output = nil
        repairAttempts = []
        structuredError = nil
        error = nil
        phase = .idle
    }

    private func beginStream(prompt: String) -> Task<Void, Never> {
        generationID &+= 1
        let currentID = generationID

        let previousTask = inFlightTask
        previousTask?.cancel()

        partial = nil
        coverage = 0
        output = nil
        repairAttempts = []
        structuredError = nil
        error = nil
        phase = .streaming

        let worker = Task { @MainActor [weak self] in
            await previousTask?.value
            guard let self, self.generationID == currentID else { return }
            if Task.isCancelled {
                self.phase = .cancelled
                return
            }

            do {
                let stream = await self.pipeline.stream(prompt: prompt)
                for try await event in stream {
                    guard self.generationID == currentID else { return }
                    switch event {
                    case .partial(let snapshot):
                        self.partial = snapshot.value
                        self.coverage = snapshot.coverage
                        self.phase = .streaming
                    case .repairing(let attempt):
                        self.repairAttempts.append(attempt)
                        self.coverage = 0
                        self.phase = .repairing(attempt)
                    case .validated(let validatedOutput):
                        self.output = validatedOutput
                        self.coverage = 1.0
                        self.phase = .validated(validatedOutput)
                    }
                }
            } catch is CancellationError {
                guard self.generationID == currentID else { return }
                self.phase = .cancelled
            } catch {
                guard self.generationID == currentID else { return }
                if Task.isCancelled {
                    self.phase = .cancelled
                } else {
                    self.error = error
                    self.structuredError = error as? StructuredOutputError
                    self.phase = .failed(error)
                }
            }
        }

        inFlightTask = worker
        return worker
    }
}
