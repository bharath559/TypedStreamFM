/// Events emitted by ``TypedStreamPipeline/stream(prompt:)``.
///
/// `Partial` is the streaming partial type — in v1, `Output.PartiallyGenerated`
/// from Foundation Models' native structured streaming. `Output` is the final,
/// validated value.
public enum StreamEvent<Partial: Sendable, Output: Sendable>: Sendable {
    /// Best-effort current value. Coverage is monotonically non-decreasing
    /// within an attempt; the pipeline drops coverage regressions.
    case partial(PartialSnapshot<Partial>)
    /// Final value: decoded AND semantically valid. Terminal.
    case validated(Output)
    /// Observability: a repair attempt started (what failed, attempt n).
    case repairing(RepairAttempt)
}

extension StreamEvent: Equatable where Partial: Equatable, Output: Equatable {}

/// A best-effort snapshot of the value being streamed.
public struct PartialSnapshot<Value: Sendable>: Sendable {
    public let value: Value
    /// 0...1 — fraction of the value decoded so far. Clamped on init.
    public let coverage: Double

    public init(value: Value, coverage: Double) {
        self.value = value
        self.coverage = min(max(coverage, 0), 1)
    }
}

extension PartialSnapshot: Equatable where Value: Equatable {}

/// Observability payload for a repair attempt.
public struct RepairAttempt: Sendable, Equatable {
    /// 1-based attempt number.
    public let index: Int
    public let error: StructuredOutputError
    /// Truncated rendering of the offending output.
    public let failedPreview: String

    public init(index: Int, error: StructuredOutputError, failedPreview: String) {
        self.index = index
        self.error = error
        self.failedPreview = failedPreview
    }
}

/// Raw model events. The pipeline layers validation, repair, budgets,
/// backpressure, and cancellation on top of these — providers stay dumb.
public enum ProviderEvent<Partial: Sendable, Output: Sendable>: Sendable {
    case partial(PartialSnapshot<Partial>)
    case final(Output)
}

extension ProviderEvent: Equatable where Partial: Equatable, Output: Equatable {}
