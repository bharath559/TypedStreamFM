/// Failures of the structured-output pipeline.
///
/// Cancellation is intentionally absent from this enum: `CancellationError`
/// propagates per Swift concurrency convention and is never mapped into a
/// domain error (see `docs/cancellation.md`).
public enum StructuredOutputError: Error, Sendable, CustomStringConvertible {
    /// The model output could not be decoded, an attempt timed out, or the
    /// provider stream ended without producing a final value.
    case undecodable(preview: String)
    /// The decoded value failed semantic validation.
    case constraintViolated(path: String, reason: String)
    /// The repair budget was exhausted. Terminal.
    case budgetExhausted(attempts: Int)

    public var description: String {
        switch self {
        case .undecodable(let preview):
            return "output was not decodable (\(preview))"
        case .constraintViolated(let path, let reason):
            return "constraint violated at '\(path)': \(reason)"
        case .budgetExhausted(let attempts):
            return "repair budget exhausted after \(attempts) attempts"
        }
    }
}
