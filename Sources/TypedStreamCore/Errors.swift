/// A single semantic constraint failure at a specific field or key path.
public struct ConstraintViolation: Sendable, Equatable, Hashable, CustomStringConvertible {
    public let path: String
    public let reason: String

    public init(path: String, reason: String) {
        self.path = path
        self.reason = reason
    }

    public var description: String {
        "'\(path)': \(reason)"
    }
}

/// Failures of the structured-output pipeline.
///
/// Cancellation is intentionally absent from this enum: `CancellationError`
/// propagates per Swift concurrency convention and is never mapped into a
/// domain error (see `docs/cancellation.md`).
public enum StructuredOutputError: Error, Sendable, Equatable, CustomStringConvertible {
    /// The model output could not be decoded, an attempt timed out, or the
    /// provider stream ended without producing a final value.
    case undecodable(preview: String)
    /// The decoded value failed a single semantic validation constraint.
    case constraintViolated(path: String, reason: String)
    /// The decoded value failed one or more semantic validation constraints
    /// collected in a single pass.
    case constraintsViolated([ConstraintViolation])
    /// The repair budget was exhausted. Terminal.
    case budgetExhausted(attempts: Int)

    /// All constraint violations carried by this error (`1` for
    /// `.constraintViolated`, `N` for `.constraintsViolated`, empty otherwise).
    public var violations: [ConstraintViolation] {
        switch self {
        case .constraintViolated(let path, let reason):
            return [ConstraintViolation(path: path, reason: reason)]
        case .constraintsViolated(let violations):
            return violations
        case .undecodable, .budgetExhausted:
            return []
        }
    }

    public var description: String {
        switch self {
        case .undecodable(let preview):
            return "output was not decodable (\(preview))"
        case .constraintViolated(let path, let reason):
            return "constraint violated at '\(path)': \(reason)"
        case .constraintsViolated(let violations):
            if violations.count == 1, let single = violations.first {
                return "constraint violated at '\(single.path)': \(single.reason)"
            }
            let details = violations.map(\.description).joined(separator: "; ")
            return "constraints violated (\(violations.count)): \(details)"
        case .budgetExhausted(let attempts):
            return "repair budget exhausted after \(attempts) attempts"
        }
    }
}
