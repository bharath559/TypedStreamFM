/// Semantic validation: constrained decoding guarantees *shape*,
/// a validator guarantees *meaning* (e.g. `endDate > startDate`).
///
/// Throw `StructuredOutputError.constraintViolated(path:reason:)` on failure —
/// the error text becomes the repair signal, so good messages are a feature.
/// Any other thrown error is wrapped as `.undecodable` and enters the bounded
/// repair loop. Only `CancellationError` escapes — it is never repaired.
public typealias Validator<Output: Sendable> = @Sendable (Output) throws -> Void
