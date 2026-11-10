/// Bounds for the repair loop.
///
/// All timeouts are implemented as cancellation races — a timed-out attempt is
/// cancelled, never orphaned. See `docs/cancellation.md`.
public struct RepairBudget: Sendable {
    /// Maximum number of repair retries after the initial generation attempt.
    public var maxAttempts: Int
    public var attemptTimeout: Duration
    public var totalTimeout: Duration

    public init(
        maxAttempts: Int = 3,
        attemptTimeout: Duration = .seconds(30),
        totalTimeout: Duration = .seconds(90)
    ) {
        self.maxAttempts = maxAttempts
        self.attemptTimeout = attemptTimeout
        self.totalTimeout = totalTimeout
    }

    public static let `default` = RepairBudget()
}

/// Builds the repair prompt: original prompt + the invalid output + the
/// validator's error text.
public func repairPrompt(
    originalPrompt: String,
    invalidPreview: String,
    error: StructuredOutputError
) -> String {
    """
    \(originalPrompt)

    Your previous response was invalid: \(error.description). \
    Offending output (truncated): \(invalidPreview). \
    Produce a corrected response that satisfies the stated constraints.
    """
}
