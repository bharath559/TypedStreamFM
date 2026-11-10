/// Bounds for the repair loop.
///
/// All timeouts are implemented as cancellation races — a timed-out attempt is
/// cancelled, never orphaned. See `docs/cancellation.md`.
public struct RepairBudget: Sendable, Equatable {
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

/// Strategy for constructing follow-up prompts during the repair loop.
///
/// Choose the strategy that matches your provider's session lifecycle so you
/// do not overflow tight on-device context windows:
/// - **Stateful sessions** (e.g. a reused `LanguageModelSession`) retain every
///   prompt and response turn in their transcript. Re-sending the original
///   prompt and invalid output on every retry duplicates context; use
///   ``.deltaPrompt`` so each retry sends only the validation error delta.
/// - **Fresh-session-per-attempt providers** (e.g.
///   `FoundationModelsProvider.provider(makeSession:)` or stateless backends)
///   discard the failed session's transcript on each retry; use ``.fullPrompt``
///   so each fresh attempt receives the original prompt, offending preview, and
///   validation error.
public enum RepairStrategy: Sendable {
    /// Builds a self-contained prompt containing the original prompt, truncated
    /// offending output, and validator error (`repairPrompt(...)`).
    ///
    /// Recommended for stateless backends and fresh-session-per-attempt
    /// providers.
    case fullPrompt

    /// Builds a compact delta prompt containing only the validator error
    /// (`deltaRepairPrompt(...)`), relying on a stateful session's existing
    /// transcript for the original prompt and previous response.
    ///
    /// Recommended when reusing a single stateful `LanguageModelSession`.
    case deltaPrompt

    /// Custom prompt builder invoked on each repair attempt.
    case custom(@Sendable (_ originalPrompt: String, _ invalidPreview: String, _ error: StructuredOutputError) -> String)

    /// Alias for ``.fullPrompt`` (stateless / fresh-session-per-attempt).
    public static let stateless: Self = .fullPrompt

    /// Alias for ``.deltaPrompt`` (stateful session transcript).
    public static let stateful: Self = .deltaPrompt

    /// Produces the prompt string for a repair retry.
    public func prompt(
        originalPrompt: String,
        invalidPreview: String,
        error: StructuredOutputError
    ) -> String {
        switch self {
        case .fullPrompt:
            return repairPrompt(
                originalPrompt: originalPrompt,
                invalidPreview: invalidPreview,
                error: error
            )
        case .deltaPrompt:
            return deltaRepairPrompt(error: error)
        case .custom(let builder):
            return builder(originalPrompt, invalidPreview, error)
        }
    }
}

/// Builds a self-contained repair prompt: original prompt + the invalid output +
/// the validator's error text.
///
/// Use with stateless providers or fresh-session-per-attempt factories where the
/// model does not retain prior turns in a session transcript.
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

/// Builds a context-window-safe delta repair prompt for stateful sessions whose
/// transcript already contains the original prompt and the offending response.
///
/// Sends only the validation failure description and correction instruction,
/// avoiding duplicate prompt and output tokens in the session transcript.
public func deltaRepairPrompt(
    error: StructuredOutputError
) -> String {
    """
    Your previous response was invalid: \(error.description). \
    Produce a corrected response that satisfies the stated constraints.
    """
}
