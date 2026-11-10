/// Best-effort coverage: fraction of non-nil fields on the partial (recursively
/// evaluating nested structs).
/// `PartiallyGenerated` is all-Optional by construction, so this is the
/// fraction of fields the model has produced so far.
internal func coverage<Partial>(of partial: Partial) -> Double {
    fieldCoverage(partial)
}

private func fieldCoverage(_ value: Any) -> Double {
    let mirror = Mirror(reflecting: value)
    switch mirror.displayStyle {
    case .optional:
        guard let unwrapped = mirror.children.first?.value else { return 0.0 }
        return fieldCoverage(unwrapped)
    case .struct:
        let children = Array(mirror.children)
        guard !children.isEmpty else { return 1.0 }
        let total = children.reduce(0.0) { $0 + fieldCoverage($1.value) }
        return total / Double(children.count)
    default:
        return 1.0
    }
}

#if canImport(FoundationModels)
import FoundationModels

// MARK: - M0 verification checklist (confirmed against Xcode 27 / macOS 26+ SDK)
//
// 1. `LanguageModelSession.streamResponse(to:generating:)` exists with the
//    shape used below, and the response stream is an AsyncSequence whose
//    snapshots expose `.content: Output.PartiallyGenerated`.
// 2. Calling `stream.collect()` after fully iterating the stream returns the
//    final `Response<Output>` (iterate-then-collect pattern).
// 3. `LanguageModelSession` conforms to `@unchecked Sendable` in FoundationModels.
// 4. `Output.PartiallyGenerated` Sendable status — see the generic
//    constraint on `provider(session:)`.

/// Native structured-partials provider — the v1 decoding strategy.
///
/// Consumes `streamResponse(to:generating:)` once: each snapshot becomes a
/// `.partial` event (coverage = non-nil field fraction), and the collected
/// final response becomes `.final`. No prefix-decoding of raw text.
@available(iOS 26.0, macOS 26.0, *)
public enum FoundationModelsProvider {
    /// Streams from a single stateful `LanguageModelSession` across attempts.
    ///
    /// Because `LanguageModelSession` retains every prompt and response in its
    /// transcript, this provider defaults to ``RepairStrategy/deltaPrompt`` so
    /// repair retries send only the compact validation failure delta instead of
    /// duplicating the original prompt and offending output in the context window.
    public static func provider<Output>(
        session: LanguageModelSession,
        repairStrategy: RepairStrategy = .deltaPrompt
    ) -> StreamProvider<Output.PartiallyGenerated, Output>
    where Output: Generable & Sendable, Output.PartiallyGenerated: Sendable
    {
        provider(repairStrategy: repairStrategy, makeSession: { session })
    }

    /// Creates a fresh `LanguageModelSession` for each generation and repair
    /// attempt via `makeSession`.
    ///
    /// Discarding the session after a failed attempt prevents failed turns from
    /// accumulating in a shared transcript and overflowing the on-device context
    /// window. Defaults to ``RepairStrategy/fullPrompt`` so each fresh session
    /// receives the self-contained prompt (`originalPrompt` + `invalidPreview`
    /// + `error`).
    public static func provider<Output>(
        repairStrategy: RepairStrategy = .fullPrompt,
        makeSession: @Sendable @escaping () async throws -> LanguageModelSession
    ) -> StreamProvider<Output.PartiallyGenerated, Output>
    where Output: Generable & Sendable, Output.PartiallyGenerated: Sendable
    {
        StreamProvider(defaultRepairStrategy: repairStrategy) { prompt in
            AsyncThrowingStream { continuation in
                let task = Task {
                    do {
                        try Task.checkCancellation()
                        let session = try await makeSession()
                        try Task.checkCancellation()
                        let stream = session.streamResponse(to: prompt, generating: Output.self)
                        for try await snapshot in stream {
                            try Task.checkCancellation()
                            let partial = snapshot.content
                            continuation.yield(.partial(PartialSnapshot(
                                value: partial,
                                coverage: coverage(of: partial)
                            )))
                        }
                        let response = try await stream.collect()
                        continuation.yield(.final(response.content))
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
#endif
