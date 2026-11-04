#if canImport(FoundationModels)
import FoundationModels

// MARK: - M0 verification checklist (confirm in Xcode 27 before shipping)
//
// 1. `LanguageModelSession.streamResponse(to:generating:)` exists with the
//    shape used below, and the response stream is an AsyncSequence whose
//    snapshots expose `.content: Output.PartiallyGenerated`.
// 2. Calling `stream.collect()` after fully iterating the stream returns the
//    final `Response<Output>` (iterate-then-collect pattern).
// 3. `LanguageModelSession` Sendable status — see UncheckedSession.
// 4. `Output.PartiallyGenerated` Sendable status — see the generic
//    constraint on `provider(session:)`.

/// Documents the v1 ownership contract: the pipeline actor is the sole owner
/// of the session and never touches it concurrently. If M0 shows the session
/// is `Sendable`, delete this wrapper and capture the session directly.
private struct UncheckedSession: @unchecked Sendable {
    let session: LanguageModelSession
}

/// Best-effort coverage: fraction of non-nil fields on the partial.
/// `PartiallyGenerated` is all-Optional by construction, so this is the
/// fraction of fields the model has produced so far.
private func coverage<Partial>(of partial: Partial) -> Double {
    let children = Array(Mirror(reflecting: partial).children)
    guard !children.isEmpty else { return 1.0 }
    let filled = children.filter { _, value in
        let valueMirror = Mirror(reflecting: value)
        guard valueMirror.displayStyle == .optional else { return true }
        return valueMirror.children.first != nil
    }.count
    return Double(filled) / Double(children.count)
}

/// Native structured-partials provider — the v1 decoding strategy.
///
/// Consumes `streamResponse(to:generating:)` once: each snapshot becomes a
/// `.partial` event (coverage = non-nil field fraction), and the collected
/// final response becomes `.final`. No prefix-decoding of raw text.
@available(iOS 26.0, macOS 26.0, *)
public enum FoundationModelsProvider {
    public static func provider<Output>(
        session: LanguageModelSession
    ) -> StreamProvider<Output.PartiallyGenerated, Output>
    where Output: Generable & Sendable, Output.PartiallyGenerated: Sendable
    {
        let unchecked = UncheckedSession(session: session)
        return StreamProvider { prompt in
            AsyncThrowingStream { continuation in
                let task = Task {
                    do {
                        let stream = unchecked.session.streamResponse(to: prompt, generating: Output.self)
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
