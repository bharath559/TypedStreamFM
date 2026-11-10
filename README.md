# TypedStreamFM

Typed, streaming, self-repairing structured output for Apple Foundation Models — with cancellation-correct Swift 6 concurrency throughout.

**Status:** v0.1 — M1 in progress. Core pipeline, semantic validation, bounded repair, and the cancellation/backpressure design are implemented. The Foundation Models adapter is written against community SDK references; the M0 checklist below must be confirmed in Xcode before shipping.

## Why

Foundation Models gives you one-shot structured output (`@Generable` constrained decoding). Real products need more:

1. **Streaming partials** — render the answer as it forms (native `T.PartiallyGenerated` snapshots).
2. **Semantic validation** — constrained decoding guarantees shape, not meaning (`endDate > startDate`).
3. **Automatic repair** — validation failures re-prompt with the error, within a budget.
4. **Concurrency correctness** — no leaked tasks, no unhandled cancellation, no unbounded buffers. The library's value proposition *is* concurrency correctness, so it must be exemplary.

## Usage

```swift
import TypedStreamCore
import FoundationModels

let session = LanguageModelSession()
let pipeline = TypedStreamPipeline(
    provider: FoundationModelsProvider.provider(session: session),
    validate: { (movie: Movie) in
        guard movie.year >= 1900 else {
            throw StructuredOutputError.constraintViolated(path: "year", reason: "must be >= 1900")
        }
    },
    budget: .default  // 3 attempts, 30s each, 90s total
)

for try await event in await pipeline.stream(prompt: "Recommend an action movie from the 2020s") {
    switch event {
    case .partial(let snapshot): render(snapshot.value)  // live UI; coverage 0...1, monotonic
    case .validated(let movie): showFinal(movie)         // decoded AND valid. Terminal.
    case .repairing(let attempt): log(attempt)           // observability
    }
}
// Cancelling the consumer task cancels generation AND any in-flight repair.
// Guaranteed by construction — see docs/cancellation.md.
```

## Playgrounds

`Playgrounds/TypedStreamFM.playground` has five pages that double as manual test cases — each mirrors a behavior in `Tests/`:

1. Basic streaming — partials with monotonic coverage, then `.validated`
2. Semantic validation — shape vs. meaning, and the repair prompt it produces
3. Repair loop — a failing first attempt converging within budget
4. Timeouts & budgets — a slow backend hitting `.budgetExhausted`, terminally
5. Cancellation — a hanging backend torn down via `CancellationError`

To run: open `Package.swift` in Xcode 27, build the package (⌘B), then open the `.playground`. The pages use `TypedStreamTestKit`'s `FakeLanguageModel` — deterministic, no network, no Apple Intelligence hardware. Swap `fake.provider()` for `FoundationModelsProvider.provider(session:)` for the live path (see the M0 checklist below).

## Design guarantees
- **Terminal events:** every stream ends with exactly one of `.validated`, a `StructuredOutputError`, or `CancellationError` — never a silent hang.
- **Bounded repair:** `RepairBudget` caps attempts and time; timeouts are implemented as cancellation races, so timed-out work is cancelled, never orphaned.
- **Latest-wins backpressure:** `.bufferingNewest(1)` — safe because partials are idempotent snapshots with monotonic coverage.
- **Cancellation is not an error case:** `CancellationError` propagates per Swift convention; it is never mapped into `StructuredOutputError`.

## Layout

```
Sources/TypedStreamCore/      # pipeline, events, validation, repair (zero deps)
Sources/TypedStreamTestKit/   # FakeLanguageModel, Probe — deterministic fixtures
Tests/TypedStreamTests/       # behavior contract: repair, timeouts, cancellation
docs/cancellation.md          # the cancellation design, as an RFC
docs/decoding-strategy.md     # v1 native partials vs prefix-decode fallback
```

## M0 checklist — verified against Xcode 27 / macOS 26+ SDK

- [x] `streamResponse(to:generating:)` shape: snapshots' `.content == T.PartiallyGenerated`
- [x] `stream.collect()` after full iteration returns the final `Response<T>`
- [x] `LanguageModelSession` `Sendable` status (`@unchecked Sendable` in `FoundationModels`)
- [x] `T.PartiallyGenerated` `Sendable` status (constrained on `provider(session:)`)
- [ ] Cancellation behavior of an in-flight `streamResponse` on live hardware

## Naming

Package is `TypedStreamFM` (not `TypedStream`) — the `TypedStream` module name collides with the Madrid (iMessage) package's typedstream-format decoder. Library products are `TypedStreamCore` and `TypedStreamTestKit`.
