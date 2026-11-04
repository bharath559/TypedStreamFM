# Cancellation design (RFC)

## Rules

1. **No detached tasks.** Every unit of work is a child of the consumer's task tree. The pipeline's producer runs in a `Task` owned by the stream's continuation; repair attempts run inside the same task. Nothing outlives the consumer.
2. **Explicit bridging.** `AsyncSequence` does not propagate cancellation, so the stream boundary bridges it manually: `continuation.onTermination = { _ in task.cancel() }`. When the consumer breaks out of `for try await` — or its task is cancelled while suspended in it — the iterator is torn down, `onTermination` fires, and the producer task is cancelled.
3. **Timeouts are cancellation.** `attemptTimeout` / `totalTimeout` are racing child tasks in a `withThrowingTaskGroup`, not timers. The loser is always cancelled via `group.cancelAll()`; timed-out work is never orphaned.
4. **Terminal-event guarantee.** Every stream ends with exactly one of `.validated` (yielded), a `StructuredOutputError` (thrown), or `CancellationError` (thrown). Never a silent hang.

## The cancellation path, end to end

Consumer cancels (or breaks out of the loop)
→ pipeline continuation's `onTermination` fires → producer `Task` cancelled
→ `for try await` over provider events is torn down → provider's own
   `onTermination` fires → provider's in-flight work cancelled
→ producer observes cancellation at the next `Task.checkCancellation()`
→ `CancellationError` propagates through `run()` untouched
   (the `catch is CancellationError { throw }` clause — it is never converted
   into a domain error and never fed into the repair loop)
→ `continuation.finish(throwing: CancellationError)`
→ consumer's `for try await` throws `CancellationError`.

## Edge cases handled

- **Provider stream ends without `.final`, task not cancelled** → `.undecodable`, which enters the repair loop (bounded by budget).
- **Provider stream torn down by cancellation without `.final`** → the `try Task.checkCancellation()` after the attempt scope surfaces `CancellationError` instead of masquerading as undecodable. Order matters: cancellation is checked *before* the outcome switch.
- **Validator throws a non-domain error** → wrapped as `.undecodable` and repaired (bounded). Only `CancellationError` escapes the repair loop.
- **Concurrent `stream(prompt:)` calls on one pipeline** → not supported. The session is one-request-at-a-time; the pipeline actor serializes state but concurrent streams would interleave provider usage. Documented, not silently allowed.

## Provider contract

Providers must be cancellation-cooperative: check for cancellation per yielded event and tear down promptly in `onTermination`. `FakeLanguageModel.hanging` is the reference implementation; `cancellationPropagatesAndCancelsProducer` asserts the full path including producer teardown via `Probe`.
