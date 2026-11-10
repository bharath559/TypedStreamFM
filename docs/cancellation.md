# Cancellation design (RFC)

## Rules

1. **No detached tasks.** Every unit of work is a child of the consumer's task tree. The pipeline's producer runs in a `Task` owned by the stream's continuation; repair attempts run inside the same task. Nothing outlives the consumer.
2. **Explicit bridging.** `AsyncSequence` does not propagate cancellation, and Swift's `AsyncThrowingStream` resolves `next()` with `nil` (rather than throwing `CancellationError`) when the consuming task is cancelled. The stream boundary bridges both explicitly: `continuation.onTermination = { _ in task.cancel() }` cancels the producer, and `TypedStream.AsyncIterator.next()` awaits producer teardown and throws `CancellationError()` when `rawIterator.next()` terminates under cancellation.
3. **Timeouts are cancellation.** `attemptTimeout` / `totalTimeout` are racing child tasks in a `withThrowingTaskGroup`, not timers. The loser is always cancelled via `group.cancelAll()`; timed-out work is never orphaned.
4. **Terminal-event guarantee.** Every stream ends with exactly one of `.validated` (yielded), a `StructuredOutputError` (thrown), or `CancellationError` (thrown). Never a silent hang.

## The cancellation path, end to end

Consumer cancels (or breaks out of the loop)
→ pipeline continuation's `onTermination` fires → producer `Task` cancelled
→ `for try await` over provider events is torn down → provider's own
   `onTermination` fires → provider's in-flight work cancelled
→ `TypedStream.AsyncIterator.next()` awaits `producerTask.value` so producer cleanup finishes
→ `TypedStream.AsyncIterator.next()` throws `CancellationError` to the consumer's `for try await`.

## Edge cases handled

- **Provider stream ends without `.final`, task not cancelled** → `.undecodable`, which enters the repair loop (bounded by budget).
- **Provider stream torn down by cancellation without `.final`** → the `try Task.checkCancellation()` after the attempt scope surfaces `CancellationError` instead of masquerading as undecodable. Order matters: cancellation is checked *before* the outcome switch.
- **Validator throws a non-domain error** → wrapped as `.undecodable` and repaired (bounded). Only `CancellationError` escapes the repair loop.
- **Concurrent `stream(prompt:)` calls on one pipeline** → actively rejected with `StructuredOutputError.undecodable`. The session is one-request-at-a-time; the pipeline tracks active streaming state so concurrent streams cannot interleave provider usage.

## Provider contract

Providers must be cancellation-cooperative: check for cancellation per yielded event and tear down promptly in `onTermination`. `FakeLanguageModel.hanging` is the reference implementation; `cancellationPropagatesAndCancelsProducer` asserts the full path including producer teardown via `Probe`.
