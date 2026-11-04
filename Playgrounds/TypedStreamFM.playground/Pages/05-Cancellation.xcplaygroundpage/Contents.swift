// 05 — Cancellation
// The backend hangs. The consumer goes away. The producer must be torn down
// and CancellationError must surface untouched — never disguised as a domain
// error, never fed into the repair loop.
// Mirrors cancellationPropagatesAndCancelsProducer in Tests/TypedStreamTests.

import PlaygroundSupport
import TypedStreamCore
import TypedStreamTestKit

PlaygroundPage.current.needsIndefiniteExecution = true

Task {
    defer { PlaygroundPage.current.finishExecution() }

    let probe = Probe()
    let fake = FakeLanguageModel<String, String>.hanging(onCancel: {
        Task { await probe.mark() }
    })
    let pipeline = TypedStreamPipeline(provider: fake.provider(), validate: { _ in })

    let consumer = Task {
        do {
            for try await _ in await pipeline.stream(prompt: "go") {}
            print("consumer finished (unexpected)")
        } catch is CancellationError {
            print("consumer: CancellationError — the terminal-event guarantee held")
        } catch {
            print("consumer: unexpected error: \(error)")
        }
    }

    try? await Task.sleep(for: .seconds(1))
    consumer.cancel()
    try? await Task.sleep(for: .seconds(1))
    print("producer torn down: \(await probe.marked)")
}
