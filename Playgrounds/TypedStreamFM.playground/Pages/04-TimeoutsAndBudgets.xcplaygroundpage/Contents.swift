// 04 — Timeouts and budgets
// A slow backend meets a tight attempt timeout. The attempt is cancelled
// (never orphaned), the timeout counts against the budget, and the stream
// ends with .budgetExhausted — terminal, never a hang.
// Mirrors attemptTimeoutCountsAgainstBudget in Tests/TypedStreamTests.

import PlaygroundSupport
import TypedStreamCore
import TypedStreamTestKit

PlaygroundPage.current.needsIndefiniteExecution = true

Task {
    defer { PlaygroundPage.current.finishExecution() }

    let fake = FakeLanguageModel<String, String>.slow(
        firstEventDelay: .seconds(5),
        partials: [],
        final: "late"
    )
    let pipeline = TypedStreamPipeline(
        provider: fake.provider(),
        validate: { _ in },
        budget: RepairBudget(
            maxAttempts: 1,
            attemptTimeout: .milliseconds(300),
            totalTimeout: .seconds(30)
        )
    )

    var repairingCount = 0
    do {
        for try await event in await pipeline.stream(prompt: "go") {
            if case .repairing(let attempt) = event {
                repairingCount += 1
                print("repairing #\(attempt.index): \(attempt.error)")
            }
        }
        print("stream completed (unexpected)")
    } catch let error as StructuredOutputError {
        print("terminal: \(error.description)  (repairs seen: \(repairingCount))")
    } catch {
        print("unexpected error: \(error)")
    }
}
