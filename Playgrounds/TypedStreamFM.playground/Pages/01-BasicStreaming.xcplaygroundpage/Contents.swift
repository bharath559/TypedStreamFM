// 01 — Basic streaming
// Streams typed partial snapshots with monotonic coverage, then a validated
// final. Uses FakeLanguageModel: deterministic, zero network, zero model
// hardware. Mirrors the happy path of Tests/TypedStreamTests.

import PlaygroundSupport
import TypedStreamCore
import TypedStreamTestKit

PlaygroundPage.current.needsIndefiniteExecution = true

Task {
    defer { PlaygroundPage.current.finishExecution() }

    let fake = FakeLanguageModel<String, String>.scripted(
        partials: [
            PartialSnapshot(value: "pa", coverage: 0.25),
            PartialSnapshot(value: "part", coverage: 0.5),
            PartialSnapshot(value: "partia", coverage: 0.75),
        ],
        final: "valid"
    )
    let pipeline = TypedStreamPipeline(
        provider: fake.provider(),
        validate: { output in
            guard output == "valid" else {
                throw StructuredOutputError.constraintViolated(path: "root", reason: "must be \"valid\"")
            }
        }
    )

    do {
        for try await event in await pipeline.stream(prompt: "go") {
            switch event {
            case .partial(let snapshot):
                print("partial: \"\(snapshot.value)\"  coverage: \(snapshot.coverage)")
            case .validated(let value):
                print("validated: \"\(value)\"")
            case .repairing(let attempt):
                print("repairing #\(attempt.index): \(attempt.error)")
            }
        }
    } catch {
        print("terminal error: \(error)")
    }
}
