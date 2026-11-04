// 03 — Repair loop
// The first attempt returns a well-formed but invalid movie. The pipeline
// re-prompts with the validator's error text (see page 02), the second
// attempt converges, and every fix is observable via .repairing.
// Mirrors repairConvergesWithinBudget in Tests/TypedStreamTests.

import PlaygroundSupport
import TypedStreamCore
import TypedStreamTestKit

struct Movie: Sendable {
    var title: String
    var year: Int
}

PlaygroundPage.current.needsIndefiniteExecution = true

Task {
    defer { PlaygroundPage.current.finishExecution() }

    // The scripted backend reads the repair prompt: once it contains the
    // validator's error text ("was invalid"), it returns the fixed value.
    let fake = FakeLanguageModel<String, Movie> { prompt in
        let final = prompt.contains("was invalid")
            ? Movie(title: "Dune: Part Two", year: 2024)
            : Movie(title: "Dune: Part Two", year: 1899)
        return AsyncThrowingStream { continuation in
            continuation.yield(.partial(PartialSnapshot(value: "Dune…", coverage: 0.4)))
            continuation.yield(.final(final))
            continuation.finish()
        }
    }
    let pipeline = TypedStreamPipeline(
        provider: fake.provider(),
        validate: { (movie: Movie) in
            guard movie.year >= 1900 else {
                throw StructuredOutputError.constraintViolated(path: "year", reason: "must be >= 1900")
            }
        }
    )

    do {
        for try await event in await pipeline.stream(prompt: "Recommend an action movie from the 2020s") {
            switch event {
            case .partial(let snapshot):
                print("partial: \"\(snapshot.value)\"  coverage: \(snapshot.coverage)")
            case .validated(let movie):
                print("validated: \(movie.title) (\(movie.year))")
            case .repairing(let attempt):
                print("repairing #\(attempt.index): \(attempt.error)")
            }
        }
    } catch {
        print("terminal error: \(error)")
    }
}
