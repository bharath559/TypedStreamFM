// 02 — Semantic validation: shape is not meaning
// Constrained decoding guarantees the JSON shape; the validator guarantees
// the meaning. Its error text becomes the repair signal, so good messages
// are a feature. This page runs synchronously — no pipeline needed.

import TypedStreamCore

struct Movie: Sendable {
    var title: String
    var year: Int
}

func validateMovie(_ movie: Movie) throws {
    guard movie.year >= 1900 else {
        throw StructuredOutputError.constraintViolated(path: "year", reason: "must be >= 1900")
    }
    guard !movie.title.isEmpty else {
        throw StructuredOutputError.constraintViolated(path: "title", reason: "must not be empty")
    }
}

// 1. The validator rejects a perfectly-shaped lie:
do {
    try validateMovie(Movie(title: "Dune: Part Two", year: 1899))
    print("valid (unexpected)")
} catch let error as StructuredOutputError {
    print("rejected: \(error.description)")
}

// 2. ...and accepts the real thing:
do {
    try validateMovie(Movie(title: "Dune: Part Two", year: 2024))
    print("accepted: Dune: Part Two (2024)")
} catch {
    print("rejected (unexpected): \(error)")
}

// 3. The error text becomes the next attempt's repair prompt:
print("---")
print(repairPrompt(
    originalPrompt: "Recommend an action movie from the 2020s",
    invalidPreview: #"Movie(title: "Dune: Part Two", year: 1899)"#,
    error: .constraintViolated(path: "year", reason: "must be >= 1900")
))
