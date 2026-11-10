import Testing
import TypedStreamCore
import TypedStreamTestKit

/// v1 behavior contract, all deterministic — zero network, zero model.
struct PipelineTests {
    struct Director: Sendable, Equatable {
        var name: String
    }

    struct Movie: Sendable, Equatable {
        var title: String
        var year: Int
        var rating: Int
        var tags: [String]
        var director: Director

        init(
            title: String,
            year: Int,
            rating: Int = 8,
            tags: [String] = ["sci-fi"],
            director: Director = Director(name: "Denis Villeneuve")
        ) {
            self.title = title
            self.year = year
            self.rating = rating
            self.tags = tags
            self.director = director
        }
    }

    // MARK: - Repair

    @Test func repairConvergesWithinBudget() async throws {
        let fake = FakeLanguageModel<String, String> { prompt in
            // The repair prompt contains the validator's error text.
            let final = prompt.contains("was invalid") ? "valid" : "nope"
            return AsyncThrowingStream { continuation in
                continuation.yield(.partial(PartialSnapshot(value: "par", coverage: 0.5)))
                continuation.yield(.final(final))
                continuation.finish()
            }
        }
        let pipeline = TypedStreamPipeline(
            provider: fake.provider(),
            validate: { output in
                guard output == "valid" else {
                    throw StructuredOutputError.constraintViolated(path: "root", reason: "must be \"valid\"")
                }
            }
        )

        var repairingCount = 0
        var validatedValue: String?
        for try await event in await pipeline.stream(prompt: "go") {
            switch event {
            case .repairing: repairingCount += 1
            case .validated(let value): validatedValue = value
            case .partial: break
            }
        }

        #expect(repairingCount == 1)
        #expect(validatedValue == "valid")
    }

    @Test func budgetExhaustionThrowsInsteadOfHanging() async throws {
        let fake = FakeLanguageModel<String, String>.scripted(partials: [], final: "bad")
        let pipeline = TypedStreamPipeline(
            provider: fake.provider(),
            validate: { _ in
                throw StructuredOutputError.constraintViolated(path: "root", reason: "always bad")
            },
            budget: RepairBudget(maxAttempts: 1, attemptTimeout: .seconds(5), totalTimeout: .seconds(30))
        )

        do {
            for try await _ in await pipeline.stream(prompt: "go") {}
            Issue.record("expected budgetExhausted, stream completed")
        } catch let error as StructuredOutputError {
            guard case .budgetExhausted = error else {
                Issue.record("wrong error: \(error)")
                return
            }
        }
    }

    @Test func repairPromptCarriesErrorAndPreview() {
        let prompt = repairPrompt(
            originalPrompt: "orig",
            invalidPreview: "bad{",
            error: .constraintViolated(path: "year", reason: "must be >= 1900")
        )
        #expect(prompt.contains("orig"))
        #expect(prompt.contains("bad{"))
        #expect(prompt.contains("year"))
        #expect(prompt.contains("was invalid"))
    }

    // MARK: - Composable & Multi-Violation KeyPath Validators

    @Test func checkCollectsAllViolationsInSinglePass() {
        let check: Check<Movie> = Check.notBlank(\Movie.title)
            && Check.greaterThanOrEqual(\Movie.year, 1900)
            && Check.inRange(\Movie.rating, 1...10)
            && Check.nonEmpty(\Movie.tags)
            && Check.nested(\Movie.director, Check.notBlank(\Director.name))
            && Check.forEach(\Movie.tags, Check(path: "root", reason: "tag must not be blank") { !$0.isEmpty })

        let invalid = Movie(
            title: "   ",
            year: 1850,
            rating: 15,
            tags: [""],
            director: Director(name: "")
        )

        let violations = check.evaluate(invalid)
        #expect(violations == [
            ConstraintViolation(path: "title", reason: "must not be blank"),
            ConstraintViolation(path: "year", reason: "must be >= 1900"),
            ConstraintViolation(path: "rating", reason: "must be in 1...10"),
            ConstraintViolation(path: "director.name", reason: "must not be blank"),
            ConstraintViolation(path: "tags[0]", reason: "tag must not be blank"),
        ])

        #expect(throws: StructuredOutputError.constraintsViolated(violations)) {
            try check.validate(invalid)
        }
    }

    @Test func multiViolationPipelineRepairsAllBrokenFieldsInOneRound() async throws {
        let recorder = PromptRecorder()
        let fake = FakeLanguageModel<String, Movie> { prompt in
            AsyncThrowingStream { continuation in
                Task {
                    await recorder.record(prompt: prompt)
                    // Repair converges in a single retry only if BOTH 'title' and 'year'
                    // violations were reported together in the first repair prompt.
                    if prompt.contains("'title'") && prompt.contains("'year'") {
                        continuation.yield(.final(Movie(title: "Dune: Part Two", year: 2024)))
                    } else {
                        continuation.yield(.final(Movie(title: "", year: 1800)))
                    }
                    continuation.finish()
                }
            }
        }

        let pipeline = TypedStreamPipeline(
            provider: fake.provider(),
            checks: [
                Check.notBlank(\Movie.title),
                Check.greaterThanOrEqual(\Movie.year, 1900),
            ],
            budget: RepairBudget(maxAttempts: 1, attemptTimeout: .seconds(5), totalTimeout: .seconds(30))
        )

        var attempts: [RepairAttempt] = []
        var finalMovie: Movie?
        for try await event in await pipeline.stream(prompt: "Recommend a 2020s movie") {
            switch event {
            case .repairing(let attempt):
                attempts.append(attempt)
            case .validated(let movie):
                finalMovie = movie
            case .partial:
                break
            }
        }

        #expect(attempts.count == 1)
        #expect(attempts.first?.error.violations.count == 2)
        #expect(finalMovie == Movie(title: "Dune: Part Two", year: 2024))
    }

    // MARK: - Context-Window-Safe Repair Strategies

    @Test func deltaRepairPromptOmitsOriginalPromptAndPreview() {
        let error = StructuredOutputError.constraintsViolated([
            ConstraintViolation(path: "year", reason: "must be >= 1900"),
            ConstraintViolation(path: "rating", reason: "must be in 1...10"),
        ])
        let delta = deltaRepairPrompt(error: error)
        #expect(delta.contains("year"))
        #expect(delta.contains("rating"))
        #expect(!delta.contains("Offending output"))

        let strategyPrompt = RepairStrategy.deltaPrompt.prompt(
            originalPrompt: "ORIGINAL_PROMPT_SENTINEL",
            invalidPreview: "INVALID_PREVIEW_SENTINEL",
            error: error
        )
        #expect(!strategyPrompt.contains("ORIGINAL_PROMPT_SENTINEL"))
        #expect(!strategyPrompt.contains("INVALID_PREVIEW_SENTINEL"))
        #expect(strategyPrompt == delta)
    }

    @Test func statefulProviderUsesDeltaRepairPromptWhileFreshSessionUsesFullPrompt() async throws {
        // 1. Stateful session provider defaults to .deltaPrompt:
        let statefulRecorder = PromptRecorder()
        let statefulFake = FakeLanguageModel<String, String>(defaultRepairStrategy: .deltaPrompt) { prompt in
            AsyncThrowingStream { continuation in
                Task {
                    await statefulRecorder.record(prompt: prompt)
                    let isRetry = prompt.contains("was invalid")
                    continuation.yield(.final(isRetry ? "ok" : "BAD_OUTPUT_PREVIEW"))
                    continuation.finish()
                }
            }
        }
        let statefulPipeline = TypedStreamPipeline(
            provider: statefulFake.provider(),
            check: Check(path: "value", reason: "must equal ok") { $0 == "ok" }
        )
        for try await _ in await statefulPipeline.stream(prompt: "INITIAL_LONG_SYSTEM_PROMPT") {}

        let statefulPrompts = await statefulRecorder.prompts
        #expect(statefulPrompts.count == 2)
        #expect(statefulPrompts[0] == "INITIAL_LONG_SYSTEM_PROMPT")
        #expect(!statefulPrompts[1].contains("INITIAL_LONG_SYSTEM_PROMPT"))
        #expect(!statefulPrompts[1].contains("BAD_OUTPUT_PREVIEW"))
        #expect(statefulPrompts[1].contains("must equal ok"))

        // 2. Fresh-session-per-attempt provider defaults to .fullPrompt:
        let statelessRecorder = PromptRecorder()
        let statelessFake = FakeLanguageModel<String, String>(defaultRepairStrategy: .fullPrompt) { prompt in
            AsyncThrowingStream { continuation in
                Task {
                    await statelessRecorder.recordSessionCreated()
                    await statelessRecorder.record(prompt: prompt)
                    let isRetry = prompt.contains("was invalid")
                    continuation.yield(.final(isRetry ? "ok" : "BAD_OUTPUT_PREVIEW"))
                    continuation.finish()
                }
            }
        }
        let statelessPipeline = TypedStreamPipeline(
            provider: statelessFake.provider(),
            check: Check(path: "value", reason: "must equal ok") { $0 == "ok" }
        )
        for try await _ in await statelessPipeline.stream(prompt: "INITIAL_LONG_SYSTEM_PROMPT") {}

        let statelessPrompts = await statelessRecorder.prompts
        let sessionCreations = await statelessRecorder.sessionCreations
        #expect(sessionCreations == 2)
        #expect(statelessPrompts.count == 2)
        #expect(statelessPrompts[1].contains("INITIAL_LONG_SYSTEM_PROMPT"))
        #expect(statelessPrompts[1].contains("BAD_OUTPUT_PREVIEW"))
        #expect(statelessPrompts[1].contains("must equal ok"))
    }

    // MARK: - Timeouts

    @Test func attemptTimeoutCountsAgainstBudget() async throws {
        let fake = FakeLanguageModel<String, String>.slow(
            firstEventDelay: .seconds(5),
            partials: [],
            final: "late"
        )
        let pipeline = TypedStreamPipeline(
            provider: fake.provider(),
            validate: { _ in },
            budget: RepairBudget(maxAttempts: 1, attemptTimeout: .milliseconds(100), totalTimeout: .seconds(30))
        )

        var repairingCount = 0
        do {
            for try await event in await pipeline.stream(prompt: "go") {
                if case .repairing = event { repairingCount += 1 }
            }
            Issue.record("expected budgetExhausted, stream completed")
        } catch let error as StructuredOutputError {
            guard case .budgetExhausted = error else {
                Issue.record("wrong error: \(error)")
                return
            }
        }
        // Attempt 1 times out -> repairing(1) -> retry -> times out -> exhausted.
        #expect(repairingCount == 1)
    }

    // MARK: - Cancellation

    @Test func cancellationPropagatesAndCancelsProducer() async throws {
        let probe = Probe()
        let fake = FakeLanguageModel<String, String>.hanging(onCancel: {
            Task { await probe.mark() }
        })
        let pipeline = TypedStreamPipeline(provider: fake.provider(), validate: { _ in })

        let consumer = Task {
            for try await _ in await pipeline.stream(prompt: "go") {}
        }
        try await Task.sleep(for: .milliseconds(200))
        consumer.cancel()

        do {
            try await consumer.value
            Issue.record("expected CancellationError, consumer completed")
        } catch is CancellationError {
            // Expected: the terminal-event guarantee.
        }
        #expect(await probe.marked)
    }

    @Test func validatorCancellationPropagatesInsteadOfRepairing() async throws {
        let fake = FakeLanguageModel<String, String>.scripted(partials: [], final: "ok")
        let pipeline = TypedStreamPipeline(
            provider: fake.provider(),
            validate: { _ in throw CancellationError() }
        )

        var repairingCount = 0
        do {
            for try await event in await pipeline.stream(prompt: "go") {
                if case .repairing = event { repairingCount += 1 }
            }
            Issue.record("expected CancellationError, stream completed")
        } catch is CancellationError {
            // Expected: CancellationError from validator escapes without entering repair.
        }
        #expect(repairingCount == 0)
    }

    @Test func concurrentStreamsAreRejected() async throws {
        let fake = FakeLanguageModel<String, String>.hanging()
        let pipeline = TypedStreamPipeline(provider: fake.provider(), validate: { _ in })

        let firstConsumer = Task {
            for try await _ in await pipeline.stream(prompt: "first") {}
        }
        defer { firstConsumer.cancel() }
        try await Task.sleep(for: .milliseconds(50))

        do {
            for try await _ in await pipeline.stream(prompt: "second") {}
            Issue.record("expected StructuredOutputError, second stream completed")
        } catch is StructuredOutputError {
            // Expected: concurrent stream(prompt:) call is rejected.
        }
    }

    // MARK: - Backpressure / invariants

    @Test func partialCoverageIsMonotonic() async throws {
        let fake = FakeLanguageModel<String, String>.scripted(
            partials: [
                PartialSnapshot(value: "a", coverage: 0.3),
                PartialSnapshot(value: "b", coverage: 0.1), // regression: dropped
                PartialSnapshot(value: "c", coverage: 0.6),
            ],
            final: "ok"
        )
        let pipeline = TypedStreamPipeline(provider: fake.provider(), validate: { _ in })

        var coverages: [Double] = []
        for try await event in await pipeline.stream(prompt: "go") {
            if case .partial(let snapshot) = event {
                coverages.append(snapshot.coverage)
            }
        }
        #expect(coverages == [0.3, 0.6])
    }

    @Test func providerErrorsAreTerminal() async throws {
        struct Boom: Error {}
        let fake = FakeLanguageModel<String, String>.failing(Boom())
        let pipeline = TypedStreamPipeline(provider: fake.provider(), validate: { _ in })

        do {
            for try await _ in await pipeline.stream(prompt: "go") {}
            Issue.record("expected Boom, stream completed")
        } catch is Boom {
            // Expected: non-structured provider errors are terminal, not repaired.
        }
    }

    // MARK: - TypedStreamController (@Observable @MainActor)

    @MainActor
    @Test func controllerTracksPartialsRepairsAndValidatedOutput() async {
        let fake = FakeLanguageModel<String, Movie> { prompt in
            let isRepair = prompt.contains("was invalid")
            return AsyncThrowingStream { continuation in
                continuation.yield(.partial(PartialSnapshot(value: "draft", coverage: 0.4)))
                continuation.yield(.final(Movie(
                    title: "Arrival",
                    year: isRepair ? 2016 : 1800
                )))
                continuation.finish()
            }
        }

        let controller = TypedStreamController(
            provider: fake.provider(),
            check: Check.greaterThanOrEqual(\Movie.year, 1900)
        )

        #expect(!controller.isStreaming)
        await controller.run(prompt: "Recommend Arrival")

        #expect(!controller.isStreaming)
        #expect(controller.partial == "draft")
        #expect(controller.coverage == 1.0)
        #expect(controller.repairAttempts.count == 1)
        #expect(controller.currentRepair?.index == 1)
        #expect(controller.output == Movie(title: "Arrival", year: 2016))
        #expect(controller.error == nil)
        if case .validated(let movie) = controller.phase {
            #expect(movie.year == 2016)
        } else {
            Issue.record("expected .validated phase, got \(controller.phase)")
        }
    }

    @MainActor
    @Test func controllerCapturesStructuredFailureOnBudgetExhaustion() async {
        let fake = FakeLanguageModel<String, Movie>.scripted(
            partials: [],
            final: Movie(title: "", year: 1800)
        )
        let controller = TypedStreamController(
            provider: fake.provider(),
            checks: [
                Check.notBlank(\Movie.title),
                Check.greaterThanOrEqual(\Movie.year, 1900),
            ],
            budget: RepairBudget(maxAttempts: 1, attemptTimeout: .seconds(5), totalTimeout: .seconds(30))
        )

        await controller.run(prompt: "go")

        #expect(!controller.isStreaming)
        #expect(controller.output == nil)
        #expect(controller.structuredError == .budgetExhausted(attempts: 2))
        if case .failed = controller.phase {
            // Expected.
        } else {
            Issue.record("expected .failed phase, got \(controller.phase)")
        }
    }

    @MainActor
    @Test func controllerCancelAndOverlappingStartCleanlyReplaceStream() async throws {
        let probe = Probe()
        let fake = FakeLanguageModel<String, String> { prompt in
            if prompt == "hang" {
                return FakeLanguageModel<String, String>.hanging(onCancel: {
                    Task { await probe.mark() }
                }).provider().events(prompt)
            }
            return FakeLanguageModel<String, String>.scripted(
                partials: [PartialSnapshot(value: "half", coverage: 0.5)],
                final: "done:\(prompt)"
            ).provider().events(prompt)
        }

        let controller = TypedStreamController(provider: fake.provider())

        // Start a hanging stream, then replace it with a second start(prompt:) call.
        controller.start(prompt: "hang")
        try await Task.sleep(for: .milliseconds(50))
        #expect(controller.isStreaming)

        let secondTask = controller.start(prompt: "second")
        await secondTask.value

        #expect(await probe.marked)
        #expect(controller.output == "done:second")
        #expect(controller.coverage == 1.0)

        controller.reset()
        #expect(controller.output == nil)
        #expect(controller.partial == nil)
        #expect(controller.coverage == 0)
        if case .idle = controller.phase {
            // Expected.
        } else {
            Issue.record("expected .idle after reset, got \(controller.phase)")
        }
    }
}
