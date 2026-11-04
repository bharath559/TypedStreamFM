import Testing
import TypedStreamCore
import TypedStreamTestKit

/// v1 behavior contract, all deterministic — zero network, zero model.
struct PipelineTests {
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
}
