# Decoding strategy

## v1: native structured partials

`streamResponse(to:generating:)` yields snapshots whose `.content` is
`T.PartiallyGenerated` — all-Optional, fields filling in declaration order.
v1 consumes these directly:

- Each snapshot becomes a `.partial` event.
- **Coverage** = fraction of non-nil fields, computed via `Mirror`
  (`FoundationModelsAdapter.coverage(of:)`). Best-effort: it measures
  *field presence*, not semantic completeness.
- The pipeline enforces monotonic coverage per attempt and drops regressions.

No string-level JSON parsing in v1. The framework already did the hard part;
the pipeline adds validation, repair, budgets, and cancellation.

## Fallback: prefix decode (not shipped in v1)

If M0 shows the native partials don't behave as the community references
describe (e.g. `collect()` can't follow iteration), the fallback is
prefix-decoding the raw text stream: attempt `JSONDecoder` on the accumulated
prefix per chunk, emit when a decode newly succeeds. That yields *progressive
complete values*, not field-level partials — a strictly weaker contract, and
the reason it's the fallback, not the plan.

## v2 roadmap: true incremental decode

A streaming JSON parser emitting SAX-like events into a per-field partial
builder derived from the `@Generable` schema. Research spike only — native
partials already cover the v1 use cases, and the Oct 13 event added no new
Foundation Models streaming APIs, so there is nothing to adapt to yet.
