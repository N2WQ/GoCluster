# ADR-0145: PSKReporter Fast Payload Parser

- Status: Accepted
- Date: 2026-06-05
- Decision Origin: Design

## Context
Long-running profiles showed the PSKReporter MQTT ingest path as a major source
of CPU time, heap churn, and allocation volume. The dominant avoidable cost was
generic JSON materialization for each flat PSKReporter MQTT payload before the
cluster could decide whether the report should become a spot.

The Phase 1 scope is intentionally narrow. It targets only JSON parse and early
normalization cost in `pskreporter`, while keeping MQTT transport behavior,
queueing, spot materialization, and CTY validation contracts unchanged.

## Decision
Add a private PSKReporter payload parser for the common flat MQTT JSON shape.
The parser reads only the fields needed by the current cluster pipeline:
frequency, mode, report, timestamp, sender call, sender grid, receiver call, and
receiver grid.

The parser still recognizes the currently modeled but unused primitive fields
`sq`, `sa`, `ra`, and `b`. It validates that they have the primitive type the
existing `jsoniter` decoder accepted, then discards them. The parser falls back
to the existing compatible JSON decoder when it sees escaped strings, non-ASCII
strings, case variants of known PSKReporter field names, malformed JSON, or any
other unusual shape.

ADIF values remain owned by CTY validation. PSKReporter sender and receiver ADIF
fields are not materialized in this fast path.

## Alternatives considered
1. Keep generic `jsoniter` decoding only. This has the lowest code complexity
   but preserves the measured per-message allocation and reflection-heavy parse
   cost on a high-volume path.
2. Replace parsing with a generated decoder. This could reduce generic decoder
   overhead, but adds generation workflow and review complexity for a small,
   flat, source-specific payload.
3. Change the MQTT callback or Paho integration to avoid payload copies. That is
   a plausible later phase, but it crosses transport, lifecycle, and queue
   boundaries and was deliberately out of Phase 1.
4. Pool or compact `spot.Spot` materialization. That may reduce the remaining
   allocations, but it changes ownership and retention semantics and belongs in
   a separate design.

## Consequences
### Benefits
- The common PSKReporter parse path performs no heap allocation.
- The `handlePayload` FT8 benchmark removes generic JSON churn from the hot
  path and leaves the remaining allocations concentrated in spot materialization
  and retained spot strings.
- Compatibility behavior is bounded by fallback to the existing decoder for
  unusual JSON.

### Risks
- A hand-written parser is more sensitive to hidden wire-format variation than a
  generic JSON decoder.
- Future PSKReporter fields required by the pipeline must be added explicitly to
  the private parser and to its compatibility tests.
- The fast parser borrows byte slices from the worker-owned MQTT payload, so its
  parsed byte fields must not escape payload handling.

### Operational impact
- No operator configuration, YAML schema, MQTT subscription, queue, or shutdown
  behavior changes.
- Normal PSKReporter messages should consume less CPU and allocation bandwidth.
- Unusual but compatible JSON remains accepted through fallback, at the older
  parse cost for those messages only.

## Links
- Related issues/PRs/commits: Phase 1 PSKReporter/MQTT profiling refactor
- Related tests: `pskreporter/pskr_payload_test.go`
- Related docs: `docs/decisions/ADR-0117-hot-path-duplicate-work-removal.md`, `docs/decisions/ADR-0121-pskreporter-trusted-spot-materialization.md`
- Related TSRs: none
- Supersedes / superseded by: none
