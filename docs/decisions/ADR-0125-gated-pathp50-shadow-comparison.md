# ADR-0125: Gated PATHP50 Shadow Glyph Comparison

- Status: Superseded
- Date: 2026-05-08
- Decision Origin: Field evidence

Superseded by ADR-0126 on 2026-05-09. This ADR records the historical gated
shadow comparison; active runtime scoring now uses p50 directly and no longer
emits shadow comparison lines.

## Context
`SET DIAG PATHP50` exposes raw p50 diagnostic values while the active path
method remains mean-based. Early propagation-log review showed many `I/H` and
`I/M` glyph-pair entries: the active method was insufficient, but the shadow p50
histogram still had a displayable glyph. That mixed two different questions:
whether p50 is a better statistic, and whether p50 can produce a value before
the active path gate has enough eligible evidence.

For method comparison, both sides need the same eligibility gate.

## Decision
Keep raw PATHP50 comment output unchanged: operators may still see p50 SNR,
mean-minus-p50 delta, and selected count when the diagnostic path computed them.

For the `Path p50 shadow` aggregate, gate the p50 comparison glyph with the same
active eligibility used by normal path display. If the active prediction is
insufficient because of low count, low weight, stale evidence, or no sample,
the p50 side is counted as insufficient in the shadow glyph-pair matrix even
when raw p50 diagnostic values exist.

## Alternatives considered
1. Leave raw p50 glyphs in the shadow comparison. Rejected because `I/H` and
   related pairs make p50 look more optimistic by comparing a statistic against
   a failed eligibility gate.
2. Hide p50 values from `SET DIAG PATHP50` when the active gate fails. Rejected
   because raw diagnostic visibility is useful for investigating warm-up and
   eligibility behavior.
3. Add new log fields for gated and ungated p50 comparison. Deferred because the
   current question needs one clean method-comparison signal without expanding
   the log format.

## Consequences
### Benefits
- `Path p50 shadow` better answers the method-comparison question.
- Active-insufficient warm-up cases no longer inflate `p50_gt`.
- Existing log line names and fields remain stable.

### Risks
- Historical `Path p50 shadow` logs before this change are not directly
  comparable to new logs.
- Raw PATHP50 comments may still show p50 values that the shadow comparison
  intentionally counts as insufficient.

### Operational impact
- Active glyphs and PATH filters remain mean-based.
- `SET DIAG PATHP50` visible comment format is unchanged.
- Slow clients, queues, reconnect handling, and shutdown behavior are unchanged.

## Links
- Related issues/PRs/commits:
- Related tests: `telnet/server_prediction_stats_test.go`, `telnet/diag_command_test.go`
- Related docs: `README.md`, `docs/OPERATOR_GUIDE.md`, `pathreliability/README.md`, `customgpt/troubleshooting-index.md`
- Related TSRs:
- Related decisions: Refines ADR-0122 and ADR-0123
