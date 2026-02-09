# ADR-0004: Slash-Variant Precedence and Canonical Grouping in Call Correction

- Status: Accepted
- Date: 2026-02-08

## Context

Call correction consensus previously treated slash and non-slash forms as unrelated calls.
This split corroboration and created two operator-facing issues:

1. Bare calls (for example `W1AW`) could outvote slash-explicit calls (for example `W1AW/1`) even when the slash was intentionally transmitted.
2. Prefix/suffix regional permutations (for example `KH6/W1AW` and `W1AW/KH6`) were counted separately, reducing support and creating non-intuitive outcomes.

Operational requirement for this change:

- Keep existing correction thresholds and gate chain.
- Prefer slash-explicit evidence over bare evidence when slash support is credible.
- Preserve deterministic behavior and bounded resources.

## Decision

Adopt correction-only canonical grouping and slash precedence semantics:

1. Build a correction identity per call with:
   - `base_key` (base call identity),
   - `vote_key` (canonical variant key used for voting),
   - slash equivalence normalization so `KH6/W1AW` and `W1AW/KH6` map to the same `vote_key`.
2. Apply slash precedence within each base group:
   - If at least one slash-explicit variant meets existing credibility (`min_consensus_reports`), exclude the bare variant from voting and anchor candidacy.
3. Keep all existing gates (`min_reports`, `advantage`, `confidence`, `freq_guard`, `cooldown`) unchanged and deterministic.
4. Use canonical keys for anchor/quality scoring while preserving a deterministic display variant (`support`, then `recency`, then lexical).
5. Record slash-precedence context in decision traces via `decision_path` suffixes and explicit rejection reasons when precedence removes all viable winners.

## Alternatives considered

1. Keep exact-string voting (no canonical grouping).
- Rejected: preserves current fragmentation and fails slash-priority requirement.

2. Add dedicated slash-threshold config knobs.
- Rejected: increases operator complexity; existing thresholds are sufficient and already understood.

3. Globally rewrite all call normalization to a single slash form.
- Rejected: too broad; risks unintended side effects across non-correction consumers.

## Consequences

### Benefits

- Slash-explicit calls are preserved when corroborated.
- Prefix/suffix regional permutations now contribute to the same correction evidence bucket.
- Decision outcomes remain deterministic with existing gate semantics.

### Risks

- Some corrections may now favor slash variants where prior behavior retained bare calls.
- Canonical grouping can increase correction pressure in mixed-variant clusters; mitigated by unchanged confidence/advantage gates.

### Operational impact

- No protocol framing changes.
- No new config fields or migration requirements.
- Decision logs include slash-precedence path/reason context for troubleshooting.

## Links

- Code:
  - `spot/correction.go`
- Tests:
  - `spot/correction_test.go`
- Docs:
  - `README.md`
  - `docs/decision-log.md`
