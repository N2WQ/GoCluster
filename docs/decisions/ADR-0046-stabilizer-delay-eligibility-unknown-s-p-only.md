# ADR-0046 - Stabilizer Delay Eligibility Scoped to ?, S, and P Glyphs

Status: Accepted
Date: 2026-03-01
Decision Makers: Cluster maintainers
Technical Area: internal/correctionflow stabilizer policy, main output pipeline, replay parity, operator docs
Decision Origin: Troubleshooting chat
Troubleshooting Record(s): TSR-0010
Tags: stabilizer, confidence-glyphs, parity

## Context

- Operators observed high delayed-turn counts for glyph `V` in runtime stats, which conflicted with intended stabilizer behavior.
- Shared stabilizer policy in `internal/correctionflow` is consumed by both main runtime and replay; any semantic fix must preserve parity.
- Stabilizer delay exists to wait for likely promotions (`?`/`S` -> `P`/`V`, low-confidence `P` -> `V`), while keeping queue pressure bounded and fail-open behavior deterministic.

## Decision

- Restrict stabilizer delay eligibility to confidence glyphs `?`, `S`, and `P`.
- `V` and `C` always pass through stabilizer delay/retry policy.
- Keep existing reason rails (`unknown_or_nonrecent`, `ambiguous_resolver`, `p_low_confidence`, `edit_neighbor_contested`) for delay-eligible glyphs only.
- Keep existing bounded queue, retry-budget, timeout-action, and fail-open overflow semantics unchanged.
- Keep implementation in shared helper (`internal/correctionflow/stabilizer.go`) so main and replay remain policy-identical.

## Alternatives Considered

1. Keep current broad ambiguity/edit-neighbor delay behavior (including `V`/`C`).
   - Pros:
     - Maximum conservatism when evidence is contested.
   - Cons:
     - Unnecessary latency for high-confidence output and avoidable queue occupancy.
2. Disable ambiguity/edit-neighbor delay rails globally.
   - Pros:
     - Simpler policy and lower latency.
   - Cons:
     - Loses targeted safety behavior that still benefits `?`/`S`/`P`.
3. Add new config knobs for per-glyph delay eligibility.
   - Pros:
     - Maximum operator tuning flexibility.
   - Cons:
     - Expands config surface and operational complexity without clear need.

## Consequences

- Positive outcomes:
  - Removes stabilizer-induced latency for `V`/`C` outputs.
  - Reduces stabilizer queue pressure under contested/high-confidence conditions.
  - Preserves main/replay parity via shared-policy change.
- Negative outcomes / risks:
  - Some previously delayed `V` contested cases now pass through immediately.
- Operational impact:
  - Stabilizer glyph-turn distributions will shift (fewer delayed `V` turns).
  - No new config knobs and no protocol/wire format changes.
- Follow-up work required:
  - Monitor stabilizer reason mix and output quality during rollout windows.

## Validation

- Updated tests:
  - `internal/correctionflow/stabilizer_test.go`
  - `main_test.go`
- Commands:
  - `go test ./internal/correctionflow -run Stabilizer -count=1`
  - `go test . -run TestEvaluateTelnetStabilizerDelay -count=1`
- Evidence that would invalidate this decision later:
  - Material quality regression traceable to immediate `V`/`C` release in contested cases.

## Rollout and Reversal

- Rollout plan:
  - Ship shared helper change with updated docs; observe stabilizer counters and glyph-turn distributions.
- Backward compatibility impact:
  - User-visible telnet timing semantics change for `V`/`C` (no delay).
  - No config schema change.
- Reversal plan:
  - Revert glyph-eligibility guard in shared helper to restore prior broad delay eligibility.

## References

- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s):
  - `docs/decisions/ADR-0029-stabilizer-targeted-ambiguity-and-lowp-delay.md`
  - `docs/decisions/ADR-0030-shared-stabilizer-policy-parity-main-replay.md`
  - `docs/decisions/ADR-0033-resolver-neighborhood-and-contested-edit-neighbor-rails.md`
- Troubleshooting Record(s):
  - `docs/troubleshooting/TSR-0010-stabilizer-v-glyph-delay-eligibility.md`
- Docs:
  - `README.md`
  - `docs/OPERATOR_GUIDE.md`
  - `data/config/pipeline.yaml`
  - `docs/decision-log.md`
  - `docs/troubleshooting-log.md`
