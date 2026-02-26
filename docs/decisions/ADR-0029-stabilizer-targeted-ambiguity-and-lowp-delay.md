# ADR-0029 - Stabilizer Targeted Delay for Resolver Ambiguity and Low-Confidence P

Status: Accepted
Date: 2026-02-26
Decision Origin: Design

## Context

- ADR-0013 introduced a telnet-only stabilizer delay for risky non-recent spots.
- ADR-0020 added multi-check retries with bounded re-delay and timeout action.
- Operators requested finer control to delay only specific high-risk outcomes instead of broad `P` delays:
  - resolver-ambiguous verdicts (`split` / `uncertain`),
  - `P` outcomes with low numeric confidence.
- The change must preserve bounded resources, deterministic retry limits, and fail-open behavior under queue pressure.

## Decision

- Extend stabilizer policy with three knobs under `call_correction`:
  - `stabilizer_p_delay_confidence_percent`
  - `stabilizer_p_delay_max_checks`
  - `stabilizer_ambiguous_max_checks`
- Keep existing `stabilizer_delay_seconds`; no per-turn interval knob is added.
- Delay decision now supports three reasons:
  - `unknown_or_nonrecent` (existing baseline gate),
  - `ambiguous_resolver` (resolver state `split`/`uncertain`),
  - `p_low_confidence` (glyph `P` and resolver-backed call confidence below configured threshold).
- Retry budget is reason-scoped:
  - baseline uses `stabilizer_max_checks`,
  - ambiguity uses `stabilizer_ambiguous_max_checks` (or falls back to baseline when `0`),
  - low-`P` uses `stabilizer_p_delay_max_checks`.
- Preserve legacy `P` pass-through unless low-`P` policy is explicitly configured with resolver confidence evidence available.
- Add stabilizer reason metrics (held/released/suppressed by reason) while preserving existing aggregate counters.

## Alternatives considered

1. Delay all `P` spots.
   - Pros: simple operator model.
   - Cons: large latency and queue-pressure cost; many `P` spots never promote.
2. Keep only non-recent gate with no new targeted policy.
   - Pros: minimum complexity.
   - Cons: misses ambiguity-driven and weak-`P` risk cases.
3. Add separate per-turn delay intervals per reason.
   - Pros: maximal tuning flexibility.
   - Cons: larger config surface and operational complexity for marginal gain.

## Consequences

- Benefits:
  - More targeted delay policy with smaller latency footprint than broad `P` delays.
  - Better operator visibility via reason-scoped stabilizer counters.
  - Backward compatibility preserved when new knobs remain at defaults (`0`).
- Risks:
  - Behavior depends on resolver snapshot availability for low-`P` policy.
  - Additional policy branches increase test and operator reasoning surface.
- Operational impact:
  - New YAML knobs in `data/config/pipeline.yaml`.
  - Updated stabilizer summary line includes per-reason counters.
  - No archive/peer behavior change (telnet-only).

## Links

- Related ADRs:
  - `docs/decisions/ADR-0013-call-correction-telnet-stabilizer.md`
  - `docs/decisions/ADR-0020-stabilizer-max-checks-retries.md`
  - `docs/decisions/ADR-0026-resolver-primary-switchover-mode.md`
- Code:
  - `main.go`
  - `stabilizer.go`
  - `stats/tracker.go`
  - `config/config.go`
- Tests:
  - `main_test.go`
  - `stabilizer_test.go`
  - `stats/tracker_stabilizer_test.go`
  - `config/call_correction_stabilizer_test.go`
