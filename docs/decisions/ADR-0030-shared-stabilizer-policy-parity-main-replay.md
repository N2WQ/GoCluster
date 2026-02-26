# ADR-0030 - Shared Stabilizer Policy Parity Across Main and Replay

Status: Accepted
Date: 2026-02-26
Decision Origin: Design

## Context

- ADR-0028 established shared call-correction flow primitives for main and replay.
- Stabilizer gating policy diverged: `main` had updated resolver-aware delay logic while replay still used a local legacy-style delay proxy path.
- This drift reduced confidence that replay faithfully predicts production behavior for stabilizer-sensitive outcomes.

## Decision

- Move stabilizer delay policy logic to shared helpers in `internal/correctionflow`:
  - delay reason model,
  - resolver-aware delay decision,
  - retry budget decision,
  - release-reason resolution,
  - family-aware recent-support check.
- Refactor `main` to consume shared stabilizer helpers instead of local duplicated logic.
- Refactor replay to consume the same shared stabilizer helper and remove legacy old/new split proxy semantics.
- Replay stabilizer proxy metrics become single-policy counters with reason distribution:
  - `would_delay`
  - `reason_unknown_or_nonrecent`
  - `reason_ambiguous_resolver`
  - `reason_p_low_confidence`

## Alternatives considered

1. Keep replay legacy/dual proxy model and leave main as-is.
   - Pros: preserves historical comparison shape.
   - Cons: continues policy drift and weakens replay-to-production parity.
2. Keep shared helper only for replay and leave main local.
   - Pros: smaller `main` code churn.
   - Cons: parity still depends on duplicated policy implementations.
3. Introduce a separate stabilizer policy package outside correctionflow.
   - Pros: cleaner isolation.
   - Cons: more structural churn than needed for current parity objective.

## Consequences

- Benefits:
  - Main and replay now evaluate stabilizer delay using identical policy code.
  - Lower risk of silent behavioral drift between live and replay.
  - Replay stabilizer telemetry is simpler and directly aligned with production policy.
- Risks:
  - Replay metrics schema changed for stabilizer proxy fields.
  - Historical scripts/docs needed updates for new field names.
- Operational impact:
  - No change to bounded queue/fail-open contracts.
  - No protocol or archive/peer behavior changes.

## Links

- Related ADRs:
  - `docs/decisions/ADR-0028-shared-correctionflow-main-replay.md`
  - `docs/decisions/ADR-0029-stabilizer-targeted-ambiguity-and-lowp-delay.md`
- Code:
  - `internal/correctionflow/stabilizer.go`
  - `main.go`
  - `cmd/rbn_replay/main.go`
  - `cmd/rbn_replay/ab_metrics.go`
- Tests:
  - `internal/correctionflow/stabilizer_test.go`
  - `main_test.go`
  - `cmd/rbn_replay/ab_metrics_test.go`
