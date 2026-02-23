# TSR-0002 - Per-Spot Call Correction Split-Evidence Ambiguity and Quality Penalty Reinforcement

Status: Resolved
Date Opened: 2026-02-23
Date Resolved: 2026-02-23
Owner: Cluster maintainers
Technical Area: spot/correction, main/telnet fan-out
Trigger Source: Chat request
Led To ADR(s): ADR-0021
Tags: call-correction, ambiguity, quality-anchors, deterministic-behavior

## Triggering Request
- Request date: 2026-02-23
- Request summary: Re-evaluate the entire call-processing method after observing contradictory call outcomes (for example DL6LD vs DL6LN on the same frequency) and rising complexity from one-off rails.
- Request reference (chat/issue/link): Chat thread in this repository workspace.

## Symptoms and Impact
- What failed or looked wrong?
  - Operators observed contradictory confidence outcomes for near-frequency call variants, interpreted as correction inconsistency.
  - Correction behavior was difficult to reason about due to layered bonuses/rails.
- User/operator impact:
  - Reduced trust in call-correction output determinism.
  - Higher tuning burden and slower troubleshooting.
- Scope and affected components:
  - `spot/correction.go` correction decision path and quality feedback behavior.
  - `README.md` operator-facing behavior notes.

## Timeline
1. 2026-02-23 - Operator reported contradictory call outcomes and requested full method re-evaluation.
2. 2026-02-23 - Code review confirmed per-spot decision architecture, quality reinforcement penalties, and high configuration surface.
3. 2026-02-23 - Phase 1 decision approved: ambiguity guard + quality penalty safety rail + observability/docs updates.

## Hypotheses and Tests
1. Hypothesis A - Contradictory outcomes are driven by per-spot local decisions rather than signal-level resolution.
   - Evidence/commands:
     - Inspected `main.go:2174` and `main.go:2272` (correction invoked per incoming/released spot).
     - Inspected `main.go:3342` and `main.go:3794` (subject-relative confidence assignment and glyph mapping).
   - Outcome: Supported
2. Hypothesis B - Quality updates can self-reinforce wrong outcomes by penalizing all non-winners.
   - Evidence/commands:
     - Inspected `spot/correction.go:1495`..`spot/correction.go:1524`.
   - Outcome: Supported
3. Hypothesis C - Existing gate stack can preserve precision while adding a conservative split-evidence rejection reason.
   - Evidence/commands:
     - Added targeted tests in `spot/correction_test.go` for ambiguous split rejection and overlap-based non-rejection.
     - Ran `go test ./spot`.
   - Outcome: Supported

## Findings
- Root cause (or best current explanation):
  - Core correction inference is per-spot and can produce locally valid but globally inconsistent outcomes under split evidence.
- Contributing factors:
  - Strong reinforcement from quality penalties on all non-winners.
  - Layered gate/bonus interactions increase interpretability cost.
- Why this did or did not require a durable decision:
  - This changes correction behavior, rejection reasons, and quality-update semantics in a shared hot path and requires durable documentation.

## Decision Linkage
- ADR created/updated:
  - Created `ADR-0021`.
- Decision delta summary:
  - Added conservative ambiguity rejection (`ambiguous_multi_signal`) for strong low-overlap split evidence.
  - Added quality-update safety rail to skip decrementing independently validated non-winners.
- Contract/behavior changes (or `No contract changes`):
  - Behavior change in correction rejection outcomes for split evidence.
  - No protocol/wire format changes.

## Verification and Monitoring
- Validation steps run:
  - `go test ./spot`
  - Full checker suite and race run are recorded in the implementation summary for this change set.
- Signals to monitor (metrics/logs):
  - `CorrGate` rejection reasons for `ambiguous_multi_signal`.
  - Applied/rejected correction mix and fallback distribution.
- Rollback triggers:
  - Material recall loss or unexpected rise in unresolved known-busted patterns.

## References
- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s): `docs/decisions/ADR-0021-call-correction-ambiguity-guard-and-quality-rail.md`
- Related docs: `README.md`
