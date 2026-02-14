# ADR-0009 - Call Correction Stacked Prior + Recent Bonus for Min-Reports

Status: Accepted
Date: 2026-02-14
Decision Makers: Cluster maintainers
Technical Area: spot/correction
Tags: call-correction, consensus, priors, recency

## Context
- ADR-0007 introduced a strict one-short prior bonus for `min_reports`.
- Operators approved a scope update where prior and recent-on-band signals can both
  contribute to `min_reports` when a candidate remains short.
- We must preserve hard-gate safety (`advantage`, `confidence`, `freq_guard`, `cooldown`).

## Decision
- Update prior bonus semantics from one-short only to bounded shortfall support:
  - `prior_bonus` can contribute up to `prior_bonus_max` for any positive
    `min_reports` gap when eligibility checks pass.
- Keep recent-on-band bonus on the remaining gap:
  - `recent_band_bonus` can contribute up to `recent_band_bonus_max` after prior
    bonus is applied.
- Combined prior+recent contribution is allowed to stack for `min_reports`.
- Hard gates and all non-min-reports checks remain unchanged.

## Alternatives Considered
1. Keep strict one-short prior bonus (ADR-0007)
   - Pros:
     - Lower false-positive risk from prior signal.
   - Cons:
     - Fails to realize approved stacked corroboration behavior.
2. Replace both bonuses with weighted support scoring
   - Pros:
     - Flexible.
   - Cons:
     - More knobs, lower determinism, larger tuning burden.
3. Allow stacking and bypass hard gates
   - Pros:
     - Highest correction rate.
   - Cons:
     - Violates correction safety contract.

## Consequences
- Positive outcomes:
  - Better recovery for near-threshold candidates with both trusted-prior and
    recent-on-band evidence.
  - Explicit alignment with approved ledger behavior.
- Negative outcomes / risks:
  - Higher correction attempt rate may increase rejection volume at later gates.
  - Requires careful monitoring of false-positive drift.
- Operational impact:
  - Existing knobs are reused; no new knobs added.
  - Decision path can include both `prior_bonus` and `recent_band_bonus`.
- Follow-up work required:
  - Monitor decision-reason distribution after rollout.

## Validation
- Add tests for:
  - explicit stacked `+2` path (prior + recent) satisfying `min_reports`;
  - stacked bonuses still blocked by hard gates (`advantage`).

## Rollout and Reversal
- Rollout plan:
  - Keep current conservative defaults (`prior_bonus_max=1`, `recent_band_bonus_max=1`)
    and tune only if needed.
- Backward compatibility impact:
  - No protocol/schema changes; only correction eligibility behavior within
    `min_reports` changes.
- Reversal plan:
  - Revert prior bonus semantics to one-short-only logic.

## References
- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s): `docs/decisions/ADR-0007-call-correction-topk-prior-bonus-observability.md`, `docs/decisions/ADR-0008-call-correction-recent-band-bonus.md`
- Docs: `README.md`, `data/config/pipeline.yaml`
