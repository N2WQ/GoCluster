# ADR-0021 - Call-Correction Split-Evidence Ambiguity Guard and Validated Non-Winner Quality Penalty Rail

Status: Accepted
Date: 2026-02-23
Decision Makers: Cluster maintainers
Technical Area: spot/correction
Decision Origin: Troubleshooting chat
Troubleshooting Record(s): TSR-0002
Tags: call-correction, ambiguity, quality-anchors, observability

## Context
- Operators reported contradictory call outcomes for close call variants on the same frequency and requested an architecture-level review.
- Inspection showed correction decisions are made per spot, while quality feedback penalized every non-winner call in a resolved cluster.
- We need a low-risk Phase 1 change that improves correctness under split evidence without adding new YAML knobs or changing protocol formats.
- Hard safety gates and bounded resource behavior must remain intact.

## Decision
- Add a conservative split-evidence ambiguity guard in correction candidate evaluation:
  - Reject with `reason=ambiguous_multi_signal` when:
    - winner/runner-up both have meaningful support,
    - support gap is narrow,
    - runner-up remains materially strong by existing ratio logic,
    - both candidates are near the same narrow frequency neighborhood,
    - spotter overlap between winner and runner-up is very low,
    - and the conflict is not a configured family relation.
- Keep existing gate stack and order otherwise (`min_reports`, `advantage`, `confidence`, `freq_guard`, `cooldown`).
- Update quality feedback semantics:
  - Keep winner increment.
  - Skip decrementing non-winners that are independently validated by known-call or recent-on-band evidence.
  - Keep decrement behavior for unvalidated non-winners.
- Do not add new config knobs in Phase 1.

## Alternatives considered
1. Add a temporary SCP-only "both valid" guard
   - Pros:
     - Fast and simple.
   - Cons:
     - Misses non-SCP split-evidence cases and does not address quality reinforcement.
2. Full signal-level resolver rewrite immediately
   - Pros:
     - Best long-term architecture.
   - Cons:
     - Too large/risky for immediate stabilization.
3. Keep existing behavior and tune thresholds only
   - Pros:
     - No code complexity increase.
   - Cons:
     - Does not address root split-evidence semantics or reinforcement bias.

## Consequences
- Positive outcomes:
  - Fewer forced corrections in split-evidence clusters with disjoint spotter support.
  - Better observability through explicit `ambiguous_multi_signal` rejection reason.
  - Reduced self-reinforcing quality penalties for independently validated calls.
- Negative outcomes / risks:
  - Some previously applied corrections will now reject as ambiguous (possible recall reduction in edge cases).
  - Slightly more computation per candidate from overlap checks.
- Operational impact:
  - No protocol/wire-format changes.
  - No config schema changes.
  - Correction reason distribution changes (new reject reason).
- Follow-up work required:
  - Continue toward signal-level resolver + policy separation in later phases.

## Validation
### 2026-10-10 resolver refinement

The approved method batch preserves the narrow guard: the closest pair of
eligible receiver frequencies from the two candidates must be strictly below
the configured separation. The existing support, ratio, overlap, edit-distance
and family conditions still apply. A distant later report cannot erase an
otherwise qualifying close conflict. An outlier can consequently retain a
split; wider signal association is deferred.

Conflicting frequencies at the same newest receiver/candidate timestamp cause
abstention until a newer observation supersedes them or they expire. Published
exact splits bind neighborhood and temporal selectors. Changed evidence may
legitimately clear protection; selector traversal alone cannot.

The binary64 comparison is unchanged. Nominal 300 Hz separation can fall on
different sides near 7 and 14 MHz because of representation error; this batch
adds neither tolerance nor quantization. Current key/window membership does not
prove a common transmission episode. Fresh release authority is a separate P3
checkpoint. See [the execution record](../call-correction-method-execution.md).

- Unit coverage added for:
  - ambiguous split rejection,
  - high-overlap non-rejection,
  - quality penalty skip for known validated non-winners,
  - quality penalty skip for recent validated non-winners.
- Required checker suite run for this non-trivial change:
  - `go test ./...`
  - `go vet ./...`
  - `staticcheck ./...`
  - `go test -race ./...`

## Rollout and Reversal
- Rollout plan:
  - Ship with fixed conservative thresholds (no new knobs), monitor correction reason mix and user-reported split outcomes.
- Backward compatibility impact:
  - No compatibility changes in wire/output format; only correction decision behavior changes.
- Reversal plan:
  - Revert ambiguity guard path and validated non-winner penalty skip in `spot/correction.go`.

## References
- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s):
  - `docs/decisions/ADR-0003-call-correction-anchor-gating.md`
  - `docs/decisions/ADR-0014-call-correction-family-policy.md`
  - `docs/decisions/ADR-0016-call-correction-family-aware-recent-and-delta2-rails.md`
- Troubleshooting Record(s):
  - `docs/troubleshooting/TSR-0002-call-correction-ambiguity-and-quality-penalty.md`
- Docs:
  - `README.md`
  - `docs/decision-log.md`
  - `docs/troubleshooting-log.md`
