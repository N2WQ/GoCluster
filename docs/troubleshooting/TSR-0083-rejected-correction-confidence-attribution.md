# TSR-0083 - Rejected Correction Confidence Attribution

Status: Monitoring
Date Opened: 2026-10-11
Date Resolved: n/a
Owner: Codex
Technical Area: resolver confidence, rejected correction, delivered percentage
Trigger Source: Chat request
Led To ADR(s): none
Tags: confidence, CTY, invalid base, temporal, stabilizer, delivered output

## RCA Summary

- What happened: retained A displayed the rejected candidate B's support share.
- Why: the invalid-base and CTY-miss broadcast branches overwrote the already
  computed original-call percentage with winnerConfidence.
- What fixed it: preserve the original's percentage/availability on rejection;
  successful identity mutation still assigns the replacement's share.
- How we know: compiled baseline DIAG TCP and sealed-ring counterexamples;
  positive checkpoint results are recorded in the linked execution record.
- Operator/support answer: B marks a rejected correction; its percentage
  describes the retained call. Support percentages are not accuracy probabilities.

## Triggering Request

- Request date: 2026-10-10
- Request summary: Approved v1, five bounded method patches with actual output checks.
- Request reference: approved method execution record, P5.

## Symptoms and Impact

At baseline `bf38280`, B had two equal-weight receivers and A one. CTY/base
rejection retained A/B but delivered 66% instead of 33%. Unequal-weight cases
delivered 64% instead of 35%, and 68% instead of 31%. An unsupported retained
identity incorrectly became available at 66% instead of DIAG `--%`.

Immediate, actual temporal Apply and actual scheduler retry reproduced the
misattribution. Normal TCP and reopened archive preserve call/glyph; PC61
confirms identity. Only DIAG TCP and sealed ring carry this percentage contract.
Archive does not persist the percentage, and PC61 does not carry DIAG confidence
fields.

## Timeline

1. Current source isolated two rejection-only percentage overwrites.
2. Compiled baseline tests reproduced wrong delivered shares. Initial retry
   fixtures also failed eligibility because they advanced receipt timestamps
   beyond the current clock; those failures are excluded from attribution evidence.
3. Corrected fixtures use an already observed clock and positively assert the
   current eligible population. All rejection paths then reproduce the share
   failure before production changes; accepted/bypass controls pass.
4. P5 removes the two overwrites. Final checkpoint results are in the execution record.

## Hypotheses and Tests

1. Initial attribution itself is wrong: rejected; it assigns original-call
   support before rejection. Other gate/split/missing/unchanged controls preserve it.
2. Rejection overwrites donate the winner's percentage: confirmed by immediate,
   temporal and retry DIAG/ring failures with literal receiver populations.
3. The initial uncertain retry output proves another production defect:
   rejected as evidence; the fixture supplied future observations. Corrected
   current-clock eligibility barriers reach the intended CTY/base rejection.

## Findings

- Root cause: outcome marker and identity-specific support were assigned from
  different identities in two rejection branches.
- The smallest repair retains existing attribution rather than adding a new
  final pass, state owner, percentage field or replay protocol contract.
- Successful nil-CTY and CTY-hit changes still emit B/C66% in the equal-weight control.

## Decision Linkage

- ADR created/updated: none; this restores the existing emitted-call contract.
- Decision delta summary: no new policy, threshold, suppression or scoring decision.
- Contract/behavior changes: rejected output retains its own share/availability;
  rejection markers, successful shares and suppressed outcomes remain unchanged.

## Verification and Monitoring

- Validation steps run: exact commands/results in the execution record.
- Monitor retained-call DIAG shares and rejection reasons separately from accepted corrections.
- Wider RF association and calibrated correctness probability remain deferred.
- Rollback trigger: changed identity/outcome, ownership or suppression behavior.

## References

- Related docs: [execution record](../call-correction-method-execution.md),
  [spot confidence](../../spot/README.md)
- Related tests: `internal/cluster/confidence_attribution_delivery_test.go`,
  `internal/cluster/confidence_attribution_controls_test.go`, existing temporal
  suppression and accepted-retry regressions.
