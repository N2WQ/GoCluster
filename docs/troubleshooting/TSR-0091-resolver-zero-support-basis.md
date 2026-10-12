# TSR-0091 - Resolver Zero Support Basis

Status: Monitoring
Date Opened: 2026-10-12
Date Resolved: n/a
Owner: Codex
Technical Area: resolver percentage, publication and temporal support
Trigger Source: Chat request
Led To ADR(s): ADR-0298
Tags: confidence, weighting, zero, temporal, neighborhood

## RCA Summary

- What happened: a zero-weight call could show receiver coverage, and an all-zero
  population could gain residual waiting-decoder support.
- Why: percentage and classifier fallback used raw counts; publication invented
  a denominator for zero; metadata did not distinguish raw from weighted zero.
- What fixed it: explicit compact basis, genuine zero publication, eligible-call
  percentage calculation, consistent aggregation and zero decoder contribution.
- How we know: the execution record maps owner, projection, union, decoder and
  delivered-output checks to actual final-state checkpoint results.
- Operator/support answer: 0% means eligible weighted evidence gives this call
  zero share; --% means the percentage is unavailable. These are distinct results.

## Triggering Request

The owner selected known/rounded zero as available, all-zero weighting as
unavailable, raw coverage only for deliberate unweighted evidence, and no decoder
support from all-zero weights. [Approved S4 scope](../call-correction-followup-scope-ledger-v2.md).

## Symptoms and Impact

With A2 zero-weight receivers and B1 positive-weight receiver, rejecting B at a
two-report minimum could previously display A/V66 rather than A/P0. Low-cardinality
all-zero events could contribute support even when every receiver weight was zero.
Missing basis during projection or mixed union could restore the same inconsistency.

## Timeline

1. Review reproduced percentage basis disagreement and found publication fallback.
2. The owner chose consistent weighted semantics and decoder neutrality.
3. Detailed test review required actual publication, retained basis, mixed union,
   low-cardinality candidate/subject checks and delivered consequences.

## Hypotheses and Tests

1. Zero numerator means no percentage: rejected; positive denominator defines 0%.
2. Zero denominator means raw coverage: rejected for weighted populations; raw
   coverage requires an explicit unweighted contract.
3. One all-zero score at N10 proves neutrality: insufficient; old smoothing could
   already floor to zero. Include N1/N2, present/absent candidates and subjects.
4. Positive aggregate totals establish weighted authority: insufficient; explicit
   unavailable must survive mixed input and later healthy buckets.
5. Final union percentage proves its state: insufficient if classification reads
   inherited bucket ranks. Check both state and share across 25%/51% boundaries.

## Findings

- Root cause: numerical zero and missing weighting were conflated across publishers
  and consumers; per-call fallback changed the calculation basis.
- Durable lesson: retain the calculation basis with evidence, distinguish defined
  zero from unavailable totals, and verify publication plus actual consumers.

## Decision Linkage

[ADR-0298](../decisions/ADR-0298-consistent-resolver-support-basis.md) refines
weighted voting and decision diagnostics while preserving independent recognition,
family support guards and accepted-observation learning.

## Verification and Monitoring

[Execution evidence](../call-correction-followup-execution.md) records observed
results and pending checkpoints. Contract tests cover real zero admission,
projection/Compact/loss, mixed basis/order, decoder neutrality, family safeguards
and delivered P/0 with unchanged peer/archive formats. Allocation checks do not
establish calibration, field accuracy or production performance.
