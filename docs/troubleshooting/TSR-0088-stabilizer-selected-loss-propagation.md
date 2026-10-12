# TSR-0088 - Stabilizer Selected Loss Propagation

Status: Monitoring
Date Opened: 2026-10-11
Date Resolved: n/a
Owner: Codex
Technical Area: resolver neighborhood and stabilizer delivery
Trigger Source: Chat request
Led To ADR(s): none
Tags: evidence-loss, stabilizer, temporal, retry, confidence

## RCA Summary

- What happened: correction could retain an identity under selected-neighborhood
  cap loss while the stabilizer released it against a healthy or absent exact
  bucket. Independent historical S could make this discrepancy easy to miss.
- Why: correction and stabilization had different snapshot inputs; temporal
  abstention paths additionally skipped the ordinary correction wrapper.
- What fixed it: carry only selected loss into initial exact delay input and
  derive retry loss from its fresh selection. Keep ordinary exact state/low-P
  policy, budgets and delay-eligible glyphs unchanged.
- How we know: the follow-up execution record maps actual cap admission,
  scheduler/controller and delivered-output checks to observed results.
- Operator/support answer: current eligible neighborhood loss can keep an
  uncertain/S/P spot waiting even when exact support appears settled. Recovery
  is re-evaluated; historical recognition is separate from current percentage.

## Triggering Request

- Request date: 2026-10-11.
- Request: implement approved follow-up S1 before S2-S4, with full checkpoints.
- Reference: [scope ledger](../call-correction-followup-scope-ledger-v2.md).

## Symptoms and Impact

Initial and retry delays can disagree with the correction decision under actual
neighbor candidate/receiver cap loss. Missing exact evidence is not proof that
the selected population is healthy. This is a method/delivery consistency issue;
it establishes no RF association or calibrated call accuracy.

## Timeline

1. Read-only feedback review identified the initial/retry exact-only inputs.
2. Scope challenge added terminal and overflow temporal abstention cases.
3. Approved S1 adds request-local propagation and fresh retry overlay; actual
   validation results are maintained in the execution record.

## Hypotheses and Tests

1. A healthy exact bucket proves healthy selected evidence: rejected by actual
   neighboring cap loss and shared selection's loss-before-comparability rule.
2. Independent S history permits current-loss bypass: rejected for existing
   delay-eligible S; historical recognition does not supply current percentage.
3. Save initial loss in the delayed envelope: rejected; expiry/recovery must use
   current eligible selection, without a sticky request-history authority.
4. Replacing every exact delay input with the neighborhood selection is harmless:
   rejected; it would broaden ordinary split/low-P policy outside this repair.

## Findings

- Root cause: the stabilizer did not receive the correction selection's loss
  authority. Wrapper-only capture also misses temporal abstention.
- Durable lesson: propagate the specific invariant across an owner boundary;
  do not silently substitute all other snapshot semantics or save stale authority.

## Decision Linkage

- ADR: existing [loss authority](../decisions/ADR-0292-resolver-cap-loss-authority.md),
  [shared stabilizer policy](../decisions/ADR-0030-shared-stabilizer-policy-parity-main-replay.md)
  and [eligible glyphs](../decisions/ADR-0046-stabilizer-delay-eligibility-unknown-s-p-only.md).
- Decision delta: none; S1 repairs propagation under these existing contracts.

## Verification and Monitoring

See [execution evidence](../call-correction-followup-execution.md) for actual
results. Healthy/absent exact controls, real cap metrics and scheduler/controller
ownership distinguish loss holding from preexisting ambiguity/history absence.
Monitor existing hold/release/suppression reasons. Production-load behavior and
field accuracy remain unverified.

## References

- Code: `internal/cluster/output_pipeline_stages.go`,
  `internal/cluster/output_pipeline_delivery.go`.
- Tests: `internal/cluster/correction_stabilizer_loss_test.go`.
- Scope: S1 in the approved follow-up ledger.
