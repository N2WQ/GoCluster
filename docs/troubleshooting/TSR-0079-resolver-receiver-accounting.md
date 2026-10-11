# TSR-0079 - Resolver Receiver Accounting

Status: Monitoring
Date Opened: 2026-10-10
Date Resolved: n/a
Owner: Codex
Technical Area: signal resolver, neighborhood selection, delivered confidence
Trigger Source: Chat request
Led To ADR(s): ADR-0280
Tags: correction, receiver union, dissent, publication

## RCA Summary

- What happened: repeated bucket observations manufactured corroboration and
  removed competing receiver support from delivered confidence.
- Why: neighborhood aggregation added only each bucket winner's tally.
- What fixed it: immutable bounded observations and receiver union accounting,
  including losing candidates inside admitted buckets.
- How we know: baseline counterexamples and repaired connected consumer tests;
  complete checkpoint evidence belongs to the linked execution record.
- Operator/support answer: displayed shares measure retained receiver evidence,
  not calibrated probabilities or independent RF correctness.

## Triggering Request

- Request date: 2026-10-10
- Request summary: implement five bounded correction/confidence patches with
  actual delivered-output regression checks, one tested local commit at a time.
- Request reference: user Approved v1, recorded in the execution ledger.

## Symptoms and Impact

Two receivers supporting A in two buckets and one receiver supporting B yielded
A support 4/4 and 100%, rather than A=2, B=1, total=3. With a minimum of three
supporters, B was delivered as A/C/100% on real normal/DIAG TCP and PC61 receipt,
sealed ring and reopened archive. Weighted dissent also disappeared.

## Timeline

1. 2026-10-10 - Baseline source and complete validation qualified at fb86e75.
2. 2026-10-10 - Existing-API accounting and connected delivery cases reproduced
   the defect before production changes.
3. 2026-10-10 - P1 changed receiver ownership/accounting; targeted repaired cases
   preserve dissent and still accept a genuine third receiver at 75%.

## Hypotheses and Tests

1. Duplicate receivers inflate support - Supported: two-bucket fixture failed
   with A=4/100%; one-bucket control passed.
2. The effect is only diagnostic - Rejected: actual TCP/peer identity, sealed
   buffer and reopened archive showed the unauthorized correction.
3. Older input rolls back metadata - Supported: baseline older-timestamp test
   changed candidate/key time, frequency and report to the older values.

The initial connected fixture failed to advance beyond evaluation cadence. Its
failure is retained separately and excluded from defect evidence. Advancing the
driver sweep made every one-bucket control pass before the production repair.

## Findings

- Root cause: neighborhood aggregation summed bucket winners, so repeated
  receivers inflated support and losing candidates disappeared.

The resolver needs immutable receiver identities for combination, not only
aggregated counts. Replacement comparability cannot discard admitted dissent.
Full retained history is unnecessary; newest observations and bounded owner
publication suffice for this batch's contract.

## Decision Linkage

- ADR created/updated: ADR-0280.
- Decision delta summary: receiver-owned current observations, immutable changed
  generations and compact temporal scoring.
- Contract/behavior changes: distinct weighted receiver support and dissent;
  no wider signal association, thresholds or protocol/schema changes.

## Verification and Monitoring

- Validation steps run: see the execution record for exact checkpoint results.
- Signals to monitor: published receiver incidences, owner caps/evictions,
  neighborhood disagreement and downstream correction outcomes.
- Rollback triggers: accounting regression, retention outside owner bounds,
  publication races or unacceptable measured production cost.
- Local checks do not establish production load, RF truth or calibration.

## References

- Related ADR(s): [ADR-0280](../decisions/ADR-0280-resolver-receiver-evidence-ownership.md)
- Related docs: [execution record](../call-correction-method-execution.md),
  [spot confidence](../../spot/README.md)
