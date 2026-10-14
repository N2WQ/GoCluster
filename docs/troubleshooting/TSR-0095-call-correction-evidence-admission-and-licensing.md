# TSR-0095 - Call Correction Evidence, Admission and Licensing

Status: Monitoring
Date Opened: 2026-10-14
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: spot, correctionflow, cluster ingest/output, ULS, historical replay
Trigger Source: Chat request
Led To ADR(s): ADR-0302
Tags: call-correction, dissent, ties, admission, expiry, replay, licensing

## RCA Summary

- What happened: Unsettled exact-bucket dissent could disappear from a nearby
  decision; unequal weights could bypass a qualifying receiver-count tie.
  Activity depended on correction execution, and an absent replacement could
  consume a licensed original before final validation rejected the changed spot.
- Why: Selection filtered whole uncertain populations; ambiguity depended on
  weighted ranking; activity was observed during gate reads; replacement
  licensing occurred after identity mutation.
- What fixed it: Preserve admitted dissent and relevant pending transitions,
  check qualifying raw ties for actual proposals, count successful primary
  admission once, expire activity during observation, and reject an ineligible
  replacement before mutation. Replay owns frozen authority and historical dates.
- How we know: Existing-API baseline regressions failed, then repaired tests
  passed. See the execution record for actual delivery, race, lifecycle and
  performance evidence and final lane results.
- Operator/support answer: A refused correction can retain and deliver the
  original with its own confidence. A pending or tied population can recover
  after expiry or stronger eligible fresh reports; permanent abstention is not
  the intended behavior. Support percentages are not accuracy probabilities.

## Triggering Request

- Request date: 2026-10-14.
- Request summary: Review external F1-F4 feedback, resolve six policies and
  implement three bounded slices with recovery, admission-cost and replay-lifetime acceptance.
- Request reference: Owner's exact `Approved v1` in the implementation chat.

## Symptoms and Impact

Eligible exact dissent could yield an apparent 100% nearby winner. A disjoint
3:3 count tie could avoid split protection solely because reliability differed.
Without correction queries, expired adaptive receivers remained retained.
Missing replacement authority could produce no delivered original.

## Timeline

1. 2026-10-14 - Owner selected policies and approved v1; affected files were
   unchanged between reviewed `99ab1e7` and implementation baseline `8f15288`.
2. 2026-10-14 - Captured failing baseline regressions before implementation.
3. 2026-10-14 - Implemented shared selection, admission and licensing repairs;
   verified actual output and local high-resolution admission measurements.

## Hypotheses and Tests

1. Uncertain exact dissent was discarded by neighborhood admission.
   - Evidence/commands: Native saved binaries, F1/F2 baseline and delivery logs
     in `.tmp/call-correction-f1-f4-20261014/`.
   - Outcome: Supported; exact A3/B5 became nearby B3/3 before the repair.
2. Counting in the output/gate path was equivalent to accepted incoming activity.
   - Evidence/commands: Existing `Observe` regression retained 100 reporters
     across 99 expiry windows; real initialized ingest tests cover duplicates,
     flood rejection, primary refusal and downstream output congestion.
   - Outcome: Rejected; successful primary admission is the selected boundary.
3. Rejecting at final validation was sufficient to preserve a licensed original.
   - Evidence/commands: `TestReplacementLicense*` baseline failed with original
     identity changed and no ring output; found-replacement positive control passed.
   - Outcome: Rejected; replacement must pass before mutation. Later authority
     changes can still legitimately cause final rejection.

## Findings

- Root cause: selection and activity were evaluated at different admission
boundaries, while replacement identity was committed before license authority.
The failing baseline controls above establish each observable failure.

The repair changes evidence and admission contracts without replacing ranking,
tuning thresholds, changing mode inference order or claiming RF accuracy.
Weighted evidence still ranks candidates; equal counts alone do not prove equal
weighted evidence. Blocking a qualifying raw tie is deliberate conservative policy.

## Decision Linkage

- ADR created/updated: [ADR-0302](../decisions/ADR-0302-correction-evidence-admission-and-license-authority.md).
- Decision delta summary: Preserve relevant dissent, separate input activity
  from output delivery, and separate replacement acceptance from final rejection.
- Contract/behavior changes: Shared runtime/replay rules and per-run replay
  license provenance; existing replay percentage-output contract is retained.

## Verification and Monitoring

- Validation steps run: See [execution evidence](../call-correction-f1-f4-execution.md).
- Signals to monitor: `resolver_license_missing` (runtime),
  `resolver_license_rejected` (replay), ambiguity/recovery decisions, primary
  admission drops, retained adaptive band/reporter counts via `DXC_MAP_LOG_INTERVAL`.
- Rollback triggers: Unjustified identity changes, failure to recover after
  qualifying new evidence, retained activity beyond the documented cadence,
  leaked frozen inputs, or unacceptable measured admission delay in service.

## References

- Related ADR(s): ADR-0021, ADR-0033, ADR-0254, ADR-0298, ADR-0302.
- Related docs: [spot contract](../../spot/README.md),
  [historical replay](../rbn_replay.md), [execution](../call-correction-f1-f4-execution.md).
- Commit(s): Implementation baseline `8f152889548ee5b4baf3059d92d5bcaee9b01346`;
  this record does not claim deployment.
