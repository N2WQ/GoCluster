# ADR-0302: Correction Evidence, Admission and License Authority

- Status: Accepted
- Date: 2026-10-14
- Decision Origin: Troubleshooting chat

## Context

The F1-F4 review exposed discarded dissent, weighted bypass of raw ambiguity,
output-dependent activity accounting and loss of a licensed original after an
ineligible replacement. The owner selected six policies and authorized the
three-slice v1 repair. The correction model, numerical thresholds, primary dedup
semantics and mode inference order remain outside this decision.

## Decision

1. Retain eligible exact-bucket dissent in neighborhood support. Additional
   comparable unsettled populations contribute evidence without creating new
   nominees. Existing settled populations retain their nominee eligibility.
   A relevant pending transition constrains a proposed replacement only while
   both endpoints have positive eligible raw receiver membership and satisfy
   comparability. A transition among comparable competitors can block a third
   candidate. Expiry removes stale constraints without advancing hysteresis.
2. Extend the existing narrow ambiguity guard to qualifying equal raw receiver
   counts despite unequal weights. Check the actual proposed candidate against
   qualifying equal-count competitors, independent of their weighted position.
   Preserve minimum support, closest frequency, overlap, distance and family
   exceptions and the existing non-tie guard. Reliability/confusion still rank
   candidates and determine support where previously applicable.
3. Count eligible incoming activity once after validation and flood policy and
   successful primary-input enqueue. Capture immutable eligibility and identity
   before publication. The existing adaptive mutex covers the nonblocking send
   and success-only update; adaptive readers use that mutex. Duplicate rejection
   and later output congestion do not undo admission. Primary refusal does not
   count. Use known eligible mode at admission, excluding unknown modes, beacons
   and local SELF. Correction enablement does not control adaptation.
4. Live activity time is sampled at admission after lock waiting and due
   maintenance; replay retains original admitted row time. Gate reads/retries
   cannot refresh activity. Observation-driven expiry uses the existing window
   and maintenance cadence, independently of state evaluation/hysteresis, with
   no new worker or retained side table.
5. Reject a definitively absent/ineligible replacement before changing identity,
   confidence, metadata, correction counters or temporal confirmation. Keep the
   original and its own available support; do not search another candidate.
   The ordinary final gate still decides output. Later confirmed absence may
   reject an accepted changed spot without rolling back mutation accounting.
6. Replay applies replacement and final-DX rejection using frozen FCC/ISED and
   allowlist bytes. Record byte hashes, sizes, source identities, actual readiness,
   enforcement and clock roles. Canadian dated eligibility uses simulated UTC
   at each check, including delayed release; cache TTL uses process time. Clear
   handles, paths, allowlist, policy and cache on success/partial failure before
   deleting owned inputs. Preserve primary errors and surface cleanup failures.

## Alternatives considered

1. Freeze every uncertain neighborhood: simpler, but unrelated transitions
   could prevent recovery indefinitely.
2. Let weights decide all equal-count conflicts: mathematically coherent for
   ranking, but does not meet the selected conservative replacement policy.
3. Count before flood policy or after delivery: each counts a different
   population from successfully admitted incoming activity.
4. Add a consumer fence/queue or per-spot marker: unnecessary ownership and
   lifecycle expansion when the existing adaptive mutex provides visibility.
5. Restore originals after final rejection or search runners: adds rollback or
   candidate-search semantics outside the bounded repair.
6. Check historical rows against today's date or mutable database path:
   makes dated eligibility and replay inputs depend on execution conditions.

## Consequences

### Benefits

- Original reports survive an already-ineligible replacement.
- Relevant dissent and ties block unjustified changes, with fresh-evidence and
  expiry recovery controls.
- Activity admission, expiry and replay authority have explicit ownership.

### Risks

- Conservative raw ties and pending transitions can delay otherwise strong
  weighted candidates. Empirical superiority is not established.
- Admission can wait for the adaptive mutex and full-map pruning. Local
  measurements and their acceptance judgment are in the execution record;
  a nonblocking channel operation does not imply a wait-free admission path.
- Frozen snapshots cannot reproduce historical/live database changes.

### Operational impact

No numerical tuning, protocol columns or new runtime settings. Replay manifests
and run history add `license_inputs` provenance. Disabled checking, unavailable
authority and confirmed per-call absence remain distinct. Malformed allowlists
retain the existing warning and empty-exception behavior. Replay still omits
production admission/enrichment surfaces and per-row support percentages.

## Links

- Related tests: `spot/f1_f2_test.go`, `internal/correctionflow/f1_f2_test.go`,
  `internal/cluster/f1_f2_delivery_test.go`, `internal/cluster/license_replacement_delivery_test.go`,
  activity-admission tests and replay/ULS license-input/policy tests.
- Related docs: [execution](../call-correction-f1-f4-execution.md),
  [spot contract](../../spot/README.md), [replay](../rbn_replay.md).
- Related TSRs: [TSR-0095](../troubleshooting/TSR-0095-call-correction-evidence-admission-and-licensing.md).
- Supersedes / superseded by: Refines raw-tie clauses of ADR-0021,
  uncertain-neighborhood and activity-observation clauses of ADR-0033,
  and replay clock/ownership application of ADR-0254. Other clauses remain accepted.
