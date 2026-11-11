# ADR-0341: Stabilizer Live Output Retention and Acceptance

- Status: Accepted
- Date: 2026-11-11
- Decision Origin: Troubleshooting chat; exact Approved v2

The grid retention/provenance portion below is historical and superseded by
[ADR-0343](ADR-0343-corrected-grid-precedence.md). Confidence, budgets, age,
learning and the original counters retain this decision. Current grid behavior
requires completed replacement lookup and clears unknown location.

## Context

Fresh rechecks could downgrade a held P to S or ?, while output location and age
were not reconciled at delayed delivery. The owner selected upgrade-only ordinary
letters with current percentages, fresh holding/learning, dynamic reason budgets,
retained grids on same/missing replacement lookup and final-delivery age limits.
Source grids can describe the actual transmission; retaining them must not teach
the replacement cache unverified location or reported provenance.

## Decision

- Carry one finite scalar for the strongest ordinary confidence reached by the
  unchanged held identity (`? < S < P < V`) in the existing scheduler ownership.
  Keep immutable entry telemetry separate. No identity history/side map is added.
- Working confidence/percentage, correction/license checks, holding and accepted
  history use fresh evidence. Apply the retained letter only at terminal
  output/suppression, after accepted learning where applicable. C/B remain
  nominal correction outcomes. An accepted replacement owns its C percentage.
- Keep dynamic reason-based budgets and accumulated checks. Weakening evidence
  may extend a low-P hold to the longer current reason allowance; no reset.
- Refresh corrected classification independently of location. Change a nonempty
  DX grid only when the accepted replacement lookup supplies a different full
  normalized locator. Same/missing results preserve attached grid/provenance.
  Suppress all corrected DX fanout writebacks; lookup owns persistence. DE
  updates and normal empty-grid fill remain. Rebuild geographic caches without
  repeating or relocating earlier predictor observations.
- Delayed location uses existing cache-first asynchronous lookup only, without
  worker/storage-response waits or new workers/timers. Ordinary lock contention
  remains; this is not a hard elapsed-time bound.
- Use strict `>` age at core acceptance and final pipeline offers, including
  support/admission/location/writeback crossings. Pre-core expiry has no accepted
  learning/terminal matrix observation. Post-core expiry preserves once-only
  learning and terminal metrics, but offers no output or family witness.
- Expose/reset two fixed counters: held paths dropped for age, and terminal
  ordinary letters retained above fresh. The latter counts suppressions/retry
  overflow and survives later thinning/final age expiry. Neither counts retries;
  held-age excludes unheld/initial-overflow work. No conservation equation with
  legacy retry-inclusive Held is implied.

| Strongest reached / fresh event | Terminal behavior |
| --- | --- |
| P / fresh S or ? for same call | P, current percentage/availability |
| P / fresh B | B, original call's current percentage |
| P / accepted replacement | C, replacement's current percentage |
| S / fresh V | V; release using fresh evidence |
| Accepted correction / different full grid | Replacement lookup grid/provenance |
| Accepted correction / same or missing grid | Keep attached grid/provenance; no DX fanout writeback |
| Age equals limit | Eligible at that boundary |
| Age exceeds limit before acceptance | Drop, held-age counter if actually held; no learning/transition |
| Age exceeds limit after acceptance | Drop output; retain once-only learning/terminal metrics |

## Alternatives considered

1. Restore both letter and percentage: rejected; this would hide current support.
2. Project retention before delay/history: rejected; it could release prematurely
   or inflate V-only learning. Fresh evidence retains those responsibilities.
3. Fix the budget at admission: deferred as a separate product change.
4. Clear every corrected grid: rejected by the owner's source-grid rule.
5. Keep inherited-grid writeback on misses/same-grid: rejected; it can poison
   target location or promote derived provenance without new evidence.
6. Add a synchronous release lookup or new lookup worker: rejected; the existing
   cache-first asynchronous owner is sufficient and avoids worker-response stalls.

## Consequences

The ordinary confidence letter never decreases for an unchanged held call.
Filters and terminal statistics see that retained letter. The percentage can be
lower/unavailable, and the matrix no longer reveals ordinary evidence weakening;
the fixed retention counter restores that terminal signal. A P can wait through
the longer reason budget. Queueing/locks add elapsed time beyond interval times
checks. Retained grids still feed location filters/path consumers on lookup miss;
earlier path learning is unchanged. Post-core age drops can leave accepted
history/matrix counts without delivered output by design.

No YAML/schema, persisted Spot field, queue capacity, scheduler owner or worker
changes are required. Two atomics and a scalar in existing padding bound the new
state. Deploying an executable, repairing old DB contents, replay fidelity and
empirical RF/correction accuracy remain separate work.

## Links

- [TSR-0127](../troubleshooting/TSR-0127-stabilizer-live-output.md)
- [Live output contract](../../spot/README.md#live-stabilizer-output)
- [Metrics](../../ui/README.md#stabilizer-matrix), [execution evidence](../stabilizer-live-repair-v2.md)
- Refines [ADR-0311](ADR-0311-stabilizer-transition-matrix.md) terminal interpretation
  and [ADR-0148](ADR-0148-post-stabilizer-shared-secondary-fanout.md) final age admission.
- Source/tests: `internal/cluster/stabilizer_confidence.go`, `stabilizer.go`,
  `output_location.go`, `output_age.go`, `output_pipeline_delivery.go` and their
  targeted tests; `stats/tracker.go`, `ui/stabilizer_matrix_test.go`.
- Supersedes: none; earlier decisions retain their historical scope.
- Superseded in part by [ADR-0343](ADR-0343-corrected-grid-precedence.md): grid
  retention/provenance only, plus one additional missing-grid output counter.
