# TSR-0127 - Stabilizer Confidence Downgrades and Late Output

Status: Monitoring
Date Opened: 2026-11-11
Date Resolved: n/a
Owner: Repository owner / Codex
Technical Area: live stabilizer, output location, acceptance and telemetry
Trigger Source: Operator report
Led To ADR(s): ADR-0341, ADR-0343
Tags: stabilizer, confidence, grid, age, bounded-state

## RCA Summary

- What happened: The operator's transition matrix showed held P spots ending S.
- Why: Each recheck overwrote confidence from fresh evidence; no ordinary-letter
  maximum survived the hold. Fresh evidence can weaken or strengthen as receiver
  rows expire or arrivals republish. Correction also retained the old grid through
  downstream writeback, and age was not enforced at delayed/final output.
- What fixed it: Separate bounded ordinary-letter retention from decision/history
  evidence, refresh corrected location without inherited DX writeback, and enforce
  pre-core/final-offer age checks with two explicit counters.
- How we know: Current-source tracing plus real resolver/scheduler, persistence,
  output-receipt, callback-crossing and race tests in the approved repair. Local
  evidence is recorded in [execution evidence](../stabilizer-live-repair-v2.md).
  Deployment and empirical accuracy are not established by these checks.
- Operator/support answer: A retained P means P was reached during that hold.
  DIAG CONF remains current and can be weaker/unavailable. Read `retained letter`
  and `age drops` alongside the matrix. Under the later v5 amendment, a corrected
  call requires a completed lookup: reported grids win, derived positives use
  current captured CTY, and incomplete lookups leave the DX grid unknown. Watch
  `Corrected output: no grid` for ordinary output admissions with that condition.

## Triggering Request

- Request date: 2026-11-11
- Request summary: Thoroughly audit P-to-S stabilizer transitions and enforce
  upgrade-only call confidence; exact Approved v2 authorized sequential repair.
- Request reference: Local owner conversation and attached transition screenshot.

## Symptoms and Impact

The matrix exposed real terminal downgrades for the same call. Fresh evidence
could also extend low-P waiting to the current unknown/ambiguity budget. A late
accepted call correction could leave stale classification/location caches and
persist the old call's reported grid under the new name. Held work admitted just
inside the age limit could be offered after it expired.

## Timeline

1. 2026-11-11 - Audit traced admission, scheduled retries, correction and output.
2. 2026-11-11 - Owner selected current percentage, final-delivery age, dynamic
   budgets, and same/missing-target grid retention; approved v2 repair.
3. 2026-11-11 - Sequential confidence, location and age slices validated locally;
   telemetry/docs and final-lane evidence tracked in the execution record.
4. 2026-11-11 - Owner clarified reported-versus-derived precedence and required
   completed lookup before CTY, clearing the old grid otherwise. Approved v5
   authorizes the bounded grid amendment and one missing-grid admission counter;
   see [v5 execution evidence](../stabilizer-grid-repair-v5.md).

## Hypotheses and Tests

1. The matrix invents the downgrade - Rejected. Entry telemetry is immutable and
   terminal glyph was read from the fresh Spot; rechecks really overwrote P.
2. Rechecks can only weaken - Rejected. Opposing evidence expiry can improve the
   surviving call's share, and new arrivals republish. Retain legitimate upgrades.
3. Average 12.0 proves timeout at exactly 60 seconds - Rejected. Rounding can hide
   a small minority of earlier completions; check twelve may clear rather than
   timeout, and elapsed time includes queueing/lock delays.
4. Lookup before acceptance is sufficient for age - Rejected. Store/admission
   contention and location/writeback callbacks can cross expiry. Deterministic
   clocks test these boundaries without changing source times or runtime config.

## Findings

- Root cause: conflating fresh evidence with the user-visible retained letter,
  and missing late identity/age boundaries.

Decision counters count evaluations,
not unique spots; repeated evaluations are not an inflation defect. Original
source grids exist for PSKReporter CW/RTTY. The initial v2 policy retained them
on missing/equal replacement lookups. The owner later selected corrected-call
lookup authority: an old identity's attached grid must not survive an incomplete
lookup. Stored reported grids beat CTY estimates, while stored derived positives
permit recomputation from the correction's CTY snapshot. Lookup provenance and
writeback ownership remain separate from attached output location.

Cold cache is not proof of store absence. Existing synchronous main lookup can
time out; delayed lookup never waits. An unsuccessful result does not authorize
direct CTY fallback. Real blocked-worker tests deliver unknown location before
unblocking, then verify late backfill cannot mutate sealed output. Equal locator
tests also inspect provenance; text alone would miss a reported/derived mistake.

## Decision Linkage

- ADR created: [ADR-0341](../decisions/ADR-0341-stabilizer-live-output.md).
- Grid/counter amendment: [ADR-0343](../decisions/ADR-0343-corrected-grid-precedence.md).
- Contract changes: retained output letter, corrected-grid ownership, age
  admission boundaries and fixed counters. Fresh algorithm/scoring inputs,
  dynamic reason budgets and completed-check counts remain unchanged.
- Earlier path learning, H3 initialization, replay fidelity, existing DB repair,
  deployment and RF accuracy qualification remain outside this repair.

## Verification and Monitoring

Targeted native ordinary/race evidence covers intermediate retention, timeout
release/suppression, retry overflow, B/C/V, current percentage, real source-grid
ingest and reopened grid provenance, no worker wait/repeated path learning,
strict age boundaries, accepted occurrence counts and final output/witness
exclusion. Review [execution evidence](../stabilizer-live-repair-v2.md) for exact
counts, commands and final-lane results, and [v5 evidence](../stabilizer-grid-repair-v5.md)
for corrected-grid precedence/clearing and once-only missing-grid output counts.
The counter excludes pre-admission drops and counts later queue refusal; do not
read it as client receipt or a count of storage errors. Monitor fixed counters and reason counts;
legacy Held includes retries and is not a unique-spot conservation measure.
Unexpected lower ordinary terminal transitions, stale final offers or target
grid corruption warrant stopping rollout and preserving exact revision/evidence.

## References

- [Live output contract](../../spot/README.md#live-stabilizer-output)
- [Matrix/counter guide](../../ui/README.md#stabilizer-matrix)
- Source: `internal/cluster/output_pipeline_delivery.go`, `output_location.go`,
  `output_age.go`, `stabilizer_confidence.go`, `stabilizer.go`, `stats/tracker.go`.
- Tests: `stabilizer_confidence_test.go`, `corrected_grid_delivery_test.go`,
  `output_age_test.go`, `pskreporter/client_test.go`, `ui/stabilizer_matrix_test.go`.
