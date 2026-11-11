# TSR-0120 - Flood Cleanup Live History

Status: Monitoring
Date Opened: 2026-11-10
Date Resolved: n/a
Owner: Cluster maintainers
Technical Area: shared ingest flood control
Trigger Source: Chat request
Led To ADR(s): none
Tags: flood, sliding window, cleanup, thresholds

## RCA Summary
- What happened: Periodic cleanup forgot still-valid actor events and could
  undercount later floods inside the configured sliding window.
- Why: Cleanup retained at most one timestamp; observation requires up to the
  source partition's threshold plus one to distinguish over-threshold traffic.
- What fixed it: Cleanup now uses that same partition threshold while expiring
  timestamps and removing empty actors/partitions.
- How we know: Deterministic fixed-clock tests failed before the correction for
  all three actions, source-specific limits, the inclusive cutoff, and each DX
  mode. Ordinary/race floodcontrol suites passed after the correction.
- Operator/support answer: The configured windows and actions are unchanged.
  Observe-only defaults continue forwarding; configured suppress/drop actions
  now retain the intended evidence across cleanup ticks.

## Triggering Request
- Request date: 2026-11-10.
- Request summary: Repair the externally reported cleanup accounting defect.
- Request reference: Owner's exact `Approved v1` overnight scope ledger.

## Symptoms and Impact
- Three events at threshold three became one remembered event after cleanup;
  the fourth event could be forwarded instead of classified over threshold.
- Repeated cleanup could repeatedly erase live evidence. Threshold-one fixtures
  could not reveal the defect.
- State still uses the existing per-source actor caps and timestamp cap.

## Timeline
1. 2026-11-10 - Current source and ADR-0062 were reconciled before approval.
2. 2026-11-10 - Fixed-clock owner regressions reproduced the reset.
3. 2026-11-10 - Source-specific cleanup retention passed ordinary/race checks.

## Hypotheses and Tests
1. Cleanup only expires events outside the window.
   - Evidence: Red tests retained one of three/five live source events.
   - Outcome: Rejected. The retained-count parameter also discarded live data.
2. One retained timestamp is enough because arrivals reapply the threshold.
   - Evidence: Fourth/fifth arrivals after repeated cleanup missed their expected
     observe/suppress/drop decisions.
   - Outcome: Rejected. Arrival handling cannot reconstruct erased events.
3. Correct cleanup requires changing thresholds or adding another data structure.
   - Evidence: The existing partition threshold and bounded window suffice;
     expiry and selected DX-mode tests pass with the original representation.
   - Outcome: Rejected.

## Findings
- Root cause: Cleanup used a different live-history cap than observation.
- Contributing factor: Existing action fixtures used threshold one.
- Durable decision: Existing sliding-window policy is restored; no ADR change.

## Decision Linkage
- ADR created/updated: none; preserves ADR-0062.
- Decision delta summary: No policy, action, partition, default or schema change.
- Contract/behavior changes: Valid live counting evidence survives cleanup.

## Verification and Monitoring
- Validation steps run: Full native floodcontrol ordinary and race suites each
  passed 17 cases, zero skips. Red/green evidence is in the overnight record.
- Signals to monitor: Flood decision counts, source partition actor counts,
  configured actions, window size and fail-open overflow events.
- Rollback triggers: Changed expiry cutoff, source/mode selection, actor caps,
  forwarding, action precedence or cleanup concurrency.

## References
- ADR: `docs/decisions/ADR-0062-shared-ingest-flood-control.md`.
- Execution: `docs/codebase-correctness-overnight-2026-11-10.md`.
- Source/tests: `floodcontrol/controller.go`, `floodcontrol/controller_test.go`.
