---
title: "TSR-0106: Log Rotation Test Retention"
description: Archive assertions raced legitimate one-day cleanup in two logging fixtures.
---

Status: Resolved
Date Opened: 2026-10-22
Date Resolved: 2026-10-22
Owner: GoCluster engineering
Technical Area: internal/cluster logging tests
Trigger Source: Chat request
Led To ADR(s): none
Tags: logging, tests, retention, asynchronous-cleanup

## RCA Summary

- What happened: archive tests intermittently could not read the previous day's
  archive, preventing a clean broad validation result.
- Why: the fixtures configured one-day retention but expected yesterday's
  archive to remain. Real asynchronous cleanup legitimately deleted that file.
- What fixed it: retain two days in those archive-preservation fixtures and
  observe completion of real cleanup before asserting contents.
- How we know: waiting for cleanup with the original one-day setting made both
  tests fail deterministically; two-day fixtures passed the focused race suite
  and 50 repetitions of the archive and retention checks.
- Operator/support answer: this was a contradictory test setup, not evidence
  that production retention incorrectly removed logs. Production logging code
  and configuration were not changed.

## Triggering Request

- Request date: 2026-10-22
- Request summary: resolve the outstanding logging-test failure and complete
  validation with a clean result.
- Request reference: Boolean-filter qualification and upgrade conversation.

## Symptoms and Impact

TestDailyFileSinkArchivesStaleActiveOnStartup and the archive-collision test
expected a January 22 archive after writing on January 23. Both used one-day
retention. Depending on cleanup scheduling, the assertion could find the file,
find no file, or encounter concurrent filesystem activity.

The observed failure was a missing archive, not a Go data-race report. Test
success before cleanup completed was a false green.

## Timeline

1. 2026-10-21 - Broad Boolean validation exposed missing-archive failures.
2. 2026-10-22 - Initial isolated reproduction passed, confirming that one retry
   cannot establish the underlying issue is fixed.
3. 2026-10-22 - Test-only observation of the existing cleanup callback made both
   one-day archive assertions fail after actual cleanup.
4. 2026-10-22 - Two-day fixtures with cleanup completion checks passed focused
   race validation and 50 repeated runs.
5. 2026-10-22 - Complete normal and race repository lanes both passed, followed
   by clean vet, Staticcheck and golangci-lint results.

## Hypotheses and Tests

1. Startup wall-clock October cleanup conflicts with simulated January dates.
   - Evidence: construction uses the wall clock, but startup cleanup completes
     synchronously before the new archive exists.
   - Outcome: Inconclusive as a general mixed-clock concern; not the confirmed
     cause of these failures.
2. Rotation's one-day cleanup legitimately removes the asserted archive.
   - Evidence: January 23 with retention one yields a January 23 cutoff;
     January 22 is expired. Waiting for cleanup makes both tests fail.
   - Outcome: Supported.
3. Disable retention to make rotation tests pass.
   - Evidence: unnecessary; the same real cleanup succeeds with a retention
     window that includes the archive being asserted.
   - Outcome: Rejected.

## Findings

- Root cause: fixture retention contradicted archive-preservation assertions.
- Contributing factor: assertions did not wait for asynchronous cleanup,
  allowing a scheduling-dependent pass.
- The correction reuses the existing operation-local cleanup seam. It adds no
  production hooks, clocks, caches or background workers.
- Existing retention-deletion coverage remains in TestCleanupOldLogs; archive
  merge/startup tests still verify exact preserved and active contents.

## Decision Linkage

- ADR created/updated: none.
- Decision delta summary: no durable runtime decision changed.
- Contract/behavior changes: none; preserves
  [ADR-0152](../decisions/ADR-0152-stable-active-log-filenames-with-date-only-archives.md).

## Verification and Monitoring

- Deterministic before-fix check: both archive tests failed under race after
  real one-day cleanup completed.
- Focused after-fix check: ten logging tests passed under race, zero skips.
- Repetition: two archive tests plus retention cleanup passed 50 runs each.
- Complete normal and race repository lanes each passed 11,014 cases across
  71 packages (55 executed and 16 build-only), with 99 opt-in/helper skips
  reported separately per lane. No failed tests or race reports were observed.
- `go vet ./...`, `staticcheck ./...` and
  `golangci-lint run ./... --config=.golangci.yaml` all exited successfully;
  lint reported zero issues.
- Keep completion checks when modifying archive tests; do not replace real
  cleanup with a no-op to hide expired-file assertions.

## References

- Tests: [logging tests](../../internal/cluster/logging_test.go).
- Runtime unchanged: [logging](../../internal/cluster/logging.go).
- Related decision: [ADR-0152](../decisions/ADR-0152-stable-active-log-filenames-with-date-only-archives.md).
- Issues/PRs/commits: none created for this repair.
