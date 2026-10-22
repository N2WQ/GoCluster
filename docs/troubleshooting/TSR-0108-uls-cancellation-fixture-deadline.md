---
title: "TSR-0108: ULS Cancellation Fixture Deadline"
description: Preserve progress-triggered cancellation checks without a hidden race-build throughput limit.
---

Status: Resolved
Date Opened: 2026-10-22
Date Resolved: 2026-10-22
Owner: GoCluster engineering
Technical Area: ULS cancellation test fixtures
Trigger Source: Chat request
Led To ADR(s): none
Tags: uls, cancellation, race, fixture, deadline, rollback

## RCA Summary

- What happened: race validation failed TestStateImportCancellationAfterSubstantialRows.
  The fixture expected context.Canceled but received context.DeadlineExceeded.
- Why: its independent ten-second deadline expired before the reader's deliberate
  cancellation boundary after 4096 imported records. Instrumented execution
  exceeded that budget; production cancellation behavior was not the failing oracle.
- What fixed it: progress still triggers cancellation, but the fixture inherits
  the outer test deadline as its hang watchdog. The related extraction fixture
  uses the same bounded context helper.
- How we know: the original failure reproduced in isolation at 10.79s. After
  repair, the unchanged 4096-row race workload reached the intended boundary
  at 13.62s and passed every original rollback and database-reuse assertion.
- Operator/support answer: no ULS import, production timeout, filter or model
  behavior changed. This removes an unintended throughput requirement from a
  cancellation correctness test; it does not accept deadline expiry as success.

## Triggering Request

- Request date: 2026-10-22.
- Request summary: the owner authorized an additional test-only deadline repair
  after the onboarding race lane and isolated reproduction exposed this blocker.
- Request reference: explicit repair-test-only selection during Approved v5
  validation closeout.

## Symptoms and Impact

The full normal lane passed. The race run later reached ULS and failed after
10.72s with "join EN: context deadline exceeded". Isolated race execution
reproduced the failure at 10.79s. The intended source-reader cancellation had
not won the race against the fixture's ten-second timer.

## Timeline

1. 2026-10-22 - Race validation stopped at the cancellation fixture.
2. 2026-10-22 - An isolated unchanged test reproduced deadline expiry.
3. 2026-10-22 - Owner authorized a bounded test-only repair.
4. 2026-10-22 - Reused the outer test deadline; retained every workload,
   cancellation-boundary, rollback, scratch-cleanup and connection-reuse check.
5. 2026-10-22 - Complete ULS normal and race suites each passed 184 cases with
   four reported skips. Two additional isolated race repetitions passed.

## Hypotheses and Tests

1. Boolean filtering changed ULS cancellation behavior.
   - Evidence: the untouched ULS fixture failed in an isolated package run.
   - Outcome: Rejected.
2. The import failed to honor deliberate cancellation.
   - Evidence: the repaired stimulus reached 4096 records, returned the required
     context.Canceled error, retained unread valid tail data and rolled back.
   - Outcome: Rejected by the corrected progress-based check.
3. The fixture imposed an unrelated performance limit.
   - Evidence: original execution stopped at ten seconds; successful race
     execution needed 13.62s to reach the same deliberate boundary.
   - Outcome: Supported.

## Findings

- Root cause: an independent short deadline preempted the test's actual oracle.
- Both progress-driven fixtures now require a bounded outer test deadline.
  They explicitly refuse unbounded invocation rather than silently dropping
  hang protection.
- No row count, input bytes, tail data or expected error was reduced or relaxed.
- Cancellation after 4096 complete EN records retains 407606 unread tail bytes;
  the last-good database, scratch cleanup, integrity and connection reuse
  remain asserted.

## Decision Linkage

- ADR created/updated: none.
- Decision delta summary: no durable runtime or operator policy changed.
- Contract/behavior changes: test-only watchdog ownership. Production deadlines
  and import cancellation are unchanged.

## Verification and Monitoring

- Before repair: isolated race test failed at 10.79s with DeadlineExceeded.
- Complete native ULS normal suite: 184 passed, four reported skips.
- Complete native ULS race suite: 184 passed, four reported skips.
- Additional race repetitions: two passed at 8.37s and 8.50s.
- Successful race package run: progress cancellation at 13.62s, all original
  state-preservation checks passed.
- Retain exact context.Canceled and reached-boundary assertions. Do not accept
  arbitrary errors or deadline expiry as equivalent cancellation.

## References

- [Progress-driven cancellation fixtures](../../uls/state_cancellation_coverage_test.go).
- Issues/PRs/commits: none created.
