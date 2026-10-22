---
title: "TSR-0107: Short Wait Clock Domains"
description: Repair a cross-clock sleep assumption without changing clocks or weakening duration checks.
---

Status: Resolved
Date Opened: 2026-10-22
Date Resolved: 2026-10-22
Owner: GoCluster engineering
Technical Area: testsupport clock fixtures
Trigger Source: Chat request
Led To ADR(s): none
Tags: windows, qpc, sleep, race, timing, tests

## RCA Summary

- What happened: the full race lane stopped in unchanged TestClockShortWait.
  A nominal 2ms sleep measured 1.6305ms with the QPC test clock.
- Why: the fixture assumed Go's sleep clock and QPC define the same short
  interval. Installed Go 1.27.1 Windows runtime sleep uses interrupt time;
  the custom clock and testing.B.Elapsed use QPC. The nominal sleep is not a
  valid QPC lower-bound oracle on this host.
- What fixed it: use the existing fixed-benchmark child to extend the stimulus
  until testing.B.Elapsed independently confirms at least 2ms. Keep the
  original 2ms minimum and 5s maximum, and also bracket the custom interval
  with the standard high-resolution reference.
- How we know: the original failure reproduced in a clock-only race run.
  A separate diagnostic observed 1.6744ms QPC and 2.0628ms Go monotonic time
  over the same sleep. Corrected normal/race package tests and 100 repeated
  race clock checks passed. Deliberate 1000x unit errors are rejected.
- Operator/support answer: this was a test stimulus defect, not a Boolean
  assertion failure. No production clock, sleep implementation, benchmark
  protocol or performance qualification threshold changed.

## Triggering Request

- Request date: 2026-10-22.
- Request summary: finish Boolean onboarding validation; after the separate
  timing failure, the owner authorized investigation and a test-only repair
  without weakening checks.
- Request reference: Approved v5 onboarding execution and explicit
  investigate-test-fix selection.

## Symptoms and Impact

The complete normal lane passed 11,065 cases with 99 reported helper/opt-in
skips. The race lane then stopped in internal/testsupport. An isolated
50-repeat run of the original short-wait test reproduced one failure at
1.5488ms. This prevented a clean full race result; it was not waived or
relabelled as passed.

## Timeline

1. 2026-10-22 - The normal lane passed; race validation stopped at the 2ms check.
2. 2026-10-22 - Clock-only repetition reproduced the failure without filter code.
3. 2026-10-22 - Read installed Go timer sources and the existing clock ownership
   contract. A temporary 1000-sleep diagnostic observed one cross-domain
   discrepancy and was removed afterward.
4. 2026-10-22 - Retained a test-only high-resolution stimulus and strict interval
   oracle in the existing fixed-benchmark harness.
5. 2026-10-22 - Both complete testsupport lanes passed 34 cases each. The
   direct-clock wait and shared-domain checks each passed 50 further race runs.

## Hypotheses and Tests

1. Boolean source changes caused the timing failure.
   - Evidence: the unchanged test reproduced in an isolated testsupport
     process, outside filter/login execution.
   - Outcome: Rejected.
2. The custom nanosecond conversion is the established cause.
   - Evidence: the diagnostic's two clock domains disagreed about a nominal
     short sleep. Installed Go sources deliberately use a separate
     high-precision benchmarking timer on Windows.
   - Outcome: Inconclusive; do not change the shared clock based on this
     fixture failure.
3. A nominal Go sleep is an exact minimum-duration QPC stimulus.
   - Evidence: QPC 1.6744ms versus Go monotonic 2.0628ms in the diagnostic;
     installed runtime sleep uses a different time source.
   - Outcome: Rejected on this host.
4. The repair could hide wrong clock units.
   - Evidence: the original absolute limits remain; independent inner/outer
     reference bounds additionally constrain conversion. Injected 1000x smaller
     and larger results must emit the exact interval refusal and fail the child,
     with no valid receipt.
   - Outcome: Rejected by the retained negative checks.

## Findings

- Root cause: a cross-clock assumption in the test's wait stimulus.
- The shared-domain benchmark-clock fixture used the same nominal-sleep
  assumption and now uses the same independently timed wait.
- Existing subprocess ownership, deadlines and receipt checks are reused;
  no new runner, global timing flag or system timer setting is introduced.
- Standard-reference inner/outer bounds allow only one nanosecond of integer
  conversion rounding, not a widened sleep tolerance.

## Decision Linkage

- ADR created/updated: none.
- Decision delta summary: no durable runtime or qualification decision changed.
- Contract/behavior changes: test-only stimulus and stronger falsifiability.
  The supported timer ownership in
  [TSR-0097](TSR-0097-path-sharding-measurement-clock.md) remains unchanged.

## Verification and Monitoring

- Original reproduction: isolated race ShortWait, 1 failure in 50 repetitions.
- Diagnostic: 1 cross-domain short observation in 1000 nominal sleeps;
  diagnostic source removed after capture.
- Corrected native normal and race testsupport suites: 34 passed each, no skips.
- Corrected direct/shared-domain race repetition: 50 each, 100 passed.
- Monitor interval-refusal output for the measured custom/reference bounds.
  Do not remove lower bounds, discard short samples or clip measured durations.
- Full onboarding closeout separately records complete repository validation;
  these targeted checks alone do not establish that full lane.

## References

- [Direct clock and negative unit checks](../../internal/testsupport/clock_test.go).
- [High-resolution stimulus and domain checks](../../internal/testsupport/fixed_benchmark_clock_test.go).
- [Unchanged Windows clock](../../internal/testsupport/clock_windows.go).
- Issues/PRs/commits: none created.
