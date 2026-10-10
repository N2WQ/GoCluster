# TSR-0072 - Sunspot Watcher Outcomes

Status: Monitoring
Date Opened: 2026-10-10
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: sunspot watcher, one-shot reporting and PowerShell wrapper
Trigger Source: Chat request
Led To ADR(s): none
Tags: watcher, CLI, exit-status, output, persistence, retry

## RCA Summary
- What happened: a failed polling operation logged an error but one-shot mode
  returned success. Report write errors were ignored.
- Why: run returned nil when once was true, independently of poll's result;
  report helpers had no error return.
- What fixed it: return one-shot errors, retain continuous retries, report output
  errors and join them with the existing independent state-write attempt.
- How we know: actual-main loopback fixtures verify success, HTTP/read/parse and
  late-state-write failures, closed stdout and continued retries. Consumer
  controls observe header/sample/unchanged failures and simultaneous output/state
  errors. Native PowerShell -File propagation has a falsifying no-exit control.
  Qualification results are in the checkpoint 23 execution entry.
- Operator/support answer: one-shot failure exits nonzero; continuous polling
  logs errors and retries. Successful sample output can coexist with a state-save
  failure, so inspect both stderr and command status.

## Triggering Request
- Request date: 2026-10-10
- Request summary: implement approved v3 checkpoint 23.
- Request reference: `docs/command-fix-scope-ledger-v3.md`.

## Symptoms and Impact
- Automation can mistake HTTP, parsing or state-save failures for success.
- Lost stdout can silently hide observations while state persistence proceeds.
- This tool reports local NOAA-derived Wolf estimates/EWMA; it does not run
  forecasts or establish accuracy/calibration.

## Hypotheses and Tests
1. Logging a one-shot error supplies enough failure feedback.
   - Evidence: old run always returned nil after one poll; actual-main fixtures
     now require status 1 with request and causal-error witnesses.
   - Outcome: rejected; automation needs unsuccessful status.
2. Report failure should prevent the existing state attempt.
   - Evidence: prior report helpers ignored errors and state writes followed;
     preserving those independent effects requires attempting both and joining
     failures. Header failure also preserves the original poll/state attempt.
   - Outcome: preserve effects, repair truthful outcomes.
3. PowerShell -Command proves the wrapper's exact failure status.
   - Evidence: the initial status-7 probe surfaced host status 1. Native -File
     preserves the real Go preflight failure; a copied wrapper omitting explicit
     exit returns zero under the same fixture.
   - Outcome: use -Command for the argument-array probe and -File for status.

## Findings
- Root cause: unconditional one-shot success and unchecked report output.
- Existing request context/body limits, conditional headers, state format,
  calculation, durations and continuous cadence remain unchanged.
- Fixture execution bounds are checked separately from child exit status;
  Windows forced termination can itself use status 1 and must not become green.
- Child HTTP transport rejects non-loopback destinations before DNS/dial;
  wrapper probes prevent compilation/NOAA fallback with child-only invalid
  GOFLAGS and local toolchain mode.

## Decision Linkage
- ADR created/updated: none; accepted v3 repairs tool outcomes without changing
  scientific or runtime architecture. Existing SSN decisions remain intact.
- Contract changes: fetch/report/state failure is visible to one-shot automation.
  Existing partial effects and continuous retries are preserved.

## Verification and Monitoring
- Validation steps: see checkpoint 23 in `docs/command-fix-execution.md`.
- Signals: command status, causal stderr, valid observation/state continuity,
  continuous retry cadence and native wrapper status.
- Rollback triggers: polling failure exits zero, output failure skips existing
  state effects, continuous mode terminates on ordinary poll failure, or a changed
  SSN calculation/state format appears.
- Limits: deliberate continuous-test termination is not graceful production
  shutdown evidence; fixtures do not establish live NOAA or model health.

## References
- Issue(s): approved v3 checkpoint 23.
- PR(s): none.
- Commit(s): checkpoint 23 on `fix_commands`; see execution history.
- Related ADR(s): [ADR-0157](../decisions/ADR-0157-voacap-ssn-moving-average-experiment.md),
  [ADR-0182](../decisions/ADR-0182-voacap-runtime-ssn-state-persistence.md).
- Related docs: `scripts/README.md`, `scripts/watch-voacap-ssn.ps1`.
