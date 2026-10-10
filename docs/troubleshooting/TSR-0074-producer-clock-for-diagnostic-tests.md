# TSR-0074 - Producer Clock for Diagnostic Tests

Status: Monitoring
Date Opened: 2026-10-10
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: peer diagnostic timing tests
Trigger Source: Chat request
Led To ADR(s): none
Tags: tests, diagnostics, timestamp, asynchronous, retry

## RCA Summary
- What happened: complete command-fix baselines intermittently measured a 100 ms
  retry below the unchanged test's 99 ms lower bound.
- Why: the test stamped asynchronously delivered callbacks, incorporating
  mailbox polling delay instead of the event's recorded emission time.
- What fixed it: preserve the producer UnixNano timestamp already in each
  mailbox record. Existing untimed test consumers discard the extra value.
- How we know: a held first callback delays a second already emitted event;
  its timestamp must remain inside the literal Emit interval, before delivery
  resumes. Original retry bounds remain unchanged. Qualification is retained
  in the checkpoint 00 validation follow-up execution entry.
- Operator/support answer: this repairs developer evidence; it changes no
  production retry timer or operator setting.

## Triggering Request
- Request date: 2026-10-10.
- Request summary: unattended validation for approved v3 command fixes.
- Request reference: `docs/command-fix-scope-ledger-v3.md`, validation lane.

## Symptoms and Impact
- Callback scheduling could make a correct retry look too early.
- Passing repetitions alone could miss the wrong timing origin.

## Hypotheses and Tests
1. The production retry delay changed with a standalone command patch.
   - Evidence: command patches leave peer production code untouched; timing
     failures occurred at 98.9956 and 98.9038 ms in two baseline attempts.
   - Outcome: no change established; inspect the actual measurement path.
2. Observer time represents the producer event time.
   - Evidence: a polling goroutine delivers records after Emit; immutable
     UnixNano is already recorded before retry begins.
   - Outcome: rejected; delayed-delivery control distinguishes the clocks.

## Findings
- Root cause: test clock attribution at asynchronous delivery.
- No acceptance threshold was relaxed, production callback installed, or
  runtime timestamp/persistence contract changed.
- Producer timestamps here are UTC wall timestamps; the short local fixtures
  assume the host clock does not step during a measurement.

## Decision Linkage
- ADR created/updated: none; no durable architecture change.
- Contract changes: none; qualification measures the existing event origin.

## Verification and Monitoring
- Validation steps: checkpoint 00 validation follow-up in the execution record.
- Signals: original retry bounds, producer interval and delayed callback witness.
- Rollback triggers: timestamps follow delivery or untimed consumers regress.
- Limits: local timing controls do not certify external networking or production
  clock synchronization.

## References
- Issue(s): approved v3 unattended validation.
- PR(s): none.
- Commit(s): validation follow-up on `fix_commands`; see execution history.
- Related ADR(s): none.
- Related docs: `peer/diagnostic_observer_test.go`,
  `peer/outbound_backoff_tcp_test.go`, `internal/peerdiag/mailbox.go`.
