# TSR-0037 - Peer Session Cancellation Publication

Status: Resolved
Date Opened: 2026-10-03
Date Resolved: 2026-10-03
Owner: GoCluster maintainers
Technical Area: peer session startup, cancellation, shutdown
Trigger Source: Chat request
Led To ADR(s): none
Tags: peer, cancellation, lifecycle, race, ownership

## RCA Summary

- What happened: concurrent session startup and closure raced on the cancellation
  function. Closing before installation could leave Run waiting for its
  cancellation worker until the manager's parent context was canceled.
- Why: Run registered the candidate before assigning `s.cancel`; close read it
  without synchronization. `sync.Once` serialized closers but did not synchronize
  installation or remember that an earlier close needed to cancel a later operation.
- What fixed it: a session-local mutex and terminal flag synchronize installation
  with closure. An early close cancels a later-installed operation and refuses
  startup. The cancellation worker uses the same one-time socket closure.
- How we know: the baseline overlap reproducer reported a data race between
  `session.go:204` and `session_transport.go:453`, and failed the bounded
  cancellation/termination assertions. Patched focused and repository-wide race
  tests and the 50-repeat overlap run pass; execution results are recorded below.
- Operator/support answer: this is an internal lifecycle defect, not a release
  identity or configuration problem. There is no configuration workaround required
  by the patch. Local test evidence does not establish long-running production
  leak freedom or close unrelated lifecycle/monitoring findings.

## Triggering Request

- Request date: 2026-10-03.
- Request summary: surgically fix the session cancellation publication race and
  verify overlapping startup/shutdown under the race detector.
- Request reference: chat Scope Ledger v5, authorized by `Approved v5`.

## Symptoms and Impact

The isolated baseline at `f85d610e52311b94b925077627f11f351111852b` reproduced
unsynchronized cancellation publication under `-race`. Close-before-installation
could consume the closure Once without canceling the operation; Run's deferred
worker join then needed parent cancellation to finish. The socket cancellation
worker also bypassed the one-time socket close path.

## Hypotheses and Tests

1. Candidate registration exposes incomplete cancellation state: supported by
   the baseline-compatible `TestSessionCancellationPublicationRunCloseOverlap`.
   In an isolated detached baseline checkout, `go test -race ./peer -run
   '^TestSessionCancellationPublicationRunCloseOverlap$' -count=1 -timeout=45s`
   failed with the exact Run-write/close-read race and lost-cancellation failures.
2. Moving one assignment before registration is a complete fix: rejected by
   source inspection. Retry ownership can retain a session before Run, and
   terminal close-before-start must still be remembered.
3. Race-detector silence alone proves retirement: rejected. Tests separately
   assert operation cancellation, one socket close, bounded Run/Stop completion,
   and empty registries, context operations and transport reservations.

## Findings and Decision Linkage

Run owns one-time context installation, workers and operation retirement.
The cancellation mutex owns the installed function and terminal flag. No
cancellation, socket I/O, manager lock or worker join occurs while it is held.
Run installs an immutable context before workers and controller requests observe
it. Existing manager, retry, queue and context-pool ownership remains intact.

No new ADR: this restores the existing terminal-close and joined-retirement
contract rather than selecting a new shutdown or retry policy.

## Verification and Monitoring

The changed session layout is 2,808 bytes, rounded to the same 3,072-byte class
as the baseline's 2,792-byte layout. The existing source-derived inventory remains
5,632/8,192 bytes per owner and 3,312/4,096 active-only bytes. These are local
structural accounting checks, not a whole-process or runtime leak proof.

Focused checks:

```powershell
go test -race ./peer -run '^TestSessionCancellationPublication' -count=50 -timeout=10m
go test -race ./peer -run '^Test(SessionCancellationInterruptsReadAndJoinsWorkers|SessionOwnerReservation|SessionTerminalRunReleasesQueuedPayloads|PC92StopJoinsParkedReplayAndReleasesOwnership|ContextOwner|PC92MetadataOwnerLayout)' -count=1 -v -timeout=3m
```

Observed checks on the patched production code:

- Cancellation publication regressions repeated 50 times under `-race`: PASS,
  500.518 seconds, no race reports. This includes 400 Run/close cycles and 100
  forced manager-stop overlaps across inbound and outbound startup, plus the
  close-first/install-first/concurrent-installation cases and repeated closers.
- `go test ./...`: PASS; peer package 70.517 seconds.
- `go test -race ./... -timeout=15m`: PASS; peer package 93.523 seconds,
  with no race reports. The explicit external receiver is checked separately.
- `go vet ./...` and `staticcheck ./...`: PASS.
- Configured pinned DXSpider `PC18IdentityAndK` and `GoSessionStartup` tests:
  PASS, 35.326 seconds. Both empty/populated release tags and all four startup
  direction/capability cases executed.
- Full golangci-lint: FAIL, 42 findings. The clean isolated baseline has the
  same count and category distribution. Incremental lint with
  `--new-from-rev=f85d610e52311b94b925077627f11f351111852b`: PASS, zero issues.
  Existing lint findings remain outside this cancellation fix.
- Context-parent churn, parked replay shutdown, queued-payload retirement,
  cancellation/ownership and metadata-envelope checks: PASS under `-race`.

The ten-minute repetition timeout accommodates existing controller round trips;
each regression still asserts bounded completion. Monitor session retirement and shutdown completion;
any new race report, stranded worker or retained ownership requires investigation.
Reverting the production handoff restores the reproduced defect and is not a
safe routine rollback.

## References

- `peer/session.go`, `peer/session_transport.go`.
- `peer/session_lifecycle_test.go`, `peer/pc92_metadata_allocation_test.go`.
- [Peer lifecycle](../../peer/README.md).
- [Allocation accounting](../pc92-allocation-proof.md).
