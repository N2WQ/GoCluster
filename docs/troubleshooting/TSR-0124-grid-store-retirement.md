# TSR-0124 - Grid Store Retirement

Status: Monitoring
Date Opened: 2026-11-11
Date Resolved: n/a
Owner: Cluster maintainers
Technical Area: grid writer, maintenance and recovery ownership
Trigger Source: Chat request
Led To ADR(s): ADR-0338
Tags: gridstore, shutdown, lookup, checkpoint, recovery, ownership

## RCA Summary
- What happened: Lookup producers could send to a closed update channel, retired
  hooks could still access cache, and background work could outlive store close.
- Why: Writer stop preceded lookup joins; background starters returned no
  completion handles, and recovery's warning child was not joined.
- What fixed it: Whole-hook terminal admission, producer-before-writer joins,
  grid-only background cancellation/completion and one runtime retirement owner.
- How we know: The old terminal lookup regression returned cached FN20 after
  stop. Controlled real worker/store tests now cover admitted hooks, late derived
  persistence, overflow, maintenance, both recovery publication branches and
  warning-child completion; ordinary/race evidence is in the overnight ledger.
- Operator/support answer: A slow synchronous database operation or callback
  remains owned until return. Grid cancellation is not a forced shutdown timer.

## Triggering Request
- Request date: 2026-11-10.
- Request summary: Execute reviewed correctness fixes unattended, validating each
  slice before the next under exact `Approved v1`.
- Request reference: `docs/codebase-correctness-overnight-2026-11-10.md`.

## Symptoms and Impact
- A timed-out synchronous caller does not cancel its queued lookup's derived send.
- Maintenance/recovery can overlap store closure without a join contract.
- Calling stop twice previously risks duplicate channel closure.

## Timeline
1. 2026-11-10 - Pre-approval owner and design analysis identified all grid users.
2. 2026-11-11 - A terminal-cache regression reproduced the old behavior.
3. 2026-11-11 - Real barrier/store tests and source review verified retirement.

## Hypotheses and Tests
1. Joining the writer proves lookup work completed.
   - Evidence: Blocked CTY workers can still create updates after caller timeout.
   - Outcome: Rejected. Reopened-store assertions require their final persistence.
2. Canceling recovery is enough to prevent all late publication/ownership.
   - Evidence: Store return and cache clearing are separate completion branches.
   - Outcome: Rejected. Cancellation plus complete owner join is required.
3. Restore completion proves the warning child finished.
   - Evidence: The warning operation can already be executing synchronously.
   - Outcome: Rejected. The child's own completion participates in recovery done.
4. A child context may be created before any grid-open result is classified.
   - Evidence: The full ordinary suite's existing startup-error test reaches
     initializeGridStore before a runtime context exists. The first repair
     panicked on its nil parent instead of logging the intended open failure.
   - Outcome: Rejected. Non-corruption open failure returns before acquiring
     grid background ownership; the child still exists before recovery launches.

## Findings
- Root cause: Incomplete component ownership at the downstream retirement boundary.
- Durable decision: ADR-0338 records whole-hook admission and complete grid joins;
  ADR-0275's global shutdown dependencies and no-detach rule remain authoritative.

## Decision Linkage
- ADR created/updated: ADR-0338.
- Decision delta summary: Complete grid component retirement and concrete Close.
- Contract/behavior changes: Terminal hooks miss without side effects; admitted
  lookup sends finish before final writer flush; close errors are now logged.

## Verification and Monitoring
- Validation steps run: Targeted ordinary/race lifecycle selections, real-store
  reopen assertions and matched local hook benchmarks; see execution ledger.
- Signals to inspect: Shutdown completion, disk/callback latency, grid close and
  maintenance/recovery errors. No production latency improvement is claimed.
- Rollback triggers: Lost final writes, send-after-close panic, post-retirement
  publication, duplicate close or abandoned storage owners.

## References
- Source: `internal/cluster/bootstrap.go`, `main_runtime.go`.
- Tests: `internal/cluster/grid_retirement_test.go`.
- Decision: `docs/decisions/ADR-0338-grid-store-retirement.md`.
- Execution: `docs/codebase-correctness-overnight-2026-11-10.md`.
