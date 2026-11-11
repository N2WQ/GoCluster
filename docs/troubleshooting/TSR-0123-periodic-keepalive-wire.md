# TSR-0123 - Periodic Keepalive Wire Delivery

Status: Monitoring
Date Opened: 2026-11-10
Date Resolved: n/a
Owner: Cluster maintainers
Technical Area: telnet periodic control output
Trigger Source: Chat request
Led To ADR(s): none
Tags: keepalive, CRLF, control queue, membership, pause

## RCA Summary
- What happened: Enabled server keepalives appeared to enqueue successfully but
  produced no heartbeat bytes on quiet client connections.
- Why: Periodic CRLF passed through generic whitespace-only message suppression;
  the writer has a second whitespace guard. The old loop also held a membership
  read lock while sending, which would make real queue-full callbacks unsafe.
- What fixed it: Periodic heartbeat output now owns exact raw CRLF bytes in the
  existing bounded control queue. Recipients are snapshotted under the lock and
  queued after unlocking. Generic whitespace and blank-command behavior are
  unchanged.
- How we know: Red owner/timer tests queued zero records on the original path.
  Native and ziutek sessions now deliver literal CRLF followed by a sentinel.
  Virtual-time checks exercise actual cadence and retirement; queue-full,
  callback-lock, writer, pause and failure checks passed ordinary/race selections.
- Operator/support answer: `telnet.keepalive_seconds` controls the periodic
  server CRLF; zero disables it. Heartbeats continue during live-spot pause.
  This application heartbeat is distinct from TCP keepalive and does not change
  the existing control-queue-full disconnect policy.

## Triggering Request
- Request date: 2026-11-10.
- Request summary: Repair the reviewed silent periodic heartbeat discard.
- Request reference: Exact `Approved v1` overnight scope ledger.

## Symptoms and Impact
- A quiet connection lacked the configured application heartbeat despite a
  successful generic send return.
- Simply accepting CRLF while retaining the membership lock could invoke a
  disconnect reporter under that lock when the control queue is full.
- Blank client command handling is a separate path and remains outside scope.

## Timeline
1. 2026-11-10 - Both whitespace guards and callback lock ordering were inspected.
2. 2026-11-10 - Actual timer/admission regressions reproduced zero heartbeat output.
3. 2026-11-10 - Periodic-only raw control passed transport and lifecycle selections.

## Hypotheses and Tests
1. Successful generic Send proves the heartbeat reached the writer.
   - Evidence: Original admission/timer tests observed zero queued messages.
   - Outcome: Rejected. Both admission and writer can suppress whitespace.
2. A reporter's nested read lock proves the emitter released membership.
   - Evidence: Recursive read locking can succeed without a pending writer.
   - Outcome: Rejected. A quiet fixture's write TryLock detects retained locks
     without stranding a deadlocked callback.
3. Seeing socket bytes proves post-flush pause effects have completed.
   - Evidence: The sink signal occurs inside Write, before writer completion.
   - Outcome: Rejected. State checks use virtual-time quiescence after byte
     observation and seed distinct preferences plus real readback authority.

## Findings
- Root cause: Periodic protocol bytes were treated as generic empty text.
- Resource ownership: One emitter-local client-pointer snapshot and one owned
  two-byte payload per admitted control message; no new cache or goroutine.
- Durable decision: Restores existing configured heartbeat/pause behavior;
  no new protocol policy or ADR is required.

## Decision Linkage
- ADR created/updated: none; preserves ADR-0195's control-path pause exception.
- Decision delta summary: Periodic keepalive only; generic whitespace unchanged.
- Contract/behavior changes: Configured server heartbeat bytes now reach the
  ordinary sole writer through existing admission and failure behavior.

## Verification and Monitoring
- Validation steps run: Targeted telnet ordinary/race selections each passed
  46 cases, zero skips. Review refinements are recorded in the overnight log.
- Signals to inspect: Actual quiet-connection CRLF bytes, configured interval,
  control queue pressure, disconnect reasons and sender failures.
- Limits: Arbitrary synchronous connection reporters are not forcibly
  interruptible; shutdown retains their owner until the callback returns.
- Rollback triggers: Missing/duplicate heartbeat bytes, changed generic blank
  behavior, altered pause/settings/counters, reporter lock retention or leaks.

## References
- Source/tests: `telnet/server.go`, `telnet/keepalive_test.go`.
- Operator contract: `telnet/README.md`, `data/config/runtime.yaml`.
- Decision: `docs/decisions/ADR-0195-telnet-auto-read-pause.md`.
- Execution: `docs/codebase-correctness-overnight-2026-11-10.md`.
