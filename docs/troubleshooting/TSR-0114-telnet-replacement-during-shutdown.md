# TSR-0114 - Telnet Replacement During Shutdown

Status: Monitoring
Date Opened: 2026-10-28
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: Telnet registration, reconnect and shutdown cleanup
Trigger Source: Chat request
Led To ADR(s): ADR-0325
Tags: telnet, shutdown, registration, reconnect, lifecycle

## RCA Summary

- What happened: Review of `319132a` found a shutdown race during reconnect.
- Why: Shutdown could be signaled after registration's early check while it
  waited on the previous owner's readback lock. The timeout worker could lose
  its scheduling race with the final ownership change.
- What fixed it: Shutdown signaling and the final ownership change share a short
  ordering gate. The existing session writer also observes shutdown when the
  old owner has already been displaced.
- How we know: Controlled Windows tests cover shutdown before the final
  decision, shutdown between the check and publication, and a replacement
  published before shutdown while notification callbacks are blocked. These
  are local tests; no production incident was observed.
- Operator/support answer: Deploy the updated executable. YAML values do not
  change. A blocked write or other synchronous work can still delay cleanup.

## Triggering Request

- Request date: 2026-10-28.
- Request summary: Preserve the old owner if shutdown starts before a waiting
  replacement's final registration decision, and clean up both connections.
- Request reference: Source review of commit `319132ae1dfc1e3596d7c7ad8bfbec151d3d1023`.

## Symptoms and Impact

A replacement could remove the previous owner from the registered-session map
after shutdown was signaled. Stop would find the replacement, while the old
connection could wait for a duplicate-login path delayed by a callback. If the
replacement won just before shutdown, the same callback could also delay old
connection cleanup. The review did not establish a production occurrence.

## Timeline

1. 2026-10-28 - `319132a` introduced the accept-to-registration budget.
2. 2026-10-28 - Source review identified the shutdown/replacement interleaving.
3. 2026-10-28 - The owner approved the ordering and cleanup follow-up.

## Hypotheses and Tests

1. A final shutdown check by itself orders shutdown and publication.
   - Evidence: Shutdown can be signaled after a check and before publication.
   - Outcome: Rejected; a controlled gate test exercises that interval.
2. Stop closes every displaced connection through the registered-session map.
   - Evidence: Replacement removes the old owner before notification callbacks.
   - Outcome: Rejected; controlled idle and blocked-write tests hold a callback
     across shutdown and observe the old connection's retirement.

## Findings

- Root cause: The shutdown signal and ownership publication were not ordered.
- Contributing factor: A displaced connection was no longer in Stop's registry
  and could depend on a callback before its duplicate notice was scheduled.
- Durable decision: ADR-0325 records shutdown ordering and displaced-session
  cleanup while preserving normal duplicate-notice behavior.

## Decision Linkage

- ADR updated: [ADR-0325](../decisions/ADR-0325-strict-telnet-session-ceiling.md).
- Decision delta: Order shutdown with publication and let the displaced session
  observe shutdown without waiting for notification callbacks.
- Contract/behavior changes: Shutdown winning the ordering prevents replacement;
  already-displaced writers can interrupt their sockets on shutdown.

## Verification and Monitoring

- Validation: Targeted native Windows ordinary and race tests each passed 49
  cases, including both transports, pacing on/off, blocked callbacks and a blocked
  socket write. Full-suite outcomes are reported with the change closeout.
- Signals to monitor: shutdown completion, retained Telnet sessions and
  socket/write errors; inspect blocked storage, locks and reporters separately.
- Rollback triggers: shutdown deadlock, lost normal duplicate notice, or a
  session left open independently of known synchronous-work delays.

## References

- Original commit: `319132ae1dfc1e3596d7c7ad8bfbec151d3d1023`.
- Related ADR: [ADR-0325](../decisions/ADR-0325-strict-telnet-session-ceiling.md).
- Code: [registration](../../telnet/configuration_session.go),
  [server and writer](../../telnet/server.go).
- Tests: [shutdown and replacement](../../telnet/shutdown_replacement_test.go),
  [pacing retirement](../../telnet/command_pacing_retirement_test.go).
