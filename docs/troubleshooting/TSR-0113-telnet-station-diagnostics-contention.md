# TSR-0113 - Telnet Station Diagnostics Contention

Status: Monitoring
Date Opened: 2026-10-28
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: Telnet station admission diagnostics and shutdown
Trigger Source: Chat request
Led To ADR(s): ADR-0326
Tags: telnet, session capacity, diagnostics, shutdown, locks

## RCA Summary

- What happened: Review of commit `302fbde` identified two conditional ways
  station-count diagnostics can delay Telnet cleanup. No production incident
  was established.
- Why: Both the admission snapshot and sampled station-rejection logging read
  the tracked-station map under the client registry lock. Shutdown holds that
  lock while invoking a connection reporter, and rejection logging precedes
  the timer for sending and closing a rejected connection.
- What addresses it: The approved correction publishes the map's cardinality
  through an atomic diagnostic gauge while updating the map under its existing
  lock. Snapshot and sampled-log reads no longer take the registry lock for
  this value. The registry map remains the authority for admission.
- How we know: Source inspection established both lock interactions. The
  standard connection reporter does not call the admission snapshot; the
  shutdown deadlock requires a reporter that does. Controlled local tests
  exercised both interleavings in the ordinary and race lanes.
- Operator/support answer: Check the executable revision and capture the
  shutdown or rejection context before attributing a delay to these paths.
  The correction does not make arbitrary reporters or log sinks nonblocking.

## Triggering Request

- Request date: 2026-10-28.
- Request summary: Review station ceiling diagnostics and correct their
  interaction with shutdown reporting and sampled rejection cleanup.
- Request reference: Review of commit `302fbde1022ae38df0172e4bd941cbb447dc7099`.

## Symptoms and Impact

A custom reporter that reads the admission snapshot during a shutdown
disconnect can wait for the registry lock held by that same shutdown call.
Separately, a station-limit rejection selected for sampled logging can wait
for the registry lock before its notice-and-close timer starts. An extended
registry hold can therefore retain that connection and its pre-login ticket.
The review did not establish a session-ceiling breach or a profile write
caused by either diagnostic interaction.

## Timeline

1. 2026-10-28 - Commit `302fbde` added station-count diagnostics with the
   per-base-callsign session ceiling.
2. 2026-10-28 - Review identified the shutdown and sampled-rejection lock
   interactions; current-source inspection confirmed their ordering.
3. 2026-10-28 - The owner approved a diagnostic-only correction and controlled
   regression checks.

## Hypotheses and Tests

1. A shutdown reporter can reenter the admission snapshot while shutdown owns
   the client registry lock.
   - Evidence: Shutdown closes clients under that lock; client closure calls
     the connection reporter synchronously, and the snapshot took the lock.
   - Outcome: Supported by source inspection for a reporter that reads the
     snapshot. The standard reporter does not make that call.
2. A sampled station rejection can wait on the registry lock before the
   rejection timer starts.
   - Evidence: The rejection handler calls the sampled logger before creating
     its timer, and the logger took the registry lock to count tracked bases.
   - Outcome: Supported by source inspection for a sampled rejection when
     the registry is held at that point.

## Findings

- Root cause: Diagnostic reads reused the client registry lock in lifecycle
  paths where waiting for that lock can prevent or delay cleanup.
- Contributing factor: Prior tests exercised sampling, nonreading rejected
  peers, and shutdown separately, without forcing these lock interleavings.
- Durable decision: ADR-0326 retains the station quota and observability
  contract; the correction publishes a separate observational count and does
  not change session admission or reporter ownership.

## Decision Linkage

- ADR updated: [ADR-0326](../decisions/ADR-0326-per-base-callsign-session-ceiling.md).
- Decision delta: Publish tracked-station cardinality without acquiring the
  registry lock on the snapshot and sampled rejection paths.
- Contract/behavior changes: Station and global admission continue to use
  registry-protected state. A multi-field diagnostic snapshot is not a single
  atomic transaction.

## Verification and Monitoring

- Validation: The targeted native Windows Telnet run selected the diagnostic,
  station-quota, and global session-ceiling tests: 63 cases passed, none
  skipped. Full native Windows ordinary and race lanes each ran 56 test
  packages and build-checked 16 more, with 11,429 passing cases and 93 skips.
  The diagnostic tests passed in both lanes; skips included opt-in, external,
  subprocess-helper, and platform-dependent cases. Vet, Staticcheck, and
  golangci-lint passed.
- Signals to monitor: Shutdown completion, station rejection counts and
  sampled `tracked_session_bases`, pre-login active capacity, and log or
  storage errors.
- Boundaries: Initial registry acquisition, unrelated blocked reporters,
  pre-login synchronization, storage, and log sinks can still delay work.
  This correction does not establish an absolute end-to-end login or shutdown
  deadline or confirm behavior in a live deployment.

## References

- Reviewed commit: `302fbde1022ae38df0172e4bd941cbb447dc7099`.
- Related ADR: [ADR-0326](../decisions/ADR-0326-per-base-callsign-session-ceiling.md).
- Related code: [Telnet server](../../telnet/server.go),
  [station admission](../../telnet/configuration_session.go),
  [connection reporting](../../telnet/login_validation.go).
