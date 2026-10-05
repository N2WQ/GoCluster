# ADR-0243: Manual Telnet Spot Pause

- Status: Accepted
- Date: 2026-10-04
- Decision Origin: Design

## Context

Telnet users need to pause live spots deliberately, independently of long
command output. ADR-0195 already provides a bounded per-client pause, `SHOW HOLD`,
and `RESUME`. Reusing that mechanism avoids a second suppression path,
timer, replay buffer, or persistent preference.

Pause-control replies previously passed through the automatic row trigger. At
a threshold of one row, even a RESUME acknowledgement could start another pause.

## Decision

Refine ADR-0195's command and deadline behavior:

- Add `PAUSE [seconds]` in both command dialects. The default is 30 seconds;
  accepted explicit durations are whole seconds from 1 to 300. Malformed,
  overflowing, out-of-range, and extra arguments return usage without changing
  pause state.
- Manual PAUSE is available even when automatic pausing is disabled. Its
  default is independent of the configured automatic duration.
- A manual PAUSE replaces the deadline with the requested duration from now,
  including when that shortens the remaining pause. Automatic pauses use the
  later of the active deadline and their configured duration from now; their
  footer reports the effective remaining duration.
- Use one `Client.startReadPause` helper and the existing atomic deadlines and
  suppression counter. Deadline changes remain owned by the client's command
  goroutine; fan-out and the writer read them and count suppression atomically.
- Preserve the suppressed count while a pause remains active. Starting a new
  pause after expiry resets the count. RESUME retains its immediate clear,
  count reporting/reset, and stale-envelope cutoff behavior.
- SHOW HOLD describes manual or automatic pausing without tracking its cause.
- Send PAUSE, SHOW HOLD, RESUME, and PAUSE usage replies through the control
  path without the automatic row trigger.
- Suppress only live spots, with checks at enqueue and writer consumption.
  Suppressed or stale queued spots are discarded without replay and do not
  count as slow-client drops. Other control traffic continues normally.

No exported Go interface, runtime configuration, persistent state, resource
bound, queue, timer, or goroutine is added or changed.

## Alternatives considered

1. Keep separate manual and automatic pause state or timers.
   - Rejected because the existing deadline and cutoff satisfy both behaviors
     with constant per-client state and one helper.
2. Let every automatic response replace the deadline.
   - Rejected because a long response could shorten a user-selected pause.
3. Disable manual PAUSE with the automatic-pause settings.
   - Rejected because users must be able to request a pause independently.

## Consequences

### Benefits

- Users can pause and resume the live stream deliberately.
- A long response cannot shorten an active pause.
- Pause controls remain effective at every supported automatic row threshold.
- Delivery, counters, bounded state, and stale-spot discard reuse one mechanism.

### Risks

- Users miss live spots while paused by design.
- Output already prepared for a socket write can complete; the existing pause
  checks do not recall in-flight output.
- Suppression counts remain per-client delivery counts, not cluster-wide totals.

### Operational impact

- PAUSE, SHOW HOLD, and RESUME are available in both dialects.
- Setting either automatic-pause configuration value to zero disables only the
  automatic trigger. PAUSE still defaults to 30 seconds.
- Support should check SHOW HOLD and use RESUME before treating a deliberate
  pause as an ingest, filter, or dedupe failure.

## Links

- Related issues/PRs/commits: -
- Related tests: `telnet/read_pause_command_test.go`, `telnet/read_pause_test.go`,
  `telnet/writer_v15_test.go`, `commands/processor_test.go`,
  `commands/readme_sync_test.go`
- Related docs: `README.md`, `telnet/README.md`, `commands/README.md`,
  `docs/OPERATOR_GUIDE.md`, `data/config/README.md`, `customgpt/source-map.md`,
  `customgpt/troubleshooting-index.md`
- Related TSRs: -
- Supersedes / superseded by: Refines
  [ADR-0195](ADR-0195-telnet-auto-read-pause.md) for commands, deadline precedence,
  and pause-control replies; all other ADR-0195 decisions remain accepted.
