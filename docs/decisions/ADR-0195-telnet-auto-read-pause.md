# ADR-0195: Telnet Auto Read Pause

- Status: Accepted
- Date: 2026-06-18
- Decision Origin: Design

## Context

Telnet users can run commands that intentionally return many rows, including
`SHOW PROP`, `WHOSPOTSME`, `HELP`, and archive-backed `SHOW DX` or
`SHOW MYDX`. Live spot traffic can immediately scroll that response away before
the user has time to read it.

The cluster must improve readability without adding unbounded per-client state,
blocking the fan-out path, replaying stale live traffic, or changing control
traffic behavior.

## Decision

Add an automatic per-client read pause after long command responses.

- The trigger is rendered output rows, not bytes.
- The shipped runtime config uses `telnet.auto_read_pause_min_rows: 10` and
  `telnet.auto_read_pause_seconds: 30`.
- Setting either value to `0` disables the feature.
- Blank separator rows count because they scroll the user's terminal.
- The final trailing newline does not add an extra row.
- Only live spot lines are suppressed.
- Suppressed live spots are not buffered or replayed.
- Command replies, errors, bulletins, announcements, direct talks, keepalives,
  and close messages remain on the existing control path.
- `SHOW HOLD` reports remaining pause time and suppressed spot count.
- `RESUME` ends the pause immediately and discards stale queued spot envelopes
  so old spots do not burst after resume.

Implementation keeps read-pause state on `Client` as atomic deadlines and
counters. There is no per-client timer or pause goroutine. Spot suppression is
checked when a spot would otherwise be enqueued for a client and again in the
writer loop for spot envelopes already queued before the pause began.

## Alternatives considered

1. Use byte count as the trigger.
   - Rejected because the user-visible problem is terminal rows, and commands
     such as `SHOW PROP` and `WHOSPOTSME` are naturally row-oriented.
2. Implement pager-style `MORE` prompts.
   - Rejected for this slice because it changes command interaction semantics
     and can leave long-lived sessions waiting on a prompt.
3. Buffer and replay missed spots after the pause.
   - Rejected because it creates per-client retained state and can recreate the
     scroll flood immediately after the reading window.

## Consequences

### Benefits

- Long command output is easier to read in ordinary telnet clients.
- The feature is generic across command responses instead of command-specific.
- Operators can tune or disable the feature through YAML.
- Read-pause suppression is separate from slow-client drop accounting.

### Risks

- A user can miss live spots during the pause by design.
- `SHOW HOLD` counts spots that passed existing per-user delivery checks and
  were suppressed; it is not a cluster-wide count.
- If an operator sets the threshold too low, short informational commands may
  pause live spots more often than desired.

### Operational impact

- Default behavior pauses live spot lines for 30 seconds after command output
  reaches 10 rendered rows.
- Operators can disable the feature by setting either read-pause YAML value to
  `0`.
- Support workflows should check `SHOW HOLD` before diagnosing missing spots as
  ingest, filter, or dedupe problems.

## Links

- Related issues/PRs/commits: -
- Related tests: `telnet/read_pause_test.go`, `telnet/server_options_test.go`,
  `config/telnet_auto_read_pause_test.go`, `config/config_dir_load_test.go`,
  `commands/processor_test.go`, `commands/readme_sync_test.go`
- Related docs: `README.md`, `telnet/README.md`, `commands/README.md`,
  `docs/OPERATOR_GUIDE.md`, `data/config/README.md`,
  `customgpt/source-map.md`, `customgpt/troubleshooting-index.md`
- Related TSRs: -
- Supersedes / superseded by: -
