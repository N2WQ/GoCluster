# ADR-0328: Per-Session Telnet Command Pacing

- Status: Accepted
- Date: 2026-10-28
- Decision Origin: Design

## Context

The global, station and IP session ceilings bound registered connections, but
an admitted client can still send commands rapidly. Commands can produce replies
and execute substantial work. The goal here is to pace each session without
changing login admission or the existing machine-upload and input-recovery
contracts. The selected startup and contest settings are operator choices; no
production workload measurement establishes that they fit every client.

## Decision

Require `telnet.command_rate_per_second` and `telnet.command_burst` in runtime
YAML. The shipped rate is 5 attempts per second and the burst is 30. Rate must
be an integer from 0 to 1,000,000,000; burst must be an integer from 1 to
1,000,000,000, including when the rate is zero. Rate zero disables pacing.

Give each logged-in connection an initially full, bounded allowance. A completed
line consumes one allowance, including a blank keepalive, unknown command, or
machine request header. An invalid or overlong human line recovered at a safe
boundary also consumes one. Login input, Telnet negotiation, idle timeouts and
the lines of a framed machine YAML body do not consume separate allowances.

Once the burst is spent, wait for the next allowance before processing the
attempt, instead of rejecting it or disconnecting for its rate. Preserve the
order of all attempts. Read no more than one attempt ahead of pacing, beyond
existing bounded input buffers. A wait is cancelled by shutdown or session
replacement. On replacement the reader leaves its pending attempt unprocessed
and waits for the existing duplicate-notice writer to close, or for shutdown.
A command already past its final check can still race with replacement. A peer
disconnect can remain unnoticed until the next read or a
writer failure. At the lowest enabled rate of one per second, the ideal wait for
one next allowance is at most one second, subject to scheduling and other work.

Preserve immediate terminal handling for unsafe reader-level failures. A
complete invalid machine header is paced before its existing dispatch and
terminal handling. Continue to honor bounded human line recovery and the
separate machine framing and upload limits; payload tails must not become
ordinary commands. Echo, validation, recovery and existing rejected-input logs
can happen before pacing. This decision is not a byte-rate limiter.

Count each attempt entering a pacing wait once, including a wait later cancelled.
Expose the cumulative `Command pacing waits` count in Telnet status; it resets
when the server restarts. It does not count unique users, rejected attempts,
wait duration or abuse. Do not emit a log per paced attempt.

## Alternatives considered

1. Reject or disconnect on excess: this can disrupt clients that send a normal
   startup sequence in a burst.
2. Delay before reading any input: idle sessions or Telnet negotiation could
   consume allowances without a command attempt.
3. Pace within byte reception: this changes the established editing, framing,
   echo and recovery ownership and does not fit this narrow command policy.
4. Share an allowance across sessions or callsigns: this adds aggregation and
   identity policy beyond the per-session change.

## Consequences

Pacing bounds each session's command processing rate while retaining ordered
delivery. A long queued startup, keepalive or logout can be delayed. A machine
upload counts as one request regardless of its existing bounded body size.
Slow input, byte floods, echo and pre-dispatch logging remain separate concerns.
Multiple sessions can send commands concurrently, and a reconnect starts a new
burst; this is not aggregate flood control.

## Operational impact

Add both keys to the active private `runtime.yaml` and deploy the updated
executable together. Missing or invalid values prevent startup; an older
executable does not enforce this setting. Operators can adjust both values or
set the rate to zero. The shipped values are not calibrated to observed client
startup or contest traffic. Monitor the wait count and investigate reported
delays; the count alone cannot identify their cause or impact.

## Links

- [Telnet command pacing](../../telnet/README.md#command-pacing),
  [runtime config](../../data/config/README.md),
  [domain contract](../domain-contract.md#telnet-command-pacing).
- Related decisions: [ADR-0266](ADR-0266-bounded-human-line-rejection.md),
  [ADR-0325](ADR-0325-strict-telnet-session-ceiling.md),
  [ADR-0326](ADR-0326-per-base-callsign-session-ceiling.md),
  [ADR-0327](ADR-0327-per-ip-telnet-session-ceiling.md).
- Supersedes / superseded by: none.
