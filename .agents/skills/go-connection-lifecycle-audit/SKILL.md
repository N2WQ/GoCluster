---
name: go-connection-lifecycle-audit
description: "Use when Go work involves long-lived inbound or outbound network connections, reconnect/retry/backoff behavior, keepalives, deadlines, silent-stall or zero-data modes, EOF/read-loop recovery, connection shutdown, source liveness, or operator-visible connection diagnostics. Compose with go-leak-detection when goroutines, sockets, timers, channels, queues, cancellation, or resource cleanup are touched."
---

# Go Connection Lifecycle Audit

## Overview

Use this skill to audit connection state machines in the Go cluster before
planning, changing, or reviewing reconnect and liveness behavior. It focuses on
whether a connection path keeps the data stream alive, recovers safely, shuts
down cleanly, and tells operators what happened.

## Workflow

1. Classify the connection role.
   - Inbound telnet client, outbound ingest source, peer connection, HTTP
     client/server, local replay/tool connection, or test-only transport.
   - Identify whether the path is process-lifetime, feature-lifetime,
     session-lifetime, request-lifetime, or one-shot.

2. Map ownership and state transitions.
   - Name the goroutine or component that owns dialing/listening, login,
     reading, writing, keepalive, retry, shutdown, and cleanup.
   - Identify the states that matter: disabled, starting, connecting,
     connected, authenticating, streaming, idle, failed, backing off,
     stopping, and stopped.
   - Compare sibling source types when relevant. A path that behaves
     differently from RBN, PSKReporter, telnet, or peer sources needs an
     explicit reason.

3. Audit failure and recovery modes.
   - Initial connect failure
   - Mid-stream EOF, timeout, reset, or parse/read error
   - Login/auth/banner failure
   - Upstream idle or silent-zero-data mode
   - Keepalive failure
   - Config-disabled or permanently invalid configuration
   - Shutdown while dialing, reading, writing, or backing off

4. Check retry and backoff bounds.
   - Reconnect loops must be bounded by context/shutdown and should cap retry
     frequency so a down upstream cannot create a connection storm.
   - Backoff reset, max interval, jitter, and log cadence should be explicit
     when they affect operators or overload behavior.
   - Keepalives on an existing socket are not a substitute for reconnect after
     a lost or never-established connection.

5. Check liveness and operator detectability.
   - Define what proves the source is healthy: connected socket, recent lines,
     accepted spots, heartbeat, explicit idle state, or feature-specific
     signal.
   - Operators should be able to distinguish disabled, waiting to reconnect,
     connected-but-idle, connected-and-streaming, and permanently failed states.
   - Logs/metrics should show connection attempt, failure reason, backoff,
     recovery, shutdown, and suppressed-noise decisions without flooding.

6. Check resource and lifecycle safety.
   - Verify sockets, response bodies, timers/tickers, channels, queues, and
     goroutines are closed or stopped on every failure and shutdown path.
   - Use `go-leak-detection` when cleanup, cancellation, timers, sockets,
     channels, queues, or goroutine ownership are materially touched.
   - Use `go-blast-radius-audit` when shared source interfaces, config,
     diagnostics, or support docs can be affected.

7. Define validation before code.
   - Prefer targeted lifecycle tests for initial dial failure, mid-stream drop,
     repeated reconnect churn, shutdown during backoff, keepalive failure, and
     sibling-source parity.
   - Use `go test -race ./...` when implementation touches goroutines,
     channels, timers, cancellation, lifecycle, shutdown, or long-lived
     connections.
   - Use runtime captures only when the claim requires long-running evidence;
     otherwise say the result is locally validated, not runtime-confirmed.

## Output Expectations

- State the owner, state machine, failure modes, retry/backoff contract,
  operator-visible diagnostics, tests, and remaining evidence gaps.
- Do not require a fixed report heading.
- Do not claim reconnect behavior is fixed unless initial failure, mid-stream
  drop, and shutdown/retry interactions were actually tested or explicitly
  scoped out.
- Do not treat an existing keepalive as proof that lost connections recover.
