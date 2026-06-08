## Telnet Input Guardrails

Telnet ingress now enforces strict limits to prevent memory abuse and control characters from ever reaching the command processor. Two YAML-backed knobs expose these guardrails so operators can tune them per environment:

- `telnet.login_line_limit` &mdash; defaults to `32`. This caps how many bytes a connecting client may type before the login prompt rejects the session. Keep this low so a single unauthenticated socket cannot allocate huge buffers.
- `telnet.command_line_limit` &mdash; defaults to `128`. All post-login commands (PASS/REJECT/SHOW FILTER, HELP, etc.) must fit within this byte budget. Raise the value if you run bulk filter automation that sends long comma-delimited lists.

Both limits apply before parsing; the server drops the connection and logs the offending callsign when the guardrail triggers. Commands continue to support comma-separated filter inputs, so scripted clients do not need to swap to space delimiters when the limit is increased.

## Telnet Session Timeouts

These knobs govern how long the server waits for input before taking action:

- `telnet.max_prelogin_sessions` &mdash; defaults to `256`. Hard cap on unauthenticated sessions to bound socket usage during floods.
- `telnet.prelogin_timeout_seconds` &mdash; defaults to `15`. Total accept-to-callsign budget for unauthenticated sessions.
- `telnet.accept_rate_per_ip` / `telnet.accept_burst_per_ip` &mdash; defaults to `3` and `6`. Per-IP pre-login admission limiter (Go `x/time/rate` token bucket).
- `telnet.accept_rate_per_subnet` / `telnet.accept_burst_per_subnet` &mdash; defaults to `24` and `48`. Per-subnet pre-login limiter (`/24` IPv4, `/48` IPv6).
- `telnet.accept_rate_global` / `telnet.accept_burst_global` &mdash; defaults to `300` and `600`. Cluster-wide pre-login limiter.
- `telnet.accept_rate_per_asn` / `telnet.accept_burst_per_asn` &mdash; defaults to `40` and `80`. Per-ASN pre-login limiter using IPinfo metadata.
- `telnet.accept_rate_per_country` / `telnet.accept_burst_per_country` &mdash; defaults to `120` and `240`. Per-country pre-login limiter using IPinfo metadata.
- `telnet.prelogin_concurrency_per_ip` &mdash; defaults to `3`. Simultaneous unauthenticated session cap per source IP.
- `telnet.admission_log_interval_seconds` &mdash; defaults to `10`. Aggregation window for rejection summary logs.
- `telnet.admission_log_sample_rate` &mdash; defaults to `0.05` (5%). Sample rate for per-event reject logs; clamped to `[0,1]`.
- `telnet.admission_log_max_reason_lines_per_interval` &mdash; defaults to `20`. Per-interval cap for sampled reject log lines.
- `telnet.reject_workers` / `telnet.reject_queue_size` &mdash; defaults to `2` and `1024`. Moves reject-banner I/O off the accept loop using a bounded worker queue.
- `telnet.reject_write_deadline_ms` &mdash; defaults to `500`. Reject-banner write deadline before forced close.
- `telnet.writer_batch_max_bytes` / `telnet.writer_batch_wait_ms` &mdash; defaults to `16384` and `5`. Per-connection writer micro-batching cap and max wait.
- `telnet.read_idle_timeout_seconds` &mdash; defaults to `86400` (24 hours). The server refreshes a read deadline for logged-in sessions; timeouts do **not** disconnect clients and simply continue waiting for input.
- `telnet.login_timeout_seconds` &mdash; legacy fallback knob (default `120`). Tier-A admission uses `prelogin_timeout_seconds`.

## PSKReporter MQTT Debug Logging

Set `DXC_PSKR_MQTT_DEBUG=true` to enable verbose Paho MQTT debug logs for the PSKReporter client. Logs include DEBUG/WARN/ERROR/CRITICAL lines and should be used only while diagnosing reconnects or payload handling issues.

## Codex Skills

This repo vendors gocluster's project skills under `codex-skills/`. They are
part of the checkout and should be used as the project authority on every
machine.

- Verify the repo skill bundle:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify-codex-skills.ps1
```

Credentials and connector/plugin setup remain machine-local; do not commit
tokens, auth files, or personal plugin state.
