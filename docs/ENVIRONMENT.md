## Telnet Input Guardrails

Telnet ingress now enforces strict limits to prevent memory abuse and control characters from ever reaching the command processor. Two YAML-backed knobs expose these guardrails so operators can tune them per environment:

- `telnet.login_line_limit` &mdash; defaults to `32`. This caps how many bytes a connecting client may type before the login prompt rejects the session. Keep this low so a single unauthenticated socket cannot allocate huge buffers.
- `telnet.command_line_limit` &mdash; defaults to `128`. All post-login commands (PASS/REJECT/SHOW FILTER, HELP, etc.) must fit within this byte budget. Raise the value if you run bulk filter automation that sends long comma-delimited lists.

Both limits apply before parsing; the server drops the connection and logs the offending callsign when the guardrail triggers. Commands continue to support comma-separated filter inputs, so scripted clients do not need to swap to space delimiters when the limit is increased.

## Telnet Session Timeouts

These knobs govern how long the server waits for input before taking action:

- `telnet.read_idle_timeout_seconds` &mdash; defaults to `86400` (24 hours). The server refreshes a read deadline for logged-in sessions; timeouts do **not** disconnect clients and simply continue waiting for input.
- `telnet.login_timeout_seconds` &mdash; defaults to `120`. Pre-login sessions that do not complete callsign entry within this window are disconnected.

## PSKReporter MQTT Debug Logging

Set `DXC_PSKR_MQTT_DEBUG=true` to enable verbose Paho MQTT debug logs for the PSKReporter client. Logs include DEBUG/WARN/ERROR/CRITICAL lines and should be used only while diagnosing reconnects or payload handling issues.

## Codex Skills Setup (Repo-Managed)

This repo vendors the highest-ROI troubleshooting skills under `codex-skills/` so they can be installed consistently on any machine that pulls the code.

- Install/update bundled skills (`gh-fix-ci`, `sentry`):

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\install-codex-skills.ps1
```

- Verify local skills match the repo copies:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify-codex-skills.ps1
```

Scripts install to `${CODEX_HOME}\skills` when `CODEX_HOME` is set, otherwise `%USERPROFILE%\.codex\skills`.
