## Telnet Input Guardrails

Telnet ingress now enforces strict limits to prevent memory abuse and control characters from ever reaching the command processor. Two YAML-backed knobs expose these guardrails so operators can tune them per environment:

- `telnet.login_line_limit` &mdash; defaults to `32`. This caps how many bytes a connecting client may type before the login prompt rejects the session. Keep this low so a single unauthenticated socket cannot allocate huge buffers.
- `telnet.command_line_limit` &mdash; defaults to `128`. Ordinary post-login commands and machine command headers must fit within this byte budget. Raise the value if human filter automation sends long comma-delimited lists; structured YAML bodies use the separate limit below.

Both limits apply before parsing, and rejected input is logged. Ordinary
post-login validation errors use the existing error-and-continue path; recognized
machine input errors use the terminal handling below. Commands continue to
support comma-separated filter inputs when the command limit is increased.

`PUT YAML`, `PATCH YAML` and `VALIDATE YAML CONFIG` enter a separate bounded
upload reader after a valid command header. Their body may contain at most
65,536 bytes, counting its original LF or CRLF endings, within a 30-second total
deadline. The body sits between standalone `---` and `...` lines and preserves
case and YAML punctuation. It must contain one plain YAML document; anchors,
aliases, merge keys, custom tags and null values are rejected.

Rejected upload headers, oversized bodies, deadline expiry and incomplete or
unreliable framing close the connection without changing configuration. Payload
tails never become ordinary commands. A completely received, correctly framed
document that fails schema or value validation receives a framed YAML error and
keeps the connection open. See the [client protocol](../telnet/README.md) for
request fields and examples.

Every new SHOW or GET YAML readback is capped at 65,536 final response bytes,
including CRLF conversion, framing and human footers. Oversized responses return
an explicit error. `telnet.writer_batch_max_bytes` controls batching; it does not
raise this response limit. A valid preset larger than 64 KiB can still LOAD under
the independent 256 KiB preset budget, while its FULL/YAML readback may fail.

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

The 30-second YAML upload deadline is separate from the ordinary read-idle
timeout. It runs from acceptance of the valid upload header and is checked even
if a timeout callback runs late.

## User Configuration Backups and Downgrades

User configuration is runtime state under `filter.UserDataDir`, normally
`data/users`; it is separate from the deployment YAML in `data/config` or
`data/config.local`. Full login callsigns retain their SSIDs in filenames such as
`data/users/N2WQ-1.yaml`. Shared named presets are stored below `data/users/presets`
in a file named with the hex-encoded owner callsign. Back up the entire user-data
directory so current preferences, the applied preset name and baseline, login
metadata and the shared preset library remain together.

Current user records and preset snapshots contain `configuration_version: 1`.
An absent marker selects legacy migration on read; current versions preserve
explicit false, empty and default preferences. Malformed or unsupported records
are preserved. A user can connect with temporary defaults and a warning, but
LOAD, SAVE PRESET and YAML writes cannot overwrite the protected configuration;
ordinary session changes are temporary. Failure to save login timestamp/IP after
a successful record read instead restores that configuration and continues with
a warning.

Before upgrading, stop server writers and external editors, and retain a backup
of the user-data directory and its matching binary. Older user-record writers
can drop the new marker and preset reference or normalize exact values; older
strict preset decoders reject the new snapshot fields. For downgrade, stop
writers again and restore the matching earlier binary and data backup. Keep the
current backup intact; do not try an older writer against its only copy. The
directory has one writer process; transaction locks do not coordinate separate
server processes or external edits. See
[ADR-0244](decisions/ADR-0244-exact-configuration-persistence.md).

## PSKReporter MQTT Debug Logging

Set `DXC_PSKR_MQTT_DEBUG=true` to enable verbose Paho MQTT debug logs for the PSKReporter client. Logs include DEBUG/WARN/ERROR/CRITICAL lines and should be used only while diagnosing reconnects or payload handling issues.

## Codex Skills

This repo vendors gocluster's project skills under `.agents/skills/`. They are
part of the checkout and should be used as the project authority on every
machine.

- Verify the repo skill bundle:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify-codex-skills.ps1
```

Credentials and connector/plugin setup remain machine-local; do not commit
tokens, auth files, or personal plugin state.

## Development Tools And WSL

The module requires Go 1.27.1. CI pins Staticcheck 2026.2.1 (`v0.8.1`) and
golangci-lint 2.14.0. Run `scripts/verify-agentic-tools.ps1` in the shell used
for development; required version probes must succeed, including in quiet
mode. Commands without version probes are reported as presence checks.
Recommended and optional tools retain their existing classifications.

For Windows, install a compatible MinGW-w64 compiler when CGO or race testing
is required. The repaired host uses WinLibs GCC 16.2.0 with UCRT and Strawberry
Perl 5.42.3.1. Finding a compiler can change Go's automatic `CGO_ENABLED` from
0 to 1; inspect `go env CGO_ENABLED` before comparing validation results.
Some CGO-built test executables on the migration host were denied by Windows
Application Control, while the full race suite and a CGO-disabled cluster
test passed. Do not treat that denial as a source-test failure or disable the
security policy. Native CGO execution requires resolving the machine policy;
WSL validation does not prove Windows executables are permitted to run.

The selected Codex environment is WSL2 with Ubuntu 24.04. Install Go and
development tools inside Linux rather than relying on Windows executables in
the inherited PATH. The migration host has Go 1.27.1, Node 24.19.0,
PowerShell 7.6.6, Codex CLI 0.160.1, and Claude Code 2.1.292 available in WSL.
Install VS Code's `ms-vscode-remote.remote-wsl` extension and reopen the
checkout in WSL. Both `.vscode/settings.json` and `gocluster.code-workspace`
select WSL for Codex. The operational PowerShell launcher builds and runs
Windows executable pairs; selecting WSL for Codex does not convert it into a
Linux server launcher.

The shared checkout is `/mnt/c/src/gocluster` in WSL. Windows-mounted paths
can be slower than Linux home directories. Keep one authoritative checkout;
moving it is a separate operation that must account for runtime writers and
operator paths. Match Git line-ending settings when accessing this existing
Windows checkout, and trust only its exact path if Git reports ownership
differences. Never set a wildcard `safe.directory` exception.

Linux authentication and user settings are separate from the Windows profile.
The new `r_bak` WSL user has no password set; use
`wsl -d Ubuntu-24.04 -u root -- passwd r_bak` interactively to set one.
Codex, Claude, and GitHub CLI require Linux login (`codex login`,
`claude auth login`, `gh auth login`) unless the selected client supplies its
own authenticated session. CLI installation and a sandbox smoke check do not
establish account access. Never copy Windows configuration blindly: paths,
notification commands, and plugin bindings can be platform-specific.

DXSpider reference qualification uses the exact pinned external checkout and
an explicit Perl interpreter. Ubuntu prerequisites are `perl`, `libdbi-perl`,
`libmojolicious-perl`, `libdata-structure-util-perl`, `libjson-perl`, and
`libmath-round-perl`, plus CPAN `Net::CIDR::Lite` (DB_File is bundled with Perl).
Then run from the repo root:

```powershell
pwsh -NoProfile -File scripts/pc92-dxspider-interop.ps1 `
  -DXSpiderRoot /path/to/pinned/dxspider -PerlPath /usr/bin/perl
```

On Windows, provide Strawberry's `perl.exe` and its `c/bin` directory through
`-PerlDLLDirectory`. Tests skipped because reference environment variables
are unset provide no sender/receiver interoperability evidence. The configured
suite passed on both platforms during the migration repair.

Repository files do not carry installed tools, user skills, VS Code extensions,
Codex/Claude history or settings, credential storage, caches, or external
VOACAP/DXSpider installations. The old disk was unavailable; current checks
establish a working setup, not byte-for-byte completeness against that disk.
See [ADR-0250](decisions/ADR-0250-go127-development-and-launcher.md).
