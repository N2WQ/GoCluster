# Runtime Config Layout

The checked-in files in this directory are public example config. They must not
contain real peer callsigns, peer hostnames or IP addresses, passwords, service
tokens, or other private operational state. For a real node, copy the whole
directory to ignored `data/config.local`, edit that private copy, and run with
`DXC_CONFIG_PATH=data/config.local`.

## YAML Ownership Classes

The active config directory is both an operator contract and a behavior
contract. Not every YAML file is the same kind of knob.

| Class | Meaning | Normal operator action |
| --- | --- | --- |
| Deployment/runtime settings | Node identity, ports, source credentials, storage paths, logging, memory, enabled sources, and report scheduling. | Review before running a node and edit for the local deployment. |
| Operator policy settings | Explicit cluster policy that changes what users see, such as dedupe windows, flood rails, filter defaults, supported mode/event routing, path sample floors, and logging/event retention. | Change deliberately, document the operational reason, restart when required, and validate behavior. |
| Reference tables | Domain tables consumed at startup, such as supported taxonomy, regional mode inference, IARU region mapping, and seeded digital frequencies. | Edit only to correct or extend known domain/reference data; deploy with the matching binary/config directory. |
| Algorithm calibration | Thresholds, weights, distance models, decay/merge rules, correction rails, and path/scoring calibration used by call correction, path reliability, mode inference, solar overrides, and similar methods. | Do not change during normal operation. Change only with field evidence, replay/validation, documentation review, and decision-memory handling. |
| Optional tool config | Settings loaded only by a specific command or experiment boundary, not by server startup. | Keep optional at startup; validate strictly at the tool boundary before use. |

If a setting is unclear, treat it as algorithm calibration until the owning
README or ADR says it is a normal operator knob.

## YAML File Headers

Every checked-in first-party runtime config YAML in this directory starts with
a compact operator/support header:

```yaml
# Purpose: <what this file controls>
# Ownership: <deployment/runtime | operator policy | reference table | algorithm calibration | mixed>
# Runtime behavior: <required/optional, loader class, startup/tool-boundary behavior>
# Safe edits: <normal operator edits, or warning when edits need evidence/validation>
# Source: data/config/README.md.
```

These headers are local context for operators, support agents, and developers.
They do not define schema, defaults, or loader behavior; the active YAML values,
this README, package docs, current code, and ADRs remain authoritative. Header
updates must stay comment-only unless a separate approved config/schema change
explicitly changes runtime behavior.

Private or optional secret-bearing config, such as a local `openai.yaml`, should
keep secret-handling comments but must not be committed with real credentials.

## YAML Key Comments

Config comments should explain purpose where the key name and section are not
enough. Comment keys when units, sentinel values, ownership, side effects,
runtime consequences, or safe-edit boundaries are non-obvious.

Do not add noise comments for obvious boolean toggles such as `enabled:
true/false` unless the boolean has non-obvious side effects. When the same key
repeats across homogeneous sections or list entries, document the first
occurrence or use a field guide, then avoid repeating the same comment on every
row.

Configuration is split by concern so you only edit the relevant file:

- `app.yaml` - deployment/runtime settings for server identity, stats interval, console UI, system logging, propagation logging, and optional dropped-call logs.
- `runtime.yaml` - deployment/runtime settings plus operator policy for default filters, telnet messages, and `who_spots_me.window_minutes`.
- `ingest.yaml` - deployment/runtime settings for RBN/PSKReporter/human/DXSummit source enablement, source cadence, and call cache bounds.
- `peering.yaml` - deployment/runtime settings for peer links and ACLs.
- `reputation.yaml` - deployment/runtime settings plus operator policy for reputation gates.
- `archive.yaml` - deployment/runtime settings for archive enablement, storage path, backpressure, cleanup cadence, and single-window retention; expired rows are removed with timestamp range deletion rather than operator-tuned cleanup batches.
- `data.yaml` - deployment/runtime settings for CTY/FCC/skew sources, grid/cache tuning, data paths, and H3 table path.
- `prop_report.yaml` - deployment/runtime settings for scheduled propagation-report generation controls.
- `openai.yaml` - optional secret-bearing tool config for LLM report generation.
- `dedupe.yaml` - operator policy settings for primary/secondary dedupe windows and the default telnet dedupe policy for new users.
- `floodcontrol.yaml` - operator policy settings for shared-ingest flood rails, actions, windows, and per-source thresholds.
- `spot_taxonomy.yaml` - reference table plus limited operator policy for supported modes, events, and PSKReporter routing; YAML can only select behavior families already implemented by the binary.
- `mode_seeds.yaml` - reference table / algorithm calibration for digital frequency hints.
- `iaru_regions.yaml` - reference table for DXCC/ADIF to IARU region mapping.
- `iaru_mode_inference.yaml` - reference table / algorithm calibration for final regional frequency policy.
- `pipeline.yaml` - algorithm calibration for call correction, harmonics, mode inference, and spot-quality policy; not a normal operator tuning surface.
- `path_reliability.yaml` - operator policy for enable/display/sample-floor/receiver-cap mode/glyphs, plus active p50 histogram compatibility and algorithm calibration for decay, weights, thresholds, offsets, and noise tables.
- `solarweather.yaml` - operator policy for enable/fetch/reporting controls, plus algorithm calibration for daylight/high-latitude/level thresholds and override glyph behavior.
- `toxicity.yaml` - optional deployment/runtime settings for the Cloudflare Worker human-comment toxicity classifier.
- `toxicity_safe_gate.yaml` - optional reference table for routine ham-radio comments that may bypass the classifier when the Worker is enabled.
- `voacap_experiment.yaml` - optional tool config for manual VOACAP SSN forecast experiments; ignored by server startup and loaded by `voacap_ssn_forecast_watch`.

Normal operator edits:
- identity, ports, source credentials, source enablement, peer details, paths,
  logs, Go runtime controls, retention, and scheduled reports.

Advanced policy edits:
- dedupe/flood rails, supported taxonomy/routing, filter defaults, the cluster
  `SET PATHSAMPLES` floor, and receiver cap enforcement.

Algorithm calibration edits:
- `pipeline.yaml`, most of `path_reliability.yaml`, numerical solarweather
  gates, and mode inference reference/calibration require validation and
  decision-memory handling before changes.

Loader behavior:
- The server defaults to this directory (`data/config`). Override with `DXC_CONFIG_PATH` to point at another complete config directory, such as ignored `data/config.local`.
- The loader uses a filename registry. Unknown `.yaml`/`.yml` files fail config load instead of being accidentally merged.
- Merged runtime files populate the main `config.Config` tree. `path_reliability.yaml` and `solarweather.yaml` keep their file-local shapes and are loaded as typed feature-root config.
- `iaru_regions.yaml`, `iaru_mode_inference.yaml`, and `spot_taxonomy.yaml` are required reference tables. Startup fails if they are missing or malformed; there is no built-in table fallback.
- Required startup YAML files and required YAML-owned settings must be present and non-null in YAML. The startup loader walks the required config set and reports all missing required files and settings it can find before aborting.
- Extra YAML keys are logged as `Config warning` messages in the system log and ignored after logging is configured. If fatal config load errors happen before the configured system log can be opened, startup writes an explicit fallback message and the diagnostics to the default process logger. Known removed migration keys still fail startup with migration hints, such as removed archive cleanup keys, removed archive Pebble compatibility keys, legacy PSKReporter mode-routing keys, and removed path-reliability clamp/legacy threshold keys.
- When `path_reliability.enabled` is true, `h3_table_path` must contain valid `res1.bin` and `res2.bin` tables. Missing, malformed, or wrong-sized H3 tables fail startup because H3 cells are critical to path predictions.
- Gridstore startup open failures are logged to the system log. Corruption opens a checkpoint-restore path and the process temporarily runs without grid persistence while recovery proceeds; non-corruption open failures abort startup.
- Documented zero values are meaningful. For example, `telnet.broadcast_batch_interval_ms: 0` means immediate delivery, and `*_keepalive_seconds: 0` means the keepalive is disabled.
- `go_runtime.memory_limit_mib`, `go_runtime.gc_percent`, and `go_runtime.max_procs` apply the same process-wide Go runtime controls as `GOMEMLIMIT`, `GOGC`, and `GOMAXPROCS` without requiring a wrapper script. Set any value to `0` to leave the Go runtime or environment-provided value unchanged.
- `openai.yaml` is optional for server startup and `prop_report -no-llm`. When propagation-report LLM generation is enabled, the file is required and validated at that tool boundary. Secret values must not be logged or committed.
- `toxicity.yaml` is optional for server startup. When enabled, it requires a Worker endpoint, bearer token environment variable, bounded worker/queue/cache settings, and `toxicity_safe_gate.yaml`; secret bearer token values must stay in the environment or private config, not checked-in YAML.
- `voacap_experiment.yaml` is optional for server startup. The file is validated only by the VOACAP experiment command, and its SSN smoothing, recompute delta, forecast duration, VOACAP timeout/home, and center-frequency values are experiment-owned rather than production path-reliability settings.
- `peering.yaml` in the public example uses disabled `.example.invalid` peers, blank passwords, and placeholder callsigns. Put real peer connection details only in a private config directory.
- `reputation.yaml` in the public example disables IPinfo download/API usage and uses a placeholder download token so the strict loader still sees the required key. Put real IPinfo tokens only in a private config directory.
- Use `prop_report -config-dir <dir>` to point report generation at an alternate config directory. The older `-path-config` flag accepts either a directory or `path_reliability.yaml` path for compatibility.

Runtime log file names:
- System, dropped-call, file-only event, and propagation logs keep a stable
  active filename derived from the configured `dir` basename, such as
  `system.log` in `data/logs/system`.
- Dropped-call category logs use category subdirectories under
  `logging.dropped_calls.dir`, such as `bad_de_dx/bad_de_dx.log`.
- On UTC day rotation, the completed active file is archived with the existing
  date-only format, such as `07-Jun-2026.log`. The active file is then reopened
  empty before new-day events are written.

Propagation logs:
- `logging.propagation` writes path/propagation aggregate lines to a separate
  file sink and does not add UI/console output.
- Each block supports `enabled`, `dir`, and `retention_days`.
- `retention_days: 0` inherits `logging.retention_days`.
- The active propagation file is `propagation.log`; archives retain the
  date-only format, such as `07-Jun-2026.log`.
- The daily `prop_report` tool defaults to the date-only propagation archive;
  pass `-log` for historical system-log files or an explicit active/archive
  path.

File-only event logs:
- `logging.login_attempts`, `logging.reputation_drops`, `logging.telnet_connections`, `logging.ingest_connections`, and `logging.peer_connections` write separate file sinks and do not add UI/console output.
- Each block supports `enabled`, `dir`, `retention_days`, and `dedupe_window_seconds`.
- `retention_days: 0` inherits `logging.retention_days`; omitted `dedupe_window_seconds` inherits `logging.drop_dedupe_window_seconds`; explicit `dedupe_window_seconds: 0` disables de-dupe for that event log.
- Active event files use the directory basename, such as
  `login_attempts.log`; archives retain the date-only format, such as
  `07-Jun-2026.log`.
- Login attempt logs record failed or blocked login attempts only, not successful login audits. Telnet connection logs record successful login lifecycle separately.
- Event log values are sanitized and truncated; peer passwords, raw commands, raw peer frames, and payload bodies are not logged.

Spot taxonomy:
- `spot_taxonomy.yaml` is the only YAML surface for supported MODE tokens, EVENT families, EVENT reference prefixes, and PSKReporter mode routing.
- `ingest.yaml` owns PSKReporter transport/runtime settings only. Legacy `pskreporter.modes` and `pskreporter.path_only_modes` are rejected; use `pskreporter_route: normal`, `path_only`, or `ignore` on taxonomy modes instead.
- Archive retention is not taxonomy-owned; `archive.retention_seconds` applies one retention window to all archived modes.
- EVENT filtering is family-level. Standalone tokens such as `POTA` and acronym-prefixed references such as `POTA-1234` both tag `POTA`; reference values are not retained or filterable.
- Adding taxonomy modes/events requires editing `spot_taxonomy.yaml` and restarting the cluster with the matching binary/config directory.

Telnet message tokens (usable in `runtime.yaml`):
- Pre-login `welcome_message`, `login_prompt`, `login_empty_message`, `login_invalid_message`: `<CALL>`, `<CLUSTER>`, `<DATE>`, `<TIME>`, `<DATETIME>`, `<UPTIME>`, `<USER_COUNT>`, `<LAST_LOGIN>`, `<LAST_IP>`, `<DIALECT>`, `<DIALECT_SOURCE>`, `<DIALECT_DEFAULT>`, `<DEDUPE>`, `<GRID>`, `<NOISE>`.
- Post-login `login_greeting`: same tokens as above (with real values after login).
- Input guardrails `input_too_long_message`/`input_invalid_char_message`: `<CONTEXT>`, `<MAX_LEN>`, `<ALLOWED>`.
- Dialect status `dialect_welcome_message`: `<DIALECT>`, `<DIALECT_SOURCE>`, `<DIALECT_DEFAULT>` (source labels come from `dialect_source_default_label`/`dialect_source_persisted_label`).
- Path reliability `path_status_message`: `<GRID>`, `<NOISE>`.

Input behavior:
- Human telnet input is normalized to uppercase as it is read; the echoed characters are uppercase as well.
- Telnet IAC negotiation bytes (including subnegotiation) are stripped from input before validation.

Bulletin behavior:
- `telnet.bulletin_dedupe_window_seconds` suppresses identical WWV, WCY, and `TO ALL` announcement lines across all bulletin sources before they enter client control queues.
- Set `telnet.bulletin_dedupe_window_seconds: 0` to disable bulletin dedupe.
- `telnet.bulletin_dedupe_max_entries` is the hard cap on retained bulletin keys while dedupe is enabled.

DXSummit ingest:
- `dxsummit.enabled` is owned by the checked-in/operator `ingest.yaml` block. Missing DXSummit settings fail config load rather than receiving loader defaults.
- When enabled, one HTTP polling goroutine reads `dxsummit.base_url` and forwards accepted rows into the shared ingest pipeline as human `UPSTREAM` spots with `SourceNode=DXSUMMIT`.
- `dxsummit.poll_interval_seconds` controls poll cadence. Use the effective YAML value in `ingest.yaml` to know what this node will run.
- `dxsummit.max_records_per_poll` maps directly to the DXSummit `limit` query parameter. The shipped value is `500`; valid range is `1..10000`.
- Normal polls use `from_time=now-lookback_seconds`, `to_time=now`, `limit=max_records_per_poll`, and `include=HF,VHF,UHF`.
- `dxsummit.include_bands` is limited to `HF`, `VHF`, and `UHF`.
- `dxsummit.startup_backfill_seconds: 0` means seed-only startup: the initial page sets the high-water cursor and emits no historical rows.
- `dxsummit.spot_channel_size` and `dxsummit.max_response_bytes` bound retained queue and response memory.
- DXSummit spotter calls ending in `-@` preserve that marker for display/archive provenance. Relayed spotter calls ending in the skimmer marker `-#` strip only that terminal marker; numeric SSIDs are preserved.
- DXSummit latitude/longitude fields are not used to populate grids. Existing CTY and grid-cache enrichment may fill grids later from callsign-derived data.
- The console dashboard counts DXSummit in `Ingest Sources` only when `dxsummit.enabled` is true. It shows `DXSUMMIT` connected after a recent successful poll, including seed-only startup polls that emit no spots.
