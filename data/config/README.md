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
- `archive.yaml` also requires `bulletin_archive.path`, the independent WWV/WCY JSON snapshot file. Set it to a writable file outside the spot archive's Pebble directory. For example, `data/bulletins/history.json` works whether spot archiving is enabled or disabled. Deploy this setting with the updated executable.
- If the bulletin snapshot cannot be loaded, the cluster preserves a recovery copy before replacing it. Check the storage warning and keep the recovery file for investigation. If preservation fails, new history stays in memory until the storage problem is resolved. See [bulletin history](../../telnet/README.md#wwv-and-wcy-bulletin-history) for recovery details.
- Before rolling back to a bulletin-history executable limited to 24 records per type, back up the snapshot and restore a compatible copy. That older executable can reject and overwrite a snapshot containing 25–48 records. A pre-bulletin executable does not use the snapshot.
- `data.yaml` - deployment/runtime settings for CTY/FCC/ISED/skew sources, grid/cache tuning, data paths, and H3 table path.
- `prop_report.yaml` - deployment/runtime settings for scheduled propagation-report generation controls.
- `openai.yaml` - optional secret-bearing tool config for LLM report generation.
- `dedupe.yaml` - operator policy settings for primary/secondary dedupe windows and the default telnet dedupe policy for new users.
- `floodcontrol.yaml` - operator policy settings for shared-ingest flood rails, actions, windows, and per-source thresholds.
- `spot_taxonomy.yaml` - reference table plus limited operator policy for supported modes, events, and PSKReporter routing; YAML can only select behavior families already implemented by the binary.
- `mode_seeds.yaml` - reference table / algorithm calibration for digital frequency hints.
- `iaru_regions.yaml` - reference table for DXCC/ADIF to IARU region mapping.
- `iaru_mode_inference.yaml` - reference table / algorithm calibration for final regional frequency policy.
- `pipeline.yaml` - algorithm calibration for call correction, harmonics, mode inference, and spot-quality policy; not a normal operator tuning surface.
- `path_reliability.yaml` - operator policy for enable/display/sample-floor/receiver-cap mode/glyphs and the optional VOACAP sparse-data fallback, plus active p50 histogram compatibility and algorithm calibration for decay, weights, thresholds, offsets, and noise tables.
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
- Local console UI mode is limited to `headless` and `tview-v2`. Legacy `ansi`, `tview`, `auto`, `ansi_poc`, and `none` modes fail config load with a migration error; stale legacy UI keys are treated as ordinary extra keys and logged as config warnings.
- `iaru_regions.yaml`, `iaru_mode_inference.yaml`, and `spot_taxonomy.yaml` are required reference tables. Startup fails if they are missing or malformed; there is no built-in table fallback.
- Required startup YAML files and required YAML-owned settings must be present and non-null in YAML. The startup loader walks the required config set and reports all missing required files and settings it can find before aborting.
- `human_telnet` accepts the canonical ordered sequence or the historical single mapping. A sequence may contain zero to 64 entries; every entry must contain all documented keys even when disabled. Names must match `[A-Za-z0-9][A-Za-z0-9._-]{0,31}` and be unique without regard to case. Duplicate host/port/callsign identities are rejected after trimming and case normalization. Each `slot_buffer` is `1..64000`, and enabled entries may total at most `64000`. Enabled connections start and retry independently; the sequence order is also the console `HUMAN/<name>` display order.
- Extra YAML keys are logged as `Config warning` messages in the system log and ignored after logging is configured. If fatal config load errors happen before the configured system log can be opened, startup writes an explicit fallback message and the diagnostics to the default process logger. Known removed migration keys still fail startup with migration hints, such as removed archive cleanup keys, removed archive Pebble compatibility keys, legacy PSKReporter mode-routing keys, and removed path-reliability clamp/legacy threshold keys.
- When `path_reliability.enabled` is true, `h3_table_path` must contain valid `res1.bin` and `res2.bin` tables. Missing, malformed, or wrong-sized H3 tables fail startup because H3 cells are critical to path predictions.
- Gridstore startup open failures are logged to the system log. Corruption opens a checkpoint-restore path and the process temporarily runs without grid persistence while recovery proceeds; non-corruption open failures abort startup.
- Documented zero values are meaningful. For example, `telnet.broadcast_batch_interval_ms: 0` means immediate delivery, either automatic read-pause setting at `0` disables the generic row-based trigger, and `*_keepalive_seconds: 0` means the keepalive is disabled. The new human SHOW readbacks have the reading-pause exception described below.
- `go_runtime.memory_limit_mib`, `go_runtime.gc_percent`, and `go_runtime.max_procs` apply the same process-wide Go runtime controls as `GOMEMLIMIT`, `GOGC`, and `GOMAXPROCS` without requiring a wrapper script. Set any value to `0` to leave the Go runtime or environment-provided value unchanged.
- `pskreporter.workers: 0` follows the effective `go_runtime.max_procs` / `GOMAXPROCS` scheduler width; set a positive value only when you deliberately want wider PSKReporter burst processing than the process CPU budget.
- `openai.yaml` is optional for server startup and `prop_report -no-llm`. When propagation-report LLM generation is enabled, the file is required and validated at that tool boundary. Secret values must not be logged or committed.
- `toxicity.yaml` is optional for server startup. When enabled, it requires a Worker endpoint, bearer token environment variable, bounded worker/queue/cache settings, and `toxicity_safe_gate.yaml`; secret bearer token values must stay in the environment or private config, not checked-in YAML.
- In a present `toxicity.yaml`, all ten keys are required and non-null even when
  disabled. Explicit numeric `0` requests the existing defaults: timeout 1200 ms,
  workers 2, queue 128, cache 4096 entries, TTL 86400 seconds and comment payload
  512 bytes. This is a documented exception to the general no-hidden-default
  rule; omission is never the same as zero. Negative values fail load. Positive
  `timeout_ms` must be at most 2147483647 (the existing consumer ceiling), and
  positive `cache_ttl_seconds` must fit Go's duration, at most 9223372036 on a
  64-bit host. Native integer decoding limits also apply. These are admission
  limits, not recommended operating values. See [ADR-0276](../../docs/decisions/ADR-0276-toxicity-config-zero-and-duration-admission.md).
- Classifier `Stop` is terminal: it cancels HTTP work, joins workers and releases
  queued jobs and unreceived results. Later eligible human submissions fail open
  as `UNAVAILABLE`. Terminal cancellation is cleanup, not an AI timeout or
  malformed response; it does not itself classify the discarded spots.
- Runtime shutdown gives the output pipeline the existing bounded classifier
  grace, then cancels unfinished HTTP work. Already admitted processing finishes
  before its downstream services close; this can outlast grace. Late temporal/FT
  holds and released stabilizer processing retire before completion. Normal
  suppression and bounded queue drops still apply; handoff is not network receipt.
- `path_reliability.native_160m_fallback.enabled: true` uses exact solar path geometry to fill insufficient 160m p50 results with conservative `LOW` or `UNLIKELY` glyphs when enough of the great-circle path is darker than civil twilight. It does not predict SNR, never emits `HIGH` or `MEDIUM`, never overrides sufficient p50, and yields to any usable current-hour VOACAP fallback result. `display_enabled: false` keeps the evaluation in aggregate counters without changing glyphs.
- `path_reliability.voacap_fallback.enabled: false` starts no SSN polling or VOACAP worker. When enabled on Windows, it fetches NOAA SSN on the configured cadence, uses rounded integer 8-hour EWMA SSN generations, persists SSN monitor continuity to `voacap_fallback.ssn_state_path`, persists completed forecast windows to the per-node Pebble DB at `voacap_fallback.forecast_cache_db_path`, delays fallback after insufficient bucket evidence, and can only replace insufficient results when VOACAP is closed, when sparse bucket p50 and the cached current-hour VOACAP forecast map to the same path class, or when REL-gated open fallback rules pass. On Linux and other non-Windows builds, startup logs that runtime VOACAP execution is unsupported and continues without the VOACAP SSN monitor, worker, or forecast cache. Existing sufficient bucket p50 results remain authoritative. `ssn_state_path` is per-node runtime state for NOAA validators, last observation, EWMA, and current rounded SSN generation; `forecast_cache_db_path` is per-node derived VOACAP prediction state and should not be shared by running cluster processes. Current restored forecast-cache records hydrate memory before workers start and bypass `voacap_fallback.delay_seconds`; stale or malformed records are pruned and a missing/unavailable cache cold-starts normal delay/queue behavior. `sparse_p50_diagnostic_max_observation_count` only defines the very-low-count diagnostic bucket for `Sparse p50 VOACAP (5m)` logs, including invalid-request reason splits; it does not change gates or queue more VOACAP work.
- `voacap_experiment.yaml` is optional for server startup. The file is validated only by the VOACAP experiment command, and its SSN smoothing, recompute delta, forecast duration, VOACAP timeout/home, and center-frequency values are experiment-owned rather than production path-reliability settings.
- `peering.yaml` in the public example uses disabled `.example.invalid` peers, blank passwords, and placeholder callsigns. Put real peer connection details only in a private config directory.
- `peering.max_peers` is a required integer from 1 through 64, including when peering is disabled. The shipped value is 64; there is no omitted, null, zero, or unlimited fallback. Existing private configurations must add this key explicitly. When peering is enabled, the number of enabled direct peer identities must fit this cap; disabled rows do not count, and a `both` direction counts once. An over-cap dormant registry remains loadable while peering is disabled, but enabling it fails startup. Changes require a restart, and lowering the cap never silently selects a subset of peers. The cap bounds direct sessions and recovery identities; the pending-handshake limit remains 128, with at most `max_peers + 128` transport owners. It does not limit remote topology population or raise the existing memory budget.
- `reputation.yaml` in the public example disables IPinfo download/API usage and uses a placeholder download token so the strict loader still sees the required key. Put real IPinfo tokens only in a private config directory.
- Manual DX reputation uses the baseline station across numeric SSIDs. Reconnect preserves active spent counts and deadlines, with normal elapsed-window advancement and configured risk tightening. Baseline ASN/country history is reused; old SSID histories are untouched and unmerged. Counters are runtime-only and can be lost on restart or state eviction. Configuration remains full-SSID. See [ADR-0271](../../docs/decisions/ADR-0271-shared-station-reputation.md).
- Use `rbnskewfetch -config <dir> -out <file>` to fetch the configured skew CSV
  and write JSON. Rows meet `abs(skew) >= skew.min_abs_skew`; factor-1 rows remain.
  Fetch, parse, empty-selection, JSON-write or success-report failure exits
  unsuccessfully. Pre-write failure preserves an existing output. A report
  failure can occur after the new table was written; inspect stderr and status.
- Use `prop_report -config-dir <dir>` to point report generation at an alternate config directory. The older `-path-config` flag accepts either a directory or `path_reliability.yaml` path for compatibility.
  Generation, JSON/Markdown writes and final success-report failures exit
  unsuccessfully. JSON is written first and may remain after later failure.
  `-no-llm` needs no OpenAI file. With narrative enabled, invalid optional config
  is a hard error before output; a request failure warns and keeps the local
  report. Inspect command status and stderr as well as any files left behind.

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
- Peer records use the sibling `peerdiag` companion and a bounded diagnostic
  mailbox. Peer status reports known dropped records separately from writes
  whose outcome is unconfirmed. Missing, blocked or resource-refused diagnostic
  storage degrades logging while the protocol continues; other event sinks
  retain their existing ownership. See [peer diagnostics](../../peer/README.md#diagnostics-and-persistence).
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
- Post-login `login_greeting`: same tokens as above (with real values after login),
  plus `<SETTINGS_FILTERS>` for the bounded settings/rule block and first-time
  guidance. The shipped greeting uses this token instead of duplicating
  dialect/grid/noise/dedupe lines. Custom templates can place or omit the block.
- `<STARTUP_INFO>` places the startup notices and WWV/WCY history in the
  post-login greeting. Only its first occurrence displays the block; later
  occurrences are removed. The shipped greeting places it before the prompt.
  Without it, startup notices and history precede the complete greeting.
  Deploy the updated executable and greeting together. Before rolling back to
  an older executable, remove this token from the active greeting; older
  executables display it literally.
- Input guardrails `input_too_long_message`/`input_invalid_char_message`: `<CONTEXT>`, `<MAX_LEN>`, `<ALLOWED>`.
- Dialect status `dialect_welcome_message`: `<DIALECT>`, `<DIALECT_SOURCE>`, `<DIALECT_DEFAULT>` (source labels come from `dialect_source_default_label`/`dialect_source_persisted_label`).
- Path reliability `path_status_message`: `<GRID>`, `<NOISE>`.

Input behavior:
- Ordinary human command headers and their echo normalize to uppercase. After
  `PASS RULE <slot>` or `REJECT RULE <slot>`, printable ASCII expression bytes
  retain their case and spaces in input and echo. Compilation normalizes syntax
  and field-specific operands, but preserves COMMENT literal case, punctuation
  and spaces. Expression quotes use doubled `""` for a literal quote;
  backslashes are literal, not escapes.
- Telnet IAC negotiation bytes (including subnegotiation) are stripped from input before validation.
- Recognized machine command headers preserve request identifier case. Structured
  YAML bodies preserve case and punctuation and use their own 65,536-byte limit
  and 30-second total upload deadline. These protocol limits do not change
  `runtime.yaml` or turn user YAML into deployment configuration. See the
  [client protocol](../../telnet/README.md).

Read-pause behavior:
- `telnet.auto_read_pause_min_rows` is the rendered command-output row count
  that starts a temporary live-spot pause; blank separator rows count, while the
  final trailing newline does not.
- `telnet.auto_read_pause_seconds` is how long live spot lines are suppressed
  after long command output.
- Set either read-pause value to `0` to disable the generic automatic trigger. Manual
  `PAUSE [seconds]` remains available, defaults to `30` seconds independently of
  these settings, and accepts whole seconds from `1` to `300`.
- The shipped values are `10` rows and `30` seconds so commands such as
  `SHOW PROP`, `WHOSPOTSME`, and long `HELP` output can be read before live
  spots resume.
- Read pause suppresses live spot lines only; bulletins, talks, command
  replies, errors, keepalives, and close messages continue on the control path.
- A manual `PAUSE` sets a fresh duration. Automatic pauses may extend an active
  pause but cannot shorten it; their footer reports the effective remaining
  duration. Replies to `PAUSE`, `SHOW HOLD`, and `RESUME` do not trigger another
  automatic pause. Missed spots are not replayed.
- Human `SHOW FILTER`, `SHOW FILTER RULE`, `SHOW FILTER FULL` and
  `SHOW SETTINGS` always start a reading pause, including their size-error
  responses. They ignore the row threshold, including `0`, use a positive
  `auto_read_pause_seconds` value, and use 30 seconds when it is `0`.
- For those readbacks, suppression begins at request acceptance, covers output
  preparation, queueing and delivery, then continues for the full reading interval
  after successful server write and flush. Terminal rendering time is unknown.
  Preserve any longer active pause and suppression count. A later valid processed
  `PAUSE` or `RESUME` supersedes an earlier response's pending completion; another
  human SHOW starts a fresh reading pause. Close or session replacement retires
  pending completion effects.
- Machine GET, PUT, PATCH and VALIDATE YAML commands, including errors, do not
  start, extend, resume or reset pauses or suppression counters. Counters can
  still increase naturally if an existing pause suppresses live traffic.
- Every new readback has a 65,536-byte final response limit, including CRLF,
  headers, YAML document markers and human pause footers. Generate the complete
  response before queueing; oversized output fails explicitly without a truncated
  success. Human errors follow the reading-pause policy; YAML errors remain framed
  YAML with no human footer or pause effects. Existing row-based behavior for
  other commands and the checked-in runtime YAML values remain unchanged.

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


PC18/PC92 active wire configuration:
- `local_callsign` and every effective peer `login_callsign` must normalize to
  the same valid receiver identity, at most15 canonical bytes. Enabled peer
  `remote_callsign` values must be distinct after normalization and different
  from the local identity. Authentication still uses the configured literal
  allowlist/login boundaries; publication aliases do not broaden access.
- Local `pc92_bitmap` accepts4 or5. External6/7 are received topology flags,
  not a valid local root identity. Numeric compatibility version/legacy version
  have1-10 digits; build may also be explicitly empty.
- Positive `backoff.base_ms` and `backoff.max_ms` cannot exceed300000 before
  conversion to durations. Existing nonpositive sentinel/default handling is
  preserved. Successful establishment resets retry to its normalized base.
- These wire checks also run for direct `NewManager` callers before storage
  opens; constructor callers must supply effective values, not loader omissions.

## User Configuration Storage

User preferences are stored under `filter.UserDataDir` (normally `data/users`),
separately from the files in this deployment-config directory. Each full callsign,
including its SSID, owns one record such as `N2WQ-1.yaml`. The `presets` subdirectory
holds named snapshots shared by the base callsign's numeric SSIDs; back it up
with user records.

Native user records require `configuration_version: 5` and nested
`configuration.filters` and `configuration.settings`. Rules are numbered Boolean
definitions, not category maps. The six separate controls preserve `DEFAULT`
versus explicit `false`/`true`; settings preserve empty default selections and
numeric zero sentinels. `recent_ips` and `last_login_utc` remain user metadata
outside `configuration`, not preset preferences.

This source-derived example is a native user record, not a machine upload:

```yaml
configuration_version: 5
configuration:
  filters:
    rules:
      - {slot: 0, action: pass, expr: 'MODE=CW AND SOURCE=HUMAN'}
    nearby_enabled: false
    include_beacons: DEFAULT
    allow_wwv: DEFAULT
    allow_wcy: DEFAULT
    allow_announce: DEFAULT
    allow_self: DEFAULT
    allow_toxic: DEFAULT
  settings:
    dialect: ""
    grid: ""
    noise_class: ""
    dedupe_policy: ""
    path_min_observation_count: 0
    solar_summary_minutes: 0
recent_ips:
  - 192.0.2.1
last_login_utc: 2026-10-21T18:00:00Z
```

An optional `preset` mapping contains `name` and `baseline`. The baseline is an
independent snapshot with its own `configuration_version: 5` and nested
`configuration.filters/settings`, not a lookup of the library's current entry.
The named library is a `presets` mapping; each normalized name owns one versioned
snapshot, for example:

```yaml
presets:
  CONTEST:
    configuration_version: 5
    configuration:
      filters:
        rules:
          - {slot: 0, action: pass, expr: 'DXSTATE=NY,ON'}
        nearby_enabled: false
        include_beacons: DEFAULT
        allow_wwv: DEFAULT
        allow_wcy: DEFAULT
        allow_announce: DEFAULT
        allow_self: DEFAULT
        allow_toxic: DEFAULT
      settings:
        dialect: ""
        grid: ""
        noise_class: ""
        dedupe_policy: ""
        path_min_observation_count: 0
        solar_summary_minutes: 0
```

There is no top-level library version or machine `schema_version`/request
envelope on disk. Every current entry and applied baseline must be v5. Strict
v5 decoding refuses missing, zero, old and future disk version markers. A zero version
is only a newly constructed in-memory encoder DTO convention; it is never a
supported disk version.

Login has a separate reset-only upgrade path for recognized unversioned and
version 1–4 records. It preserves settings, separate controls and metadata,
discards old filters/NEARBY/applied association, and installs the same configured
mode/source rule as new accounts in editable PASS slot 0. Old preset entries
are cleared while existing v5 entries survive. Originals are backed up before
replacement; subsequent logins do not reinstall this rule or wipe new presets.
This is not translation of old filters or acceptance of older machine schemas.
The original-file backup suffix is `.pre-v5.bak`. Backups must match the original
bytes on retry; a conflicting backup stops the reset. Restore or rollback
requires stopping writers and using compatible binary/data copies, not editing
the version marker. Historical unversioned settings retain the old loader's
normalization/default meaning; marked version 1–4 preferences retain exact
empty/zero/false selections.

Unreadable, malformed, over-budget or unrecognized-version records are
protected and left untouched. A login unable to restore or upgrade its record uses warned
temporary defaults, not a successful migration. LOAD, machine writes and SAVE
PRESET are rejected for that protected record; SAVE fails before writing the
named library. Ordinary preference and teardown saves also preserve it. If only
the login timestamp/IP save fails after a successful read, login continues with
the restored preferences and preset reference plus a warning.

New accounts and recognized old-profile resets receive the validated configured
`filter.default_modes` and `filter.default_sources` as an
ordinary editable PASS rule in slot 0. The expression combines MODE and SOURCE
with AND; ALL, both sources or an empty source selection explicitly writes
SOURCE=HUMAN,SKIMMER. The shipped default_sources lists HUMAN and SKIMMER.
A PASS without SOURCE imposes no source restriction, just like REJECT.
Explicit SOURCE=HUMAN rules remain human-only, including saved defaults.
Changing the shipped configuration does not rewrite saved profiles or presets.
Startup validates the configured rule after taxonomy loading. Existing/restored v5
records with `rules: []` stay empty. Adding another PASS broadens selection;
edit or remove slot 0 to narrow it. `RESET FILTER RULE <0..9|ALL>` clears only
selected slots and preserves NEARBY. `RESET FILTER` clears all rules and turns
NEARBY OFF. Neither reinstalls slot 0 or resets separate controls/settings.

SNR is a Boolean rule field, separate from deployment YAML. For example,
`REJECT RULE 9 SOURCE=SKIMMER AND MODE=CW,RTTY AND SNR<10` explicitly limits the
rejection to skimmer reports. SNR is known whenever `HasReport` is true,
including human reports, zero and negative values. Missing-report comparisons
are false, including `<>`; NOT negates that result. There is no implicit human
exemption, saved `min_snr` map or old-schema preservation path. See `HELP RULE`
and the [telnet filter contract](../../telnet/README.md#filters).

User-record writes atomically commit preferences together with the applied preset
name and one bounded baseline. LOAD rebuilds runtime geometry, retains stored
NEARBY ON with a warning when geometry is unavailable, and commits the full-SSID
record before live publication. It records the applied snapshot, including
reported server adjustments; SAVE associates the same capture written to the
named library. If the named save succeeds but its association fails, the previous
live and disk association/baseline remain recoverable. Ordinary saves preserve
that reference, and library overwrite or deletion does not alter the applied
snapshot. Reconnect restores the reference for that full callsign. Preferences
differing from the baseline show `(modified)`; reversing them clears the label.

User records have separate 1 MiB (1,048,576-byte) raw-read, normalized-encoding
and final-write limits. Each configuration, preset and applied baseline has a
256 KiB (262,144-byte) standalone SavedPreset encoding limit. Decoding has one
shared physical-plus-logical processing budget for node visits and decoded
scalar bytes, including alias expansion: 2,097,152 units per record and
16,777,216 per collection. Nested baselines do not start a new budget. These are
work units, not heap or file bytes.
Refused files remain untouched. Human candidates are admitted before publication;
a warm session with a refused destination can use fitting temporary changes with
a warning, then retry a repaired file on a later save or ordinary logout. A
login that started with protected defaults remains protected for that session.
SAVE admits the library and association before either write; only later I/O
failure can produce its documented partial success.

Preset budgets remain 20 names per owner, 256 KiB per encoded snapshot and
8 MiB (8,388,608 bytes) per collection. Trial rule guardrails are ten slots
(0-9), 128 total atoms and 512 total values; each expression is limited to
1,024 raw and canonical bytes, 32 atoms, 128 nodes and nesting depth 8.
Each atom has at most 32 values; each COMMENT operand has 1-64 printable ASCII
bytes including a non-space byte. Nesting counts parentheses and NOT, not flat
AND/OR height. These limits do not enlarge the configured human line limit
(128 bytes by default) or the 65,536-byte machine upload/final-response limits.
Valid large presets can LOAD even when a FULL or YAML readback exceeds 64 KiB.
PUT/PATCH/VALIDATE instead require the resulting
complete CONFIG readback to fit, including reserved response metadata. An
unchanged PUT still persists before success, repairing an earlier failed human
save without resetting unrelated solar, GRID, NEARBY, diagnostic or pause state.

The live experiment uses isolated storage. Recognized old profiles now have
the explicit reset-only login upgrade above; ordinary strict reads do not
convert profiles or reinterpret an edited version marker. No production bulk
rewrite is performed by the implementation task.
Stop writers before backup, repair or restore; use one writer process per
user-data directory. Writes replace a destination only after the temporary file
has been written, synced and closed. Downgrade requires a matching binary and
data backup; never delete or relabel a protected record to force admission.
See [backup instructions](../../docs/ENVIRONMENT.md#user-configuration-backups-and-downgrades).
[ADR-0244](../../docs/decisions/ADR-0244-exact-configuration-persistence.md)
records earlier persistence history, not an active pre-v5 conversion contract.

The current native boundary is owned by `filter/configuration_v5.go`,
`filter/storage_v5.go`, `filter/storage_v5_decode.go`,
`filter/storage_v5_io_adapter.go` and `filter/preset_v5.go` in the checkout.
Human edits admit detached candidates before publication, then keep accepted
edits live with a warning if a later save fails. Machine writes commit before
publication and preserve both live and saved state on failure. All machine GETs,
including SETTINGS and CAPABILITIES, require explicit `SCHEMA 5`; upload bodies
require `schema_version: 5`, not the native disk envelope. See the
[client protocol](../../telnet/README.md#client-yaml-configuration) and
[normative domain contract](../../docs/domain-contract.md#boolean-personal-rules)
for rule semantics, structural NEARBY and the required unavailable-geometry
rule-only recovery behavior. The unchanged saved GRID may remain unmappable
while exact syntax and every other settings check stay enforced.

## FCC And ISED Reference Data And Enforcement

`data.yaml` owns the FCC URL, archive/database/temp paths, refresh time, allowlist
path and lookup TTL. `fcc_uls.enabled` controls **license rejection only**.
`false` still initializes and refreshes the database and enriches live and
archived spots with known FCC mailing-address state/territory. Existing values
and defaults are unchanged. Startup reporting displays enforcement separately
from reference-data refresh. This differs from older releases, where false also
stopped acquisition.

Old state-less databases are rebuilt promptly on startup, including when remote
metadata is unchanged. During acquisition/build, unavailable lookups allow
spots through the license gate and leave state empty. A failed build preserves
the last good database; its membership remains usable even if it has no state
column. First-time success activates the new database without restarting.
The existing refresh schedule and lookup TTL still apply.

Only the new two-letter state is added to the existing active-license projection;
no full address/contact table persists. Blank, invalid or ambiguous state leaves
the license intact. General US allowlist entries cover all supported FCC
jurisdictions; entity-specific entries remain entity-specific. Refresh logs
report known/unknown state and malformed input counts; the existing map logger
reports cache cardinality/capacity/generation when enabled.

Canadian reference data uses the required `ised` block in the same `data.yaml`,
loaded through the startup configuration loader. Existing private config
directories must add the complete block from the
[public example](data.yaml). Missing or null settings fail startup; required
URLs, paths and refresh times cannot be empty. The loader preserves explicit
`ised.enabled: false` and supplies no omitted ISED defaults.

| Setting | Meaning |
| --- | --- |
| `ised.enabled` | License rejection only. `false` still refreshes the snapshot and enriches provinces. |
| `ised.url` | Absolute HTTP(S) URL for ISED's assigned individual and club callsign archive. |
| `ised.special_url` | Absolute HTTP(S) URL for ISED's dated special-event and temporary-prefix archive. |
| `ised.archive_path` | Local ZIP path for assigned callsigns. |
| `ised.special_archive_path` | Local ZIP path for special events and prefix substitutions. |
| `ised.db_path` | Canadian SQLite snapshot, independently owned from the FCC database. |
| `ised.temp_dir` | Canadian extraction directory; may share a directory with FCC work. Database scratch stays beside `ised.db_path` for atomic publication. |
| `ised.refresh_utc` | Daily refresh time in `HH:MM` UTC; the public example uses `22:15`. |

Archive, database and metadata paths must be distinct across both sources and
must not overwrite the existing FCC allowlist. The loader rejects cleaned path,
case, existing symlink and hard-link collisions, including a managed file used
as another path's parent. Shared temp directories are allowed.

The Canadian snapshot publishes only after both archives have been downloaded,
parsed and committed together. Literal quotes and empty semicolon-delimited
fields are preserved. The projection retains callsign/province and dated event
evidence, rather than names, full addresses, qualifications or event prose.
The two archive SHA-256 values stored in the completed database identify the
published input pair; HTTP sidecars alone do not prove that pair was published.
A failed download, parse, build or publication retains the last successful
database and remains retryable when upstream HTTP validators are unchanged.
FCC lookups continue independently during a Canadian refresh.

`fcc_uls.cache_ttl_seconds` is the common lookup TTL for both sources. There is
one aggregate 200,000-entry cache cap, rather than a second Canadian cache.
Canadian answers additionally expire at UTC midnight so an unchanged snapshot
still observes special-event start and end dates. Events are active on their
inclusive UTC calendar dates. An active prefix substitution must match an
assigned ordinary base call; this is a callsign plausibility check and does not
prove residency, club membership or other event eligibility. Unsupported or
ambiguous membership evidence remains unknown and fails open at admission.

Province uses the club address when club information is present, otherwise the
individual address. Exact special-event calls use the listed trustee's province;
temporary prefix substitutions use the ordinary base call's province. Blank,
unsupported or conflicting province evidence stays empty. A registered address
can differ from the operating location, as with FCC mailing state.

The existing `CallMetadata.State`, `DESTATE` and `DXSTATE` contracts carry both
the 60 FCC codes and 13 Canadian codes (`AB`, `BC`, `MB`, `NB`, `NL`, `NS`, `NT`,
`NU`, `ON`, `PE`, `QC`, `SK`, `YT`). Mixed selections such as
`PASS RULE 0 DXSTATE=NY,ON` compare observed canonical State metadata. Missing,
invalid or noncanonical State makes comparisons false, including `<>`; NOT
negates that result. NEARBY structurally removes geographic atoms from effective
rules without rewriting full saved expressions. History uses the state
recorded when the spot was archived; existing rows are not backfilled from the
current snapshot.

Back up user profiles, preset libraries and the archive with writers stopped
before changing binaries. Native user records, named entries and applied
baselines use configuration version 5; machine configuration uses explicit
schema 5 only. Archive version 6 is separate factual metadata history: it
records observed DX/DE State, while archive versions 2-5 retain unknown State
without consulting today's registry. Historically, earlier Canadian-data
readers could reject codes or skip archive rows even when a version matched;
a downgrade requires a matching backup. Do not repair a failed import by
deleting the last good database or rewriting protected files. See
[ADR-0254](../../docs/decisions/ADR-0254-canadian-ised-license-and-state-reuse.md).

### FCC/ISED diagnostic lookup

Check the complete redacted warning, effective `data.yaml`, source archive pair
and last successful publication before changing files. These diagnostics do not
by themselves prove that a callsign is unassigned or that stored history is wrong.

| Exact diagnostic | What it establishes | First safe check |
| --- | --- | --- |
| `FCC ULS refresh failed` | A startup FCC acquisition/build attempt returned an error. | Read the appended cause and check the configured archive/database paths and permissions; failed replacement retains the last good database. |
| `ISED startup refresh failed` | Startup reconciliation or Canadian refresh returned an error. | Read the appended cause and check both source URLs, archive paths and database path as one pair. |
| `ISED processing metadata unavailable` | Writing an archive processing-status sidecar failed. | Check the appended filesystem error and sidecar permissions; sidecar success/failure alone does not establish database publication. |
| `ised: invalid source manifest` | A database source-pair hash is not a 64-character hexadecimal SHA-256 value. | Check that the configured Canadian database belongs to this installation and inspect preceding refresh/publication messages; do not edit database rows as a first step. |
| `unterminated final record` | An ISED input ends with a nonempty record lacking its terminating newline. | Preserve the error and check whether both downloaded archives completed; a failed import must not be treated as a successful new snapshot. |

Source ownership: [FCC refresh](../../uls/downloader.go),
[Canadian refresh](../../uls/ised_refresh.go),
[database probe](../../uls/ised_lookup.go), and
[ISED record framing](../../uls/ised_import.go). Existing qualification evidence
is in [FCC validation](../../docs/fcc-state-validation.md) and
[Canadian validation](../../docs/canadian-state-validation.md).
