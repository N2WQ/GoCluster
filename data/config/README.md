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
- `path_reliability.native_160m_fallback.enabled: true` uses exact solar path geometry to fill insufficient 160m p50 results with conservative `LOW` or `UNLIKELY` glyphs when enough of the great-circle path is darker than civil twilight. It does not predict SNR, never emits `HIGH` or `MEDIUM`, never overrides sufficient p50, and yields to any usable current-hour VOACAP fallback result. `display_enabled: false` keeps the evaluation in aggregate counters without changing glyphs.
- `path_reliability.voacap_fallback.enabled: false` starts no SSN polling or VOACAP worker. When enabled on Windows, it fetches NOAA SSN on the configured cadence, uses rounded integer 8-hour EWMA SSN generations, persists SSN monitor continuity to `voacap_fallback.ssn_state_path`, persists completed forecast windows to the per-node Pebble DB at `voacap_fallback.forecast_cache_db_path`, delays fallback after insufficient bucket evidence, and can only replace insufficient results when VOACAP is closed, when sparse bucket p50 and the cached current-hour VOACAP forecast map to the same path class, or when REL-gated open fallback rules pass. On Linux and other non-Windows builds, startup logs that runtime VOACAP execution is unsupported and continues without the VOACAP SSN monitor, worker, or forecast cache. Existing sufficient bucket p50 results remain authoritative. `ssn_state_path` is per-node runtime state for NOAA validators, last observation, EWMA, and current rounded SSN generation; `forecast_cache_db_path` is per-node derived VOACAP prediction state and should not be shared by running cluster processes. Current restored forecast-cache records hydrate memory before workers start and bypass `voacap_fallback.delay_seconds`; stale or malformed records are pruned and a missing/unavailable cache cold-starts normal delay/queue behavior. `sparse_p50_diagnostic_max_observation_count` only defines the very-low-count diagnostic bucket for `Sparse p50 VOACAP (5m)` logs, including invalid-request reason splits; it does not change gates or queue more VOACAP work.
- `voacap_experiment.yaml` is optional for server startup. The file is validated only by the VOACAP experiment command, and its SSN smoothing, recompute delta, forecast duration, VOACAP timeout/home, and center-frequency values are experiment-owned rather than production path-reliability settings.
- `peering.yaml` in the public example uses disabled `.example.invalid` peers, blank passwords, and placeholder callsigns. Put real peer connection details only in a private config directory.
- `peering.max_peers` is a required integer from 1 through 64, including when peering is disabled. The shipped value is 64; there is no omitted, null, zero, or unlimited fallback. Existing private configurations must add this key explicitly. When peering is enabled, the number of enabled direct peer identities must fit this cap; disabled rows do not count, and a `both` direction counts once. An over-cap dormant registry remains loadable while peering is disabled, but enabling it fails startup. Changes require a restart, and lowering the cap never silently selects a subset of peers. The cap bounds direct sessions and recovery identities; the pending-handshake limit remains 128, with at most `max_peers + 128` transport owners. It does not limit remote topology population or raise the existing memory budget.
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
- Post-login `login_greeting`: same tokens as above (with real values after login).
- Input guardrails `input_too_long_message`/`input_invalid_char_message`: `<CONTEXT>`, `<MAX_LEN>`, `<ALLOWED>`.
- Dialect status `dialect_welcome_message`: `<DIALECT>`, `<DIALECT_SOURCE>`, `<DIALECT_DEFAULT>` (source labels come from `dialect_source_default_label`/`dialect_source_persisted_label`).
- Path reliability `path_status_message`: `<GRID>`, `<NOISE>`.

Input behavior:
- Human telnet input is normalized to uppercase as it is read; the echoed characters are uppercase as well.
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
- Human `SHOW FILTER`, `SHOW FILTER FULL`, `SHOW FILTER <category>` and
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
holds shared named snapshots for the owner callsign; back it up with user records.

`configuration_version: 2` identifies exact current records and snapshots. An
absent marker selects legacy migration; version 1 preserves old exact rules and
adds unrestricted DX/DE state domains, including nested preset baselines. Migration
is written on the next ordinary successful save. Current-version reads preserve false
map entries, empty selections, zero values and default choices. An unreadable,
malformed or unsupported record is preserved, and the session uses temporary
defaults with a warning. LOAD, machine writes and SAVE PRESET are rejected for
that protected record; SAVE fails before writing the named library. Ordinary
preference and teardown saves also preserve it. If only the login timestamp/IP
save fails after a successful read, login continues with the restored preferences
and preset reference plus a warning.

User-record writes atomically commit preferences together with the applied preset
name and one bounded baseline. LOAD records the applied snapshot, including
reported server adjustments; SAVE associates the same capture written to the
named library. If the named save succeeds but its association fails, the previous
live and disk association/baseline remain recoverable. Ordinary saves preserve
that reference, and library overwrite or deletion does not alter the applied
snapshot. Reconnect restores the reference for that full callsign. Preferences
differing from the baseline show `(modified)`; reversing them clears the label.

The existing preset budgets remain 20 names per owner, 256 KiB per encoded
snapshot and 8 MiB per collection. Valid large presets can LOAD even when a FULL
or YAML readback exceeds 64 KiB. PUT/PATCH/VALIDATE instead require the resulting
complete CONFIG readback to fit, including reserved response metadata. An
unchanged PUT still persists before success, repairing an earlier failed human
save without resetting unrelated solar, GRID, NEARBY, diagnostic or pause state.

There is no separate migration job. Stop writers before backup, repair or restore;
use one writer process per user-data directory. Older binaries may discard the
new fields or reject marked preset snapshots, so downgrade with a matching binary
and data backup. See [backup instructions](../../docs/ENVIRONMENT.md#user-configuration-backups-and-downgrades)
and [ADR-0244](../../docs/decisions/ADR-0244-exact-configuration-persistence.md).

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
`NU`, `ON`, `PE`, `QC`, `SK`, `YT`). Mixed filters such as `PASS DXSTATE NY,ON`
use the existing filter composition and NEARBY behavior. History uses the state
recorded when the spot was archived; existing rows are not backfilled from the
current snapshot.

Back up user profiles, preset libraries and the archive with writers stopped
before upgrading. New saved records use version 2 and new archive records use
version 6; the Canadian extension retains those versions and machine YAML
schemas 1 and 2. Schema 1 retains its existing hidden State rules, and schema 2
exposes the shared 73-code vocabulary. Earlier binaries can reject Canadian
codes in profiles/presets or skip Canadian archive rows even when the version
number matches; a deployment downgrade requires a matching backup. Do not
repair a failed import by deleting the last good database or rewriting
protected files. See
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
