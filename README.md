# DX Cluster Server

A modern Go-based DX cluster that aggregates amateur radio spots, enriches them with CTY metadata, and broadcasts them to telnet clients.

## Architecture and Spot Sources

1. **Telnet Server** (`telnet/server.go`) handles client connections, commands, and spot broadcasting using worker goroutines.
2. **RBN Clients** (`rbn/client.go`) maintain connections to the CW/RTTY (port 7000) and Digital (port 7001) feeds. Each line is parsed, normalized, validated against the CTY database, and enriched before queuing.
   - The parser no longer assumes rigid column offsets. `splitSpotterToken` first separates the `DX de CALL:freq` token so the spotter callsign stays clean even when the telnet feed glues the dial frequency directly to the colon (`DX de JI1HFJ-#:1294068.2 ...`). The detached fragment is inserted back into the token slice so the rest of the pipeline still sees a proper numeric field.
   - After token splitting, `findFrequencyField` scans the remaining slices for the first numeric value that looks like a dial frequency (currently `100.0`–`3,000,000.0` kHz). This protects against feeds that echo the spotter call or inject other text ahead of the frequency and eliminates the "`RBN spot missing numeric frequency`" warnings that appeared when a string such as `JJ1QLT` occupied the fixed slot.
   - The dynamic index returned by `findFrequencyField` is now used to pick the DX call, mode, signal report, CW WPM section, and comment/time window. As a result, even malformed-but-salvageable lines stay in sync, and the skew-correction logic keeps receiving the exact numeric dial value.
3. **PSKReporter MQTT** (`pskreporter/client.go`) subscribes to `pskr/filter/v2/+/+/#` (or one or more `pskr/filter/v2/+/<MODE>/#` topics when `pskreporter.modes` is configured), converts JSON payloads into canonical spots, and applies locator-based metadata. Set `pskreporter.append_spotter_ssid: true` if you want receiver callsigns that lack SSIDs to pick up a `-#` suffix for deduplication.
4. **CTY Database** (`cty/parser.go` + `data/cty/cty.plist`) performs longest-prefix lookups so both spotters and spotted stations carry continent/country/CQ/ITU/grid metadata.
5. **Dedup Engine** (`dedup/deduplicator.go`) filters duplicates before they reach the ring buffer. A zero-second window effectively disables dedup, but the pipeline stays unified.
6. **Frequency Averager** (`spot/frequency_averager.go`) merges CW/RTTY skimmer reports by averaging corroborating reports within a tolerance and rounding to 0.1 kHz once the minimum corroborators is met.
7. **Call/Harmonic Guards** (`spot/correction.go`, `spot/harmonics.go`, `main.go`) apply consensus-based call corrections and suppress harmonics; the pipeline logs/dashboards both the correction and the suppressed harmonic frequency. Harmonic suppression now supports a stepped minimum dB delta (configured via `harmonics.min_report_delta_step`) so higher-order harmonics must be progressively weaker. Call correction also honours `call_correction.min_snr_cw` / `min_snr_rtty` so marginal decodes can be ignored when counting corroborators. Calls ending in `/B` (standard beacon IDs) are auto-tagged and bypass these correction/harmonic steps.
8. **Skimmer Frequency Corrections** (`cmd/rbnskewfetch`, `skew/`, `rbn/client.go`, `pskreporter/client.go`) download SM7IUN’s skew list, convert it to JSON, and apply per-spotter multiplicative factors before any callsign normalization for every CW/RTTY skimmer feed.

## Data Flow and Spot Record Format

```
[Source: RBN/PSKReporter] → Parser → CTY Lookup → Dedup (window-driven) → Ring Buffer → Telnet Broadcast
```

```
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃                         DXCluster Spot Ingestion & Delivery                         ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
	┌────────────────────┐    ┌────────────────────┐    ┌─────────────────────────┐
	│ RBN CW/RTTY client │    │ RBN FT4/FT8 client │    │ PSKReporter MQTT client │
	└──────────┬─────────┘    └──────────┬─────────┘    └──────────┬──────────────┘
		   │                         │                         │
		   ▼                         ▼                         ▼
	┌────────────────────┐    ┌────────────────────┐    ┌─────────────────────────┐
	│ RBN line parsers   │    │ RBN digital parsers│    │ PSKReporter worker pool │
	└──────────┬─────────┘    └──────────┬─────────┘    └──────────┬──────────────┘
		   │                         │                         │
		   ├─────────────────────────┴─────────────────────────┤
		   ▼                                                   ▼
	 ┌────────────────────────────────────────────────────────────────────┐
	 │ Normalize callsigns → validate → CTY lookup → enrich metadata      │
	 │ (shared logic in `spot` + `cty` packages)                           │
	 └──────────────────────────────┬──────────────────────────────────────┘
					│
					▼
			  ┌──────────────────────────────┐
			  │ Dedup engine (cluster/user)  │
			  └───────────────┬──────────────┘
					  │
					  ▼
			  ┌──────────────────────────────┐
			  │ Ring buffer (`buffer/`)      │
			  └───────────────┬──────────────┘
					  │
					  ▼
			  ┌──────────────────────────────┐
			  │ Telnet server (`telnet/`)    │
			  └───────────────┬──────────────┘
					  │
					  ▼
			  Connected telnet clients + filters
```

Each `spot.Spot` stores:
- **ID** – monotonic identifier
- **DXCall / DECall** – uppercased callsigns
- **Frequency** (kHz), **Band**, **Mode**, **Report** (dB/SNR)
- **Time** – UTC timestamp from the source
- **Comment** – parsed message or `Locator>Locator`
- **SourceType / SourceNode** - origin tags (`RBN`, `RBN-DIGITAL`, `PSKREPORTER`, etc.)
- **TTL** - hop count preventing loops
- **IsHuman** - whether the spot was reported by a human operator (automated feeds set this to false)
- **IsBeacon** - true when the DX call ends with `/B` or the comment mentions `NCDXF`/`BEACON` (used to suppress beacon corrections/filtering)
- **DXMetadata / DEMetadata** - structured `CallMetadata` each containing:
	- `Continent`
	- `Country`
	- `CQZone`
	- `ITUZone`
	- `Grid`

Both the RBN (standard and digital) and PSKReporter feeds run the same normalization + CTY lookup validation before putting a spot into the ring buffer. Callsigns containing `.` suffixes (e.g., `JA1CTC.P` or `W6.UT5UF`) now have their periods converted to `/` so the full call-plus-suffix reaches CTY lookup rather than being truncated to the base call, and validation now requires at least one digit to avoid non-amateur identifiers before malformed or unknown calls are filtered out prior to hashing or deduplication. Automated feeds mark the `IsHuman` flag as `false` so downstream processors can tell which spots originated from telescopic inputs versus human operator submissions.

## Commands

Telnet clients can issue commands via the prompt once logged in. The processor, located in `commands/processor.go`, supports the following general commands:

- `HELP` / `H` – display the help text that includes short summaries and valid bands/modes at the bottom of the message.
- `SHOW DX [N]` / `SHOW/DX [N]` – stream the most recent `N` spots directly from the shared ring buffer (`N` ranges from 1–100, default 10). The command accepts the alias `SH DX` as well.
- `BYE`, `QUIT`, `EXIT` – request a graceful logout; the server replies with `73!` and closes the connection.

Filter management commands are implemented directly in `telnet/server.go` and operate on each client’s `filter.Filter`. They can be used at any time and fall into `SET`, `UNSET`, and `SHOW` groups:

- `SHOW/FILTER` – prints the current filter state for bands, modes, and callsigns.
- `SHOW/FILTER MODES` – lists every supported mode along with whether it is currently enabled for the session.
- `SHOW/FILTER BANDS` – lists all supported bands that can be enabled.
- `SET/FILTER BAND <band>[,<band>...]` – enables filtering for the comma- or space-separated list (each item normalized via `spot.NormalizeBand`), or specify `ALL` to accept every band; use the band names from `spot.SupportedBandNames()`.
- `SET/FILTER MODE <mode>[,<mode>...]` - enables one or more modes (comma- or space-separated) that must exist in `filter.SupportedModes`, or specify `ALL` to accept every mode.
- `SET/FILTER CALL <pattern>` - begins delivering only spots matching the supplied callsign pattern.
- `SET/FILTER CONFIDENCE <symbol>[,<symbol>...]` - enables the comma- or space-separated list of consensus glyphs (valid symbols: `?`, `S`, `C`, `P`, `V`, `B`; use `ALL` to accept every glyph).
- `SET/FILTER BEACON` - explicitly enable delivery of beacon spots (DX calls ending `/B`; enabled by default).
- `UNSET/FILTER ALL` - resets every filter back to the default (no filtering).
- `UNSET/FILTER BAND <band>[,<band>...]` - disables only the comma- or space-separated list of bands provided (use `ALL` to clear every band filter).
- `UNSET/FILTER MODE <mode>[,<mode>...]` - disables only the comma- or space-separated list of modes provided (specify `ALL` to clear every mode filter).
- `UNSET/FILTER CALL` - removes all callsign patterns.
- `UNSET/FILTER CONFIDENCE <symbol>[,<symbol>...]` - disables only the comma- or space-separated list of glyphs provided (use `ALL` to clear the whitelist).
- `UNSET/FILTER BEACON` - drop beacon spots entirely (they remain tagged internally for future processing).
- `SHOW/FILTER BEACON` - display the current beacon-filter state.
- `SHOW/FILTER CONFIDENCE` - lists each glyph alongside whether it is currently enabled.

Confidence glyphs are only emitted for modes that run consensus-based correction (CW/RTTY/SSB). FT8/FT4 spots carry no confidence glyphs, so confidence filters do not affect them.

Band, mode, and confidence commands share identical semantics: they accept comma- or space-separated lists, ignore duplicates/case, and treat the literal `ALL` as a shorthand to reset that filter back to "allow every band/mode/confidence glyph."

Confidence indicator legend in telnet output:

- `?` - 25% consensus or less
- `S` - 25% or less but spotted callsign is in `MASTER.SCP`
- `B` - consensus suggested a correction but CTY validation failed (busted call retained)
- `P` - 25-75% consensus
- `V` - more than 75% consensus
- `C` - callsign was corrected by consensus

Use `SET/FILTER CONFIDENCE` with the glyphs above to whitelist the consensus levels you want to see (for example, `SET/FILTER CONFIDENCE P,V` keeps strong/very strong reports while dropping `?`/`S`/`B` entries).

Use `UNSET/FILTER BEACON` to suppress DX beacons when you only want live operator traffic; `SET/FILTER BEACON` re-enables them, and `SHOW/FILTER BEACON` reports the current state. Regardless of delivery, `/B` spots are excluded from call-correction, frequency-averaging, and harmonic checks.
Errors during filter commands return a usage message (e.g., invalid bands or modes refer to the supported lists) and the `SHOW/FILTER` commands help confirm the active settings.

## RBN Skew Corrections

1. Enable the `skew` block in `config.yaml` (the server writes to `skew.file` after each refresh):

```yaml
skew:
  enabled: true
  url: "https://sm7iun.se/rbnskew.csv"
  file: "data/skm_correction/rbnskew.json"
```

2. (Optional) Run `go run ./cmd/rbnskewfetch -out data/skm_correction/rbnskew.json` once to pre-seed the JSON file before enabling the feature.
3. Restart the cluster. At startup, it loads the JSON file (if present) and then fetches the CSV at the next `skew.refresh_utc` boundary (default `00:30` UTC). The built-in scheduler automatically refreshes the list every day at that UTC time and rewrites `skew.file`, so no external cron job is required.

Each RBN spot uses the *raw* spotter string (SSID intact, before any normalization) to look up the correction. If found, the original frequency is multiplied by the factor before any dedup, CTY validation, call correction, or harmonic detection runs. This keeps SSID-specific skew data aligned with the broadcast nodes.

To match the 100 Hz accuracy of the underlying skimmers, the corrected frequency is rounded to the nearest 0.1 kHz before it continues through the pipeline.

## Known Calls Cache

1. Populate the `known_calls` block in `config.yaml`:

```yaml
known_calls:
  enabled: true
  url: "https://www.supercheckpartial.com/MASTER.SCP"
  file: "data/scp/MASTER.SCP"
  refresh_utc: "01:15"
```

2. On startup the server checks `known_calls.file`. If it is missing and `known_calls.url` is set, the file is downloaded immediately before any spots are processed. The freshly written file is then parsed into the in-memory cache so consensus/confidence checks can use it right away.
3. When `known_calls.enabled` is true, the built-in scheduler refreshes the file every day at `known_calls.refresh_utc` (default `01:00` UTC). Each download writes to a temporary file, swaps it into place atomically, and updates the runtime cache without needing a restart.

You can disable the scheduler by setting `known_calls.enabled: false`. In that mode the server will still load whatever file already exists (and will fetch it once at startup if an URL is provided), but it will not refresh it automatically.

## Grid Persistence and Caching

- Grids and known-call flags are stored in SQLite at `grid_db` (default `data/grids/calls.db`). The DB uses WAL mode with `synchronous=NORMAL` for fast, durable batching.
- Writes are batched by `grid_flush_seconds` (default `60s`); a final flush runs during shutdown.
- The in-memory grid cache is a bounded LRU of size `grid_cache_size` (default `100000`). Cache misses fall back to SQLite, keeping startup O(1) even as the database grows.
- The stats line `Grids: +X / Y since start / Z in DB` counts accepted grid changes (not repeated identical reports); `Z` is the current row count from SQLite.
- If you set `grid_ttl_days > 0`, the store purges rows whose `updated_at` timestamp is older than that many days right after each SCP refresh. Continuous SCP membership or live grid updates keep records fresh automatically.

## Runtime Logs and Corrections

- **Call corrections**: `2025/11/19 18:50:45 Call corrected: VE3N -> VE3NE at 7011.1 kHz (8 / 88%)`
- **Frequency averaging**: `2025/11/19 18:50:45 Frequency corrected: VE3NE 7011.3 -> 7011.1 kHz (8 / 88%)`
- **Harmonic suppression**: `2025/11/19 18:50:45 Harmonic suppressed: VE3NE 14022.0 -> 7011.0 kHz (3 / 18 dB)` plus a paired frequency-corrected line indicating the fundamental retained.

### Sample Session

Below is a hypothetical telnet session showing the documented commands in action (server replies are shown after each input):

```
telnet localhost 7300
Experimental DX Cluster
Please login with your callsign

Enter your callsign:
N1ABC
Hello N1ABC, you are now connected.
Type HELP for available commands.
HELP
Available commands:
... (supported modes/bands summary)
SHOW/DX 5
DX1 14.074 FT8 599 N1ABC>W1XYZ
DX2 14.070 FT4 26 N1ABC>W2ABC
...
SET/FILTER BAND 20M
Filter set: Band 20M
SET/FILTER MODE FT8,FT4
Filter set: Modes FT8, FT4
SET/FILTER CONFIDENCE P,V
Confidence symbols enabled: P, V
SHOW/FILTER MODES
Supported modes: FT8=ENABLED, FT4=ENABLED, CW=DISABLED, ...
SHOW/FILTER CONFIDENCE
Confidence symbols: ?=DISABLED, S=DISABLED, C=ENABLED, P=ENABLED, V=ENABLED, B=DISABLED
SHOW/FILTER
Current filters: Bands: 20M | Modes: FT8, FT4 | Confidence: P, V
UNSET/FILTER MODE FT4
Mode filters disabled: FT4
UNSET/FILTER ALL
All filters cleared
BYE
73!
```

Use these commands interactively to tailor the spot stream to your operating preferences.

### Telnet Throughput Controls

The telnet server fans every post-dedup spot to every connected client. When PSKReporter or both RBN feeds spike, the broadcast queue can saturate and you'll see `Broadcast channel full, dropping spot` along with a rising `Telnet drops` metric in the stats ticker. Tune the `telnet` block in `config.yaml` to match your load profile:

- `broadcast_workers` keeps the existing behavior (`0` = auto at half your CPUs, minimum 2).
- `broadcast_queue_size` controls the global queue depth ahead of the worker pool (default `2048`); larger buffers smooth bursty ingest before anything is dropped.
- `worker_queue_size` controls how many per-shard jobs each worker buffers before dropping a shard assignment (default `128`).
- `client_buffer_size` defines how many spots a single telnet session can fall behind before its personal queue starts dropping (default `128`).

Increase the queue sizes if you see the broadcast-channel drop message frequently, or raise `broadcast_workers` when you have CPU headroom and thousands of concurrent clients.

## Project Structure

```
C:\src\gocluster\
├── buffer\              # In-memory ring buffer storing processed spots
│   └── ringbuffer.go
├── commands\            # Command parser/processor for telnet sessions
│   └── processor.go
├── config\              # YAML configuration loader (`config.yaml`)
│   └── config.go
├── cty\                 # CTY prefix parsing and lookup (data enrichment)
│   └── parser.go
├── cmd\                 # Utilities (interactive CTY lookup CLI)
│   └── ctylookup\main.go
├── dedup\               # Deduplication engine and window management
│   └── deduplicator.go
├── filter\              # Per-user filter defaults and helpers
│   └── filter.go
├── pskreporter\         # MQTT client for PSKReporter FT8/FT4 spots
│   └── client.go
├── rbn\                 # Reverse Beacon Network TCP client/parser
│   └── client.go
├── spot\                # Canonical spot definition and helpers
│   └── spot.go
├── stats\               # Runtime statistics tracking
│   └── stats.go
├── telnet\              # Telnet server and broadcast helpers
│   └── server.go
├── main.go               # Entry point wiring config, clients, dedup, and telnet server
├── config.yaml          # Runtime configuration
├── data/cty/cty.plist   # CTY prefix database for metadata lookups
├── go.mod               # Go module definition
├── go.sum               # Dependency checksums
└── README.md            # This documentation
```

## Getting Started

1. Update `config.yaml` with your preferred callsigns for the `rbn`, `rbn_digital`, and optional `pskreporter` sections. Optionally list `pskreporter.modes` (e.g., [`FT8`, `FT4`]) to subscribe to just those MQTT feeds simultaneously.
2. Optionally enable/tune `call_correction` (master `enabled` switch, minimum corroborating spotters, required advantage, confidence percent, recency window, max edit distance, per-mode distance models, and `invalid_action` failover). `distance_model_cw` switches CW between the baseline rune-based Levenshtein (`plain`) and a Morse-aware cost function (`morse`), `distance_model_rtty` toggles RTTY between `plain` and a Baudot/ITA2-aware scorer (`baudot`), while SSB/voice modes always stay on `plain` because those reports are typed by humans.
3. Optionally enable/tune `harmonics` to drop harmonic CW/USB/LSB/RTTY spots (master `enabled`, recency window, maximum harmonic multiple, frequency tolerance, and minimum report delta).
4. Set `spot_policy.max_age_seconds` to drop stale spots before they're processed further. For CW/RTTY frequency smoothing, tune `spot_policy.frequency_averaging_seconds` (window), `spot_policy.frequency_averaging_tolerance_hz` (allowed deviation), and `spot_policy.frequency_averaging_min_reports` (minimum corroborating reports).
5. (Optional) Enable `skew.enabled` after generating `skew.file` via `go run ./cmd/rbnskewfetch` (or let the server fetch it at the next 00:30 UTC window). The server applies each skimmer's multiplicative correction before normalization so SSIDs stay unique.
6. If you maintain a historical callsign list, set `known_calls.file` plus `known_calls.url` (leave `enabled: true` to keep it refreshed). On first launch the server downloads the file if missing, loads it into memory, and then refreshes it daily at `known_calls.refresh_utc`.
7. Grids/known calls are persisted in SQLite (`grid_db`, default `data/grids/calls.db`). Tune `grid_flush_seconds` for batch cadence, `grid_cache_size` to bound the in-memory LRU used for grid comparisons, and `grid_ttl_days` if you want old, unobserved calls to expire automatically after a configurable number of days.
8. Adjust `stats.display_interval_seconds` in `config.yaml` to control how frequently runtime statistics print to the console (defaults to 30 seconds).
9. Install dependencies and run:
	 ```pwsh
	 go mod tidy
	 go run main.go
	 ```
10. Connect via `telnet localhost 7300`, enter your callsign, and the server will immediately stream real-time spots.

## Testing & Tooling

- `go test ./...` validates packages (not all directories contain tests yet).
- `gofmt -w ./...` keeps formatting consistent.
- `go run cmd/ctylookup -data data/cty/cty.plist` lets you interactively inspect CTY entries used for validation.
- `go run cmd/rbnskewfetch -out data/skm_correction/rbnskew.json` forces an immediate skew-table refresh (the server still performs automatic downloads at 00:30 UTC daily).

Let me know if you want diagrams, sample logs, or scripted deployment steps added next.
