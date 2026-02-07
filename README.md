# DX Cluster Server

A modern Go-based DX cluster that aggregates amateur radio spots, enriches them with CTY metadata, and broadcasts them to telnet clients.

## Quickstart

1. Install Go `1.25+` (see `go.mod`).
2. Edit `data/config/` (at minimum: set your callsigns in `ingest.yaml` and `telnet.port` in `runtime.yaml`). If you plan to peer with other DXSpider nodes, populate `peering.yaml` (local callsign, peers, ACL/passwords). You can override the path with `DXC_CONFIG_PATH` if you want to point at a different config directory.
3. Run:
   ```pwsh
   go mod tidy
   go run .
   ```
4. Connect: `telnet localhost 9300` (or whatever `telnet.port` is set to in `data/config/runtime.yaml`).

## Diagnostics (optional)

- `DXC_PPROF_ADDR` enables the `/debug/pprof/*` server (example: `localhost:6061`).
- `DXC_HEAP_LOG_INTERVAL` logs heap stats every interval (e.g., `60s`).
- `DXC_MAP_LOG_INTERVAL` logs key map sizes (call-quality, stats cardinality, dedup caches, path buckets) every interval.
- `DXC_BLOCK_PROFILE_RATE` enables block profiling (Go duration or nanoseconds).
- `DXC_MUTEX_PROFILE_FRACTION` enables mutex profiling (integer 1/N).

## Architecture and Spot Sources

1. **Telnet Server** (`telnet/server.go`) handles client connections, commands, and spot broadcasting using worker goroutines.
2. **RBN Clients** (`rbn/client.go`) maintain connections to the CW/RTTY (port 7000) and Digital (port 7001) feeds. Each line is parsed and normalized, then sent through the shared ingest CTY/ULS gate for validation and enrichment before queuing.
   - Parsing uses a single left-to-right token walk assisted by an Aho–Corasick (AC) keyword scanner so the line only needs to be scanned once.
   - The parser supports both `DX de CALL: 14074.0 ...` and the glued form `DX de CALL:14074.0 ...` by splitting the spotter token into `DECall` + optional attached frequency.
   - Frequency is the first token that parses as a plausible dial frequency (currently `100.0`-`3,000,000.0` kHz), rather than assuming a fixed column index.
   - Mode is taken from the first explicit mode token (`CW`, `USB`, `JS8`, `SSTV`, `FT8`, `MSK144`, etc.). If mode is absent, it is inferred from `data/config/mode_allocations.yaml` (with a simple fallback: `USB` >= 10 MHz else `CW`).
   - Report/SNR is recognized in both `+5 dB` and `-13dB` forms; `HasReport` is set whenever a report is present (RBN/RBN-Digital zero-SNR reports are dropped before ingest).
   - Ingest burst protection is sized per source via `rbn.slot_buffer` / `rbn_digital.slot_buffer` in `data/config/ingest.yaml`; overflow logs are tagged by source for easier diagnosis.
3. **PSKReporter MQTT** (`pskreporter/client.go`) subscribes to a single catch-all `pskr/filter/v2/+/+/#` topic and filters modes downstream according to `pskreporter.modes`. It converts JSON payloads into canonical spots and preserves locator-based grids. Set `pskreporter.append_spotter_ssid: true` if you want receiver callsigns that lack SSIDs to pick up a `-#` suffix for deduplication. PSKReporter spots no longer carry a comment string; DX/DE grids stay in metadata and are shown in the fixed tail of telnet output. Configure `pskreporter.max_payload_bytes` to guard against oversized payloads; CTY caching is handled by the unified call metadata cache. PSKReporter spots with explicit `0 dB` reports (rp=0) are dropped before ingest; missing reports are treated as absent. `pskreporter.path_only_modes` routes specific modes (e.g., WSPR) directly into path prediction only—they never reach dedup, telnet, archive, or peer output, and they bypass CTY validation. MQTT ingest is bounded by `pskreporter.mqtt_inbound_workers`, `pskreporter.mqtt_inbound_queue_depth`, and `pskreporter.mqtt_qos12_enqueue_timeout_ms` (QoS0 drops when full; QoS1/2 disconnect after the enqueue timeout).
   - PSK modes are normalized to a canonical `PSK` family for filtering, dedupe, and stats while preserving the reported variant (PSK31/63/125) in telnet/archive output.
4. **CTY Database** (`cty/parser.go` + `data/cty/cty.plist`) performs longest-prefix lookups; when a callsign includes slashes, it prefers the shortest matching segment (portable/location prefix), so `N2WQ/VE3` and `VE3/N2WQ` both resolve to `VE3` (Canada) for metadata. The in-memory CTY DB is paired with a unified call metadata cache so repeated lookups do not thrash the trie.
5. **Dedup Engine** (`dedup/deduplicator.go`) filters duplicates before they reach the ring buffer. A zero-second window effectively disables dedup, but the pipeline stays unified. A secondary, broadcast-only dedupe runs after call correction/harmonic/frequency adjustments to collapse repeat DX reports without altering ring/history. It hashes band + DE ADIF (DXCC) + DE grid2 prefix (FAST/MED) or DE CQ zone (SLOW) + normalized DX call + source class (human vs skimmer); the time window is enforced by the cache, so one spot per window per key reaches clients while the ring/history remain intact. Three secondary policies are available: **fast** (120s, grid2), **med** (300s, grid2), and **slow** (480s, CQ zone), each with its own `secondary_*_prefer_stronger_snr` toggle in `data/config/dedupe.yaml`. Telnet clients select with `SET DEDUPE FAST|MED|SLOW` (use `SHOW DEDUPE` to confirm); default is MED. Archive and peer publishing use the MED policy. The console pipeline line reports per-policy output as `<count>/<percent> (F) / <count>/<percent> (M) / <count>/<percent> (S)`. When a policy's prefer-stronger flag is true, the stronger SNR duplicate replaces the cached entry and is broadcast for that policy. Spotter SSID display is controlled at broadcast time (see `rbn.keep_ssid_suffix`); when disabled, telnet output, archive, and filters use stripped DE calls while peers keep the raw calls.
6. **Frequency Averager** (`spot/frequency_averager.go`) merges CW/RTTY skimmer reports by averaging corroborating reports within a tolerance and rounding to 0.1 kHz once the minimum corroborators is met.
7. **Call/Harmonic/License Guards** (`spot/correction.go`, `spot/harmonics.go`, `main.go`) apply consensus-based call corrections, suppress harmonics, and finally run FCC license gating for DX right before broadcast/buffering (CTY validation runs in the ingest gate; corrected calls are re-validated against CTY before acceptance). Harmonic suppression supports a stepped minimum dB delta (configured via `harmonics.min_report_delta_step`) so higher-order harmonics must be progressively weaker. Call correction honours `call_correction.min_snr_cw` / `min_snr_rtty` (and `min_snr_voice` if set) so marginal decodes can be ignored when counting corroborators; USB/LSB uses a wider frequency tolerance and candidate search window to reflect 3 kHz SSB bandwidth. Quality anchors are still available, but anchor-selected winners now pass the same gates as consensus winners (min reports, advantage, confidence, freq guard, cooldown) before apply. An optional cooldown gate (`call_correction.cooldown_*`) can temporarily refuse to flip away from a call that already has recent diverse support unless the alternate is decisively stronger; `cooldown_min_reporters` follows `adaptive_min_reports` per band when enabled, and cooldown rejections log as `reason=cooldown` in the decision DB. Decision traces also include a path marker (`decision_path=anchor|consensus`) so operators can audit which apply path was used. Calls ending in `/B` (standard beacon IDs) are auto-tagged and bypass correction/harmonic/license drops (only user filters can hide them). The license gate uses a license-normalized base call (e.g., `W6/UT5UF` -> `UT5UF`) to decide if FCC checks apply and which call to query, while CTY metadata still reflects the portable/location prefix (so `N2WQ/VE3` reports Canada for DXCC but uses `N2WQ` for licensing); drops appear in the "Unlicensed US Calls" pane.
8. **Skimmer Frequency Corrections** (`cmd/rbnskewfetch`, `skew/`, `rbn/client.go`, `pskreporter/client.go`) download SM7IUN's skew list, convert it to JSON, and apply per-spotter multiplicative factors before any callsign normalization for every CW/RTTY skimmer feed.

### Aho–Corasick Spot Parsing (Non-PSKReporter)

Non-PSKReporter sources (RBN CW/RTTY, RBN digital, and upstream/human telnet feeds) arrive as DX-cluster style text lines (e.g., `DX de ...`). The parser in `rbn/client.go` uses a small Aho–Corasick (AC) automaton to recognize keywords in a single pass and drive a left-to-right extraction.

High-level flow:

- **Keyword dictionary**: `DX`, `DE`, `DB`, `WPM`, plus all supported mode tokens (`CW`, `SSB` as an alias normalized to `USB`/`LSB`, `USB`, `LSB`, `JS8`, `SSTV`, `RTTY`, `FT4`, `FT8`, `MSK144`, and common variants like `FT-8`).
- **Automaton build (once)**: patterns are compiled into a trie and failure links are built with a BFS. This runs once and is reused for every line.
- **Per-line scan**:
  - Tokenize the raw line on whitespace while tracking token byte offsets.
  - Run the AC scan over the uppercased line to find keyword hits.
  - Classify each token by checking for an exact hit that spans the token (fallback: scan the token text itself when whitespace/punctuation causes slight drift).
- **Single pass extraction**: walk tokens left-to-right, consuming fields as they are discovered: spotter call (and optional `CALL:freq` attachment), frequency, DX call, mode, report (`<signed int> dB` or `<signed int>dB`), time (`HHMMZ`), then treat any remaining unconsumed tokens as the free-form comment.
- **Mode inference**: when no explicit mode token exists, infer from `data/config/mode_allocations.yaml` by frequency (fallback: `USB` ≥ 10 MHz else `CW`).
- **Report semantics**: `HasReport` is strictly “report was present in the source line” (or PSKReporter rp field), so `0 dB` is distinct from “missing report” on sources that retain it (RBN/PSKReporter zero-SNR drops happen before ingest).

### Call-Correction Distance Tuning
- CW distance can be Morse-aware with weighted/normalized dot-dash costs (configurable via `call_correction.morse_weights`: `insert`, `delete`, `sub`, `scale`; defaults 1/1/2/2).
- RTTY distance can be ITA2-aware with similar weights (configurable via `call_correction.baudot_weights`: `insert`, `delete`, `sub`, `scale`; defaults 1/1/2/2).
- If you prefer plain rune-based Levenshtein, set `call_correction.distance_model_cw: plain` and/or `distance_model_rtty: plain`.
- You can seed call-quality anchors from your own data with `call_correction.quality_priors_file` (format: `CALL SCORE [FREQ_KHZ]`; omit/<=0 freq to apply globally). Higher scores make a call more likely to be selected as an anchor candidate in that bin, but anchor candidates still pass the same safety gates as consensus winners.
- Call-quality anchors are bounded by `call_correction.call_quality_ttl_seconds` and `call_quality_max_entries`; `call_quality_pin_priors` keeps priors non-expiring, and `call_quality_cleanup_interval_seconds` controls the cleanup cadence.
- You can down-weight noisy reporters via `call_correction.spotter_reliability_file` (format: `SPOTTER WEIGHT 0-1`) and `call_correction.min_spotter_reliability` to ignore spotters below a floor. These weights apply only to call-correction consensus; other processing is unchanged.

## UI Modes (local console)

- `ui.mode: ansi` (default) draws the fixed 90x68 ANSI console in the server's terminal when stdout is a TTY. The layout is 12 stats lines, a blank line, then Dropped/Corrected/Unlicensed/Harmonics/System Log panes with 10 lines each. Pane headers render as `<<<<<<<<<< Dropped >>>>>>>>>>`, `<<<<<<<<<< Corrected >>>>>>>>>>`, etc. Telnet clients do **not** see this UI; if the terminal is smaller than 90x68, ANSI disables and logs continue.
- `ui.mode: tview` enables the legacy framed tview dashboard (requires an interactive console).
- `ui.mode: tview-v2` enables the page-based tview dashboard with bounded buffers and navigation keys.
  - The v2 stream panes use virtualized viewport rendering with bounded rings to reduce CPU/heap pressure at sustained update rates.
- `ui.mode: headless` disables the local console; logs continue to stdout/stderr.
- `ui.pane_lines` controls the visible heights of tview panes; ANSI uses a fixed 12/10-line layout and ignores pane_lines.
- `logging.enabled` in `app.yaml` duplicates system logs to daily files in `logging.dir` (local time, `logging.retention_days` controls retention).
- Config block (excerpt):
  ```yaml
  ui:
    mode: "ansi"       # ansi | tview | tview-v2 | headless
    refresh_ms: 250    # ANSI minimum redraw spacing; 0 renders on every event
    color: true        # ANSI coloring for marked-up lines
    clear_screen: true # Ignored by ANSI; preserved for compatibility
    pane_lines:
      stats: 8
      calls: 20
      unlicensed: 20
      harmonics: 20
      system: 40
  ```

## Propagation Reports (Daily)

The cluster can generate a daily propagation report from the prior day's log file. It triggers on log rotation and also on a fixed UTC schedule so quiet systems still produce reports.

Config block (excerpt):
```yaml
prop_report:
  enabled: true
  refresh_utc: "00:05" # UTC time to enqueue yesterday's report
```

## Data Flow and Spot Record Format

```
[Source: RBN/PSKReporter] → Parser → Ingest CTY/ULS Gate → Dedup (window-driven) → Ring Buffer → Telnet Broadcast
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
	 │ Normalize callsigns → ingest CTY/ULS checks → enrich metadata      │
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

### Telnet Spot Line Format

The telnet server broadcasts spots as fixed-width DX-cluster lines:

- Exactly **78 characters**, followed by **CRLF** (line endings are normalized in `telnet.Client.Send`).
- Column numbering below is **1-based** (column 1 is the `D` in `DX de `).
- The left side is padded so **mode always starts at column 40**.
- The displayed DX callsign uses the canonical normalized call (portable suffixes stripped) and is truncated to 10 characters to preserve fixed columns (the full normalized callsign is still stored and hashed).
- The right-side tail is fixed so clients can rely on it:
  - Grid: columns 67-70 (4 chars; blank if unknown)
  - Confidence: column 72 (1 char; blank if unknown)
  - Time: columns 74-78 (`HHMMZ`)
- Any free-form comment text is sanitized (tabs/newlines removed) and truncated so it can never push the grid/confidence/time tail; a space always separates the comment area from the fixed tail.

Report formatting:

- If a report is present: `MODE <report> dB` (e.g., `FT8 -12 dB`, `CW 27 dB`, `MSK144 +7 dB`).
- Human spots without SNR omit the report entirely and show only `MODE`.

Example:

```
DX de W3LPL:       7009.5  K1ABC       FT8 -5 dB                          FN20 S 0615Z
```

Each `spot.Spot` stores:
- **ID** - monotonic identifier
- **DXCall / DECall** - normalized callsigns (portable suffixes stripped, location prefixes like `/VE3` retained; validation accepts 3-15 characters; telnet display truncates DX to 10 as described above)
- **Frequency** (kHz), **Band**, **Mode**, **Report** (dB/SNR), **HasReport** (distinguishes missing SNR from a real 0 on sources that retain it; RBN/PSKReporter zero-SNR are dropped at ingest)
- **Time** - UTC timestamp from the source
- **Comment** - free-form message (human ingest strips mode/SNR/time tokens before storing)
- **SourceType / SourceNode** - origin tags (`RBN`, `FT8`, `FT4`, `PSKREPORTER`, `UPSTREAM`, etc.)
- **TTL** - hop count preventing loops
- **IsHuman** - whether the spot was reported by a human operator (RBN/PSKReporter spots are skimmers; peer/upstream/manual spots are human)
- **IsTestSpotter** - true for CTY-valid telnet test calls (suffix `TEST` or `TEST-SSID`); such spots are broadcast locally but never peered, archived, or stored in the ring buffer
- **IsBeacon** - true when the DX call ends with `/B` or the comment mentions `NCDXF`/`BEACON` (used to suppress beacon corrections/filtering)
- **DXMetadata / DEMetadata** - structured `CallMetadata` each containing:
	- `Continent`
	- `Country`
	- `CQZone`
	- `ITUZone`
	- `ADIF` (DXCC/ADIF country code)
	- `Grid`

All ingest sources run through a shared CTY/ULS validation gate before deduplication. Callsigns are normalized once (uppercased, dots converted to `/`, trailing slashes removed, and portable suffixes like `/P`, `/M`, `/MM`, `/AM`, `/QRP` stripped) before CTY lookup, so W1AW and W1AW/P collapse to the same canonical call for hashing and filters. Location prefixes (for example `/VE3`) are retained; CTY lookup then chooses the shortest matching slash segment so `N2WQ/VE3` and `VE3/N2WQ` both resolve to `VE3` for metadata. Validation still requires at least one digit to avoid non-amateur identifiers before malformed or unknown calls are filtered out. Automated feeds mark the `IsHuman` flag as `false` so downstream processors can tell which spots originated from telescopic inputs versus human operator submissions. Call correction re-validates suggested DX calls against CTY before accepting them; FCC license gating runs after correction using a license-normalized base call (for example, `W6/UT5UF` is evaluated as `UT5UF`) and drops unlicensed US calls (beacons bypass this drop; user filters still apply).

## Commands

Telnet clients can issue commands via the prompt once logged in. The processor, located in `commands/processor.go`, requires a logged-in callsign and ignores unauthenticated commands with `No logged user found. Command ignored.` It supports the following general commands:

- Test spotter calls: when a logged-in callsign ends with `TEST` (optionally `-<SSID>`) and has no slash segments, the base call (SSID stripped) must resolve in CTY or the `DX` command is rejected with a message. Accepted test spots are still filtered/broadcast locally and subject to reputation gating; they bypass FCC ULS validation, but are not stored in the ring buffer, not archived, and never peered.

- `HELP [command]` / `H` - list commands for the active dialect or show detailed help for a specific command (for example, `HELP DX`).
- `SHOW DX [N]` / `SHOW/DX [N]` - alias of `SHOW MYDX`, streaming the most recent `N` filtered spots (`N` ranges from 1-250, default 50). Archive-only: if the Pebble archive is unavailable, the command returns `No spots available.` The command accepts the alias `SH DX` (or `SH/DX` in cc) as well.
- `SHOW MYDX [N]` - stream the most recent `N` spots that match your filters (self-spots always pass; `N` ranges from 1-250, default 50). Archive-only: if the Pebble archive is unavailable, the command returns `No spots available.` Very narrow filters may return fewer than `N` results.
- `SET DIAG <ON|OFF>` - replace the comment field with a diagnostic tag: `<source><DEDXCC><DEGRID2><band><policy>`, where source is `R` (RBN), `P` (PSK), or `H` (human/peer) and policy is `F`/`S`.
- `SET SOLAR <15|30|60|OFF>` - opt into wall-clock aligned solar summaries (OFF by default).
- `BYE`, `QUIT`, `EXIT` - request a graceful logout; the server replies with `73!` and closes the connection.

Filter management commands use a table-driven engine in `telnet/server.go` with explicit dialect selection. The default `go` dialect uses `PASS`/`REJECT`/`SHOW FILTER`. A CC-style subset is available via `DIALECT cc` (aliases: `SET/ANN`, `SET/NOANN`, `SET/BEACON`, `SET/NOBEACON`, `SET/WWV`, `SET/NOWWV`, `SET/WCY`, `SET/NOWCY`, `SET/SELF`, `SET/NOSELF`, `SET/SKIMMER`, `SET/NOSKIMMER`, `SET/<MODE>`, `SET/NO<MODE>`, `SET/FILTER DXBM/PASS|REJECT <band>` mapping CC DXBM bands to our band filters, `SET/NOFILTER`, plus `SET/FILTER`/`UNSET/FILTER`/`SHOW/FILTER`). `DIALECT LIST` shows the available dialects, the chosen dialect is persisted per callsign along with filter state, and HELP renders the verbs for the active dialect. Classic/go commands operate on each client's `filter.Filter` and fall into `PASS`, `REJECT`, and `SHOW FILTER` groups:

- Operator note: `RESET FILTER` re-applies the configured defaults from `data/config/runtime.yaml` (`filter.default_modes` and `filter.default_sources`). `SET/NOFILTER` is CC-only and resets to a fully permissive "all pass" state; it does not use configured defaults.

- `SHOW FILTER` - prints a full snapshot of filter state (allow/block + effective) for bands, modes, sources, continents, zones, DXCC, grids, confidence, path classes, callsigns, and toggles.

Tokenized `SHOW FILTER <type>` / `SHOW/FILTER <type>` forms are deprecated; they return the full snapshot with a warning.
Effective labels in the snapshot use a fixed vocabulary: `all pass`, `all except: <items>`, `only: <items>`, `none pass`, `all blocked`.
- `PASS BAND <band>[,<band>...]` - enables filtering for the comma- or space-separated list (each item normalized via `spot.NormalizeBand`), or specify `ALL` to accept every band; use the band names from `spot.SupportedBandNames()`.
- `PASS MODE <mode>[,<mode>...]` - enables one or more modes (comma- or space-separated) that must exist in `filter.SupportedModes`, or specify `ALL` to accept every mode.
- `PASS SOURCE <HUMAN|SKIMMER|ALL>` - filter by spot origin: `HUMAN` passes only spots with `IsHuman=true`, `SKIMMER` passes only spots with `IsHuman=false`, and `ALL` disables source filtering.
- `PASS DXCONT <cont>[,<cont>...]` / `DECONT <cont>[,<cont>...]` - enable only the listed DX/spotter continents (AF, AN, AS, EU, NA, OC, SA), or `ALL`.
- `PASS DXZONE <zone>[,<zone>...]` / `DEZONE <zone>[,<zone>...]` - enable only the listed DX/spotter CQ zones (1-40), or `ALL`.
- `PASS DXGRID2 <grid>[,<grid>...]` - enable only the listed 2-character DX grid prefixes. Tokens longer than two characters are truncated (e.g., `FN05` -> `FN`); `ALL` resets to accept every DX 2-character grid.
- `PASS DEGRID2 <grid>[,<grid>...]` - enable only the listed 2-character DE grid prefixes (same parsing/truncation as DXGRID2); `ALL` resets to accept every DE 2-character grid.
- `PASS DXCALL <pattern>[,<pattern>...]` - begins delivering only spots with DX calls matching the supplied patterns.
- `PASS DECALL <pattern>[,<pattern>...]` - begins delivering only spots with DE/spotter calls matching the supplied patterns.
- `PASS CONFIDENCE <symbol>[,<symbol>...]` - enables the comma- or space-separated list of consensus glyphs (valid symbols: `?`, `S`, `C`, `P`, `V`, `B`; use `ALL` to accept every glyph).
- `PASS PATH <class>[,<class>...]` - enables the comma- or space-separated list of path prediction classes (HIGH/MEDIUM/LOW/UNLIKELY/INSUFFICIENT; use `ALL` to accept every class). When the path predictor is disabled, PATH commands are ignored with a warning.
- `PASS NEARBY ON|OFF` - when ON, deliver spots whose DX or DE H3 cell matches your grid (L1 for 160/80/60m, L2 otherwise). Location filters (DX/DE CONT, ZONE, GRID2, DXCC) are suspended while NEARBY is ON, and attempts to change them are rejected with a warning. OFF restores the prior location filter state. State persists across sessions and a login warning is shown when active. Requires `SET GRID`.
- `PASS BEACON` - explicitly enable delivery of beacon spots (DX calls ending `/B`; enabled by default).
- `PASS SELF` - always deliver spots where the DX callsign matches your normalized callsign (even if filters would normally block).
- `REJECT BAND <band>[,<band>...]` - disables only the comma- or space-separated list of bands provided (use `ALL` to block every band).
- `REJECT MODE <mode>[,<mode>...]` - disables only the comma- or space-separated list of modes provided (specify `ALL` to block every mode).
- `REJECT SOURCE <HUMAN|SKIMMER|ALL>` - blocks one origin category (human/operator spots vs automated/skimmer spots), or `ALL`.
- `REJECT DXCONT` / `DECONT` / `DXZONE` / `DEZONE` - block continent/zone filters (use `ALL` to block all).
- `REJECT DXGRID2 <grid>[,<grid>...]` - remove specific 2-character DX grid prefixes (tokens truncated to two characters); `ALL` blocks every DX 2-character grid.
- `REJECT DEGRID2 <grid>[,<grid>...]` - remove specific 2-character DE grid prefixes (tokens truncated to two characters); `ALL` blocks every DE 2-character grid.
- `REJECT DXCALL <pattern>[,<pattern>...]` - blocks the supplied DX callsign patterns.
- `REJECT DECALL <pattern>[,<pattern>...]` - blocks the supplied DE callsign patterns.
- `REJECT CONFIDENCE <symbol>[,<symbol>...]` - disables only the comma- or space-separated list of glyphs provided (use `ALL` to block every glyph).
- `REJECT PATH <class>[,<class>...]` - disables the comma- or space-separated list of path prediction classes (use `ALL` to block every class).
- `REJECT BEACON` - drop beacon spots entirely (they remain tagged internally for future processing).
- `REJECT SELF` - suppress all spots where the DX callsign matches your normalized callsign.

Confidence glyphs are only emitted for modes that run consensus-based correction (CW/RTTY/USB/LSB voice modes). FT8/FT4 spots carry no confidence glyphs, so confidence filters do not affect them. After correction assigns `P`/`V`/`C`/`?`, any remaining `?` is upgraded to `S` when the DX call is present in `MASTER.SCP`.

Band, mode, confidence, PATH, and DXGRID2/DEGRID2 commands share identical semantics: they accept comma- or space-separated lists, ignore duplicates/case, and treat the literal `ALL` as a shorthand to allow or block everything for that type. PASS/REJECT add to allow/block lists and remove the same items from the opposite list. DXGRID2 applies only to the DX grid when it is exactly two characters long; DEGRID2 applies only to the DE grid when it is exactly two characters long. 4/6-character or empty grids are unaffected, and longer tokens provided by the user are truncated to their first two characters before validation.
SELF matches the normalized DX callsign only; when a spot is suppressed by secondary dedupe, a matching client still receives it if SELF is enabled. This delivery is per-client and does not bypass secondary dedupe for the global broadcast stream.

Confidence indicator legend in telnet output:

- `?` - Unknown/low support
- `S` - DX call is present in `MASTER.SCP` and the post-correction confidence would otherwise be `?`
- `P` - 25-50% consensus for the subject call (no correction applied)
- `V` - More than 50% consensus for the subject call (no correction applied)
- `B` - Correction was suggested but CTY validation failed (call left unchanged)
- `C` - Callsign was corrected and CTY-validated

### Telnet Reputation Gate

The passwordless reputation gate throttles telnet `DX` commands based on call history, ASN/geo consistency, and prefix pressure. It is designed to slow down new or suspicious senders while keeping known-good calls flowing. Drops are silent to clients, but surfaced in the console stats and system pane.

Core behavior:
- New calls wait an initial probation window before any spots are accepted.
- Per-band limits ramp by one each window up to a cap; total cap increases after ramp completion.
- Country mismatch (IP vs CTY) or Cymru/IPinfo disagreement adds an extra delay before ramping.
- New ASN or geo flips reset the call to probation.
- Prefix token buckets (/24, /48) shed load before per-call limits.

Data sources:
- IPinfo Lite CSV is imported into a local Pebble DB; IPv4 ranges are loaded into RAM for microsecond lookups, IPv6 stays on disk and is served via Pebble + cache.
- Team Cymru DNS TXT is a fallback when Pebble misses or is unavailable; answers are cached for 24h with a tight timeout.
- IPinfo live API is the last resort when both the local store and Cymru miss.
  - The downloader uses `curl` with the configured token, imports into Pebble, and cleans up the CSV if configured.
  - Optional full compaction after import reduces read amplification; see `ipinfo_pebble_compact` in the reputation config.

Configuration:
- See the `reputation` section in `data/config/reputation.yaml` for lookup order, TTLs, download/API tokens, and thresholds; extensive comments document each knob for operators.

Use `PASS CONFIDENCE` with the glyphs above to whitelist the consensus levels you want to see (for example, `PASS CONFIDENCE P,V` keeps strong/very strong reports while dropping `?`/`S`/`B` entries).

Use `REJECT BEACON` to suppress DX beacons when you only want live operator traffic; `PASS BEACON` re-enables them, and `SHOW FILTER` reports the current state. Regardless of delivery, `/B` spots are excluded from call-correction, frequency-averaging, and harmonic checks.
Errors during filter commands return a usage message (e.g., invalid bands or modes refer to the supported lists) and the `SHOW FILTER` command helps confirm the active settings.

Continent and CQ-zone filters behave like the band/mode whitelists: start permissive, tighten with `PASS`, reset with `ALL`. When a continent/zone filter is active, spots missing that metadata are rejected so the whitelist cannot be bypassed by incomplete records.

New-user filter defaults are configured in `data/config/runtime.yaml` under `filter:` and are only applied when a callsign has no saved filter file in `data/users/`:

- `filter.default_modes`: initial mode selection for `PASS/REJECT MODE`.
- `filter.default_sources`: initial SOURCE selection (`HUMAN` for `IsHuman=true`, `SKIMMER` for `IsHuman=false`). Omit the field or list both categories to disable SOURCE filtering (equivalent to `PASS SOURCE ALL`).

Existing users keep whatever is stored in their `data/users/<CALL>.yaml` file; changing these defaults only affects newly created users.

## RBN Skew Corrections

1. Enable the `skew` block in `data/config/data.yaml` (the server writes to `skew.file` after each refresh):

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

1. Populate the `known_calls` block in `data/config/data.yaml`:

```yaml
 known_calls:
  enabled: true
  url: "https://www.supercheckpartial.com/MASTER.SCP"
  file: "data/scp/MASTER.SCP"
  refresh_utc: "01:15"
```

2. On startup the server checks `known_calls.file`. If it is missing and `known_calls.url` is set, the file is downloaded immediately before any spots are processed. A `.status.json` sidecar is written next to the file so subsequent refreshes can use conditional GETs and avoid unnecessary reloads.
3. When `known_calls.enabled` is true, the built-in scheduler refreshes the file every day at `known_calls.refresh_utc` (default `01:00` UTC). The unified downloader only reloads the in-memory cache on content changes (CTY/SCP reloads clear the unified call metadata cache), so no restart is needed.

You can disable the scheduler by setting `known_calls.enabled: false`. In that mode the server will still load whatever file already exists (and will fetch it once at startup if an URL is provided), but it will not refresh it automatically.

## CTY Database Refresh

1. Configure the `cty` block in `data/config/data.yaml`:

```yaml
cty:
  enabled: true
  url: "https://www.country-files.com/cty/cty.plist"
  file: "data/cty/cty.plist"
  refresh_utc: "00:45"
```

2. On startup the server downloads `cty.plist` if it is missing and a URL is configured, then loads it into the in-memory CTY database. A `.status.json` sidecar tracks ETag/Last-Modified so subsequent refreshes can skip unchanged files.
3. When `cty.enabled` is true, the scheduler checks the plist daily at `cty.refresh_utc` using conditional requests and reloads the CTY DB only when content changes; the unified call metadata cache is cleared on each CTY reload. Failures log a warning and retry with backoff; the last-good CTY DB remains active.
4. The stats pane includes a `CTY: age ...` line that shows how long it has been since the last successful refresh (and a failure count when retries are failing), so staleness is visible at a glance.

## FCC ULS Downloads

1. Configure the `fcc_uls` block in `data/config/data.yaml`:

```yaml
fcc_uls:
  enabled: true
  url: "https://data.fcc.gov/download/pub/uls/complete/l_amat.zip"
  archive_path: "data/fcc/l_amat.zip"
  db_path: "data/fcc/fcc_uls.db"
  refresh_utc: "02:15"
```

2. On startup the cluster launches a background job that checks for the SQLite DB. If the DB is missing, it immediately downloads the archive (ignoring cache headers), extracts the AM/EN/HD tables, and builds a fresh SQLite database at `fcc_uls.db_path`. If the DB is present, it waits for the scheduled refresh time. Both the ZIP and DB are written via temp files and swapped atomically; metadata/status is stored at `archive_path + ".status.json"` (the previous `.meta.json` is still read for compatibility).
3. During the load, only active licenses are kept (`HD.license_status = 'A'`). HD is slimmed to a few useful fields (unique ID, call sign, status, service, grant/expire/cancel/last-action dates), and AM is reduced to just unique ID + call sign for active records. EN is not loaded. The downloaded ZIP is deleted after a successful build to save space.
4. When `fcc_uls.enabled` is true, a built-in scheduler refreshes the archive and rebuilds the database once per day at `fcc_uls.refresh_utc` (UTC). The refresh uses conditional requests when metadata is present, even if the archive was deleted after the prior build. The job runs independently of spot processing, so the rest of the cluster continues handling spots while the download, unzip, and load proceed.
5. The console/TUI stats include an FCC line showing active-record counts and the DB size.

## Grid Persistence and Caching

- Grids, known-call flags, and CTY metadata (ADIF/CQ/ITU/continent/country) are stored in Pebble at `grid_db` (default `data/grids/pebble`, a directory). Each batch is committed with `Sync` for durability; the in-memory cache continues serving while backfills rebuild on new spots.
- Gridstore checkpoints are created hourly under `grid_db/checkpoint` and retained for 24 hours. On startup, corruption triggers an automatic restore from the newest verified checkpoint; the cluster continues running while the restore rebuilds the Pebble directory. A daily integrity scan runs at 05:00 UTC and logs the result.
- When a call lacks a stored grid, the CTY prefix latitude/longitude is used to derive a 4-character Maidenhead grid and persist it as "derived"; derived grids never overwrite non-derived entries. Telnet output renders derived grids in lowercase while internal storage and computations remain uppercase.
- Writes are batched by `grid_flush_seconds` (default `60s`); a final flush runs during shutdown.
- The unified call metadata cache is a bounded LRU of size `grid_cache_size` (default `100000`). It caches grid/CTY/known lookups and only applies the TTL (`grid_cache_ttl_seconds`) to grid entries; CTY/SCP refreshes clear the cache. Cache misses fall back to Pebble via the async backfill path when `grid_db_check_on_miss` is true; RBN grid misses also attempt a tight-timeout sync lookup to seed the cache before secondary dedupe/path reliability.
- Pebble tuning knobs (defaults tuned for read-heavy durability): `grid_block_cache_mb=64`, `grid_bloom_filter_bits=10`, `grid_memtable_size_mb=32`, `grid_l0_compaction_threshold=4`, `grid_l0_stop_writes_threshold=16`, `grid_write_queue_depth=64`.
- The stats line `Grids: <TOTAL|UPDATED> / <hit%> / <lookups/min> | Drop aX sY` reports gridstore totals (or updates since start if the DB is unavailable), cache hit rate, lookup rate per minute, and async/sync lookup queue drops.
- If you set `grid_ttl_days > 0`, the store purges rows whose `updated_at` timestamp is older than that many days right after each SCP refresh. Continuous SCP membership or live grid updates keep records fresh automatically.
- `grid_preflight_timeout_ms` is ignored for the Pebble prototype (retained for config compatibility).

## Runtime Logs and Corrections

- **Call corrections**: `2025/11/19 18:50:45 Call corrected: VE3N -> VE3NE at 7011.1 kHz (8 / 88%)`
- **Frequency averaging**: `2025/11/19 18:50:45 Frequency corrected: VE3NE 7011.3 -> 7011.1 kHz (8 / 88%)`
- **Harmonic suppression**: `2025/11/19 18:50:45 Harmonic suppressed: VE3NE 14022.0 -> 7011.0 kHz (3 / 18 dB)` plus a paired frequency-corrected line indicating the fundamental retained.
- **Stats ticker** (per `stats.display_interval_seconds`): `PSKReporter: <TOTAL> TOTAL / <CW> CW / <RTTY> RTTY / <FT8> FT8 / <FT4> FT4 / <MSK144> MSK144`

### Sample Session

Below is a hypothetical telnet session showing the documented commands in action (server replies are shown after each input):

```
telnet localhost 9300
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
PASS BAND 20M
Filter set: Band 20m
PASS MODE FT8,FT4
Filter set: Modes FT8, FT4
PASS CONFIDENCE P,V
Confidence symbols enabled: P, V
SHOW FILTER
Current filters: BAND=only: 20m | MODE=only: FT8, FT4 | CONFIDENCE=only: P, V | ...
BAND: allow=20m block=NONE (effective: only: 20m)
MODE: allow=FT8, FT4 block=NONE (effective: only: FT8, FT4)
CONFIDENCE: allow=P, V block=NONE (effective: only: P, V)
...
REJECT MODE FT4
Mode filters disabled: FT4
RESET FILTER
Filters reset to defaults
BYE
73!
```

Use these commands interactively to tailor the spot stream to your operating preferences.

### Telnet Throughput Controls

The telnet server fans every post-dedup spot to every connected client. When PSKReporter or both RBN feeds spike, the broadcast queue can saturate and you'll see `Broadcast channel full, dropping spot` along with a rising `Telnet drops` metric in the stats ticker (Q/C/W = broadcast queue drops / per-client queue drops / sender write-failure disconnects). Tune the `telnet` block in `data/config/runtime.yaml` to match your load profile:

- `broadcast_workers` keeps the existing behavior (`0` = auto at half your CPUs, minimum 2).
- `broadcast_queue_size` controls the global queue depth ahead of the worker pool (default `2048`); larger buffers smooth bursty ingest before anything is dropped.
- `worker_queue_size` controls how many per-shard jobs each worker buffers before dropping a shard assignment (default `128`).
- `client_buffer_size` defines how many spots a single telnet session can fall behind before its personal queue starts dropping (default `128`).
- `control_queue_size` bounds per-client control output (bulletins, prompts, keepalives). Control always drains before spots; a full control queue disconnects the client.
- `broadcast_batch_interval_ms` micro-batches outbound broadcasts to reduce mutex/IO churn (default `250`; set to `0` for immediate sends). Each shard flushes on interval or when the batch reaches its max size, preserving order per shard.
- `login_line_limit` caps how many bytes a user can enter at the login prompt (default `32`). Keep this tight to prevent hostile clients from allocating massive buffers before authentication.
- `command_line_limit` caps how long any post-login command may be (default `128`). Raise this when operators expect comma-heavy filter commands or scripted clients that send longer payloads.
- `read_idle_timeout_seconds` refreshes the read deadline for logged-in sessions (default `86400`); timeouts do not disconnect, they simply continue waiting for input.
- `login_timeout_seconds` disconnects if pre-login callsign entry does not complete within the window (default `120`).
- `drop_extreme_rate`, `drop_extreme_window_seconds`, and `drop_extreme_min_attempts` enforce a safety valve for slow clients: once the drop rate crosses the threshold over the window (after the minimum attempts), the session is disconnected.
- `keepalive_seconds` emits a CRLF to every connected client on a cadence (default `120`; `0` disables). Blank lines sent by clients are treated as keepalives and get an immediate CRLF reply so idle TCP sessions stay open.

Increase the queue sizes if you see the broadcast-channel drop message frequently, or raise `broadcast_workers` when you have CPU headroom and thousands of concurrent clients.

### Archive Durability (Pebble)

The optional Pebble archive is built to stay out of the hot path: enqueue is non-blocking and drops when backpressure builds. With the archive enabled, you can tune durability vs throughput:

- `archive.synchronous`: defaults to `off` for maximum throughput when the archive is disposable; `normal`, `full`, or `extra` enable fsync for stronger crash safety.
- `archive.auto_delete_corrupt_db`: when true, the server deletes the archive directory on startup if Pebble reports corruption (or the path is not a directory), then recreates an empty store.
- `archive.busy_timeout_ms` and `archive.preflight_timeout_ms` are ignored for Pebble (retained for compatibility).

Operational guidance: enable `auto_delete_corrupt_db` only if the archive is truly disposable. If you need to preserve data through crashes, leave auto-delete off and raise synchronous to `normal`/`full` (or disable the archive entirely).

## Project Structure

```
.
├─ data/config/            # Runtime configuration (split YAML files)
│  ├─ app.yaml             # Server identity, stats interval, console UI
│  ├─ ingest.yaml          # RBN/PSKReporter/human ingest + call cache
│  ├─ dedupe.yaml          # Primary/secondary dedupe policy windows
│  ├─ pipeline.yaml        # Call correction, harmonics, spot policy
│  ├─ data.yaml            # CTY/known_calls/FCC/skew + grid DB tuning
│  ├─ runtime.yaml         # Telnet server settings, buffer/filter defaults
│  └─ mode_allocations.yaml # Mode inference for RBN/human ingest
├─ config/                 # YAML loader + defaults (merges config directory)
├─ cmd/                    # Helper binaries (CTY lookup, skew fetch, analysis)
├─ rbn/, pskreporter/, telnet/, dedup/, filter/, spot/, stats/, gridstore/  # Core packages
├─ data/cty/cty.plist      # CTY prefix database for metadata lookups
├─ go.mod / go.sum         # Go module definition + checksums
└─ main.go                 # Entry point wiring ingest, protections, telnet server
```
## Code Walkthrough

- `main.go` glues together ingest clients (RBN/PSKReporter), protections (dedup, call correction, harmonics, frequency averaging), persistence (grid store), telnet server, dashboard, schedulers (FCC ULS, known calls, skew), and graceful shutdown. Helpers are commented so you can follow the pipeline without prior cluster context.
- `telnet/server.go` documents the connection lifecycle, broadcast sharding, filter commands, and how per-client filters interact with the shared ring buffer.
- `buffer/` explains the lock-free ring buffer used by SHOW/DX and broadcasts; it stores atomic spot pointers and IDs to avoid partial reads.
- `config/` describes the YAML schema, default normalization, and `Print` diagnostics. The “Configuration Loader Defaults” section mirrors these behaviors.
- `cty/` covers longest-prefix CTY lookups and cache metrics. `spot/` holds the canonical spot struct, formatting, hashing, validation, callsign utilities, harmonics/frequency averaging/correction helpers, and known-calls cache.
- `dedup/`, `filter/`, `gridstore/`, `skew/`, and `uls/` each have package-level docs and function comments outlining how they feed or persist data without blocking ingest.
- `rbn/` and `pskreporter/` detail how each source is parsed, normalized, skew-corrected, and routed into the ingest CTY/ULS gate before deduplication.
- `commands/` and `cmd/*` binaries include focused comments explaining the helper CLIs for SHOW/DX, CTY lookup, and skew prefetching.

## Getting Started

1. Update `data/config/ingest.yaml` with your preferred callsigns for the `rbn`, `rbn_digital`, optional `human_telnet`, and optional `pskreporter` sections. Optionally list `pskreporter.modes` (e.g., [`FT8`, `FT4`]) to accept only those modes after subscribing to the catch-all topic. If you enable `human_telnet`, review `data/config/mode_allocations.yaml` (used to infer CW vs LSB/USB when the incoming spot line does not include an explicit mode token).
2. If peering with DXSpider clusters, edit `data/config/peering.yaml`: set `local_callsign`, optional `listen_port`, hop/version fields, and add one or more peers (host/port/password/prefer_pc9x). ACLs are available for inbound (`allow_ips`/`allow_callsigns`). Topology persistence is optional (disabled by default); set `peering.topology.db_path` to enable SQLite caching (WAL mode) with the configured retention.
2. Optionally enable/tune `call_correction` (master `enabled` switch, minimum corroborating spotters, required advantage, confidence percent, recency window, max edit distance, per-mode distance models, and `invalid_action` failover). `distance_model_cw` switches CW between the baseline rune-based Levenshtein (`plain`) and a Morse-aware cost function (`morse`), `distance_model_rtty` toggles RTTY between `plain` and a Baudot/ITA2-aware scorer (`baudot`), while USB/LSB voice modes always stay on `plain` because those reports are typed by humans.
3. Optionally enable/tune `harmonics` to drop harmonic CW/USB/LSB/RTTY spots (master `enabled`, recency window, maximum harmonic multiple, frequency tolerance, and minimum report delta).
4. Set `spot_policy.max_age_seconds` to drop stale spots before they're processed further. For CW/RTTY frequency smoothing, tune `spot_policy.frequency_averaging_seconds` (window), `spot_policy.frequency_averaging_tolerance_hz` (allowed deviation), and `spot_policy.frequency_averaging_min_reports` (minimum corroborating reports).
5. (Optional) Enable `skew.enabled` after generating `skew.file` via `go run ./cmd/rbnskewfetch` (or let the server fetch it at the next 00:30 UTC window). The server applies each skimmer's multiplicative correction before normalization so SSIDs stay unique.
6. If you maintain a historical callsign list, set `known_calls.file` plus `known_calls.url` (leave `enabled: true` to keep it refreshed). On first launch the server downloads the file if missing, loads it into memory, and then refreshes it daily at `known_calls.refresh_utc`.
7. Grids/known calls/CTY metadata are persisted in Pebble (`grid_db`, default `data/grids/pebble`). Tune `grid_flush_seconds` for batch cadence, `grid_cache_size` for the unified call metadata LRU, `grid_cache_ttl_seconds` for grid TTL inside the cache, `grid_block_cache_mb`/`grid_bloom_filter_bits`/`grid_memtable_size_mb`/`grid_l0_stop_writes_threshold` for Pebble read/write tuning, and `grid_write_queue_depth`/`grid_ttl_days` for buffering and retention.
8. Adjust `stats.display_interval_seconds` in `data/config/app.yaml` to control how frequently runtime statistics print to the console (defaults to 30 seconds).
9. Install dependencies and run:
	 ```pwsh
	 go mod tidy
	 go run .
	 ```
10. Connect via `telnet localhost 9300` (or your configured `telnet.port`), enter your callsign, and the server will immediately stream real-time spots.

## Path Reliability (telnet)

- Maintains a single directional FT8-equivalent bucket family per path: FT8/FT4/CW/RTTY/PSK all feed the same buckets. Voice modes (LSB/USB) are display-only. Buckets store linear power (FT8-equivalent) with exponential decay, using H3 res-2 buckets plus coarse H3 res-1 buckets, per-band half-lives, and staleness purging (per-band stale window).
- H3 size reference (average edge length): res-2 ≈ 158 km; res-1 ≈ 418 km.
- Maidenhead grids (4–6 chars) are converted to a representative lat/lon by taking the center of the grid square (4‑char: 2° × 1°). That point is mapped into H3 res‑2 (fine/local) and res‑1 (coarse/regional) cells so we can blend local and regional evidence deterministically.
- H3 cells are stored as stable 16‑bit proxy IDs via precomputed tables in `data/h3`. If grids are invalid or H3 tables are unavailable, the path is treated as insufficient data.
- Telnet lines show a single glyph in the comment area when enabled, reflecting a merged path estimate adjusted for the user's noise class. Glyph symbols are configurable via `glyph_symbols` (defaults: `+` high, `=` medium, `-` low, `!` unlikely); insufficient data uses `glyph_symbols.insufficient` (default `?`).
- Commands: `SET GRID <grid>` to set/confirm your location (4-6 char), `SET NOISE <QUIET|RURAL|SUBURBAN|URBAN|INDUSTRIAL>` to apply a DX->you noise penalty. Defaults: QUIET/0 dB.
- System log: every 5 minutes emits `Path predictions (5m)` (combined vs insufficient, with no-sample vs low-weight split), `Path buckets (5m)` (per-band bucket counts), and `Path weight dist (5m)` (per-band combined weight histogram).
- Config: `data/config/path_reliability.yaml` controls clamps (-25..15 dB FT8-equiv), per-band half-life, stale window multiplier (`stale_after_half_life_multiplier`), min effective weights (`min_effective_weight`), min fine weight blending (`min_fine_weight`), reverse hint discount, merge weights, per-mode glyph thresholds (`mode_thresholds` with `high/medium/low/unlikely` keys, including LSB/USB), fallback `glyph_thresholds`, glyph symbols (`glyph_symbols`), beacon weight cap (default 1), mode offsets (FT4/CW/RTTY/PSK; CW/RTTY/PSK defaults assume 500 Hz -> 2500 Hz bandwidth correction of -7 dB), and noise offsets.

## Propagation Glyphs (operator meaning)
- We build a path score from recent spots on each band, weighted so newer reports matter most and noise environment (QUIET/RURAL/SUBURBAN/URBAN/INDUSTRIAL) is accounted for.
- The score maps to a likelihood glyph: High / Medium / Low / Unlikely. If there is not enough data, we show the Insufficient glyph instead of guessing.
- Only after a normal glyph is chosen do we check for rare space‑weather overrides.
- `R` (radio blackout) appears only when the path is mostly sunlit and the X‑ray level meets the configured R‑threshold; it is band‑specific.
- `G` (geomagnetic storm) appears only when the path is high‑latitude and Kp meets the configured G‑threshold; it is band‑specific.
- Overrides are intentional and rare: they mean strong, path‑relevant space weather is likely to invalidate the normal estimate.

## Solar Weather Overrides (optional)
- Optional single-slot glyph overrides for strong space-weather events: `R` (radio blackout, R2+ thresholds) and `G` (geomagnetic storm, G2+ thresholds). Overrides only appear when the event is active *and* the path is relevant (sunlit fraction for `R`, high-latitude exposure for `G`), and they never replace the insufficient-data symbol.
- Overrides are band-aware: each R/G severity level has an explicit band list, and unknown/empty bands never receive overrides. R has precedence per-band; if a band is not eligible for the active R level, an eligible G can still apply.
- Inputs: GOES X-ray primary feed (corrected 0.1–0.8 nm flux) and observed 3-hour Kp. Fetches run every 60 seconds with conditional GET and in-memory caching.
- Config: `data/config/solarweather.yaml` (disabled by default) pins thresholds, band lists, hold-down windows, hysteresis rules, and gating tolerances with detailed operator notes.

## Configuration Loader Defaults

`config.Load` accepts a directory (merging all YAML files); the server defaults to `data/config`. It normalizes missing fields and refuses to start when time strings are invalid. Key fallbacks:

- Stats tickers default to `30s` when unset. Telnet queues fall back to `broadcast_queue_size=2048`, `worker_queue_size=128`, `client_buffer_size=128`, and friendly greeting/duplicate-login messages are injected if blank.
- Call correction uses conservative baselines unless overridden: `min_consensus_reports=4`, `min_advantage=1`, `min_confidence_percent=70`, `recency_seconds=45`, `max_edit_distance=2`, `frequency_tolerance_hz=500`, `voice_frequency_tolerance_hz=2000`, `voice_candidate_window_khz=2`, `min_snr_voice=0`, `invalid_action=broadcast`, `call_quality_ttl_seconds=86400`, `call_quality_max_entries=200000`, `call_quality_cleanup_interval_seconds=600`, `call_quality_pin_priors=true`. Empty distance models inherit from `distance_model` or default to `plain`; negative SNR floors/extras are clamped to zero.
- Harmonic suppression clamps to sane minimums (`recency_seconds=120`, `max_harmonic_multiple=4`, `frequency_tolerance_hz=20`, `min_report_delta=6`, `min_report_delta_step>=0`).
- Spot policy defaults prevent runaway averaging: `max_age_seconds=120`, `frequency_averaging_seconds=45`, `frequency_averaging_tolerance_hz=300`, `frequency_averaging_min_reports=4`.
- Archive defaults keep writes lightweight: `queue_size=10000`, `batch_size=500`, `batch_interval_ms=200`, `cleanup_interval_seconds=3600`, `synchronous=off`, `auto_delete_corrupt_db=false` (`busy_timeout_ms`/`preflight_timeout_ms` are ignored for Pebble).
- Known calls default to `data/scp/MASTER.SCP` and refresh at `01:00` UTC if unspecified. CTY falls back to `data/cty/cty.plist`.
- FCC ULS fetches use the official URL/paths (`archive_path=data/fcc/l_amat.zip`, `db_path=data/fcc/fcc_uls.db`, `temp_dir` inherits from `db_path`), and refresh times must parse as `HH:MM` or loading fails fast.
- Grid store defaults: `grid_db=data/grids/pebble`, `grid_flush_seconds=60`, `grid_cache_size=100000`, `grid_cache_ttl_seconds=0`, `grid_block_cache_mb=64`, `grid_bloom_filter_bits=10`, `grid_memtable_size_mb=32`, `grid_l0_compaction_threshold=4`, `grid_l0_stop_writes_threshold=16`, `grid_write_queue_depth=64`, with TTL/retention floors of zero to avoid negative durations.
- Dedup windows are coerced to zero-or-greater; `output_buffer_size` defaults to `1000` so bursts do not immediately drop spots.
- Buffer capacity defaults to `300000` spots; skew downloads default to SM7IUN's CSV (`url=https://sm7iun.se/rbnskew.csv`, `file=data/skm_correction/rbnskew.json`, `refresh_utc=00:30`) with non-negative `min_spots`.
- `config.Print` writes a concise summary of the loaded settings to stdout for easy startup diagnostics.

## Testing & Tooling

- `go test ./...` validates packages (not all directories contain tests yet).
- `gofmt -w ./...` keeps formatting consistent.
- `go run cmd/ctylookup -data data/cty/cty.plist` lets you interactively inspect CTY entries used for validation (portable calls resolve by location prefix).
- `go run cmd/rbnskewfetch -out data/skm_correction/rbnskew.json` forces an immediate skew-table refresh (the server still performs automatic downloads at 00:30 UTC daily).

Let me know if you want diagrams, sample logs, or scripted deployment steps added next.
