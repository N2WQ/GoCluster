# ADR-0382: Opt-In Live Space-Weather Pollers for Propagation Prior (`internal/propprior/feed`)

- Status: Accepted
- Date: 2026-11-27
- Decision Origin: Design

## Context

Phase 2A-1 introduced `internal/propprior`, a pure-Go, zero-allocation HF propagation prior inference engine and bounded causal `SpaceWeatherStore` (`ring48` for 30-min `Hp30`, `ring24` for hourly `Bz_gsm`/`Vsw`/`Dst`, `ring4` for daily `F10.7_obs`). To soak-test live space-weather hydration before Phase 2B shadow scoring or VOACAP replacement, GoCluster needs bounded, fault-tolerant HTTP ingestion for the four space-weather streams without altering live path scoring, breaking existing private `data/config.local` directories, or introducing `net/http` dependencies into `internal/propprior`.

Live endpoint verification on 2026-11-27 also revealed concrete schema and causality constraints:
1. GFZ `kp.gfz.de/app/json/` returns columnar JSON (`{"datetime":[...],"Hp30":[...]}` / `"Fobs":[...]`) with no `ETag`/`Last-Modified` headers, and can include `null` values where `0.0` is a valid observation.
2. NOAA SWPC `noaa-planetary-k-index.json` and `kyoto-dst.json` now serve arrays of JSON objects with UTC timestamps lacking a trailing `Z` (while legacy 2D string-array fallback remains defensive).
3. NOAA `rtsw_mag_1m.json` and `rtsw_wind_1m.json` interleave multiple L1 spacecraft (`SOLAR1`, `ACE`, `IMAP`), are reverse chronological, and include `null` `proton_speed` on `active: true` rows.
4. NOAA `geospace_dst_7_day.json` has 1-minute cadence and publishes ~1 hour of future forecast bins. Because `SpaceWeatherStore`'s `ring48` (`w=48`) and `ring24` (`w=24`) advance `headBin` on any newer bin, ingesting in-progress or future bins would prematurely evict the oldest completed bin in the 24-hour window.

## Decision

1. **Package boundary:** Implement live HTTP polling and parsing in a child package `internal/propprior/feed`, keeping `internal/propprior` pure zero-`net/http` inference and state. Add atomic single-publication batch methods (`RecordHp30Batch`, `RecordBzGSMBatch`, `RecordVswBatch`, `RecordDstBatch`, `RecordSFIBatch`) to `SpaceWeatherStore` while preserving `0 B/op`, `0 allocs/op` lock-free `Evaluate(now)`.
2. **Disabled-by-default optional config:** Add an optional-when-omitted `prop_prior_feeds:` block to `data/config/path_reliability.yaml` shipped with `enabled: false`, integer `*_seconds` intervals/timeouts/thresholds, explicit URLs, byte caps, and `user_agent`. Omitting the section keeps existing private config directories bootable as disabled; when present, `null` is rejected and every key is required and validated even when `enabled: false`. Register all string keys in `cmd/release_assets/classify.go` and enforce `enabled: false` in `cmd/release_smoke_config/main.go`.
3. **Completed-bin causality & numeric decoding:**
   - Decode all upstream numeric values via `*float64` / `[]*float64` so JSON `null` never decodes as valid `0.0`.
   - Parse no-`Z` NOAA timestamps explicitly as UTC and sort descending feeds ascending before batch ingest.
   - Drop in-progress and future bins (`bin >= floor(now/step)`) for `Hp30`, `Dst` (Kyoto and Geospace), and `RTSW` (`Bz_gsm`/`Vsw`) so `ring48`/`ring24` never evict their oldest completed bin.
   - Allow today's UTC day (`bin <= floor(now/86400)`, rejecting future days) for `SFI` (`ring4`), preferring `reporting_schedule == "Noon"` and replacing intraday readings cleanly once `Noon` arrives.
4. **Failover hysteresis & non-overwrite guard:**
   - Run 4 independent per-stream goroutines (`hp30`, `sfi`, `rtsw`, `dst`); inside `rtsw`, poll and back off `rtsw_mag_1m` and `rtsw_wind_1m` independently so one failure does not block the other.
   - Activate fallback (`Hp30 → NOAA Kp`, `Kyoto Dst → Geospace 7-day Dst`, `NOAA F10.7 → GFZ Fobs` default / `10cm-flux-30-day` auto-detected) on staleness (`90m` / `3h` / `36h` measured from the end of the newest accepted completed primary bin, `(lastValidPrimaryBin+1)*stepSec`) or consecutive errors (`3`).
   - Expand completed 3-hour `Kp` bins across their six 30-min sub-bins (`< floor(now/1800)`) and downsample 1-minute Geospace Dst into completed UTC hourly means (`< floor(now/3600)`).
   - Gate all fallback ingestion strictly to bins `> lastValidPrimaryBin` (except on cold start) so fallback never overwrites real primary data already in the ring; returning primary payloads remain authoritative and overwrite temporary fallback bins cleanly.
5. **Bounded resources & shutdown:** Enforce per-request timeouts, `io.LimitReader(body, cap+1)` body caps, redirect limit without `https -> http` downgrade, bounded conditional-GET header storage (`<= 256 B`), jittered exponential backoff with capped `Retry-After`, lock-free bounded `HealthSnapshot`, and clean `Poller.Stop()` in `clusterRuntime.close()` after context cancellation.

## Alternatives considered

1. **Place `feed_*.go` directly inside `internal/propprior`:** Rejected to preserve zero `net/http` and `encoding/json` dependencies for offline inference/replay consumers.
2. **Add a brand-new required `data/config/propprior.yaml` file:** Rejected because adding a required file to `configFileRegistry` breaks existing private `data/config.local` deployments on upgrade and expands blast radius across release/test file lists.
3. **Reuse or modify legacy `solarweather/` (`solarweather.yaml`):** Rejected to keep legacy R/G path-override behavior completely isolated from Phase 2A-2 soak ingestion.

## Consequences

### Benefits
- Safe opt-in soak testing of live GFZ/NOAA ingestion with zero runtime impact when disabled (`0` goroutines, `0` HTTP calls).
- Guaranteed train/serve parity and ring-eviction safety across all four streams, plus cross-provider SFI fallback (`NOAA → GFZ Fobs`).
- `SpaceWeatherStore.Evaluate` remains lock-free and zero-allocation.

### Risks
- Upstream GFZ or NOAA schema changes outside tested formats will cause parse errors until updated (mitigated by dual-schema Kp/Dst parsers, fallback hysteresis, and bounded error health telemetry).
- Long-running upstream soak behavior against live GFZ/NOAA endpoints is locally validated with offline `httptest` fixtures and requires opt-in live soak confirmation before Phase 2B scoring integration.

### Operational impact
- Shipped configuration disables `prop_prior_feeds` (`enabled: false`); existing private configs without the section continue booting unchanged.
- Operators opting into Phase 2A-2 soak testing set `prop_prior_feeds.enabled: true` in `path_reliability.yaml` and restart; live path reliability glyphs and VOACAP fallback remain unchanged.

## Links
- Related issues/PRs/commits: Phase 2A-2 (`Approved v2`)
- Related tests: `internal/propprior/spaceweather_test.go`, `internal/propprior/feed/*_test.go`, `pathreliability/prop_prior_feeds_config_test.go`, `internal/cluster/prop_prior_feeds_runtime_test.go`, `config/public_release_config_test.go`
- Related docs: `data/config/README.md`, `customgpt/source-map.md`, `customgpt/operator-guide-index.md`, `docs/code-maps/path-reliability-voacap.md`
- Related TSRs: none
- Supersedes / superseded by: default enablement and live fallback consumer updated by [ADR-0383](ADR-0383-propprior-fallback-voacap-cutover.md)
