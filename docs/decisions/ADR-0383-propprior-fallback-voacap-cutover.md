# ADR-0383: Pure-Go `propprior` Sparse-Path Fallback & Soft VOACAP Cutover

- Status: Accepted
- Date: 2026-11-27
- Decision Origin: Design

## Context

Prior to Phase 2B, GoCluster's sparse-path HF fallback and on-demand `SHOW PROP` outlooks relied on Windows-only external VOACAP (`voacap_fallback`), requiring `C:\itshfbc`, single-worker subprocess execution, a per-node Pebble forecast cache, a 600-second initial warm-up delay, and NOAA sunspot report polling (`SunspotMonitor`). Linux and non-Windows builds had no HF fallback or `SHOW PROP` support.

Phase 2A-1 introduced `internal/propprior`, a pure-Go, zero-allocation HF skywave MLP (frozen v4.1 weights CRC `0xf13b4b5d`, ocean mask CRC `0xc380b37e`) with a lock-free causal `SpaceWeatherStore`. Phase 2A-2 (`ADR-0382`) added bounded GFZ/NOAA HTTP space-weather pollers (`internal/propprior/feed`). During live soak testing on `2026-11-27 18:16:55 UTC`, `streamTracker.isPrimaryStale` measured staleness from the start of the newest accepted primary bin rather than its completed end, causing healthy daily NOAA F10.7 (`2026-11-26`) to trigger a premature daily fallback fetch before Penticton's `20:00 UTC` publication.

## Decision

1. **Completed-bin-end primary staleness (`internal/propprior/feed`):**
   - Measure primary staleness from `(lastValidPrimaryBin + 1) * stepSec` across `SFI` (`36h`), `Hp30` (`90m`), and `Dst` (`3h`) so each stream keeps its full grace window past the newest completed bin end.
2. **Atomic space-weather generation counter (`internal/propprior`):**
   - Embed a monotonic `gen uint64` inside immutable `weatherState`, incremented on every accepted `Record*` / `Record*Batch` swap and exposed via lock-free `SpaceWeatherStore.Generation()` and `SpaceWeatherSnapshot.Generation`. Coupling `Generation` inside the immutable snapshot eliminates snapshot/cache generation skew under concurrent live updates while preserving `0 B/op`, `0 allocs/op` on `Evaluate(now)`.
3. **Pure-Go `PropPriorFallback` & fixed-capacity reciprocal cache (`pathreliability/`):**
   - Implement `PropPriorFallback` (zero goroutines, zero subprocesses, zero disk DB) implementing `ClosedFallback`, `DetailedForecastProvider`, `CachedForecastProvider` (strictly observation-only), `VOACAPForecastWindowProvider`, `VOACAPForecastWindowWaitProvider` (instant 24-hour multi-band `SHOW PROP` on Windows and Linux), and `ClosedFallbackStatsProvider`.
   - Back `PropPriorFallback` with a fixed power-of-2 sharded 4-way set-associative value-only cache (`64` shards, default `16,384` slots ≈ `1 MiB`, zero pointers → zero GC scan overhead, `0 B/op`, `0 allocs/op` steady state on both cache hits and cache misses).
   - Key cache entries by canonical reciprocal cell pair `{loCell, hiCell, bandIdx, timeBucket}` (`loCell <= hiCell`, default `300s` bucket) + `swGen` invalidation, caching only unpenalized `(SnrTotal, SigmaTotal)` so users with different `SET NOISE` classes share cached skywave predictions with zero cross-user leakage.
4. **Calibration, `SET NOISE`, band gating & `160m` precedence:**
   - Evaluate `Rel(req_snr_db)` at the unpenalized reference floor (default `req_snr_db: -24.0` dB, matching VOACAP's `10 dB-Hz` / 2500 Hz FT8 BW contract) after optional `snr_offset_db` (default `0.0`).
   - Apply `SET NOISE` strictly on the receive leg (`ReceiveDB = snrAdj - penalty`, `TransmitDB = snrAdj`, `EffectiveDB = MergeReceiveWeight*ReceiveDB + MergeTransmitWeight*TransmitDB`).
   - Restrict `prop_prior_fallback.center_frequencies_mhz` strictly to the 9 HF skywave bands (`80m`–`10m`), preserving `native_160m_fallback` precedence on `160m` and preventing VHF/UHF (`6m`+) extrapolation.
5. **Soft VOACAP retirement & shipped defaults:**
   - Add optional-when-omitted `prop_prior_fallback:` (`yaml:"prop_prior_fallback,omitempty" json:"-"`) to `data/config/path_reliability.yaml`, preserving `cmd/rbn_replay`'s `TestCCV2V8EffectiveOffFingerprint` and private `data/config.local` compatibility.
   - Enforce mutual exclusivity between `voacap_fallback.enabled` and `prop_prior_fallback.enabled` at config validation.
   - Ship `voacap_fallback.enabled: false`, `prop_prior_fallback.enabled: true`, and `prop_prior_feeds.enabled: true` in `data/config/path_reliability.yaml`, while `cmd/release_smoke_config` disables `path_reliability`, `prop_prior_fallback`, and `prop_prior_feeds` so release smoke remains strictly offline.
   - Retain `internal/voacap/`, `pathreliability/voacap_*.go`, `cmd/voacap_*`, and `voacap_experiment.yaml` intact for instant one-line config rollback and offline comparison.
   - Startup is non-blocking: `PropPriorFallback` serves immediately on quiet defaults until the first background GFZ/NOAA poll commits (or continuously on quiet defaults if `prop_prior_feeds.enabled: false`).
6. **Observability & `prop_report` compatibility:**
   - Preserve existing `Path predictions (5m)` counter keys (`voacap_closed`, `voacap_aligned`, `voacap_sparse_upgrade`, `voacap_open`, `beacon_rx_voacap_*`) so `internal/propreport/report.go` continues parsing without schema breakage.
   - Emit distinct `SET DIAG PATH` provenance tags (`ppcap`, `ppaln`, `ppup`, `ppop` and beacon `bpp*` variants, omitting VOACAP's `s<SSN>` suffix), `engine=propprior` (omitting `ssn=`) in `SHOW PROP` headers, and compact `Prop prior cache:` / `Prop prior weather:` Overview lines when `PropPriorFallback` is active.

## Alternatives considered

1. **Hard-delete `internal/voacap/` and `voacap_fallback` immediately:** Rejected because soft-retiring (`voacap_fallback.enabled: false`) eliminates all runtime VOACAP overhead (zero subprocesses, zero Pebble DB, zero SSN polling) while preserving instant one-line config rollback and existing offline VOACAP tools.
2. **Use a Go `map` cache with periodic eviction scans:** Rejected to avoid dynamic map resizing, GC pointer scanning, and eviction tail-latency spikes under contest bursts; a fixed power-of-2 sharded 4-way set-associative value slab bounds memory statically at startup and achieves `0 B/op`, `0 allocs/op`.
3. **Rename `Path predictions (5m)` counter keys to `propprior_*`:** Rejected to preserve drop-in compatibility with existing `internal/propreport` (`cmd/prop_report`) parsing and historical trend logs; distinct provenance is surfaced in `SET DIAG PATH`, `SHOW PROP`, startup logs, and Overview.

## Consequences

### Benefits
- Pure-Go HF sparse-path fallback and instant 24-hour multi-band `SHOW PROP` work identically on Windows and Linux with zero external binaries, zero subprocesses, and zero Pebble forecast DB.
- Hot-path reciprocal cache hits execute in ~`130 ns/op` (`0 B/op`, `0 allocs/op`) and cache misses execute in ~`65 µs/op` (`0 B/op`, `0 allocs/op`) with instant space-weather invalidation.
- Eliminates the daily `SFI` false-positive staleness fallback observed during live soak.

### Risks
- Offline unit tests, canonical physics matrices, and benchmarks prove mathematical parity, zero allocations, and bounded memory, but live contest-load CPU/pprof behavior and multi-hour glyph accuracy against real RBN/PSK traffic require post-cutover live cluster observation.
- Operators with older private `data/config.local/path_reliability.yaml` files that omit `prop_prior_fallback:` keep their existing VOACAP setting until updated to copy the new `prop_prior_fallback:` block and set `voacap_fallback.enabled: false`.

### Operational impact
- Fresh deployments and public configs run `prop_prior_fallback.enabled: true` + `prop_prior_feeds.enabled: true` with `voacap_fallback.enabled: false`.
- Instant rollback to Windows VOACAP requires only toggling `prop_prior_fallback.enabled: false` and `voacap_fallback.enabled: true` in `path_reliability.yaml` and restarting.

## Links
- Related issues/PRs/commits: Phase 2B (`Approved v2`)
- Related ADRs: [ADR-0382](ADR-0382-propprior-live-space-weather-pollers.md), [ADR-0161](ADR-0161-voacap-closed-only-path-fallback.md), [ADR-0172](ADR-0172-show-prop-worker-refresh-and-glyph-columns.md), [ADR-0173](ADR-0173-voacap-rel-gated-open-fallback.md), [ADR-0201](ADR-0201-native-160m-endpoint-daylight-gate.md)
- Related tests: `internal/propprior/spaceweather_test.go`, `internal/propprior/feed/poller_test.go`, `pathreliability/prop_prior_fallback_test.go`, `pathreliability/prop_prior_feeds_config_test.go`, `telnet/prop_prior_integration_test.go`, `internal/cluster/prop_prior_feeds_runtime_test.go`, `config/public_release_config_test.go`
- Related docs: `data/config/README.md`, `data/config/PATH_PREDICTIONS.md`, `docs/OPERATOR_GUIDE.md`, `pathreliability/README.md`, `docs/decisions/current-path-voacap-contract-map.md`, `customgpt/source-map.md`, `customgpt/operator-guide-index.md`, `customgpt/support-cards/path-reliability.md`
- Related TSRs: none
- Supersedes / superseded by: soft-retires default runtime enablement of `voacap_fallback` (instant config rollback retained)
