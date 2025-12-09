## Performance Optimization Plan (CPU/Alloc Focus)

### Baseline (for before/after comparison)
- **CPU profile (2 min, 2025-12-08 11:24:20):** `data/diagnostics/cpu-20251208-112420.pprof`
  - Top costs: `memeqbody` 22.7% flat, `runtime.cgocall` 15.7% flat, `runtime.scanobject` 6.1% flat.
  - App hotspots (cumulative): `cty.lookupCallsignNoCache` ~28%, `pskreporter.workerLoop/convertToSpot/fetchCallsignInfo` ~25-40%, `processOutputSpots` ~18%, `uls.IsLicensedUS` ~12%, `gridstore.Get` ~11%.
- **Heap snapshot (same time):** `data/diagnostics/heap-20251208-112420.pprof` (steady-state; ring buffer dominates; no leak seen).
- **GC settings during capture:** `GOGC=50`, `DXC_PPROF_ADDR=localhost:6061`, `DXC_HEAP_LOG_INTERVAL=60s`, `ui.mode: headless` in `config.yaml`.

### Goals
- Reduce CPU by cutting redundant string normalization and DB lookups on hot paths.
- Lower allocation rate to reduce GC overhead (`scanobject/findObject`).
- Keep behavior identical; changes are perf-only.

### Phased Plan
**Phase 1 (completed): Normalize once, reuse everywhere**
- Add normalized fields to `Spot`: mode/band/calls/continents/grids (and 2-char grid prefixes).
- Use normalized fields in filters, hashing, license/grid checks, correction, and output; remove repeated `ToUpper/TrimSpace` and duplicate call normalization/cache hits.
- Hoist duplicate mode normalization in `processOutputSpots`.

**Before/After (CPU)**
- Baseline CPU: `cpu-20251208-112420.pprof` (120s).
- After Phase 1: `cpu-after-20251208-115632.pprof` (120s).
- Diff highlights (after – before):
  - `gridstore.Get`: −0.56s (fewer grid lookups)
  - `cty.lookupCallsignNoCache`: −0.10s (small win)
  - `processOutputSpots`: −0.22s; `applyLicenseGate`: −0.08s
  - `runtime.cgocall`/`memeqbody` still dominate; PSKReporter ingest remains the top app hotspot (~43% cumulative).

**Phase 2 (completed): Reduce per-spot DB/call lookups**
- CTY cache reuse and normalized Spot metadata; small drop in CTY cumulative time.
- License: added short-TTL cache to avoid repeated FCC checks for the same calls.
- PSKReporter: added per-callsign CTY cache to avoid repeated lookups.
- After Phase 2 CPU: `cpu-after-phase2-20251208-121926.pprof` (120s).
  - `runtime.cgocall` 17.1% flat (was ~21% after Phase 1).
  - `memeqbody` 13.7% flat (was ~21% after Phase 1).
  - `cty.lookupCallsignNoCache` ~18.4% cum (down from ~24.5% after Phase 1).
  - `uls.IsLicensedUS` ~15.6% cum (down from ~21% after Phase 1).
  - `gridstore.Get` ~6.3% cum (down from ~7% after Phase 1).
  - PSKReporter ingest still largest app hotspot (~32% cum, down from ~43%).

**Phase 3 (completed): Allocation trims in correction/dedup and hot caches**
- Distance cache key builder uses pre-sized `strings.Builder`; correction maps/slices pre-sized.
- Levenshtein buffers pooled via `sync.Pool`; optional, bounded sizes.
- Dedup cleanup switched to short-lock two-phase delete.
- Added license cache (5m TTL) to avoid repeated FCC checks; PSKReporter CTY cache TTL 5m.
- After Phase 3 CPU: `cpu-after-phase3-20251208-123914.pprof` (120s).
  - `memeqbody` 26.2% flat, `runtime.cgocall` 17.3% flat, `cty.lookupCallsignNoCache` ~32.2% cum, PSKReporter ingest ~46.6% cum, `uls.IsLicensedUS` ~16.7% cum, `gridstore.Get` ~15.3% cum.

**Post-Phase 3 re-run (headless)**
- Captured with `ui.mode: headless`, `GOGC=50`: `cpu-pskopt-20251208-130954.pprof` and `heap-pskopt-20251208-130954.pprof`.
- Results: `memeqbody` 25.3% flat, `runtime.cgocall` 22.3% flat, `cty.lookupCallsignNoCache` ~31.7% cum, PSKReporter ingest ~45.8% cum, `uls.IsLicensedUS` ~15.3% cum, `gridstore.Get` ~12.8% cum.
- Delta vs Phase 3 profile is within noise (traffic variance). No additional CPU drop observed from PSKReporter/CTY changes; controlled load is needed to make a clean comparison.

### Measurement Harness (already available)
- Enable profiling: set `DXC_PPROF_ADDR=localhost:6061` and `DXC_HEAP_LOG_INTERVAL=60s` (opt-in) and run the cluster.
- Collect CPU: `curl "http://localhost:6061/debug/pprof/profile?seconds=120" -o data/diagnostics/cpu-<ts>.pprof`
- Collect heap: `curl http://localhost:6061/debug/pprof/heap -o data/diagnostics/heap-<ts>.pprof`
- Compare with `go tool pprof -top/-cum` against baseline.

### Recommendations / Next Steps
- Run the profiling harness under a controlled workload (same duration/rate, `ui.mode: headless`) to isolate code effects from traffic variance and re-compare to baseline/Phase 3.
- Target remaining hotspots (unchanged): PSKReporter ingest and CTY lookups dominate; consider inlining normalized fields further and cutting repeated prefix checks.
- Keep Phase summaries and profile references here until optimizations are finalized; then we can archive or slim this file.
