## Performance Optimization Plan (CPU/Alloc Focus)

### Baseline (for before/after comparison)
- **CPU profile (2 min, 2025-12-08 11:24:20):** `data/diagnostics/cpu-20251208-112420.pprof`
  - Top costs: `memeqbody` 22.7% flat, `runtime.cgocall` 15.7% flat, `runtime.scanobject` 6.1% flat.
  - App hotspots (cumulative): `cty.lookupCallsignNoCache` ~28%, `pskreporter.workerLoop/convertToSpot/fetchCallsignInfo` ~25–40%, `processOutputSpots` ~18%, `uls.IsLicensedUS` ~12%, `gridstore.Get` ~11%.
- **Heap snapshot (same time):** `data/diagnostics/heap-20251208-112420.pprof` (steady-state; ring buffer dominates; no leak seen).
- **GC settings during capture:** `GOGC=50`, `DXC_PPROF_ADDR=localhost:6061`, `DXC_HEAP_LOG_INTERVAL=60s`, `DXC_NO_TUI=1`.

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

**Phase 3 (next): Allocation trims in correction/dedup**
- Distance cache key builder with pre-sized buffer.
- Map/slice preallocation for correction maps/lists.
- Optional Levenshtein buffer pooling (guarded).
- Dedup cleanup short-lock pattern (if not already done).

**Phase 3:** Allocation trims in correction/dedup
- Distance cache key builder with pre-sized buffer.
- Map/slice preallocation for correction maps/lists.
- Optional Levenshtein buffer pooling (guarded).
- Dedup cleanup short-lock pattern (if not already done).

**Phase 4:** Re-measure and tune
- Repeat the same profiling harness to compare `-top`/`-cum` against the baseline CPU/heap files above.

### Measurement Harness (already available)
- Enable profiling: set `DXC_PPROF_ADDR=localhost:6061` and `DXC_HEAP_LOG_INTERVAL=60s` (opt-in) and run the cluster.
- Collect CPU: `curl "http://localhost:6061/debug/pprof/profile?seconds=120" -o data/diagnostics/cpu-<ts>.pprof`
- Collect heap: `curl http://localhost:6061/debug/pprof/heap -o data/diagnostics/heap-<ts>.pprof`
- Compare with `go tool pprof -top/-cum` against baseline.

### Expected Impact (Phase 1)
- Fewer string ops/allocs per spot; reduced contention on call cache.
- Lower CPU in `memeqbody`, `strings.HasPrefix`/ToUpper hotspots.
- Reduced GC pressure; expect lower `scanobject/findObject` time.
