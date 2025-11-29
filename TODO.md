## TODO

- Telnet: enforce `MaxConnections` in `acceptConnections` and close/deny beyond limit.
- Telnet broadcast: avoid per-spot shard rebuilding/allocations; maintain per-worker client lists or reuse buffers to cut GC/CPU cost with hundreds of clients.
- Dedup: current single worker with 1k in/out queues will drop under bursts; consider multiple workers, larger queues, and pooling to sustain tens of thousands of spots per minute.
- Queues: increase/configure PSKReporter processing, broadcast queues, worker queues, and dedup buffers to reduce drops at high ingest rates; expose drop metrics clearly.
- Testing: add synthetic load tests/stress harness for telnet fan-out and spot pipeline to verify throughput at target volumes.
- Long-running schedulers never stop: `startKnownCallScheduler` and `startSkewScheduler` spin forever without cancelation/shutdown, so embedding/restarts or tests will leak goroutines; add context-driven stop hooks.
- Recorder inserts spawn a goroutine per spot: `Record` launches `insert` in its own goroutine until limits are hit; with higher limits/many modes, bursts will create many goroutines contending on one SQLite handle—consider a bounded worker pool or buffered channel.
- Grid writer silently drops updates: when the `updates` channel is full, new grid updates are dropped without metrics/logs, and pending batches are cleared even if `UpsertBatch` failed; add backpressure or accounting to avoid losing grids quietly.
- Make dedup input/output channel sizes configurable (currently hard-coded to 1000) and optionally the PSKReporter spot channel (also 1000).

CPU-reduction recommendations and status

Done:
- Call-correction distance memoization (spot/correction.go): added size/TTL-bound cache for call distance calculations. Knobs in call_correction: distance_cache_size (default 5000) and distance_cache_ttl_seconds (default recency window). Tests still use legacy callDistance wrapper.

Remaining opportunities:
- Telnet filter matching (telnet.Client.filter.Matches):
  * Fast-path when filters are "all enabled" to bypass per-spot checks.
  * Optional: cache per-spot band/mode/confidence pre-check results in the broadcast loop to reduce per-client cost when many clients connect.

- Grid lookups (startGridWriter lookupFn):
  * Add a short-TTL negative cache for calls with no grid record to avoid repeated SQLite misses on every spot lacking grid info.

- Callsign normalization (spot.NormalizeCallsign, normalizeRBNCallsign, decorateSpotterCall):
  * Add a tiny LRU for normalized calls (including "-#" decorations) to avoid repeated string munging for hot skimmers/reporters during bursts.

- Skimmer skew corrections (skew.ApplyCorrection):
  * Cache corrections per (skimmer, rounded freq) if profiling shows this lookup is hot.

- Band mapping (spot.FreqToBand):
  * If profiling shows cost, replace the linear scan with a precomputed lookup table or cache last-N freq→band results (likely minor gain).

- Dedup hash:
  * Optionally store a precomputed hash on Spot at creation if profiling shows Hash32 CPU cost; hash currently recomputes per call.

- PSKReporter JSON decode:
  * Use a sync.Pool for PSKRMessage and reuse payload buffers if profiling shows allocations/GC are notable; this reduces CPU indirectly.

Notes:
- Memory headroom: current runtime uses <400 MB; the remaining caches (small LRUs/TTL maps) should add only a few MB.
- Next best ROI: telnet filter fast-path/caching; grid negative cache if backend/DB misses show up in profiling.

- PSKReporter JSON: consider a generated decoder (easyjson/ffjson) to eliminate reflection and further cut allocs/CPU on 25k+/min ingest.
 - dgryski/go-farm-like variants: there are bit-parallel/optimized algorithms (e.g., Myers algorithm) for small alphabets and short strings; for callsigns (<=10 chars), bit-parallel implementations can be faster.
