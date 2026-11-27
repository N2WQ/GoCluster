# ADR-0381: Custom SCP 6-Hour Refresh Cooldown and Pebble Batching

- Status: Accepted
- Date: 2026-11-27
- Decision Origin: Troubleshooting chat

## Context
Overnight live profiling of GoCluster showed two concentrated sources of avoidable Pebble write and compaction work across the cluster's five Pebble databases:

1. `CustomSCPStore.RecordSpot` ran on the output-pipeline hot path and executed 2 to 6+ separate unbatched `pebble.NoSync` `Set`/`Delete` commits per accepted `V`-confidence spot while holding `s.mu` (blocking lookups) and `s.diskMu`. It also rewrote `m|<call>` (395-day static membership) and already-retained `o|<call>|<band>|<bucket>|<spotter>` records on every repeat spot even seconds apart.
2. `archive` opened Pebble with an empty `&pebble.Options{}`, inheriting Pebble's 4 MiB `MemTableSize` and `L0CompactionThreshold = 4` defaults. With ~395 MiB of 24-hour spot history, this produced ~99 tiny 4 MiB SSTables and recurring 1–2s compaction bursts.

Meanwhile, `gridstore`, `VOACAPForecastCacheStore`, and read-only `ipinfo` consumed negligible CPU and required no changes.

## Decision
1. **Custom SCP per-spot single-batch commit:** Combine all Pebble mutations from one `CustomSCPStore.RecordSpot` / `recordObservation` call — static `m|<call>` set, observation `o|...` set, pruned/overflow spotter deletes, and any capacity-eviction prefix deletes across both family keys — into one lazily allocated `pebble.Batch`, committed once with `pebble.NoSync` after releasing `s.mu` while keeping `s.dbMu` (read) and `s.diskMu` held. This preserves [ADR-0279](ADR-0279-custom-scp-cleanup-and-database-ownership.md)'s lock order (`dbMu -> diskMu -> mu`) and synchronous persistence contract.
2. **Custom SCP 6-hour refresh cooldown (both memory and disk):**
   - **Static membership (`m|<call>`):** Always admit missing or stale (`< staticCutoff`) callsigns immediately. For an already-retained fresh callsign, advance `s.static[call]`, `staticExpiry`, and Pebble only when `seenUnix - current >= 6h`.
   - **Observation evidence (`o|<call>|<band>|<bucket>|<spotter>`):** Always admit new spotters (and spotters just pruned as expired) immediately so multi-spotter and H3-cell corroboration remain unaffected. For an already-retained spotter, update memory and Pebble only when `seenUnix > prevSeen` and either `seenUnix - prevSeen >= 6h` or `cellRes1 != prevCell`.
   - When a repeat observation hits the cooldown with no pruning or capacity change, skip `entry.lastSeen` updates, expiry/eviction index mutations, and Pebble batch allocation/commit.
   - Startup `loadFromDB` retains lossless newest-wins replay with no cooldown suppression.
3. **Archive internal Pebble defaults:** Replace empty `&pebble.Options{}` on both open paths in `openArchiveDB` (initial open and post-corrupt-delete reopen) with fixed internal defaults: `MemTableSize = 16 MiB`, `L0CompactionThreshold = 8`, and `L0StopWritesThreshold = 16`, without adding YAML config keys.

## Alternatives considered
1. **Throttle `m|<call>` only (leave repeat same-spotter `o|...` writes unthrottled):** Preserves second-exact observation refreshes, but leaves repeat same-spotter Pebble writes and write amplification intact.
2. **Throttle disk writes only while updating memory every second:** Preserves second-exact live timestamps, but adds retained "last persisted" side tables (~several MiB across 250k keys), leaves in-memory expiry-heap churn, and creates memory-vs-disk drift across restarts and [ADR-0279](ADR-0279-custom-scp-cleanup-and-database-ownership.md) cleanup checks.
3. **24-hour cooldown or freezing an entire `(call, band, mode)` key:** Freezing the entire key blocks new spotters and breaks 3–5 spotter corroboration; a 24-hour same-spotter cooldown would prevent active receivers from staying fresh inside the 12-hour `T0` (score 10) tier if a future policy tightens `min_score` above 7. A 6-hour cooldown keeps active receivers inside `T0` (`<= 12h`).
4. **Async write queue for Custom SCP:** Violates [ADR-0279](ADR-0279-custom-scp-cleanup-and-database-ownership.md)'s synchronous persistence contract and adds a new goroutine, queue, and shutdown-drain surface.
5. **Expose archive Pebble knobs in `archive.yaml`:** Unnecessary config surface compared to fixed internal defaults matching [ADR-0151](ADR-0151-archive-range-deletion-cleanup.md)'s minimal `archive.yaml`.

## Consequences
### Benefits
- Collapses multi-operation `CustomSCPStore.RecordSpot` commits into a single `NoSync` batch and moves Pebble I/O outside `s.mu`.
- Eliminates redundant sub-6h static and same-spotter writes in both memory and Pebble with zero extra retained state.
- Reduces `archive` memtable flushes and L0 compactions by ~4x–8x with no durability or config changes.

### Risks
- Repeat timestamps for already-retained static calls and same-spotter observations advance in ~6-hour steps instead of every second (shifting horizon or spotter-eviction edges by at most 6 hours).
- `archive` uses ~12–24 MiB more RAM for its 16 MiB memtable than Pebble's 4 MiB default.

### Operational impact
- Zero YAML schema changes and zero migration steps.
- Multi-spotter corroboration, H3 cell diversity, age tiers ([ADR-0285](ADR-0285-individual-historical-receiver-freshness.md)), consumer requirements ([ADR-0286](ADR-0286-consumer-specific-historical-requirements.md)), and archive range-delete cleanup ([ADR-0151](ADR-0151-archive-range-deletion-cleanup.md)) remain unchanged.

## Links
- Related issues/PRs/commits: approved Scope Ledger v1 (2026-11-27)
- Related tests: `spot/custom_scp_cooldown_batch_test.go`, `spot/custom_scp_store_test.go`, `spot/custom_scp_cleanup_test.go`, `spot/custom_scp_freshness_test.go`, `spot/custom_scp_persistence_bench_test.go`, `archive/archive_corruption_test.go`
- Related docs: [decision-log.md](../decision-log.md), [ADR-0279](ADR-0279-custom-scp-cleanup-and-database-ownership.md), [ADR-0285](ADR-0285-individual-historical-receiver-freshness.md), [ADR-0151](ADR-0151-archive-range-deletion-cleanup.md)
- Related TSRs: none
- Supersedes / superseded by: refines [ADR-0279](ADR-0279-custom-scp-cleanup-and-database-ownership.md) and [ADR-0285](ADR-0285-individual-historical-receiver-freshness.md) live write cadence; complements [ADR-0151](ADR-0151-archive-range-deletion-cleanup.md).
