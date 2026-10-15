# ADR-0305: Path Map Compaction After Distributed Contraction

- Status: Accepted
- Date: 2026-10-14
- Decision Origin: Design

## Context

The existing path store rebuilds a contracted shard map only after that shard
has reached 1,000 entries. Spreading entries over the existing 64 shards can
leave substantial retained map storage below every individual threshold.

## Decision

Maintenance also examines total current occupancy and the sum of existing
per-shard peaks. At least 1,000 aggregate peak entries and occupancy strictly
below half that peak permit rebuilding contracted small shards. Each shard
rechecks its current occupancy under its own write lock. The snapshot is
advisory and need not represent one atomic store-wide instant.

Keep the old large-shard predicate, including its integer floor. The added
route applies to shards below the individual minimum and uses the untruncated
ratio: a peak of 17 with 8 retained entries qualifies when the store qualifies.
A peak of 1,001 with 500 retained entries keeps its previous no-rebuild result.
Rebuilding copies existing nonnil bucket pointers and resets the shard peak;
it neither copies buckets nor changes their contents.

Only existing shard maps and peaks are retained. No hot-update global counter,
history, side table, worker or new maintenance interval is introduced. Minute
stale purging and hourly compaction retain their existing schedule. Existing
bucket-count reporting remains the primary-state cardinality view.

## Alternatives considered

1. Keep only per-shard minimums: fails after distributed contraction.
2. Lower every shard minimum: rebuilds small stores without substantial total
   contraction and changes the existing large-shard boundary.
3. Maintain global hot-update counters: adds contention and deletion coupling.
4. Advisory scan of the fixed shard collection with locked local rechecks:
   selected for bounded maintenance work and no update-path cost.

## Consequences

### Benefits

- Map backing storage can be released after contraction spread across shards.
- Existing keys, bucket identities, predictions and active reports survive.

### Risks

- Concurrent refill can defer rebuilding; the next scheduled maintenance can
  retry. Local locked rechecks prevent stale advice from overriding refill.
- GC heap measurements have process noise and do not prove immediate Windows
  working-set reduction. The resource witness isolates map storage from bucket
  and receiver-sidecar deletion.

### Operational impact

- No setting, glyph rule, purge threshold or firewall change is required.
- Fine-key placement remains unchanged in this slice.

## Links

- Related issues/PRs/commits: none
- Related code: `pathreliability/store.go`, `pathreliability/store_compaction.go`,
  `internal/cluster/main_runtime.go`
- Related tests: `pathreliability/compaction_contract_test.go`,
  `pathreliability/compaction_connected_test.go`,
  `pathreliability/compaction_heap_test.go`, `pathreliability/compact_test.go`
- Related docs: `pathreliability/README.md`,
  `docs/path-reliability-first-wave-execution.md`
- Related TSRs: none
- Supersedes / superseded by: none; extends the existing map maintenance rule.
