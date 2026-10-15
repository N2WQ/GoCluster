# ADR-0304: Bounded Incremental Forecast Persistence

- Status: Accepted
- Date: 2026-10-14
- Decision Origin: Design

## Context

ADR-0184 persisted each completed forecast, then decoded and sorted the whole
database to prune it. That work grows with unrelated forecasts. ADR-0303 gives
the persisted cache an exact deterministic retention policy.

## Decision

The store owns one payload-free metadata node per maintained canonical key,
a key map and two indexed heaps. Capacity bounds all three cardinalities.
Retention uses storage time and ADR-0303's canonical tie order. Eligibility
caches subtree hour-mask intersection and minimum/maximum window starts under
one shared horizon. At the actual operation time, a certificate can skip a
whole valid subtree; an uncertified subtree directs a path to a real victim.
Skipping a missing hour does not expire a record that has a later actual hour.
Refresh reuses the node. Deletion removes it from both heaps and the map.

Ordinary persistence atomically batches the replacement and actual removals.
Speculative metadata is private under the disk mutex; trust is published only
after success. An unsuccessful or uncertain commit discards indexes and marks
them dirty. Recovery reads actual disk state, retaining at most capacity
candidates and pruning at most 256 keys per batch. No forecast payload stays
in the indexes. Failed iterator or pruning operations do not certify recovery.
Hydration copies accepted rows into compact owned payloads before live-cache
retention; it must not retain JSON decoder growth capacity. TSR-0096 records
the candidate regression and its falsifying whole-day retained-cache oracle.
Missing SSN leaves hydration uninitialized, including a repeated attachment.
Generation/configuration changes and backward time rebuild exceptionally.

Cached diagnostics expose attached/ready/dirty and each structure's cardinality
through the existing overview. They use a separate short snapshot lock and
never acquire the disk mutex. No worker, timer or reporting cadence is added.

Disk schema/model generation, namespace, key, current-hour/window/TTL validity,
NoSync durability, memory-first publication, waiter notification, warning-only
persistence failure and shutdown fencing remain unchanged. A completed job's
SSN is authoritative for its write. Live-memory eviction remains unchanged.

## Alternatives considered

1. Keep the completion full scan: simpler, but performs work on unrelated data.
2. Lazy heaps with obsolete refresh nodes: rejected because historical writes
   can grow retained state beyond the cache's cardinality.
3. Schedule eviction at the next missing hour: rejected because a direct
   transition from hour 20 to 22 must retain a forecast containing both hours.
4. Coupled indexed heaps with subtree certificates: selected for bounded
   ownership and actual-time eligibility without benign per-hour reindexing.

## Consequences

### Benefits

- Ordinary writes touch the replacement and actual victims.
- Raw disk before repair, uncertain commits and index invariants are testable.
- Diagnostics distinguish unknown bookkeeping from a certified empty database.

### Risks

- Heap aggregates add complexity and retained metadata. Required checks include
  independent descendant aggregates, mixed sifts/removals, churn, race testing,
  paired performance guards, retained heap and controlled worker profiles.
- Exceptional recovery still scans disk and can partially prune before an I/O
  error; dirty state explicitly requires another recovery.

### Operational impact

- Existing databases require no migration. No setting or firewall change is
  required. Cached overview counters may lag an in-progress disk operation.
- This decision does not change forecast or observed-glyph classification.

## Links

- Related issues/PRs/commits: none
- Related code: `pathreliability/voacap_forecast_cache_store.go`,
  `pathreliability/voacap_forecast_cache_index.go`,
  `pathreliability/voacap_forecast_cache_index_heap.go`,
  `pathreliability/voacap_forecast_cache_index_scan.go`,
  `pathreliability/voacap_fallback.go`, `internal/cluster/bootstrap.go`
- Related tests: `pathreliability/voacap_incremental_cache_test.go`,
  `pathreliability/voacap_cache_index_test.go`,
  `pathreliability/voacap_persistence_bench_test.go`,
  `pathreliability/voacap_persistence_heap_test.go`,
  `internal/cluster/voacap_persistence_profile_test.go`,
  `internal/cluster/voacap_cache_diagnostics_test.go`
- Related docs: `pathreliability/README.md`, `docs/OPERATOR_GUIDE.md`,
  `customgpt/support-cards/path-reliability.md`
- Related TSRs: TSR-0096
- Supersedes / superseded by: refines ADR-0184's maintenance algorithm and
  ADR-0191's overview; retains ADR-0303's capacity ordering.
