# ADR-0355: Shared Reputation Lookup Retention

- Status: Accepted
- Date: 2026-11-15
- Decision Origin: Troubleshooting chat; exact Approved v1

## Context

The owner selected `lookup_cache_max_entries` as one hard maximum of cached
records across main enrichment and Cymru DNS, with duplicate addresses counting
once per cache namespace. Previously 32 main and 16 DNS shards each received the
entire limit, allowing 48 times the configured total. Overflow scanned the whole
selected shard under lock. See
[TSR-0134](../troubleshooting/TSR-0134-reputation-lookup-cap-and-overflow.md).

The setting bounds address lookup history, including reached prelogin lookups,
rather than concurrent clients. About 50 clients now and up to 1,000 expected do
not establish cache occupancy. No current production latency or memory problem
was measured; the correction follows source-proven resource and work bounds.

## Decision

Use one Gate-owned store with fixed main and Cymru namespaces. Preserve the
existing lookup tuples, source precedence, positive/negative TTLs, default and
sentinel handling, prelogin's no-network behavior, and DNS queue/pending ownership.
The same address in both namespaces consumes two records.

Use at most 32 shards, reduced to the normalized positive cap when smaller.
Divide capacity by shard count and distribute the remainder so quotas sum
exactly to the cap. Deterministic address hashing selects a shard; either
namespace can consume any of its slots. No capacity is reserved by source and
no shard borrows another shard's backing.

Each used shard allocates its fixed slot array and map on first admission.
Reusable slot indices own intrusive FIFO links and a free list. At capacity,
remove the FIFO head before adding a new map membership. Writes refresh order;
reads do not. There is no record scan, free-slot search, per-eviction list node,
append-only history or background cache worker. Initial shard allocation,
bounded key hashing, payload copying and runtime map behavior remain separate
from the constant explicit FIFO/free-list work.

Keep lazy expiry with strict `now.After(expiresAt)` semantics; equality is a hit.
Overflow need not find every expired record before removing a live FIFO head.
All retained records, including unread expired ones, consume the same quota.

Admit at most 1,024 total bytes in the address key plus the five `LookupResult`
strings: ASN, country code, country name, continent code and source. Cache-owned
exact copies prevent substrings from retaining larger source backing; unchanged
already-owned fields can be reused on refresh. Oversized payloads are usable
through the existing lookup path but are not cached. An oversized replacement
removes its old cached record. There is no truncation, parser/protocol limit or
new YAML setting. The 1 KiB threshold is an approved retention safeguard, not a
source-derived metadata requirement.

Retained owned string bytes are at most `1024 * normalized_cap`. Fixed slot/index
state is bounded by the cap. This excludes map/allocator overhead, `time.Time`
locations, upstream stores and caller-held values; it is not a process heap or
RSS ceiling. During a refresh, old and replacement fields can overlap in the
operation's local values. Updates serialize within each shard, bounding this
copy overlap by at most one additional 1 KiB payload per shard, apart from
caller inputs and garbage awaiting collection. Diagnostic retained-byte counts
describe the published records, not that temporary overlap.

Expose `Gate.LookupCacheStats()` as a detached diagnostic snapshot. Lock the
fixed shard set in index order and release before formatting or logging. Report
configured cap, namespace/shard counts, allocated slots, owned string bytes,
capacity evictions, read-observed expiry and oversized-write bypasses. Aggregate
bypasses also include oversized keys counted without a shard lock. Add no UI or
logging stream.

## Alternatives considered

1. One global FIFO pool: uses all free capacity but serializes main/DNS hits
   through one mutex. Fixed shard budgets preserve local hit locking.
2. Sharded indexed expiry heaps: preserve expired-first victim selection without
   a full scan, but add coupled index state and logarithmic update/removal work
   without an accepted need for exhaustive expiry preference.
3. Reuse the existing ULS CLOCK cache: it demonstrates namespace/slot ownership,
   but slot search can scan its array and it carries unrelated generation/date
   semantics. Existing allocated LRU lists also add per-record nodes and hit
   ordering not required here.

## Consequences

### Benefits

- One combined record maximum, bounded reusable structural state and bounded
  cache-owned string payload.
- Overflow victim selection does not grow with shard population.
- Separate namespaces and TTL views retain source and negative-cache isolation.

### Risks

- Effective retention is smaller than the former multiplied allowance, which
  can increase local relookup, fallback/API calls or queued DNS work.
- Shard skew can evict below total capacity. FIFO can remove live records while
  expired records remain elsewhere in the shard. Payload bypass can cause
  repeated legitimate lookup work.
- Snapshot collection briefly acquires every shard lock; callers should use it
  as a diagnostic operation rather than on every lookup.
- Fixed slot/map backing is reserved on the first admission to each shard.
  At low occupancy this can use more structural memory than the former growing
  maps; `AllocatedSlots` distinguishes that reservation from retained records.
- Local invariants and benchmarks do not establish production p99 or RSS gains.

### Operational impact

No YAML values, cache persistence, startup/shutdown workers or DNS admission
policy change. Restart starts cold caches; no stored data migrates. Reversion
restores the original excessive allowance and overflow scans. Monitor shard
pressure, bypasses and upstream lookup work before changing configured capacity.

## Links

- Related tests: `reputation/cache_test.go`, `cache_consumer_test.go`,
  `cache_benchmark_test.go`, `cymru_test.go`, `lookup_order_test.go`, `gate_test.go`.
- Related docs: [operator configuration](../../data/config/README.md),
  [domain contract](../domain-contract.md#station-reputation).
- Related TSR: [TSR-0134](../troubleshooting/TSR-0134-reputation-lookup-cap-and-overflow.md).
- Supersedes / superseded by: none. Existing prelogin no-network policy in
  [ADR-0048](ADR-0048-multi-dimensional-prelogin-admission-and-logging-guardrails.md)
  and station allowance ownership in
  [ADR-0271](ADR-0271-shared-station-reputation.md) remain in force.
