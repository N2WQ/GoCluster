# ADR-0345: Grid Active Work Authority

- Status: Accepted
- Date: 2026-11-11
- Decision Origin: Troubleshooting chat; exact Approved v6

## Context

ADR-0343 retained a known late-derived-hydration window. Cache rejection did not
also reject a synchronous reply, and queued report commits did not repair a
derived cache value created after Clear. Aggregate record UpdatedAt could not
identify which report a coalesced commit represented.

## Decision

One private grid owner orders changed reports, candidate selection, replies,
writer ownership and publication. Worker registration begins after dequeue and
before reading one captured concrete store. Existing hook signatures, queues,
workers, timers and the eight-millisecond synchronous deadline remain unchanged.
Only a synthesized candidate that actually wins selection may be queued for
persistence; a newer selected cache estimate does not authorize writing the old one.
Warm cache hits keep their shard-lock read; unchanged reported updates preserve
LRU recency without entering the coordinator. Changed work rechecks under
coordinator then shard. Storage and callbacks run outside these locks.

Reported locator, full precision, provenance and success are selected together.
The actual response uses that selection. Pending actual reports plus absent
usable reported cache are contested; a worker can return unknown even when it
read an older reported row. CTY-only and derived pending updates do not create
that conflict. Cache hits still complete a lookup without mandatory disk reads.

Report envelopes retain the selected locator's live revision through coalescing.
CTY-only timestamps cannot replace that revision. A successful commit publishes
eligible reported authority before releasing its owner. A separate publication
identity fences old reads without invalidating a newer queued report's ticket.
Old A cannot overwrite newer live B when B's best-effort write fails or drops.

Cache Clear and store replacement have distinct identities. Same-store commits
may establish authority after Clear. Retired-store work only retires ownership.
Set(nil) preserves existing offline live-cache availability; restored publication
and cache invalidation are one ordered operation. Existing complete shutdown
ownership and its lack of an overall disk/callback deadline remain.

At most five temporary compact stored-report copies arbitrate already-positive
reported reads, including nil or immediately expired caches. They never promote
MISS/derived/error, or retain an unpersisted live grid. Negative witnesses prevent
pin borrowing/recreation; they do not disprove an independently positive raw-call
fallback or strengthen existing overlapping-read/purge freshness semantics.

Active entries belong only to 8,192 queued reports, one writer handoff, 500 batch
entries and five workers: at most 8,698 distinct keys. Coalescing transfers owners
without historical lists. Keys and stored locator copies are compact clones.
Copied text is bounded by 8,698*K + 5*G, where K=3*max(1024,effective command-line
limit,15), and G=3*max(65535,effective PSK payload limit,effective command-line
limit,1024). Bookkeeping and map rebuild overlap are separate bounded costs.
Empty backing is released; anchored churn triggers compaction. Nonzero immutable
identities cannot wrap or exhibit ABA while an owner retains them.

Four fixed headless/cache-pane rows show active keys, copied bytes, pending
reports and superseded candidates. Gauges describe active coordination; the
superseded total follows the existing grid metric lifetime. The cache pane's
default height grows from 9 to 13; rows fit a 60-column layout unsigned-safely.

## Alternatives considered

1. Provenance checks or lookup tokens alone: omit raw replies or pending commits.
2. Cache-owned tokens plus a separate pending index: viable, but spreads lifecycle
   and nil-cache authority across two owners.
3. Shard-wide invalidation on every report: can suppress unrelated derived grids.
4. Disk reads or waits under the cache lock: can stall delayed release.
5. Fresh disk lookup for every request or permanent report history: changes the
   accepted cache/latency/resource contract.

## Consequences

### Benefits
- Runtime selection and real worker replies share reported authority.
- Cache loss, store replacement and writer failure retain bounded ownership.
- Fixed diagnostics expose active work and discarded stale candidates.

### Risks
- Cold work allocates compact keys, identities and bounded stored copies.
- Contested corrections may still leave with unknown grid; late work cannot
  alter already sealed output. Local checks do not establish production p99.

### Operational impact
- No YAML default/schema, store encoding, retention or best-effort durability change.
- Cache TTL preserves persisted row age; an old disk row can still serve a
  synchronous positive response while being immediately expired in cache.
- No old database/provenance repair, feed-specific provenance or CTY re-estimation.

## Links
- [TSR-0129](../troubleshooting/TSR-0129-grid-cache-authority.md).
- [Execution and validation](../cache-geography-repair-v6.md).
- Tests: internal/cluster/grid_authority*_test.go, grid_writer_test.go,
  grid_retirement_test.go and corrected_grid_delivery_test.go.
- Refines only the cache-hydration limitation of [ADR-0340](ADR-0340-cty-grid-authority.md)
  and [ADR-0343](ADR-0343-corrected-grid-precedence.md).
- Superseded by: none. Other CTY science, v5 output and ADR-0338 retirement remain.
