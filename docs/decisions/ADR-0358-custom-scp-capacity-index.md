# ADR-0358: Custom SCP Capacity Index

- Status: Accepted
- Date: 2026-11-18
- Decision Origin: Troubleshooting chat, Approved v1

## Context

At the shipped 250,000-observation-key cap, accepting a new key selected the
oldest existing key by scanning the complete map under the evidence lock.
Disposable copies of a full checkpoint averaged 21–22 ms per insertion at the
cap. A CPU profile showed that selecting the victim dominated the cost.

The receiver-expiry queue orders keys by the oldest retained receiver and can
prioritize oversized entries. The eviction rule orders keys by their newest
retained receiver and then by the existing lexicographic call/band/mode key.

## Decision

Maintain an indexed min-heap for capacity eviction under the evidence lock.
An observation entry holds its one-based position. Each heap slot holds the
canonical key, the entry pointer and the indexed rank. Identical ranks skip
heap repair. Capacity admission selects the minimum existing key before
inserting the incoming key, including during synchronous startup load.

Couple the index to primary admission, rank changes, replacement, and removal,
including cleanup's expiry-pop path and lazy support-query pruning. The heap
does not acquire intern references. It clears removed slots and releases its
backing array when empty. Geometric growth clamps capacity to the effective
observation-key cap. For an effective cap N, the structure uses at most N
slots and one position per primary entry. A growing slice can temporarily
overlap with the preceding array, with fewer than 2N slots across both arrays.
At 250,000 keys on amd64, the nominal steady payload is about 17 MiB including
positions, before allocation size classes. This is a structural budget.

Keep the established database lifetime and mutation order, receiver expiry,
in-memory static membership, persistence, and prefix deletion. The index is
reconstructed on open and carries no persisted representation. The opener
also releases its caller-owned Pebble cache reference after open returns.

## Alternatives considered

1. Augment the expiry tree with a second ordering. This saves separate
   backing storage but couples eviction to expiry mutation and temporary
   removal during cleanup.
2. Keep scanning the primary map. This avoids index memory but has measured
   linear cost in the capacity workload.

## Consequences

### Benefits

- Capacity victim selection is constant-time. Index repair takes logarithmic
  work, with bounded geometric allocation growth during admission.
- The existing diagnostics report occupied index items, backing slots and
  live observation-key evictions. The live counter excludes startup and
  cleanup repair.

### Risks

- The additional index occupies memory and must be repaired after every
  retained-rank change or primary removal.
- Prefix deletion and slow Pebble I/O can still delay writes. A production
  latency claim requires observation of live capacity churn.

### Operational impact

- Existing files and the prior executable remain compatible. The shipped
  YAML explains that max_db_bytes is an inactive compatibility setting.

## Links

- Related tests: spot/custom_scp_eviction_test.go and the connected Custom SCP
  storage and delivery regression.
- Related docs: [ADR-0080](ADR-0080-custom-scp-retained-heap-layout.md),
  [ADR-0279](ADR-0279-custom-scp-cleanup-and-database-ownership.md).
- Related TSR: [TSR-0137](../troubleshooting/TSR-0137-custom-scp-capacity-eviction-and-pebble-ownership.md).
- Supersedes / superseded by: none.
