# ADR-0281: Archive Worker Private Normalization

- Status: Accepted
- Date: 2026-10-10
- Decision Origin: Troubleshooting chat

## Context

Final fanout seals one spot shared by ring history, telnet, archive and peer.
Archive owned enqueue explicitly reuses that immutable pointer. `flush` called
`EnsureNormalized`, which writes cached fields and always assigns a populated
DX cache back to `DXCall`. A new connected delivery test reproduced that write
against the peer formatter's read, even with a fully normalized input.

## Decision

The archive insertion worker clones each non-nil queued spot before running the
existing normalization and record encoder. Only its private clone is mutable.
Use `Spot.Clone`, which copies semantic fields and categories without copying
the formatter's `sync.Once`. Keep owned queue handoff pointer reuse, existing
normalization/cache precedence, record bytes/schema, queue drops, batching,
retention and Stop drain/join behavior.

The private copy is per-row scratch in the existing single insertion worker.
No additional retained state, locks, workers or queues are introduced.

## Alternatives considered

1. Remove normalization: raw inputs would persist different identities, modes
   or metadata, and reopen normalization could conceal that difference.
2. Normalize before sealing in one caller: public archive inputs and direct
   flush callers would remain exposed; normalized inputs still receive writes.
3. Read-only codec normalization: possible, but duplicates normalization and
   populated-cache precedence with a larger compatibility proof burden.
4. Copy the full Spot by value or protect only the writer with a mutex: the first
   copies `sync.Once`; the second does not synchronize other consumers.

## Consequences

### Benefits

- Async consumers retain an immutable shared identity.
- Existing normalization and stored-record semantics remain centralized.

### Risks

- Per-row cloning increases allocation and CPU; measure locally and monitor
  archive workload/GC under production load before claiming capacity.
- A race report does not itself prove corrupted persisted data.

### Operational impact

- No migration, configuration, protocol or confidence-rule change.
- Existing bounded overflow still drops archive writes without blocking ingest.
- Reopen checks establish graceful persistence, not power-loss durability.

## Links

- Related tests: `archive/ownership_test.go`, archive normalization regressions,
  `internal/cluster/archive_fanout_delivery_test.go`.
- Related docs: [execution](../archive-fanout-execution.md).
- Related TSRs: [TSR-0080](../troubleshooting/TSR-0080-archive-shared-snapshot-normalization.md).
- Supersedes / superseded by: none; refines worker ownership under
  [ADR-0148](ADR-0148-post-stabilizer-shared-secondary-fanout.md) and
  [ADR-0275](ADR-0275-runtime-output-retirement.md).
