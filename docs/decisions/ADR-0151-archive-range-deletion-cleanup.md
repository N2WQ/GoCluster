# ADR-0151: Archive Range Deletion Cleanup

- Status: Accepted
- Date: 2026-06-06
- Decision Origin: Troubleshooting chat

## Context

After ADR-0149, archive retention uses one timestamp cutoff for every mode.
Profiling still showed archive cleanup as a CPU spike because cleanup walked
expired Pebble keys with an iterator and deleted each row through batches. That
work was scan-bound: expired-row count drove iterator, decompression, batch
delete, and yield overhead even though the archive key layout is already
timestamp ordered.

The optimization target is cleanup scan cost, not archive encode cost.

## Decision

Archive cleanup will remove expired rows with Pebble timestamp range deletion:

- compute the cutoff from `archive.retention_seconds`;
- use `spotKeyBytes(cutoff, 0)` as the exclusive range end;
- delete `[spotIterLower, rangeEnd)` with `DeleteRange`;
- probe the oldest visible archive key before writing the range tombstone so
  steady-state cleanup does not write empty delete ranges;
- repeat the same lower-bound-to-cutoff range on later passes so late-arriving
  old rows inserted after a prior range tombstone are removed by the next
  cleanup.

The operator-visible cleanup batch settings are removed:

- `archive.cleanup_batch_size`;
- `archive.cleanup_batch_yield_ms`.

Those YAML keys are rejected at startup with a migration error. The remaining
cleanup knobs are `archive.cleanup_interval_seconds` and
`archive.retention_seconds`.

The ignored Pebble compatibility fields `archive.busy_timeout_ms` and
`archive.preflight_timeout_ms` are also removed from the archive config
contract. They had no runtime consumer after the archive moved to Pebble.

## Alternatives Considered

1. Keep batched point deletes with a smaller batch size.
   - Rejected because it still scales CPU with the number of expired rows and
     keeps the same iterator/decode pressure that profiling identified.
2. Add an in-memory cleanup watermark.
   - Rejected because old spots can arrive after a cleanup pass. A watermark
     would either miss those late old rows or need more retained state and edge
     handling.
3. Trigger manual compaction after every cleanup.
   - Rejected for this phase because range deletion makes expired rows
     invisible immediately, while forced compaction can add large I/O and CPU
     spikes on a small EC2 instance.

## Consequences

### Benefits

- Cleanup CPU no longer scales with the number of expired archive rows.
- Heap and allocation pressure from cleanup iteration and batch delete work is
  materially reduced.
- The archive config surface is simpler and no longer exposes misleading batch
  knobs.

### Risks

- Physical disk reclamation is compaction-driven; expired rows become invisible
  immediately, but bytes may remain on disk until Pebble compacts.
- Private config directories that still contain the removed cleanup keys must
  remove them before startup.
- Range tombstones must be considered if future archive key layouts add other
  point-key families under the same lower/upper bounds.

### Operational Impact

- No archive record schema, telnet protocol, peer protocol, or spot parsing
  change.
- `SHOW DX` archive reads stop seeing expired rows after cleanup because Pebble
  applies the range tombstone during iteration.
- Cleanup cadence remains operator-owned through
  `archive.cleanup_interval_seconds`.

## Links

- Related tests: `archive/archive_cleanup_test.go`,
  `config/archive_config_test.go`
- Related benchmark: `BenchmarkCleanupOnceRangeDeleteLargeExpired`
- Related config: `data/config/archive.yaml`
- Related docs: `data/config/README.md`
- Related TSRs: TSR-0026
- Supersedes / superseded by: refines the cleanup implementation accepted in
  ADR-0149
