# ADR-0338: Complete Grid Store Retirement

- Status: Accepted
- Date: 2026-11-11
- Decision Origin: Troubleshooting chat

## Context

Grid lookup workers can create derived updates after a caller's tight timeout.
Closing the update channel first races these producers. Cached hook calls could
also continue after stop. Checkpoint, integrity and corruption recovery workers
had no completion handle and could access or publish storage after runtime close.
The owner approved this bounded lifecycle repair in overnight Scope Ledger v1.

## Decision

- Retain read admission for each complete external grid/CTY/lookup hook. Stop
  takes exclusive admission, makes the component terminal, closes lookup queues,
  joins lookup producers, then closes and drains the update writer. Subsequent
  hooks do nothing or return a miss without cache or metric mutation.
- Keep internally owned derived sends valid until their lookup workers finish.
  Preserve queue sizes, overflow/drop behavior, lookup timeout and provenance.
- Create a grid-only child context before recovery can start. Expose completion
  for checkpoint, integrity and recovery work; recovery completion includes its
  warning child, late returned-store disposal and cache invalidation.
- Runtime retirement cancels that child, retires hooks/writer, joins all grid
  background owners and closes the concrete store. One runtime Once covers the
  entire sequence, including Close; repeated callers await the same completion.
- Keep this retirement at the existing downstream-close position. Preserve
  ADR-0275's output/peer/global-cancellation order and ownership until synchronous
  storage or callbacks return. There is no overall shutdown wall-clock deadline.

## Alternatives considered

1. Close updates before lookups: unsafe because an admitted lookup can still send.
2. Check only a terminal atomic flag: does not fence an entire admitted hook body.
3. Cancel the global context early: changes output/peer dependency retirement.
4. Wait a fixed grace then detach: abandons owners still using storage.
5. Redesign public storage interfaces: unnecessary; private operation seams let
   tests control the actual loops without global hooks or a second implementation.

## Consequences

### Benefits
- Completion covers every grid storage owner, including partial startup and
  recovery's nested warning work.
- Admitted final updates reach the existing writer before its final flush.

### Risks
- Each external hook now takes a read lock; measured local overhead and unchanged
  allocation counts are recorded in the execution ledger.
- A hung synchronous disk operation or callback can extend shutdown. This repair
  retains ownership rather than promising forced termination.

### Operational impact
- No config, schema, queue size, lookup timeout or disk-recovery algorithm change.
- Concrete store-close errors are now logged instead of silently ignored.
- This does not repair old stored provenance or guarantee network delivery.

## Links
- Scope/evidence: [overnight ledger](../codebase-correctness-overnight-2026-11-10.md).
- Tests: `internal/cluster/grid_retirement_test.go`, `grid_hook_bench_test.go`.
- Related TSR: [TSR-0124](../troubleshooting/TSR-0124-grid-store-retirement.md).
- Supersedes / superseded by: none; refines grid ownership under
  [ADR-0275](ADR-0275-runtime-output-retirement.md).
