# ADR-0324: Bulletin Recovery and Login Order

- Status: Accepted
- Date: 2026-10-28
- Decision Origin: Troubleshooting chat

## Context

Review of the implementation of ADR-0323 found that a failed snapshot load
could be followed by replacement of recoverable data, archived terminal control
characters could be replayed, and login history followed the shipped prompt.

## Decision

Before replacing an existing snapshot whose load failed, preserve an independent,
complete recovery copy. Keep recovery artifacts bounded and never overwrite a
different recovery copy. When preservation cannot be completed, keep the
original and continue accepting bounded history in memory; warn and retry.
Record possible gaps even after storage recovers. Storage I/O remains outside
the live bulletin path. Filesystem durability depends on the platform and
hardware; a test of injected failures is not proof of power-loss behavior.

Sanitize archived bulletin display, including snapshots written by older
versions. Continue to identify duplicates from the original bulletin and keep
the existing archive truncation limit. Live bulletin text is unchanged.

Use `<STARTUP_INFO>` to place startup notices and bulletin history before the
prompt in the shipped greeting. If the marker is absent, place the notices and
history before the complete greeting. The first marker inserts the block and
subsequent markers are removed. Concurrent live messages may still interleave.

## Alternatives considered

1. Keep all new history memory-only after a failed load: preserves the original,
   but prevents persistence even when an independent recovery copy is possible.
2. Recognize the shipped greeting text in code: avoids a marker but duplicates
   an operator-owned configuration format and can become stale.
3. Sanitize live delivery as well: changes the existing live bulletin contract
   and is outside the selected scope.

## Consequences

### Benefits

- A failed load does not silently destroy the recoverable snapshot.
- Archived display does not replay terminal control characters.
- The shipped greeting ends its deliberate startup output with the prompt.

### Risks

- A blocked recovery can leave new history only in memory until an operator
  repairs storage or moves a conflicting artifact aside.
- Live bulletin delivery still retains upstream text, including controls.
- Concurrent live traffic can follow the prompt; storage durability has platform
  and hardware limits.

### Operational impact

Deploy the executable and updated active greeting together for welcome-first
ordering. Existing unmarked custom greetings use the fallback. Before running an
older executable, remove the marker from the active greeting. Preserve the
snapshot and recovery artifacts when investigating a storage warning and follow
the older-snapshot rollback guidance in ADR-0323. Stop the cluster before
editing or replacing its active snapshot; recovery assumes a single writer.

## Links

- Refines: [ADR-0323](ADR-0323-wwv-wcy-bulletin-history.md).
- Troubleshooting: [TSR-0111](../troubleshooting/TSR-0111-bulletin-history-recovery-and-display.md).
- Operator guidance: [bulletin history](../../telnet/README.md#wwv-and-wcy-bulletin-history), [configuration](../../data/config/README.md).
