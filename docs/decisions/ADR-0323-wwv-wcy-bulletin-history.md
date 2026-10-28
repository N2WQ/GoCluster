# ADR-0323: WWV and WCY Bulletin History

- Status: Accepted
- Date: 2026-10-28
- Decision Origin: Design

## Context

Users need current WWV and WCY information at login and a way to inspect
recent bulletins. Bulletins are separate from spots: peer and human-feed
sources converge at the telnet bulletin path, and live delivery is subject to
per-user filters and duplicate suppression. The existing optional Pebble spot
archive has its own retention and backpressure contract.

## Decision

Maintain a dedicated, bounded bulletin history independently of the spot
archive and connected clients. Select the latest and the rolling 24-hour window
by cluster receipt time. Retain at most 48 records per type, evicting the
oldest on capacity; report detected or possible gaps. Retain the first 256
characters of longer bulletin text and mark truncation. Identify duplicates
using the original bulletin before truncation, with the configured bulletin
duplicate window, independently of eligible live recipients. Existing upstream
peer duplicate suppression remains in effect.

Show the latest retained bulletin of each type at login regardless of the live
filters. `SHOW WWV` and `SHOW WCY` display the latest five of one type; `FULL`
shows the retained 24-hour history, in receipt order. Both dialects accept
`SH WWV`, `SHOW/WWV`, and `SH/WWV`, with equivalent WCY forms; `FULL` follows
after a space and numeric count suffixes are unsupported. These requests bypass
live bulletin filters; later live announcements continue to honor them. An
announcement concurrent with login may interleave with or repeat the login
display. Live bulletin text is not truncated by the archive limit.

Use a dedicated JSON snapshot, written by a bounded background owner and
restored on orderly restart. Keep storage I/O off the live bulletin path.
Storage failures must not stop live service; report incomplete or uncertain
history and retry recovery. An unclean shutdown can lose unwritten data and
must not be presented as a proven complete history. Bound response work and
queued memory; report a retry when the history response budget is exhausted.

## Alternatives considered

1. Reuse the Pebble spot archive: fewer stores, but spot archive enablement,
   retention, backpressure, and recovery policy would couple unrelated data.
2. Use a separate Pebble database: stronger database machinery, but additional
   lifecycle and compaction overhead for at most 96 short bulletin records.
3. Keep history only in memory: simpler, but loses all history on restart.

## Consequences

### Benefits

- Users can read the latest conditions and a recent history even when live
  bulletin filters are disabled or no clients were connected at receipt.
- Separate per-type limits and a bounded background writer protect the cluster.

### Risks

- More than 48 distinct bulletins in a day can cause earlier history loss.
- The history cannot include messages lost before the cluster receives them,
  and a crash or storage outage can make completeness uncertain.
- Truncated records omit their later text; the output marks that fact.

### Operational impact

Deploy the new executable and `bulletin_archive.path` setting, ensure that its
directory is writable, monitor history warnings, and include the snapshot in
backups when needed. A rollback to the earlier 24-record bulletin executable
can reject and overwrite a snapshot with more than 24 records per type: back up
the snapshot and restore a compatible copy before starting that executable.
The pre-bulletin executable does not use this snapshot. Rollback does not
require modifying the spot archive.

## Links

- Related decision: [ADR-0063](ADR-0063-configurable-bulletin-dedupe.md)
- Recovery and login refinements: [ADR-0324](ADR-0324-bulletin-recovery-and-login-order.md)
- Related docs: [commands](../../commands/README.md#wwv-and-wcy-bulletins),
  [telnet](../../telnet/README.md#wwv-and-wcy-bulletin-history)
