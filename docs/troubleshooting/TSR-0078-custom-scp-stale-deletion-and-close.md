# TSR-0078: Custom SCP Stale Deletion and Close

- Status: Resolved
- Date opened: 2026-10-10
- Status date: 2026-10-10
- Technical Area: spot, custom_scp, persistence, cleanup, shutdown
- Led To ADR: ADR-0279

## RCA Summary

- What happened: cleanup raced mutable evidence and could delete a newer
  persisted observation or static membership after a successful refresh.
- Why: deletion requests and iterator snapshots outlived the evidence lock;
  commit did not recheck current disk contents or exclude intervening writes.
  Stop signalled the cleanup worker without joining it, and other disk users
  lacked lifetime ownership.
- What fixed it: store-local database lifetime ownership, joined cleanup,
  bounded deletion chunks, and current evidence/value checks held under write
  exclusion through commit. See ADR-0279.
- How we know: a scheduling-only overlay of baseline `4d711a4` reproduces lost
  persisted values and lost reopen support; the same generated DB causes the
  delivered-output consumer's positive expectation to fail. The repaired
  connected run preserves the refreshed values and delivers the spot.
- Operator/support answer: this is a storage repair. It protects learned
  history used by existing rules; it does not validate overall call accuracy
  or calibrate confidence percentages.

## Trigger

A call correction/confidence audit reproduced a Custom SCP cleanup race and
identified a possible stale-deletion interleaving. The owner approved a bounded
storage repair before method changes, requiring raw persistence, reopen,
delivered-output and deterministic shutdown evidence.

## Symptoms and impact

The in-memory store could retain a refreshed observer while cleanup deleted its
persisted row. A graceful reopen then lacked that observer. With identical
static membership, the fixture lost sufficient recent support and entered a
stabilizer hold instead of delivering immediately to telnet, buffer and archive.
The hold observation does not establish permanent loss after a later release.

## Hypotheses tested

1. Unlocked mutable entry reads during cleanup: supported by the earlier race
   detector counterexample. Source routed a deletion request to `s.entries`
   after releasing `s.mu`.
2. Snapshot-based deletion can erase a newer persisted refresh: supported by
   the connected baseline run, not merely by a hypothetical interleaving.
   The refresh was verified before the original cleanup resumed.
3. Missing history alone changes final fanout without changing confidence
   rules: supported by the baseline-produced consumer failure and the repaired
   run, plus an independently seeded one-observer negative control.

## Evidence

Run `pwsh -NoProfile -File scripts/test-customscp-regression.ps1 -Baseline`.
The producer succeeds so the actual defective database reaches the consumer.
Observed baseline manifest: `observation_present=false`, `static_present=false`,
`recovered_support=0`. The consumer reopens that database and reports TCP
timeout, `pending=1`, `ring=0`, `persisted_archive_rows=0`, with no archive read
error. Its fixed positive delivery assertion fails; the runner exits nonzero.

Run `pwsh -NoProfile -File scripts/test-customscp-regression.ps1`.
Observed repaired manifest: both preservation flags true, recovered qualified
support two. The consumer receives the exact 78-column TCP spot with `S`,
verifies the ring fields and decoded persisted archive row, and passes. Both
cases start with `?` and earn `S` through the existing support floor. The
one-observer control has identical static membership and remains held.

The raw/reopen tests verify exact timestamps and cells separately from delivery.
Cleanup tests also cover strict cutoff equality, expired/static removal,
overflow and key eviction across full chunks and tails, owned iterator keys,
derived indexes/counters/interner coupling, and simultaneous mutation/cleanup.
Lifecycle tests hold real checkpoint disposal and Verify scan operations,
retire the periodic owner, and queue Close against two-family-key recording.

## Root cause or best current explanation

Race freedom alone cannot protect persistence: a correct lock around the old
memory lookup still permits refresh after a deletion check and before commit.
Snapshot candidates must be revalidated, and the final check/commit interval
must exclude live persistence writes. Shutdown must wait for the operation's
resources to be disposed, not only signal its goroutine.

## Fix or mitigation

[ADR-0279](../decisions/ADR-0279-custom-scp-cleanup-and-database-ownership.md)
adds store-local ownership and deletion coordination. Admission, scoring,
horizons, encoding, config and live NoSync persistence remain unchanged. Chunk
bounds do not promise a wall-clock deadline; stalled disk I/O can delay writers
or Close. Best-effort disk errors and power-loss durability remain existing
limitations.

## References

- [Regression runbook](../runbooks/custom-scp-regression.md)
- `spot/custom_scp_cleanup.go`, `spot/custom_scp_cleanup_test.go`,
  `spot/custom_scp_lifecycle_test.go`, `spot/custom_scp_fixture_test.go`
- `internal/cluster/custom_scp_delivery_regression_test.go`
- Baseline: `4d711a4`; repair branch: `fix_customscp`
