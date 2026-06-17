# TSR-0026: Archive Cleanup Scan CPU Spikes

- Status: Resolved
- Date opened: 2026-06-06
- Status date: 2026-06-06

## RCA Summary

- What happened: Scheduled archive cleanup caused recurring CPU spikes on the
  small production instance.
- Why: Cleanup still iterated expired timestamp-ordered Pebble keys and deleted
  them one at a time even though ADR-0149 had reduced retention to a single
  timestamp cutoff.
- What fixed it: ADR-0151 changed cleanup to probe the oldest key and use a
  Pebble `DeleteRange(start, end)` tombstone for the expired timestamp range;
  obsolete cleanup batch YAML keys are now rejected.
- How we know: Source inspection showed timestamp-ordered `s|` keys and
  per-key delete batching, and archive/config tests plus
  `BenchmarkCleanupOnceRangeDeleteLargeExpired` validated the range-delete
  path.
- Operator/support answer: For cleanup CPU spikes, check whether archive range
  deletion is active and remove obsolete cleanup batch settings.

## Trigger

Production profiling on the long-running cluster showed recurring CPU spikes
from scheduled archive cleanup after the archive and stabilizer patches.

## Symptoms and impact

Cleanup work could consume a visible portion of the small EC2 instance CPU
budget during a cleanup pass. The problem was bursty rather than continuous:
normal ingest continued, but the cleanup window competed with the hot path and
other scheduled work.

## Hypotheses tested

1. Archive encode cost was the cleanup bottleneck.
2. Cleanup was dominated by scanning and deleting expired Pebble rows one key at
   a time.
3. The remaining separate-retention logic was still forcing mode decode during
   cleanup.

## Evidence

- Current code used timestamp-ordered archive keys with the `s|` prefix.
- Current cleanup opened a Pebble iterator, scanned expired keys, and submitted
  per-key batch deletes.
- ADR-0149 had already removed the separate FT/non-FT retention decision, so
  cleanup only needed a timestamp cutoff.
- Pebble supports `DeleteRange(start, end)` for point keys in `[start, end)`,
  which matches the existing archive key layout.

## Root cause or best current explanation

The remaining archive cleanup cost was caused by per-key iteration and point
delete batching over expired rows. With one retention window and timestamp
ordered keys, that work was unnecessary; a single timestamp range tombstone can
make the same expired key range invisible.

## Fix or mitigation

Archive cleanup now probes the oldest archive key and, when expired rows exist,
uses Pebble range deletion from the archive spot lower bound to the cutoff key.
The obsolete cleanup batch YAML keys were removed and are rejected on startup.

## Why an ADR was or was not required

- ADR required because the fix changes an operator-visible config contract and
  the archive cleanup resource-bound strategy.

## Links

- Related ADRs: ADR-0151
- Related tests: `archive/archive_cleanup_test.go`,
  `config/archive_config_test.go`
- Related benchmark: `BenchmarkCleanupOnceRangeDeleteLargeExpired`
