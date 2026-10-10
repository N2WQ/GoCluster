# ADR-0279: Custom SCP Cleanup and Database Ownership

- Status: Accepted
- Date: 2026-10-10
- Decision Origin: Troubleshooting chat

## Context

Custom SCP cleanup collected deletion requests under the evidence mutex, then
read mutable entries and committed deletes after releasing it. A Pebble iterator
is a snapshot: an observation refreshed after iterator creation could still be
deleted using the old snapshot. The shared periodic cleanup runner signalled
shutdown without joining the active callback; checkpoint and integrity callers
could also overlap Pebble closure.

## Decision

Keep the repair local to Custom SCP. A database lifetime reader covers each
complete disk operation, including iterator, batch and value-closer disposal.
Close joins the periodic cleanup owner, then takes the lifetime writer and
closes Pebble. Concurrent Close callers share completion. Cleanup cannot start
again after Close; Stop alone remains restartable.

Use lock order database lifetime, persistence mutation, then evidence. Live
recording takes the mutation lock before changing evidence and writing Pebble.
RecordSpot owns one lifetime reader across both family keys. Cleanup rechecks
current evidence and the current persisted value before committing each chunk,
with mutation exclusion held continuously through commit. Evidence lookups can
continue during cleanup disk I/O. Startup loading remains synchronous and
unpublished until complete.

Capture at most 512 deletion candidates per chunk and scan at most 512 iterator
rows per scan chunk. Partial pruning preserves expiry, active-count and interner
ownership. Fresh replacements survive stale or malformed-value requests even
when absent from memory. Overflow requests still remove genuinely discarded
fresh observations, unless the reporter has been reinserted into retained
evidence. Unknown disk-read errors cannot authorize deletion.

Preserve admission, horizons, strict less-than expiry boundaries, score rules,
8/10-byte encodings, live NoSync writes and best-effort disk error handling.
Close retains the previous memory-only recording behavior. No shared cleanup
runner, runtime scheduler, confidence policy or config default changes.

## Alternatives considered

1. Hold the evidence mutex throughout the complete database scan and commits.
   This blocks correction support lookups during disk I/O.
2. Recheck memory without serializing the final commit against live writes.
   This leaves the check-to-commit deletion window open.
3. Introduce generation maps or a persistence journal. This adds retained state
   and a recovery contract that the bounded repair does not need.
4. Change the shared runner and runtime shutdown orchestration. Store-local
   ownership covers all Custom SCP callers without affecting other stores.

## Consequences

### Benefits

Refreshed evidence survives cleanup and graceful reopen. Cleanup, maintenance
and live writes cannot use a closed database. No new lifetime-sized side table
is introduced.

### Risks

A chunk bound limits candidate memory and operation counts, not elapsed time.
Per-key pruning still walks its retained spotter slice, and existing key
eviction still scans/deletes that key's persisted prefix; the 512 bound applies
to the new cleanup candidate and scan chunks, not every inherited operation.
Slow or stalled Pebble I/O can delay live writers and Close. Best-effort write
failures can still leave fresh discarded disk rows until existing expiry/load
convergence; this repair does not add a retry journal or a disk-byte limit.

### Operational impact

Storage correctness improves without changing confidence or correction rules.
The connected regression proves a history-dependent delivery boundary, not RF
identity accuracy, calibrated probabilities or production latency.

## Links

- Related tests: `spot/custom_scp_cleanup_test.go`,
  `spot/custom_scp_fixture_test.go`,
  `internal/cluster/custom_scp_delivery_regression_test.go`
- Related docs: [regression runbook](../runbooks/custom-scp-regression.md),
  [retained layout](ADR-0080-custom-scp-retained-heap-layout.md),
  [runtime evidence](ADR-0039-custom-scp-runtime-evidence-and-shared-pebble-resilience.md)
- Related TSRs: [TSR-0078](../troubleshooting/TSR-0078-custom-scp-stale-deletion-and-close.md)
- Related issues/PRs/commits: branch `fix_customscp`, baseline `4d711a4`
- Supersedes / superseded by: none; supplements the existing evidence store
  ownership contract.
