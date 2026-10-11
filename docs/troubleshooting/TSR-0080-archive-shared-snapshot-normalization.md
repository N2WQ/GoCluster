# TSR-0080 - Archive Shared Snapshot Normalization

Status: Monitoring
Date Opened: 2026-10-10
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: archive, spot, final fanout, peer
Trigger Source: Chat request
Led To ADR(s): ADR-0281
Tags: race, immutable snapshots, persisted normalization

## RCA Summary

- What happened: A connected correction delivery test exposed concurrent archive
  normalization and peer formatting of one sealed spot.
- Why: Archive owned enqueue reuses the immutable pointer, but flush mutated it.
- What fixed it: Worker-private `Spot.Clone()` before the existing normalization
  and encoding path.
- How we know: Linux and Windows P1 race stacks, followed by a standalone baseline
  fanout reproduction; fixed targeted archive and actual fanout race tests pass.
- Operator/support answer: Confirm the executable revision and the exact race
  stack. Preserve the database; this evidence does not prove archive corruption
  or call accuracy and does not justify resetting history.

## Triggering Request

- Request date: 2026-10-10
- Request summary: Complete bounded fully tested correction patches automatically;
  user approved the separate archive prerequisite with `Approved v2`.
- Request reference: `docs/archive-fanout-execution.md`.

## Symptoms and Impact

`EnsureNormalized` writes `DXCall` from the normalized cache while peer PC61
formatting reads it. Equal-value writes still violate concurrency ownership.
The reproduced tests do not establish corrupted archive contents, restart data
loss or measured production impact.

## Timeline

1. 2026-10-10 - P1 actual-output regression exposed Linux and Windows races.
2. 2026-10-10 - Read-only design/falsifiability review bounded an archive repair.
3. 2026-10-10 - User approved v2; P1 saved and baseline fanout race reproduced.

## Hypotheses and Tests

1. P1 changed archive or peer ownership.
   - Evidence: Relevant production files have no diff from fb86e75; standalone
     baseline `TestArchiveImmutableFanoutDelivered` reproduces the same race.
   - Outcome: Rejected.
2. Fully normalized spots need no writes.
   - Evidence: `EnsureNormalized` assigns nonempty `DXCallNorm` to `DXCall` on
     every call, matching the race stack.
   - Outcome: Rejected.
3. The race necessarily caused persisted corruption.
   - Evidence: Ordinary connected output/reopen assertions passed.
   - Outcome: Inconclusive; no corrupted record or restart loss reproduced.

## Findings

- Root cause: Mutation after immutable shared fanout handoff.
- Contributing factors: Earlier isolated fixtures did not exercise concurrent
  archive insertion and peer reads; reopen normalization can hide raw-byte drift.
- Durable decision: Assign normalization mutation to a worker-private clone.

## Decision Linkage

- ADR created/updated: ADR-0281.
- Decision delta summary: Private archive scratch replaces mutation of shared input.
- Contract/behavior changes: Enforce existing immutable handoff and persisted
  normalization contracts; no scoring, schema, queue or shutdown policy change.

## Verification and Monitoring

- Validation steps run: Baseline ordinary connected delivery passed; baseline
  Windows race failed with the archive-write/peer-read stack. Archive input
  preservation and normalized concurrent-read tests also fail on baseline.
  Fixed targeted race checks pass, retaining literal stored bytes and graceful
  reopen. Full native ordinary/race suites, affected Linux ordinary/race,
  analyzers and connected storage checks passed; see execution record for exact
  commands, measured copy costs and remaining production-load uncertainty.
- Signals to monitor: Race stacks, archive queue drops, allocation/GC and archive
  latency under live load. Local benchmarks are not production qualification.
- Rollback triggers: Normalization/record drift or unsustainable archive copy cost;
  do not restore shared mutation as a race workaround.

## References

- Related ADR(s): [ADR-0281](../decisions/ADR-0281-archive-worker-private-normalization.md).
- Related docs: [execution](../archive-fanout-execution.md).
