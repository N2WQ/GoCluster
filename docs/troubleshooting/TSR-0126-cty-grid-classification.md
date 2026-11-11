# TSR-0126 - CTY Grids and Classification

Status: Monitoring
Date Opened: 2026-11-11
Date Resolved: n/a
Owner: Cluster maintainers
Technical Area: CTY matching, coordinates, metadata cache, gridstore
Trigger Source: Chat request
Led To ADR(s): ADR-0340
Tags: CTY, longitude, exact calls, snapshot, grid, provenance

## RCA Summary
- What happened: Mirrored CTY grids, exact-only keys classifying longer calls,
  and stale partial disk classification producing accidental origin fallback.
- Why: Longitude convention mismatch; all keys entered the trie; grid persistence
  was trusted as classification authority without coordinates/snapshot identity.
- What fixed it: Shared CTY conversion, eligible-prefix trie, grid-only hydration
  and bounded snapshot-tagged positive/negative classification caching.
- How we know: Red consumer tests reproduced mirrored durable grids, wrong country
  filters/license routing and literal JJ00. Fixed independent vectors, actual
  consumers, expiry/purge, late publications, restart and disposable rollout pass
  their recorded checks in the execution ledger.
- Operator/support answer: Code does not repair ambiguous old rows. Use a separately
  authorized verified-backup/empty-store procedure; retain new estimate persistence.

## Triggering Request
- Request date: 2026-11-11.
- Request summary: Review external findings, then sequential validated repairs
  unattended under exact Approved v5.
- Reference: [execution scope/evidence](../cty-grid-repair-v5.md).

## Symptoms and Impact
- K1ABC cold backfill saved MN52 instead of representative FN42; direct SHOW PROP
  shared the reversal. Japan was mirrored westward.
- K1DG inherited K1D's Puerto Rico exception, changing filters and skipping the
  login's existing US license branch.
- Hydrate expired grid/legacy CTY, expire cache grid, purge row and relookup:
  missing persisted coordinates were reconstructed as zero, yielding JJ00.
- Old positive/negative classification could be reused after a new snapshot.

## Timeline
1. 2026-11-11 - Source verification and a separate non-steered coordinate oracle.
2. 2026-11-11 - Current branch already contained merge/retirement repairs; retained.
3. 2026-11-11 - Red/green consumer/cache tests under Approved v5.

## Hypotheses and Tests
1. Generic geographic conversion is wrong: rejected. Publisher CTY longitude is
   west-positive; convert at its boundary, preserving east-positive inputs.
2. Reject the longest exact-only match afterward: rejected. A shorter ordinary
   ancestor can be eligible; retain only eligible prefix trie terminals.
3. Clear-on-refresh establishes freshness: rejected by controlled old-positive/
   new-negative and old-negative/new-positive publication after the new result.
4. Disk CTY can establish classification: rejected by contradictory disk fields
   over warm/cold hits/misses, both workers and literal expiry/purge/relookup.

## Findings
- Root cause: CTY's raw coordinate/match conventions and active snapshot authority
  were lost at the grid conversion, trie construction and disk-cache boundaries.
- No production database/service was accessed; affected live-row count is unknown.
- ExactCallsign is a matching rule, not geographic precision.
- Six-character length excludes this generator, not stale/incorrect association.
- JJ00 can be legitimate; it is not an automatic deletion predicate.

## Decision Linkage
- [ADR-0340](../decisions/ADR-0340-cty-grid-authority.md): coordinate boundary and
  snapshot authority, continued persistence, planned operator empty rebuild.
- Preserve [ADR-0338](../decisions/ADR-0338-grid-store-retirement.md) and
  [TSR-0119](TSR-0119-grid-batch-provenance.md).

## Verification and Monitoring
- Actual targeted/final ordinary, race, fuzz, performance and source-review
  results are recorded in [v5 execution](../cty-grid-repair-v5.md).
- Disposable checks verify backup contents, actual recovery selection, cold
  startup/restart and storage rollback. Windows publication denial includes
  changed content, unchanged checksum, exact permission restoration and a
  successful unprotected positive control.
- Monitor deployed build/input/path identity and actual cutover results. Persisted
  estimates may age across refresh; path-learning quality/weights are unchanged.
- Rollback restores the prior compatible build/database and their old defects.
  No live rollback or cutover occurred here.

## References
- [Operator procedure](../runbooks/cty-grid-rebuild.md).
- [Execution and raw evidence locations](../cty-grid-repair-v5.md).
- [Retirement TSR-0124](TSR-0124-grid-store-retirement.md).
