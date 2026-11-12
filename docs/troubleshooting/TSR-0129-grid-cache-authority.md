# TSR-0129 - Grid Cache and Worker Authority

Status: Monitoring
Date Opened: 2026-11-11
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: grid cache, lookup workers, persistence and retirement
Trigger Source: Chat request
Led To ADR(s): ADR-0345
Tags: grid, provenance, cache, epochs, bounded state

## RCA Summary
- What happened: a derived cache value could mask a committed reported locator;
  a synchronous worker could return a candidate its cache had rejected.
- Why: cache hydration, replies and writer acknowledgement had separate authority.
- What fixed it: one active-work selector, owned report revisions, separate store
  and cache epochs, and bounded temporary arbitration of positive stored reads.
- How we know: the real asynchronous lookup/report/Clear regression failed on
  the original implementation with disk FN31AB/reported and cache FN20/derived.
  It passes after repair. Actual buffered worker replies, real writer failures,
  epoch changes and cache loss have deterministic regression coverage.
- Operator/support answer: reported grids retain full precision and provenance.
  Contested or incomplete corrections can still leave with unknown location.
  Observe the four Grid work rows; this is not a durability or delivery guarantee.

## Triggering Request
- Request date: 2026-11-11.
- Request summary: architect and execute cache precedence and geography repairs.
- Request reference: exact Approved v6; [execution record](../cache-geography-repair-v6.md).

## Symptoms and Impact
- Cache Clear or eviction could remove a report before an older lookup completed.
- A new post-Clear lookup could cache a derived value before the queued report
  committed; the writer did not refresh that cache afterward.
- Cache rejection alone did not protect the synchronous response path.
- Location filters and path preparation could therefore use the wrong authority.

## Timeline
1. 2026-11-11 - Source, existing v5 contracts and stale-result interleavings reviewed.
2. 2026-11-11 - Real old-code regression reproduced mismatched disk/cache tuples.
3. 2026-11-11 - Shared selection, ownership and deterministic regressions implemented;
   exact final commands and qualification remain recorded in the execution record.

## Hypotheses and Tests
1. Cache provenance checks alone protect the caller.
   - Evidence: synchronous workers returned their raw record after ApplyRecord.
   - Outcome: Rejected; TestGridAuthorityWorkerReply observes the actual response.
2. An admission token alone repairs cache loss.
   - Evidence: a newly admitted post-Clear lookup can precede a pending commit.
   - Outcome: Rejected; writer publication and owned pending reports are required.
3. A shared negative witness should reject every overlapping positive read.
   - Evidence: an absent base lookup and a successful raw-call fallback can overlap.
   - Outcome: Rejected; TestGridAuthorityRawFallbackAfterDerived protects the actual
     reported read. Negative witnesses fence pin borrowing/recreation only.
4. Queue size bounds every retained historical reference.
   - Evidence: duplicates can coalesce indefinitely into one pending record.
   - Outcome: Rejected; ownership transfers on coalescing and anchored churn
     forces backing compaction without waiting for an empty map.

## Findings
- Root cause: no complete authority boundary across reports, reads and commits.
- Contributing factors: rejected candidates still escaped through replies;
  aggregate UpdatedAt does not identify the reported locator selected in a batch.
- A durable decision was required for cross-owner ordering and bounded diagnostics.

## Decision Linkage
- ADR created: [ADR-0345](../decisions/ADR-0345-grid-active-work-authority.md).
- Decision delta: replace the known late-hydration window with active-work ordering.
- Contracts preserved: cache-hit sufficiency, disk/cache TTL distinction,
  persistence of estimates, best-effort queues, lookup deadlines and retirement.

## Verification and Monitoring
- Validation: TestGridAuthority families cover actual replies, commits, Clear/LRU/TTL,
  raw fallback, pins, nil/expired cache, epochs, overload, coalescing and churn.
  Final-state evidence is in the execution record; no production qualification is claimed.
- Signals: active keys, copied bytes and pending reports converge to zero after
  drain. Superseded counts accumulate for the grid metric lifetime.
- Rollback triggers: reported precision/provenance loss, abandoned owners,
  growth with historical input, or measurable warm-path regression beyond scope.

## References
- [Approved execution and evidence](../cache-geography-repair-v6.md).
- [ADR-0345](../decisions/ADR-0345-grid-active-work-authority.md),
  [ADR-0338](../decisions/ADR-0338-grid-store-retirement.md).
- Source: internal/cluster/grid_authority.go, grid_writer.go, call_meta_cache.go.
- Tests: internal/cluster/grid_authority*_test.go and grid_retirement_test.go.
