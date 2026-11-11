# TSR-0119 - Grid Batch Provenance

Status: Monitoring
Date Opened: 2026-11-10
Date Resolved: n/a
Owner: Cluster maintainers
Technical Area: grid metadata batching and persistence
Trigger Source: Chat request
Led To ADR(s): none
Tags: grid, derived, CTY, batching, persistence

## RCA Summary
- What happened: Pending grid updates could persist an approximate locator as
  actual, replace an actual locator with an approximate one, or label a later
  actual observation as derived.
- Why: The pending coalescer copied `Grid` without its `GridDerived` provenance
  or the existing actual-over-derived precedence. Durable store precedence
  could not repair a tuple already corrupted before its first write.
- What fixed it: Pending coalescing now moves locator and provenance together,
  refuses derived replacement of actual grids, and continues merging all other
  metadata. The actual writer loop has a direct deterministic test boundary.
- How we know: Regressions failed on all three broken orderings before the fix.
  The same production writer then persisted and reopened the expected literal
  tuples, and cache hydration preserved them. Targeted native ordinary/race
  checks passed; final whole-branch validation is recorded separately.
- Operator/support answer: This prevents new batching corruption. It does not
  identify or repair previously persisted provenance errors. Verify current
  source and record history before proposing any data correction.

## Triggering Request
- Request date: 2026-11-10.
- Request summary: Review external codebase findings and execute separately
  validated correctness slices under the owner's approved overnight ledger.
- Request reference: Exact `Approved v1` in the chat; execution record in
  `docs/codebase-correctness-overnight-2026-11-10.md`.

## Symptoms and Impact
- CTY-only then derived could persist the default false provenance flag.
- Actual then derived could persist the derived locator with the actual flag.
- Derived then actual could retain the derived flag on an observed locator.
- Location consumers can therefore receive inaccurate location authority even
  though the persistent store's direct merge tests pass.

## Timeline
1. 2026-11-10 - Current source and durable precedence were reconciled before approval.
2. 2026-11-10 - A fixed-record, same-pending-window writer regression reproduced
   all three failures, including after database reopen.
3. 2026-11-10 - The tuple/precedence repair passed owner ordinary/race checks.

## Hypotheses and Tests
1. Direct store precedence already protects batched first writes.
   - Evidence: The unchanged coalescer failed `TestGridWriterPendingProvenancePersistence`
     for actual-to-derived, derived-to-actual and CTY-to-derived input.
   - Outcome: Rejected. Separate durable writes bypass the broken coalescer.
2. Copying the provenance flag alone is sufficient.
   - Evidence: Actual-to-derived still requires refusing the incoming locator;
     literal helper and persisted writer expectations cover this ordering.
   - Outcome: Rejected. The tuple and precedence are one contract.
3. Rejecting the incoming derived locator permits ignoring its other metadata.
   - Evidence: Explicit CTY, observations, minimum first-seen, maximum updated-at
     and expiry expectations remain required after locator rejection.
   - Outcome: Rejected. Only locator selection is refused.

## Findings
- Root cause: In-memory coalescing did not implement the existing durable grid
  precedence contract.
- Contributing factor: Direct store tests did not exercise a shared pending batch.
- Durable decision: No new policy was selected; the change restores existing
  actual-over-derived behavior. No new ADR is required for this repair.

## Decision Linkage
- ADR created/updated: none.
- Decision delta summary: No durable policy change.
- Contract/behavior changes: Pending batching now honors the existing locator
  authority; queue sizes, flush cadence, errors, TTL and shutdown remain unchanged.

## Verification and Monitoring
- Validation steps run: Native ordinary cluster checks passed 15 cases; native
  race cluster/gridstore checks passed 17 cases. Commands and raw red/green
  evidence are retained in the overnight execution directory.
- Signals to monitor: Unexpected persisted grid/provenance changes and
  disagreement between observed locations and derived CTY metadata.
- Rollback triggers: Lost actual precedence, changed metadata aggregation,
  lost final flush, or new writer ownership/retirement failures.

## References
- Execution: `docs/codebase-correctness-overnight-2026-11-10.md`.
- Source: `internal/cluster/grid_writer.go`, `gridstore/store.go`.
- Tests: `internal/cluster/grid_writer_test.go`, `gridstore/store_test.go`.
- Configuration context: `data/config/README.md`.
