# ADR-0189: tview-v2 Pipeline Quality Retirement

Status: Accepted
Date: 2026-06-17
Decision Makers: cluster maintainers
Technical Area: ui/tview-v2, dashboard, stats, operations
Decision Origin: Approved Scope Ledger v6
Troubleshooting Record(s): none
Tags: ui, observability, stats

## Context
- ADR-0188 removed Pipeline Quality from the `tview-v2` Overview page but kept
  the dedicated Pipeline and Events page summary panels unchanged.
- After that change, the structured `PIPELINE QUALITY` snapshot section became
  lower value than the stream panels and still consumed screen space on
  non-Overview pages.
- The underlying producer-side counters remain useful for legacy stats output,
  ANSI/dashboard compatibility, runtime diagnostics, and tests. The unused
  part is the `tview-v2` structured presentation and snapshot plumbing.

## Decision
- Retire the `tview-v2` Pipeline Quality summary section from structured
  snapshots.
- Stop emitting the `PIPELINE QUALITY` marker from `buildOverviewLines`.
- Parse the Ingest section directly up to `CACHES & DATA FRESHNESS`.
- Remove Pipeline Quality text panels from the `tview-v2` Pipeline and Events
  pages.
- Keep the Pipeline page itself as the corrected-call and harmonic-suppression
  stream page.
- Keep producer-side `stats.Tracker` counters and legacy stats lines intact.
- Keep ANSI and legacy dashboard behavior intact.

## Alternatives Considered
1. Remove the entire Pipeline page.
   - Pros: simpler navigation.
   - Cons: also removes useful corrected/harmonics streams and changes page
     semantics beyond the approved cleanup.
2. Remove producer-side tracker counters.
   - Pros: larger code reduction.
   - Cons: those counters are still used by legacy stats, runtime diagnostics,
     and tests; removing them would be a behavioral observability change.
3. Keep Pipeline Quality on Pipeline/Events only.
   - Pros: preserves ADR-0188 exactly.
   - Cons: leaves low-value metrics in the `tview-v2` page set after they were
     intentionally removed from Overview.

## Consequences
- Positive outcomes:
  - `tview-v2` pages show fewer low-signal summary rows.
  - Snapshot parsing no longer depends on the `PIPELINE QUALITY` marker.
  - Dead `tview-v2` Pipeline Quality fields, placeholders, and display-only
    helpers are removed.
- Negative outcomes / risks:
  - Operators no longer see dedupe/resolver/stabilizer/temporal summary rows in
    `tview-v2`; they must use legacy stats/logs or future targeted diagnostics.
  - The Pipeline page name now primarily represents corrected/harmonics streams,
    not a summary panel.
- Operational impact:
  - No protocol, parser, config, queue, worker, path prediction, VOACAP,
    correction, stabilizer, temporal, harmonic, reputation, or dedupe behavior
    changes.
  - Runtime counter increments remain in place.
- Follow-up work required:
  - If the Pipeline page is no longer useful after this cleanup, retire or
    rename that page in a separate ledger.

## Validation
- Focused UI tests should cover:
  - Event/Overview summary boxes without Pipeline Quality;
  - path pane resizing without the old marker;
  - caches pane extraction ending at Path Predictions;
  - Pipeline stream buffering behavior.
- Repository validation remains governed by `docs/dev-runbook.md`.

## Rollout and Reversal
- Rollout plan:
  - Ship UI/snapshot cleanup, tests, and this ADR together.
- Backward compatibility impact:
  - Local `tview-v2` layout only; no client protocol or runtime behavior change.
- Reversal plan:
  - Restore the `PIPELINE QUALITY` structured section, Pipeline/Events summary
    panels, and related tests.

## References
- Issue(s): none
- PR(s): none
- Commit(s): pending
- Related ADR(s): ADR-0040, ADR-0142, ADR-0188
- Troubleshooting Record(s): none
- Docs:
  - `ui/dashboard_v2.go`
  - `internal/cluster/bootstrap.go`
  - `stats/tracker.go`
