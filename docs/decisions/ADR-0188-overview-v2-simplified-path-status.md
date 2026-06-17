# ADR-0188: Overview v2 Simplified Path Status

Status: Accepted
Date: 2026-06-17
Decision Makers: cluster maintainers
Technical Area: ui/tview-v2, dashboard, voacap, pathreliability, operations
Decision Origin: User request and approved Scope Ledger v5
Troubleshooting Record(s): none
Tags: ui, observability, voacap, pathreliability

## Context
- The `tview-v2` Overview page had grown beyond its intended role as the
  high-signal operator summary.
- Pipeline Quality details remain useful, but they are lower-value on the first
  screen than ingest, cache freshness, path prediction state, source liveness,
  and network status.
- Operators also need fast visibility into the current VOACAP fallback work
  state and the SSN value driving VOACAP forecast generation.
- ADR-0040 required Pipeline Quality on Overview. This decision narrows that
  contract while preserving the dedicated Pipeline and Events pages.

## Decision
- Remove the Pipeline Quality box from the `tview-v2` Overview page only.
- Keep the existing `PIPELINE QUALITY` snapshot section and the dedicated
  Pipeline and Events page panels unchanged.
- Extend the Caches & Data Freshness footer row with the current SSN EWMA:
  - use `SunspotMonitorSnapshot.EWMA`;
  - round to an integer for display;
  - show compact UTC month-day and minute from `LastObservedAtUTC`;
  - show `EWMA: n/a` until the monitor has an initialized EWMA.
- Extend Path Predictions with an aggregate VOACAP fallback state line before
  H3 path-pair counts:
  - `VOACAP: <cached> cached / <delayed> delayed / <inflight> inflight / <queued> queued`;
  - source the values from `VOACAPClosedFallback.Snapshot()`;
  - keep the data aggregate-only and avoid per-band VOACAP counters in this
    slice.
- Rename the path-pair row to `H3 path pairs` so it is distinct from VOACAP
  state:
  - `H3 path pairs: <L2> (L2) / <L1> (L1)`.

## Alternatives Considered
1. Keep Pipeline Quality on Overview and add SSN/VOACAP below it.
   - Pros: preserves ADR-0040 layout exactly.
   - Cons: keeps the least useful first-screen block and makes the Overview
     busier.
2. Redesign VOACAP counters through `RefreshStatsSnapshot()`.
   - Pros: could align VOACAP counters with p50 stats ownership later.
   - Cons: larger shared-stats change for a simple console aggregate and not
     needed to expose current cache/delay/inflight/queue state.
3. Add per-band VOACAP counters.
   - Pros: richer operator detail.
   - Cons: higher UI density and broader counter ownership questions; deferred
     until there is a clear operator workflow for those counts.

## Consequences
- Positive outcomes:
  - Overview becomes smaller and focused on currently actionable metrics.
  - VOACAP fallback liveness and backlog are visible without opening logs.
  - SSN display makes the moving-average input to VOACAP visible in the same
    freshness area as CTY/FCC/Skew.
- Negative outcomes / risks:
  - Operators must use the Pipeline or Events page for dedupe/resolver/
    stabilizer/temporal details.
  - The Overview still depends on marker-driven extraction, so future marker
    changes must preserve Pipeline page extraction.
- Operational impact:
  - No telnet protocol, path-prediction, VOACAP science, worker, cache, delay,
    queue, or SSN polling behavior changes.
  - The new VOACAP line is a read-only snapshot sampled on the existing stats
    tick.
- Follow-up work required:
  - Reconsider per-band VOACAP counters only if operators need them after using
    the aggregate line.

## Validation
- Focused UI and stats tests cover:
  - Overview path pane resizing with the new VOACAP/H3 rows;
  - SSN EWMA integer formatting and timestamp display;
  - VOACAP aggregate formatting;
  - FT2 ingest lines still present in Overview output.
- Repository validation remains governed by `docs/dev-runbook.md`.

## Rollout and Reversal
- Rollout plan:
  - Ship the Overview layout, stats snapshot plumbing, tests, and this ADR
    together.
- Backward compatibility impact:
  - Local console layout changes only; no client-facing protocol changes.
- Reversal plan:
  - Restore the Overview Pipeline Quality panel and remove the SSN/VOACAP
    display additions from the Overview builder.

## References
- Issue(s): none
- PR(s): none
- Commit(s): pending
- Related ADR(s): ADR-0002, ADR-0040, ADR-0142, ADR-0157, ADR-0161, ADR-0182,
  ADR-0184
- Troubleshooting Record(s): none
- Docs:
  - `ui/dashboard_v2.go`
  - `internal/cluster/bootstrap.go`
  - `pathreliability/voacap_fallback.go`
  - `internal/voacap/sunspot_monitor.go`
