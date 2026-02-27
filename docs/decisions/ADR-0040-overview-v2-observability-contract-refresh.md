# ADR-0040 - Overview v2 Observability Contract Refresh

Status: Accepted
Date: 2026-02-27
Decision Makers: cluster maintainers
Technical Area: ui/tview-v2, main stats assembly
Decision Origin: Design
Troubleshooting Record(s): none
Tags: ui, observability, stats

## Context
- Overview v2 content drifted after resolver-primary stabilizer delays, temporal decoding, and custom SCP evidence were introduced.
- Operators needed high-signal page-level status without adding tabs or replacing existing boxes.
- Legacy SCP freshness and network latency/write-stall lines no longer matched current operational priorities for this page.
- The UI contract must remain deterministic and bounded (same box model, stable section markers, and existing band ordering semantics in text rows).

## Decision
- Keep the existing Overview page structure and shared box definitions, but refresh box content contracts:
  - Ingest section now includes a PSK family counter for `PSK31/63`.
  - Pipeline Quality section shows five lines: primary/secondary dedupe summary, correction totals, resolver summary, stabilizer summary, and temporal summary.
  - Caches/Data Freshness section removes legacy SCP freshness, shows known-calls total with per-band breakdown (existing band order), and surfaces active custom-SCP evidence policy settings.
  - Network section shows telnet client/drop summary plus connected client list; latency/write-stall lines are removed from Overview.
- Make Overview section extraction marker-driven in UI v2 (`INGEST RATES (per min)`, `PIPELINE QUALITY`, `CACHES & DATA FRESHNESS`) so box rendering follows content contracts instead of fixed indexes.
- Track PSKReporter `PSK31` and `PSK63` source-mode deltas explicitly for Overview metrics while retaining canonical PSK accounting.

## Alternatives Considered
1. Keep canonical `PSK` as the only PSK-mode metric.
   - Pros: no additional counters; simplest implementation.
   - Cons: mixes PSK125 with PSK31/63 and fails operator requirement.
2. Add a new page for resolver/stabilizer/temporal details.
   - Pros: avoids expanding existing Pipeline box lines.
   - Cons: violates requirement to modify current tabs instead of replacing/splitting.
3. Retain latency/write-stall lines in Overview Network.
   - Pros: preserves prior diagnostics surface.
   - Cons: consumes space with lower-priority data and conflicts with approved scope.

## Consequences
- Positive outcomes:
  - Overview now reflects current pipeline behavior and custom-SCP operations.
  - Operators can read known-calls concentration by band directly from the main page.
  - Marker-driven extraction reduces fragility when section line counts evolve.
- Negative outcomes / risks:
  - Slight increase in `source|mode` key cardinality from PSK31/63 counters.
  - Overview box heights may grow with richer content, requiring layout resize logic.
- Operational impact:
  - No protocol or ingest/drop semantics changed.
  - Existing tabs remain in place; only content within boxes changed.
- Follow-up work required:
  - Keep future Overview edits marker-driven and preserve supported band ordering in per-band rows.

## Validation
- `go test ./...`
- `go vet ./...`
- `staticcheck ./...`
- `go test -race ./...`
- Focused checks:
  - `go test ./ui -run "TestUpdateEventsOverviewBoxesMatchesOverviewSummary|TestOverviewPathPaneGrowsToFitBandBuckets|TestOverviewCachesPaneResizesToContentHeight|TestRenderSnapshotUpdatesOnlyActivePage"`
  - `go test . -run TestBuildOverviewLinesIncludesKnownCallsByBand`

## Rollout and Reversal
- Rollout plan:
  - Ship together: `main.go` stats contract update + `ui/dashboard_v2.go` marker-driven extraction/resizing + tests/docs updates.
- Backward compatibility impact:
  - No client protocol changes; local console page content only.
- Reversal plan:
  - Revert this ADR's linked code changes to restore previous Overview content contract.

## References
- Issue(s): none
- PR(s): none
- Commit(s): pending
- Related ADR(s): ADR-0002, ADR-0013, ADR-0038, ADR-0039
- Troubleshooting Record(s): none
- Docs:
  - `README.md`
  - `docs/OPERATOR_GUIDE.md`
