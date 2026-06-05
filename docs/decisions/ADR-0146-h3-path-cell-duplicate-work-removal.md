# ADR-0146: H3 Path Cell Duplicate Work Removal

- Status: Accepted
- Date: 2026-06-05
- Decision Origin: Design

## Context
Long-running profiles after the PSKReporter fast parser change still showed
avoidable CPU and allocation pressure around path reliability updates. The
normal output pipeline and the PSKReporter path-only loop both needed the same
fine and coarse H3 cells for path report metrics and path predictor updates.

Before this decision, fine cells were cached on `spot.Spot`, but coarse cells
were recomputed at each consumer. That left repeated grid-to-H3 conversion in a
hot path even after the payload parser optimization.

## Decision
Make `spot.Spot` own both fine and coarse path cell caches:
`DXCellID`, `DECellID`, `DXCoarseCellID`, and `DECoarseCellID`.

Hydrate those cells in the cluster path update boundary and pass the hydrated
coarse cells into path report metrics. The existing `Observe` method remains as
a compatibility wrapper for callers that do not already have hydrated cells.

Metadata-derived caches remain invalidated together through
`Spot.InvalidateMetadataCache`. No process-wide H3 cache, retained map, or
operator configuration is added.

## Alternatives considered
1. Keep recomputing coarse cells in each consumer. This has the least code
   change, but preserves duplicate work in the path update hot path.
2. Add a bounded grid-to-cell cache. This could help across spots with repeated
   grids, but it adds retained state, cardinality rules, and eviction behavior
   that are outside the Phase A scope.
3. Move all path-cell ownership into `pathreliability`. That would centralize
   path concerns, but would also couple generic spot preparation to predictor
   internals and broaden the change beyond duplicate-work removal.

## Consequences
### Benefits
- Normal path updates reuse the same coarse cells for metrics and predictor
  updates.
- PSKReporter path-only updates avoid recomputing coarse cells in path report
  metrics after the loop already accepted the same H3 cells.
- Cache lifetime remains bounded by the spot object; there is no new retained
  server-lifetime state.

### Risks
- Mutating metadata without calling `Spot.InvalidateMetadataCache` can leave
  stale path cell caches, matching the existing fine-cell cache contract.
- Invalid H3 results are still represented by zero, so repeated calls on an
  invalid spot may recompute. The Phase A callers hydrate once per update path,
  so this avoids adding a second sentinel state.

### Operational impact
- No YAML, schema, protocol, MQTT transport, queue, shutdown, or operator
  behavior changes.
- The expected impact is lower CPU in path report/predictor update lanes where
  PSKReporter and FT-equivalent spots qualify for path reliability.
- Non-report spots keep the previous fine-cell fanout cache behavior and do not
  pay the new coarse-cell hydration cost.

## Links
- Related issues/PRs/commits: Phase A PSKReporter/MQTT path-cell refactor
- Related tests: `spot/spot_test.go`, `internal/cluster/path_cells_test.go`, `internal/cluster/path_report_metrics_test.go`, `internal/cluster/path_report_metrics_bench_test.go`
- Related docs: `docs/decisions/ADR-0117-hot-path-duplicate-work-removal.md`, `docs/decisions/ADR-0121-pskreporter-trusted-spot-materialization.md`, `docs/decisions/ADR-0145-pskreporter-fast-payload-parser.md`
- Related TSRs: none
- Supersedes / superseded by: none
