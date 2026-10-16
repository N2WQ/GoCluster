# ADR-0311: Stabilizer Transition Matrix

- Status: Accepted
- Date: 2026-10-16
- Decision Origin: Approved Scope Ledger v2 and owner rounding clarification

## Context

The final-glyph average retained by ADR-0189 cannot show whether a delayed spot
entered with one glyph and ended with another. Its accounting also omits
released paths subsequently thinned by secondary output filtering.

## Decision

Replace the console line with a three-row, six-column entry-to-terminal glyph
matrix. Capture entry glyph at first successful admission and carry it unchanged
through the scheduler and retries. Record one sum/sample observation at terminal
release or timeout suppression, before secondary filtering. Earlier correction
or license rejection, initial admission overflow, immediate passes, and shutdown
drops are excluded. Failed retry admission terminates an already admitted path
and contributes its completed checks.

Use fixed 18-cell process-local aggregates with coherent observation, snapshot,
and reset. Display arithmetic means rounded to the nearest tenth, halfway up,
and em dashes for empty cells. Refresh through the existing statistics snapshot
cycle on all console pages showing Pipeline Quality. Retain existing headless
diagnostics and their accounting definition.

The owner selected a `B` terminal column because current resolver rechecks can
reject a proposed correction while retaining the original call with glyph `B`.
This extends the handoff's five-column example to cover that terminal outcome.

## Alternatives considered

1. Keep final-glyph averages: cannot answer the requested entry-to-exit question.
2. Retain per-spot histories: unnecessary retained state for this aggregate.
3. Count broadcasts: omits completed stabilizer paths thinned downstream.

## Consequences

### Benefits

The operator can distinguish paths such as `S → V` from `S → S`, with bounded
storage and one observation per completed delayed spot.

### Risks

The matrix uses four more lines than the old summary. It starts empty after
restart/reset. Averages describe completed observations, not pending paths,
promotion rates, RF correctness, or elapsed time. Headless and console averages
have different accounting boundaries and cannot be compared as identical metrics.

### Operational impact

Deploy an updated executable; no YAML changes are required. Stabilizer policy,
timing, lifecycle, queue capacity, and output behavior remain unchanged.

## Links

- Source: `stats/stabilizer_transitions.go`, `internal/cluster/stabilizer.go`,
  `internal/cluster/output_pipeline_delivery.go`,
  `internal/cluster/stabilizer_matrix.go`, `ui/dashboard_v2.go`
- Tests: `stats/stabilizer_transitions_test.go`,
  `internal/cluster/stabilizer_transition_delivery_test.go`,
  `internal/cluster/stabilizer_transition_license_test.go`,
  `internal/cluster/stabilizer_matrix_test.go`, `ui/stabilizer_matrix_test.go`
- Operator documentation: [Stabilizer matrix](../../ui/README.md#stabilizer-matrix)
- Refines: [ADR-0189](ADR-0189-tview-v2-pipeline-console-simplification.md),
  console Stabilizer Glyph display only
- Related TSRs: none; requested observability change
