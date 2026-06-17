# ADR-0189: Tview V2 Pipeline Console Simplification

- Status: Accepted
- Date: 2026-06-17
- Decision Origin: Scope Ledger v2

## Context

ADR-0040 refreshed the `tview-v2` Overview page after resolver-primary,
stabilizer, temporal decoding, and custom-SCP evidence were introduced. That
made the Pipeline Quality box operationally complete, but it also left the
interactive console crowded with low-level diagnostic rows that are not needed
for routine live monitoring.

The local console is now limited to `headless` and `tview-v2` by ADR-0188. With
only one interactive renderer remaining, the Overview page should favor a
compact, readable operator surface. Detailed resolver, pressure, stabilizer,
and temporal diagnostics still matter for headless logs, runbooks, and rollback
monitoring, so removing them from the interactive console must not remove
runtime behavior or non-console observability.

ADR-0060 added FT burst counters specifically for dashboard observability. Once
the FT Burst row is removed from the console, those counters no longer have a
current non-console consumer.

## Decision

Simplify the `tview-v2` Pipeline Quality box by removing these rows:

- `Resolver`
- `Resolver Pressure`
- `Stabilizer`
- `Temporal`
- `FT Burst`

Keep `Stabilizer Glyph` visible because it is the compact signal for stabilizer
delay behavior. Keep the existing summary rows for primary/secondary dedupe,
correction/drop totals, and flood state.

Preserve resolver, resolver-pressure, stabilizer, and temporal formatting for
headless flat stats/log output. Remove only the `tview-v2` projection of those
rows. Remove FT Burst tracker counters and FT confidence reporting calls
because the row had no remaining active consumer after the console projection
was removed.

`tview-v2` also filters the removed labels from its legacy `SetStats` fallback,
so transitional or test inputs cannot reintroduce the deleted rows when a
structured snapshot is unavailable.

## Alternatives considered

1. Keep all existing Pipeline Quality rows.
   - Rejected because the remaining interactive console would stay dense and
     harder to scan during routine operation.
2. Remove `Stabilizer Glyph` with the stabilizer summary row.
   - Rejected because `Stabilizer Glyph` is the compact row the operator still
     wants to retain.
3. Delete resolver, stabilizer, and temporal counters entirely.
   - Rejected for this slice because those counters still support headless
     diagnostics, runbooks, and rollback monitoring.
4. Keep FT Burst counters after removing the row.
   - Rejected because the counters were dashboard-observability plumbing and no
     active non-console consumer remained.

## Consequences

### Benefits

- The Overview Pipeline Quality box is shorter and easier to scan.
- Routine console monitoring keeps high-level health and compact stabilizer
  glyph behavior without detailed resolver/temporal rows.
- Removed FT Burst counter plumbing reduces unused stats code.

### Risks

- Interactive-console users no longer see detailed resolver, pressure,
  stabilizer summary, temporal, or FT burst counters.
- FT burst behavior remains observable through behavior and tests, but no longer
  has a dedicated runtime counter surface.

### Operational impact

No resolver, stabilizer, temporal, FT confidence, telnet, ingest, fanout,
archive, peer, or drop behavior changes. Headless flat stats/log output keeps
resolver, resolver-pressure, stabilizer, and temporal lines for diagnostic
workflows.

## Links

- Related code: `internal/cluster/bootstrap.go`, `ui/dashboard_v2.go`,
  `internal/cluster/ft_confidence_runtime.go`, `stats/tracker.go`
- Related tests: `ui/dashboard_v2_test.go`, `ui/perf_bench_test.go`,
  `internal/cluster/ft_confidence_runtime_test.go`, `internal/cluster/main_test.go`,
  `internal/cluster/main_stats_test.go`
- Related docs: `docs/decision-log.md`
- Related ADRs: ADR-0040, ADR-0060, ADR-0142, ADR-0188
- Related TSRs: none
- Supersedes / superseded by: supersedes ADR-0040 Pipeline detail-row
  inclusion for `tview-v2` and ADR-0060 FT Burst dashboard observability
