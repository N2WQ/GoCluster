# ADR-0142: UI v2 Buffer Bounds And Render Churn

- Status: Accepted
- Date: 2026-06-04
- Decision Origin: Design

## Context
ADR-0002 moved `tview-v2` streams to a bounded virtual renderer, but the
runtime still used fixed 200-row stream buffers while `ui.v2.event_buffer` and
`ui.v2.debug_buffer` exposed larger row caps plus byte and message-size limits.
The stats path also scheduled separate legacy stats and structured snapshot
frames for the same v2 refresh cycle, and v2 retained cloned snapshot fields it
did not render.

The UI must keep local-console observability bounded without adding ingest-path
blocking or changing telnet protocol behavior.

## Decision
Make the existing v2 buffer config effective for `tview-v2` stream panels:

- validation, unlicensed, corrected, and harmonic panels use
  `ui.v2.event_buffer`;
- the system events panel uses `ui.v2.debug_buffer`;
- `max_events` caps retained rows per panel;
- `max_bytes_mb` caps retained text bytes per panel;
- `max_message_bytes` truncates single retained rows deterministically;
- `evict_on_byte_limit` selects oldest-row eviction versus new-row drop when a
  byte cap would be exceeded.

Keep the `ui.Surface` method set unchanged. For v2 snapshot refreshes, coalesce
legacy stats and structured snapshot scheduling onto the same frame ID, avoid
retaining unused snapshot line slices, and precompute overview section text once
when accepting a snapshot.

## Alternatives considered
1. Keep the fixed 200-row buffers and ignore byte/message config.
   - Rejected because checked-in YAML described a resource contract that the
     runtime did not enforce.
2. Remove the v2 buffer YAML keys.
   - Rejected because operators already have an explicit config surface for UI
     retained text limits.
3. Replace the local UI with ANSI-only output.
   - Rejected because it would remove the interactive page model and is broader
     than the performance/resource-bound issue.

## Consequences
### Benefits
- UI v2 retained stream text is bounded by operator-owned row and byte caps.
- The stats tick no longer schedules duplicate v2 render frames.
- Snapshot refresh retains less heap by dropping unused cloned line slices.
- Dynamic-color stream drawing allocates less per frame.

### Risks
- With small byte caps, local UI rows may be truncated or older rows evicted.
- Dynamic-color tags can still dominate stream draw allocation because tview
  parsing remains in the draw path.

### Operational impact
- No telnet protocol, ingest, filter, drop, or broadcast behavior changes.
- Existing `ui.v2.event_buffer` and `ui.v2.debug_buffer` settings now control
  actual local UI retention.
- Operators can increase or reduce retained local UI history through existing
  YAML keys without changing server behavior for clients.

## Links
- Related issues/PRs/commits:
- Related tests:
  - `ui/dashboard_v2_test.go`
  - `ui/virtual_log_view_test.go`
  - `ui/perf_bench_test.go`
- Related docs:
  - `data/config/app.yaml`
  - `docs/decisions/ADR-0002-ui-v2-render-pipeline.md`
  - `docs/decisions/ADR-0040-overview-v2-observability-contract-refresh.md`
- Related TSRs:
- Supersedes / superseded by:
  - Related: ADR-0002, ADR-0040
