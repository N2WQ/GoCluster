# ADR-0178: VOACAP Invalid Request Reason Diagnostics

- Status: Accepted
- Date: 2026-06-12
- Decision Origin: Design

## Context

ADR-0175 made sparse/no-p50 VOACAP outcomes visible, including an aggregate
`invalid_request` counter. Live analysis still could not tell whether those
invalid requests came from an unsupported or empty band, an invalid user or DX
grid, or invalid H3 cells derived from otherwise valid grids.

Those reasons have different operational meanings. Unsupported or unknown bands
point to frequency/mode classification and configured VOACAP center-frequency
coverage. Invalid user or DX grids point to operator/user location data. Invalid
cells point to H3 table or grid-to-cell conversion boundaries. Collapsing them
into one counter hid which surface needed investigation.

## Decision

Keep `invalid_request` as the stable aggregate counter, and add six
observability-only reason counters to both `VOACAP fallback (5m)` and
`Sparse p50 VOACAP (5m)`:

- `invalid_unsupported_band`
- `invalid_empty_unknown_band`
- `invalid_user_grid`
- `invalid_dx_grid`
- `invalid_user_cell`
- `invalid_dx_cell`

VOACAP request preparation records the first failing reason in this precedence:
empty or unknown band, unsupported configured band, invalid user grid, invalid
DX grid, invalid user cell, invalid DX cell. Invalid requests still stop before
SSN lookup, cache lookup, delay tracking, queueing, and VOACAP execution.

Sparse/no-p50 trace diagnostics carry the same invalid reason into compact
`SET DIAG PATH` suffixes:

- `vband`: unsupported band
- `vnbnd`: empty or unknown band
- `vugrd`: invalid user grid
- `vdgrd`: invalid DX grid
- `vucel`: invalid user cell
- `vdcel`: invalid DX cell
- `vbad`: fallback for any other invalid request reason

`prop_report` parses the new sparse reason fields while retaining compatibility
with older aggregate-only sparse log lines.

This decision does not change p50 gates, VOACAP fallback eligibility, cache keys,
worker queue behavior, SSN behavior, path classes, PATH filters, glyph output,
or YAML/config schema.

## Alternatives considered

1. Replace `invalid_request` with mutually exclusive reason counters.
   - Rejected because existing operators, parsers, and support text already use
     the aggregate as a stable top-level health signal.
2. Split only the sparse/no-p50 diagnostic line.
   - Rejected because the fallback worker/stage view needs the same reason
     breakdown to explain all invalid VOACAP requests, not just sparse traces.
3. Emit verbose reason text in `SET DIAG PATH`.
   - Rejected because the DX-cluster comment field is fixed-width and already
     uses compact tokens for VOACAP sparse diagnostics.
4. Add a YAML-controlled diagnostic mode for reason splitting.
   - Rejected because this is low-cardinality operational observability and
     should stay available whenever the existing counters are emitted.

## Consequences

### Benefits

- Operators can tell whether invalid VOACAP requests are caused by band
  coverage, unknown band classification, bad user location data, bad DX
  location data, or H3 cell derivation.
- Existing aggregate alerts and trend checks can continue using
  `invalid_request`.
- Per-spot sparse diagnostics and five-minute propagation counters now explain
  the same invalid-request reason model.
- `prop_report` can surface hours where invalid sparse/no-p50 requests were
  split by reason while still parsing old logs.

### Risks

- More propagation log fields increase documentation and parser compatibility
  surface.
- Compact `v*` suffixes are not self-describing without docs and support cards.
- The first-failure precedence is intentionally deterministic but means a spot
  with multiple bad inputs contributes to only one reason bucket.

### Operational impact

- Operators should compare `invalid_request` with the six reason counters when
  investigating VOACAP fallback or sparse/no-p50 diagnostics.
- Unsupported/empty band counts point first to frequency classification or
  configured VOACAP band coverage.
- User-grid and DX-grid counts point first to stored user grids, spot geography,
  and callsign/grid enrichment.
- User-cell and DX-cell counts point first to grid-to-H3 conversion and checked-in
  H3 table availability.
- No restart/config migration is required beyond deploying the binary and docs.

## Links

- Related code: `pathreliability/voacap_fallback.go`, `telnet/server.go`,
  `internal/cluster/bootstrap.go`, `internal/propreport/report.go`
- Related tests: `pathreliability/voacap_fallback_test.go`,
  `telnet/diag_command_test.go`, `telnet/server_prediction_stats_test.go`,
  `internal/propreport/report_test.go`
- Related docs: `README.md`, `docs/OPERATOR_GUIDE.md`,
  `pathreliability/README.md`, `data/config/PATH_PREDICTIONS.md`,
  `data/config/README.md`, `customgpt/support-cards/path-reliability.md`,
  `customgpt/troubleshooting-index.md`
- Related TSRs: none
- Supersedes / superseded by: extends ADR-0164, ADR-0168, and ADR-0175 without
  superseding them
