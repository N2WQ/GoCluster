# ADR-0175: Sparse P50 VOACAP Outcome Diagnostics

- Status: Accepted
- Date: 2026-06-11
- Decision Origin: Design

## Context

Operators can see final emitted path glyph totals in `Path predictions (5m)`
and can see fallback worker stages in `VOACAP fallback (5m)`, but those two
views do not fully explain the blank or very sparse path population. In
particular, `n0|none`, `n1|...`, and `n2|...` spots may remain blank because
VOACAP has not run yet, lacks a current-hour cache hit, is blocked by request
validity or SSN state, predicts closed, predicts open but fails REL or tier
guards, or simply does not classify the candidate closed.

The prior fallback decisions intentionally left broader rare-DX one-direction
policy and global p50 floor changes out of scope until diagnostics could
separate those cases. The missing observability made those decisions harder to
defend.

## Decision

Add an observability-only sparse/no-p50 VOACAP diagnostic bucket for path
predictions whose base p50 result is insufficient and either has no usable p50
or has a very low selected observation count.

The very-low-count boundary is configured by
`voacap_fallback.sparse_p50_diagnostic_max_observation_count`, shipped as `2`.
This setting only controls diagnostics. It does not change p50 gates, VOACAP
fallback eligibility, queueing, cache behavior, path classes, PATH filters, or
client display glyphs.

Five-minute propagation logs add a separate `Sparse p50 VOACAP (5m)` line when
there is activity. The line splits sparse/no-p50 candidates by:

- p50 evidence: `no_p50`, `very_low_count`
- path kind: `beacon_rx`, `non_beacon`
- VOACAP availability/work state: `cache_miss_total`, `cache_hit`, `queued`,
  `delayed`, `inflight`, `invalid_request`, `ssn_unavailable`,
  `no_current_hour`, `queue_full`, `not_running`, `disabled`, `unavailable`
- outcome: `closed`, `aligned`, `sparse_upgrade`, `open_rel_pass`,
  `open_rel_fail`, `not_closed`, `rel_missing`, `rel_below_floor`,
  `rel_multi_tier`

`SET DIAG PATH` may append compact `v*` suffixes to insufficient sparse/no-p50
diagnostics so a single spot can show why it stayed blank, such as
`n0|none|vdly` or `n2|lown|vrel`.

The existing `VOACAP fallback (5m)` line remains the worker/fallback stage view.
The existing `Path predictions (5m)` line remains the final emitted glyph view.

## Alternatives considered

1. Add only log counters.
   - Rejected because per-spot diagnosis still matters when a user asks why a
     visible blank glyph was not closed or opened by VOACAP.
2. Add only `SET DIAG PATH` suffixes.
   - Rejected because operators need aggregate evidence before changing rare-DX
     one-direction policy or global p50 floors.
3. Treat every `n0|none` as closed when VOACAP lacks a cache hit.
   - Rejected because absence of a VOACAP result is not evidence that the path
     is closed. It would turn queue/cache timing into path semantics.
4. Raise `min_observation_count` so more spots reach VOACAP fallback.
   - Rejected for this decision because it changes prediction behavior and
     would also withhold more p50 predictions. The new counters are intended to
     inform that separate policy question.

## Consequences

### Benefits

- Blank and very sparse path cases are separable into VOACAP not available,
  VOACAP closed, VOACAP open with REL pass, VOACAP open with REL fail, and
  VOACAP not closed.
- Operators can compare final glyph totals, fallback stage totals, and sparse
  candidate totals without changing runtime behavior.
- Future rare-DX and floor decisions get concrete diagnostic dimensions instead
  of anecdotal interpretation of `n0|none`.

### Risks

- More propagation log fields increase parser and documentation surface area.
- Compact `v*` suffixes require operator documentation because the normal
  comment field is fixed-width.
- `cache_miss_total` intentionally overlaps terminal work states such as
  queued, delayed, inflight, queue-full, and not-running; it is an aggregate
  "no usable current-hour cache hit" counter, not a mutually exclusive status.

### Operational impact

- `data/config/path_reliability.yaml` adds required key
  `voacap_fallback.sparse_p50_diagnostic_max_observation_count`.
- `SET DIAG PATH` can show sparse VOACAP suffixes on insufficient diagnostics.
- `prop_report` parses and summarizes `Sparse p50 VOACAP (5m)` lines.
- No path prediction behavior, filter behavior, VOACAP queue behavior, or cache
  behavior changes.

## Links

- Related code: `pathreliability/config.go`,
  `pathreliability/voacap_fallback.go`, `telnet/server.go`,
  `internal/cluster/bootstrap.go`, `internal/propreport/report.go`
- Related config: `data/config/path_reliability.yaml`
- Related tests: `pathreliability/config_test.go`,
  `pathreliability/voacap_fallback_test.go`, `telnet/diag_command_test.go`,
  `telnet/server_prediction_stats_test.go`, `internal/propreport/report_test.go`
- Related docs: `README.md`, `docs/OPERATOR_GUIDE.md`,
  `pathreliability/README.md`, `data/config/PATH_PREDICTIONS.md`,
  `data/config/README.md`, `customgpt/support-cards/path-reliability.md`
- Related TSRs: none
- Supersedes / superseded by: extends ADR-0161, ADR-0163, ADR-0167,
  ADR-0173, and ADR-0174 without superseding them
