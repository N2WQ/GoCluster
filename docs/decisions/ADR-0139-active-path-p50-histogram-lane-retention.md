# ADR-0139: Active Path P50 Histogram Lane Retention

- Status: Accepted
- Date: 2026-06-04
- Decision Origin: Incident

## Context

Production profiling after the p50 branch merge showed high memory pressure and
CPU symptoms consistent with GC pressure under tight memory limits. Overnight
profiles with `GOMEMLIMIT=1024MiB` did not reproduce pegged host CPU, but they
confirmed that path reliability retained about 60 MiB and that each p50-era
bucket retained both raw and capped SNR histograms.

The active p50 method is already accepted by ADR-0126 and refined by ADR-0131.
The fix must not change p50 bin geometry, midpoint/even-split semantics,
threshold mapping, receiver-cap gates, PATH filters, or `SET DIAG PATH`
meaning.

## Decision

Retain one fixed SNR histogram per path bucket: the histogram for the active
p50 lane selected by `receiver_contribution_mode`.

- `off` and `shadow` mode retain the raw selected-evidence histogram.
- `enforce` mode retains the capped receiver-attributed histogram.
- Raw and capped counts, weights, receiver slots, `CapLimited`, and
  `CapWouldBlock` remain separately retained as before.
- `SET DIAG PATH` continues to use the selected `Result` count, weight, age,
  and insufficient reason; it does not require an inactive p50 histogram lane.

Also remove avoidable per-observation allocation from path reporting metrics by
tracking the current hour as a numeric bucket and representing coarse grid pairs
as packed numeric keys. This preserves propagation-log counts while reducing
allocation pressure.

## Alternatives considered

1. Keep raw and capped p50 histograms in every bucket.
   Rejected because the inactive lane is not used by active scoring in a given
   receiver-contribution mode and was a material retained-heap cost.
2. Change p50 bin geometry or coarsen bins.
   Rejected because that would change the p50 method and operator-visible path
   classification.
3. Reduce stale windows or hard-cap path buckets.
   Rejected for this fix because it would change evidence retention semantics
   and could change active path predictions.

## Consequences

### Benefits

- Bucket retained size drops from 640 bytes to 440 bytes in the local
  retained-size guard, about 22 MiB saved at 110,000 buckets.
- Active p50 calculation, glyphs, PATH filters, and receiver-cap diagnostics
  remain unchanged for the configured mode.
- `pathReportMetrics.Observe` drops from 24 B/op and 2 allocs/op to 0 B/op and
  0 allocs/op in the local benchmark.

### Risks

- The inactive raw-vs-capped p50 lane is not available for future diagnostics
  unless a new bounded diagnostic design is approved.
- If runtime switching of `receiver_contribution_mode` is introduced later, it
  will need a rebuild/backfill/reset contract because existing buckets only
  retain the active lane for the startup configuration.
- Other retained owners still consume significant heap; this decision reduces
  the p50 regression but is not a full heap-remediation plan.

### Operational impact

- No telnet command, path glyph, PATH filter, or propagation-log count semantics
  change.
- The change reduces retained heap and allocation pressure under the existing
  active p50 method.
- Higher `GOMEMLIMIT` remains an operational mitigation, not the code fix.

## Links

- Related issues/PRs/commits:
- Related tests: `pathreliability/active_p50_contract_test.go`,
  `pathreliability/snr_histogram_test.go`, `pathreliability/receiver_test.go`,
  `pathreliability/store_bench_test.go`,
  `internal/cluster/path_report_metrics_test.go`,
  `internal/cluster/path_report_metrics_bench_test.go`
- Related docs: `pathreliability/README.md`,
  `data/config/PATH_PREDICTIONS.md`
- Related TSRs: TSR-0025
- Supersedes / superseded by:
