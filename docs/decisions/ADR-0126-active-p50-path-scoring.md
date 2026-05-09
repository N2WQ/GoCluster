# ADR-0126: Active P50 Path Scoring

- Status: Accepted
- Date: 2026-05-09
- Decision Origin: Design

## Context
Path reliability originally used a decayed linear-power mean for active glyphs
and PATH filters. PATHP50 diagnostics showed that the previous aggregate and
p50 could separate materially and consistently when a few strong reports raised
the aggregate while the typical report on the path remained much weaker.

The path glyph is meant to describe the typical current path experience, not the
best observed station on the path. The method must stay bounded in retained
state and must not add hot-path heap growth, maps, slices, or per-update
allocations.

## Decision
Use the fixed-bin SNR histogram p50 as the active path statistic for glyphs and
PATH filters.

Each bucket keeps fixed raw and capped histogram lanes alongside the existing
weight, count, freshness, and receiver-cap state. Ingest places each accepted
FT8-equivalent SNR into exactly one fixed 1 dB bin after applying bucket decay.
Prediction selects and merges the same fine/coarse and receive/transmit evidence
as before, then scans the fixed histogram to compute p50. Threshold comparison
uses the selected p50 dB bin directly.

There is no mean fallback. If an otherwise eligible sample has no p50, the
prediction is insufficient. Enabled scoring always requires the histogram.

`SET DIAG PATHP50` now displays `p<db>n<count>`. Propagation logs retain the
diagnostic-observed `Path p50 diag` aggregate for observed/missing p50 and
sample-count buckets.

## Alternatives considered
1. Keep the decayed linear-power mean as active scoring.
   Rejected because it can overstate the typical path when a minority of strong
   observations dominates the mean.
2. Combine mean and p50 into one score.
   Rejected because the existing thresholds are mode SNR thresholds. Mixing a
   mean and percentile would create an uncalibrated statistic with unclear
   operator meaning.
3. Retain exact observations and calculate exact percentiles.
   Rejected because retained memory would become unbounded and prediction would
   require more CPU and allocation-sensitive work.

## Consequences
### Benefits
- Active glyphs and PATH filters now represent the median-like selected path
  experience.
- Strong outliers no longer raise the path class by dominating a power-domain
  mean.
- Retained storage stays fixed per bucket, and prediction scans a bounded
  histogram array.

### Risks
- P50 values are bin lower edges, not exact raw SNRs.
- Existing SNR thresholds were inherited from mode tables and still need field
  calibration against observed operator outcomes.
- Old propagation reports are not part of current runtime scoring.

### Operational impact
- Active path glyphs and PATH filters may become more conservative on paths with
  a few strong outliers and many weaker reports.
- `SET DIAG PATHP50` displays `p<db>n<count>`.
- Slow clients, telnet queues, reconnect handling, and shutdown behavior are
  unchanged.

## Links
- Related issues/PRs/commits:
- Related tests: `pathreliability/snr_histogram_test.go`, `pathreliability/config_test.go`, `telnet/diag_command_test.go`, `telnet/server_prediction_stats_test.go`, `internal/propreport/report_test.go`
- Related docs: `README.md`, `docs/OPERATOR_GUIDE.md`, `pathreliability/README.md`, `data/config/path_reliability.yaml`, `customgpt/troubleshooting-index.md`
- Related TSRs:
- Supersedes / superseded by: Supersedes ADR-0122 and ADR-0125; supersedes the PATHP50 shadow-comparison portion of ADR-0123.
