# ADR-0168: VOACAP P50 Cache Comparison

- Status: Accepted
- Date: 2026-06-10
- Decision Origin: Scope Ledger v1

## Context

Live VOACAP fallback logs showed many closed outcomes on sparse paths. That
raised a broader calibration question: when normal bucket p50 evidence is
sufficient, how often does it agree with VOACAP for the same path and current
UTC hour?

Running VOACAP for every sufficient p50 prediction would make the telnet path
expensive and would change fallback workload. The useful first step is an
opportunistic comparison against cache data that already exists because normal
sparse-path fallback lookups populated it.

## Decision

Add a cache-only VOACAP lookup method for comparison diagnostics. The method
returns an existing current-hour cached forecast when present. It must not:

- start a fallback delay window
- enqueue VOACAP work
- mutate fallback stage counters
- prune cache state as a side effect
- change glyphs, PATH filter classes, p50 thresholds, cache keys, or VOACAP
  deck generation

For sufficient `SourceCombined` p50 results, compare the p50 SNR against the
existing cached current-hour VOACAP FT8-equivalent SNR when the cache has a
matching record. Emit a separate five-minute propagation log line:

```text
VOACAP p50 compare (5m): checked=<n> cache_hit=<n> cache_miss=<n> same_class=<n> p50_stronger=<n> voacap_stronger=<n> equal_snr=<n> voacap_closed_p50_high=<n> voacap_closed_p50_medium=<n> voacap_closed_p50_low=<n> voacap_closed_p50_unlikely=<n> delta_abs_0_3=<n> delta_abs_4_9=<n> delta_abs_10_19=<n> delta_abs_20_plus=<n>
```

`p50_stronger` means the observed p50 SNR is numerically higher than cached
VOACAP. `voacap_stronger` means cached VOACAP is numerically higher.
`same_class` uses the same path class thresholds as normal p50 glyphs.

## Alternatives considered

1. Run VOACAP on every sufficient p50 result.
   - Rejected because it would add unbounded work to a hot telnet path and
     would no longer be an observation-only comparison.
2. Compare insufficient and sufficient paths in one line.
   - Rejected because fallback-stage diagnostics and sufficient-p50 calibration
     answer different questions.
3. Log per-path comparison details.
   - Rejected for this slice because high-cardinality path logs would be noisy.
     Aggregates are enough to reveal larger calibration issues first.

## Consequences

### Benefits

- Operators can see p50-vs-VOACAP agreement opportunistically without creating
  new VOACAP runs.
- Class agreement, closed-disagreement, and SNR-delta buckets provide enough
  signal to decide whether a deeper calibration pass is warranted.
- Existing `Path predictions (5m)` and `VOACAP fallback (5m)` meanings stay
  stable.

### Risks

- The comparison sample is biased toward paths that already have VOACAP cache
  entries from sparse fallback activity.
- Cache misses do not prove VOACAP disagreement; they only mean no current-hour
  cached record existed.
- The p50 sample is final sufficient bucket evidence, while the VOACAP sample is
  a cached model forecast. Agreement does not prove either model is correct.

### Operational impact

No config or migration is required. When sufficient p50 predictions are checked
and VOACAP fallback cache is enabled, propagation logs may include a separate
`VOACAP p50 compare (5m)` line. The line is diagnostic only.

## Links

- Related code: `pathreliability/voacap_fallback.go`, `telnet/server.go`,
  `internal/cluster/bootstrap.go`
- Related docs: `README.md`, `docs/OPERATOR_GUIDE.md`,
  `pathreliability/README.md`, `data/config/PATH_PREDICTIONS.md`,
  `customgpt/support-cards/path-reliability.md`
- Related tests: `pathreliability/voacap_fallback_test.go`,
  `telnet/server_prediction_stats_test.go`
- Related ADRs: ADR-0160, ADR-0163, ADR-0164, ADR-0167
- Related TSRs: none
- Supersedes / superseded by: extends ADR-0164 and ADR-0167 observability
  without changing fallback behavior
