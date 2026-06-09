# ADR-0162: VOACAP Hourly Forecast Window Cache

- Status: Accepted
- Date: 2026-06-09
- Decision Origin: Design

## Context

ADR-0161 introduced a conservative VOACAP closed-only fallback for paths where
the bucket predictor has insufficient data. That first integration cached one
best matching-band FT8-equivalent SNR across the whole configured forecast
window.

The desired behavior is a true 8-hour forecast: the telnet spot rendered at a
given UTC hour should use VOACAP's record for that UTC hour, not the strongest
record from another hour in the same run. The cache still needs to be bounded,
lazy, nonblocking, and simple enough to run in memory without adding a restart
restore or offline analytics store.

## Decision

Cache each successful VOACAP run as an in-memory forecast window containing
hourly records for the requested band. When multiple configured center
frequencies map to the same band and hour, keep the strongest integer
FT8-equivalent SNR for that hour.

Remove the forecast-window start hour from the reusable cache key so one
successful run can serve later hours in the same configured forecast horizon.
The key still includes the directed user/DX H3 cells, band, configured center
frequency, year, month, rounded EWMA SSN generation, and direction. Month
rollover intentionally causes a new key.

At lookup time, select the cached record whose `HourUTC` equals
`now.UTC().Hour()`. Evaluate closed/open against
`mode_thresholds.<mode>.closed` for that selected record. If the cache entry has
no current-hour record, or if the lookup time is outside the cached
`WindowStartUTC + forecast_hours` horizon, discard it and let the existing lazy
delay, inflight dedupe, queue bound, TTL, and cache cap paths decide when to
run VOACAP again.

`SET DIAG PATH` for a VOACAP closed result shows the selected hourly forecast as
`vcap|<ft8_snr>|hNN|s<ssn>`. The age token is omitted from this VOACAP-specific
diagnostic so the SNR, selected hour, and SSN fit in the fixed DX-cluster
comment width.

## Alternatives considered

1. Store only the current hour in memory and future hours in Pebble.
   - Rejected for this slice because restart restore and disk cache lifecycle
     are separate decisions, while the approved need is bounded true 8-hour
     prediction during one process lifetime.
2. Keep caching the best SNR across the whole window.
   - Rejected because it can answer a current-hour path with a different hour's
     forecast and makes the 8-hour VOACAP run semantically misleading.
3. Proactively refresh windows before expiry.
   - Rejected because the approved fallback remains lazy and should not add
     background VOACAP load before the closed-only behavior is observed.

## Consequences

### Benefits

- Uses the VOACAP method-30 hourly records as true per-hour predictions.
- Reuses one successful VOACAP run across the configured horizon without
  rerunning once per hour.
- Keeps retained state bounded by `max_cache_entries` and the configured
  forecast-hour count.
- Preserves the closed-only fallback contract and per-mode threshold
  re-evaluation from ADR-0161.

### Risks

- The cache is memory-only; restart restores no prior VOACAP windows.
- At month rollover, the cache key changes and a fresh lazy lookup is required.
- If the current hour is outside a cached window, the user may briefly see
  insufficient data again until the lazy delay permits another run.
- `SET DIAG PATH` no longer shows VOACAP cache age because the fixed comment
  field cannot reliably fit SNR, selected hour, SSN, and age.

### Operational impact

When enabled, the fallback stores at most `max_cache_entries * forecast_hours`
hourly VOACAP records, plus existing map overhead. The worker count, queue
depth, lazy delay, TTL, and closed glyph behavior remain unchanged. Bucket p50
evidence remains authoritative.

## Links

- Related code: `pathreliability/voacap_fallback.go`, `telnet/server.go`
- Related docs: `pathreliability/README.md`,
  `data/config/PATH_PREDICTIONS.md`, `README.md`, `docs/OPERATOR_GUIDE.md`
- Related tests: `pathreliability/voacap_fallback_test.go`,
  `telnet/diag_command_test.go`, `telnet/path_settings_test.go`
- Related ADRs: ADR-0160, ADR-0161
- Related TSRs: none
- Supersedes / superseded by: supersedes ADR-0161's single best-window cache
  value clause
