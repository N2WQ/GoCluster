# TSR-0028: VOACAP Fallback Current-Hour Miss

- Status: Resolved
- Date opened: 2026-06-09
- Status date: 2026-06-09

## Trigger

Live propagation logs showed `combined` path predictions but
`voacap_closed=0` and `voacap_aligned=0` after `SET DIAG PATH` was enabled for
operator inspection. The user asked whether the combined VOACAP fallback method
was actually firing.

## Symptoms and impact

The optional VOACAP fallback was enabled and output files were being written,
but no VOACAP closed or aligned glyphs appeared in the five-minute path
prediction totals. Operators could not tell whether the fallback was idle,
blocked, producing open forecasts, missing the current hour, or failing before
cache use.

## Hypotheses tested

1. The fallback worker was not running.
   - Disproved by live VOACAP output files being written.
2. The final closed/aligned thresholds were too strict.
   - Not the primary explanation because no cached current-hour record was
     available to reach those decisions.
3. The cache lookup was missing the current UTC hour.
   - Confirmed by comparing generated VOACAP output hour rows with the runtime
     lookup hour.

## Evidence

- Live output for the 17:00 UTC run contained method-30 hour rows for `1..8`
  instead of the current rolling UTC window.
- `internal/voacap/deck.go` generated the `TIME` card as `1..forecast_hours`
  for every path deck.
- `pathreliability/voacap_fallback.go` selected cached forecasts by
  `now.UTC().Hour()`, so a 17:00 lookup could not use a cache containing only
  hours `1..8`.
- Existing tests used fake cached hours and did not couple runtime deck hour
  generation to current-hour cache lookup.
- Local VOACAP notes documented that beginning and ending hours may wrap across
  midnight. Existing output also showed midnight as hour `24`.

## Root cause or best current explanation

Runtime fallback decks always asked VOACAP for hours `1..forecast_hours`.
The cache lookup correctly asked for the current UTC hour, but the generated
forecast often did not contain that hour. The cache entry was then discarded as
missing the current hour, and the fallback returned no glyph.

## Fix or mitigation

- Runtime VOACAP fallback decks now start the `TIME` card at
  `WindowStartUTC` in VOACAP's `1..24` hour notation and wrap the end hour when
  needed.
- VOACAP method-30 output hour `24` is normalized to UTC hour `0` before the
  fallback cache stores hourly records.
- The fallback now exposes reset-on-snapshot stage counters for queueing,
  success, cache hit, no current hour, blocked states, and final closed/aligned
  decision gates.
- Regression tests cover deck start-hour generation, midnight normalization,
  cache-stage counters, and telnet fallback decision counters.

## Why an ADR was or was not required

- ADR required because the fix changes the durable runtime VOACAP forecast
  window contract and adds operator-visible fallback stage diagnostics.

## Links

- Related ADRs: ADR-0164
- Related issues/PRs/commits: none
- Related tests: `internal/voacap/forecast_state_test.go`,
  `internal/voacap/output_test.go`, `pathreliability/voacap_fallback_test.go`,
  `telnet/server_prediction_stats_test.go`
- Related docs: `pathreliability/README.md`, `docs/OPERATOR_GUIDE.md`,
  `data/config/PATH_PREDICTIONS.md`, `README.md`
