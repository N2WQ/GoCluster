# ADR-0164: VOACAP Rolling-Hour Runtime Forecast Window

- Status: Accepted
- Date: 2026-06-09
- Decision Origin: Troubleshooting chat

## Context

ADR-0162 made the fallback cache select the current UTC hour from a successful
VOACAP method-30 forecast window. Live troubleshooting later showed that runtime
fallback decks still generated a fixed `TIME 1..forecast_hours` card. At 17:00
UTC, for example, VOACAP produced rows for hours `1..8`, while the cache lookup
asked for hour `17` and discarded the entry as missing the current hour.

The fix must preserve the existing lazy, nonblocking fallback. It must not run
VOACAP synchronously in the telnet path, change p50 thresholds, or let VOACAP
alone emit normal glyphs beyond the ADR-0163 sparse-p50 alignment rule.

## Decision

Runtime VOACAP fallback decks start their `TIME` card at
`voacap.HourForUTC(WindowStartUTC)`, using VOACAP's `1..24` notation where
midnight is hour `24`. The end hour is computed from the configured
`forecast_hours` and wraps over midnight when needed. Experiment decks keep the
legacy default start hour of `1` unless a caller explicitly supplies a start
hour.

The method-30 parser normalizes output hour `24` to UTC hour `0` before records
enter the path-reliability cache. The cache continues to select records by
`now.UTC().Hour()` and continues to reject records outside
`WindowStartUTC + forecast_hours`.

Add a separate five-minute propagation log line for fallback stage activity:

```text
VOACAP fallback (5m): queued=<n> success=<n> failure=<n> cache_hit=<n> no_current_hour=<n> delay_wait=<n> inflight=<n> queue_full=<n> not_running=<n> ssn_unavailable=<n> invalid_request=<n> closed=<n> aligned=<n> open_no_p50=<n> class_mismatch=<n>
```

The existing `Path predictions (5m)` line remains the final emitter summary:
`voacap_closed` and `voacap_aligned` count only glyphs emitted through
`Result.Source`.

## Alternatives considered

1. Keep runtime decks fixed at `1..forecast_hours`.
   - Rejected because current-hour cache lookup can miss most UTC hours and
     silently suppress fallback output.
2. Select the nearest or strongest cached VOACAP hour when the current hour is
   missing.
   - Rejected because ADR-0162 intentionally made current-hour selection the
     semantic contract for spot display.
3. Run VOACAP synchronously when the cache misses the current hour.
   - Rejected because telnet rendering and PATH filters must remain nonblocking
     and bounded.
4. Treat any open VOACAP forecast as a normal glyph without sparse p50.
   - Rejected because ADR-0163 requires VOACAP and sparse bucket p50 to agree
     before a normal glyph is emitted.

## Consequences

### Benefits

- Runtime VOACAP forecasts cover the actual rolling UTC window that the cache
  lookup will query.
- Midnight output is normalized into the same `0..23` UTC-hour domain used by
  Go and by `SET DIAG PATH`.
- Operators can distinguish idle, queued, blocked, cache-hit, missing-hour,
  closed, aligned, open-without-p50, and class-mismatch fallback behavior.

### Risks

- The fallback remains lazy. A first insufficient spot still starts the delay
  and does not synchronously return a VOACAP glyph.
- The stage log is diagnostic, not a final emit counter. Operators must use
  `Path predictions (5m)` for emitted glyph totals and `VOACAP fallback (5m)`
  for why fallback work did or did not emit.
- VOACAP's hour `24` convention is now an explicit parser contract; future
  parser changes must preserve normalization.

### Operational impact

- No new config keys are introduced.
- Worker count, queue depth, cache TTL, delay, and p50 thresholds are unchanged.
- A `no_current_hour` stage count should be rare after this fix. Nonzero values
  now indicate malformed output, parser drift, or a cache window that does not
  cover the lookup time.

## Links

- Related issues/PRs/commits: none
- Related tests: `internal/voacap/forecast_state_test.go`,
  `internal/voacap/output_test.go`, `pathreliability/voacap_fallback_test.go`,
  `telnet/server_prediction_stats_test.go`
- Related docs: `pathreliability/README.md`, `docs/OPERATOR_GUIDE.md`,
  `data/config/PATH_PREDICTIONS.md`, `README.md`
- Related TSRs: TSR-0028
- Supersedes / superseded by: clarifies ADR-0162 runtime deck generation; does
  not supersede ADR-0162's hourly cache decision
