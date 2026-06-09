# ADR-0161: VOACAP Closed-Only Path Fallback

- Status: Accepted
- Date: 2026-06-09
- Decision Origin: Design

## Context

The path-reliability method predicts FT8-equivalent p50 SNR from recent bucketed
cluster evidence. When the buckets have insufficient data, the display currently
falls back to a blank glyph. The VOACAP experiment can produce directed,
integer FT8-equivalent SNR forecasts, but using those forecasts as a full
replacement for high/medium/low path quality would require calibration against
observed cluster buckets.

The first production integration needs to be conservative, bounded, and
operator-owned. The specific near-term goal is to forecast likely band-closed
cases when bucket evidence is missing.

## Decision

Add an optional `path_reliability.voacap_fallback` runtime path that is disabled
by default. When enabled, the cluster fetches SSN on the configured cadence,
maintains an integer rounded 8-hour EWMA SSN generation, and starts a bounded
VOACAP worker.

Bucket p50 evidence remains authoritative. The fallback is considered only when
the normal predictor returns `INSUFFICIENT`. An insufficient request starts a
15-minute delayed, nonblocking lookup keyed by directed H3 cell pair, band,
center frequency, forecast window, month, SSN generation, and direction. Cached
VOACAP output can only emit the configured closed glyph when the best
matching-band FT8-equivalent SNR is at or below the configured closed threshold.
It does not emit HIGH, MEDIUM, LOW, or ordinary UNLIKELY predictions.

The closed glyph is configurable through `glyph_symbols.closed`. The shipped
symbol is `!`. PATH filters continue to use the existing class names; the closed
fallback maps to `UNLIKELY` for filter semantics.

## Alternatives considered

1. Blend VOACAP SNR into the current p50 bucket surface.
   - Rejected for this slice because calibration and conflict handling against
     live observations are separate decisions.
2. Run VOACAP synchronously during telnet spot rendering.
   - Rejected because process launch latency would put an external executable
     on a hot user-visible path.
3. Emit full VOACAP quality classes when buckets are insufficient.
   - Rejected because the current evidence contract is p50 FT8 SNR from actual
     observations. The only approved first fallback signal is likely closed.

## Consequences

### Benefits

- Preserves the existing bucket-first path-reliability contract.
- Adds a bounded, lazy fallback for data-sparse paths without blocking telnet
  spot rendering.
- Keeps operator policy in YAML for fetch cadence, forecast bands, delay,
  cache size, queue depth, delta threshold, and closed threshold.

### Risks

- A VOACAP false-closed forecast may hide a workable opening when buckets are
  empty. The safety margin is configurable, and sufficient bucket evidence still
  overrides the fallback.
- Direction, SSN generation, month, and forecast window expand cache keys, so
  cache sizing must account for active path diversity.
- Enabling the fallback requires a valid local VOACAP installation; startup
  validation fails if the configured home is unusable.

### Operational impact

When disabled, no SSN polling, VOACAP worker, queue, or fallback cache is
started. When enabled, five-minute path prediction logs include a
`voacap_closed` counter, and propagation reports summarize hours with closed
fallback predictions. `SET DIAG PATH` shows `vcap` diagnostics for cached
closed predictions.

## Links

- Related code: `pathreliability/voacap_fallback.go`,
  `internal/voacap/sunspot_monitor.go`, `telnet/server.go`,
  `internal/cluster/main_runtime.go`
- Related config: `data/config/path_reliability.yaml`
- Related tests: `pathreliability/voacap_fallback_test.go`,
  `internal/voacap/sunspot_monitor_test.go`, `telnet/path_settings_test.go`
- Related ADRs: ADR-0157, ADR-0158, ADR-0159, ADR-0160
- Related TSRs: none
