# ADR-0163: VOACAP-Aligned Sparse P50 Fallback

- Status: Accepted
- Date: 2026-06-09
- Decision Origin: Scope Ledger v1

## Context

ADR-0161 introduced a conservative VOACAP fallback that could only emit the
closed glyph when bucket evidence was insufficient. ADR-0162 then made the
cache select the current UTC hour's VOACAP record instead of the strongest
record across the whole forecast window.

The closed-only behavior leaves a useful middle case unused: an insufficient
bucket result may still carry sparse p50 evidence. That p50 is not strong enough
to satisfy the configured count, receiver, weight, or freshness gates by
itself, but it is still local observation evidence. If the sparse bucket p50 and
the cached current-hour VOACAP forecast map to the same path class, the two
independent signals can safely corroborate a normal glyph without making VOACAP
the sole source of a high/medium/low/unlikely prediction.

## Decision

Keep sufficient bucket p50 evidence authoritative. The VOACAP fallback remains
eligible only when the normal predictor returns `INSUFFICIENT`.

For an insufficient result:

1. Select the cached current-hour VOACAP record using the ADR-0162 hourly cache
   rules.
2. If the VOACAP FT8-equivalent SNR is at or below
   `mode_thresholds.<mode>.closed`, emit the configured closed glyph as before.
   Closed behavior remains VOACAP-only because the bucket closed class does not
   exist in normal p50 output.
3. Otherwise, if the insufficient bucket result has sparse p50 evidence and the
   sparse p50 class equals the VOACAP current-hour class, emit that aligned
   normal glyph and PATH class.
4. If there is no sparse p50, or the classes disagree, keep `INSUFFICIENT`.

Aligned results use a separate prediction source and propagation-log counter:
`voacap_aligned`. `SET DIAG PATH` uses `valn|<p50>/<snr>h<hour>s<ssn>` for
aligned results. The p50 value is rounded in this compact diagnostic because the
DX-cluster fixed-width comment area cannot reliably fit decimal p50, VOACAP
SNR, hour, and SSN without truncating the SSN.

## Alternatives considered

1. Keep VOACAP fallback closed-only.
   - Rejected because sparse p50 plus matching current-hour VOACAP provides a
     useful corroborated signal without letting VOACAP alone create normal
     path classes.
2. Emit normal classes from VOACAP whenever buckets are insufficient.
   - Rejected because this would make VOACAP the sole source of normal glyphs
     and would bypass the observed-bucket evidence contract.
3. Blend VOACAP SNR into the bucket histogram.
   - Rejected because it changes the p50 surface and calibration model. The
     approved behavior is a display/filter fallback decision after the bucket
     predictor has already returned insufficient.

## Consequences

### Benefits

- Expands useful path glyph coverage for sparse paths without overriding
  sufficient observed bucket evidence.
- Keeps closed behavior conservative and explicitly VOACAP-owned.
- Makes aligned results observable through `voacap_aligned` and `valn`
  diagnostics instead of hiding them inside existing insufficient or combined
  counters.

### Risks

- VOACAP and sparse p50 can agree for the wrong reason, especially on paths
  with one or two stale-adjacent or receiver-skewed observations. The normal
  bucket result still wins once the configured evidence gates are met.
- The compact `valn` diagnostic rounds p50 for display. Operators should treat
  it as an alignment clue, not as an exact p50 audit field.
- More nonblank glyphs can appear on sparse paths, so support guidance must
  distinguish `valn` from a pure VOACAP forecast.

### Operational impact

When enabled, the fallback can produce either `voacap_closed` or
`voacap_aligned` prediction counters. Propagation reports summarize hours with
each fallback type. `SET DIAG PATH` shows `vcap` for VOACAP closed results and
`valn` for VOACAP-aligned sparse p50 results.

## Links

- Related code: `pathreliability/voacap_fallback.go`,
  `pathreliability/predictor.go`, `telnet/server.go`,
  `internal/cluster/bootstrap.go`, `internal/propreport/report.go`
- Related config: `data/config/path_reliability.yaml`
- Related docs: `README.md`, `docs/OPERATOR_GUIDE.md`,
  `pathreliability/README.md`, `data/config/PATH_PREDICTIONS.md`,
  `data/config/README.md`, `customgpt/support-cards/path-reliability.md`
- Related tests: `pathreliability/voacap_fallback_test.go`,
  `telnet/path_settings_test.go`, `telnet/diag_command_test.go`,
  `telnet/server_prediction_stats_test.go`,
  `internal/propreport/report_test.go`
- Related ADRs: ADR-0160, ADR-0161, ADR-0162
- Related TSRs: none
- Supersedes / superseded by: extends ADR-0161's closed-only limitation while
  preserving its closed glyph rule; preserves ADR-0162's current-hour cache
  selection
