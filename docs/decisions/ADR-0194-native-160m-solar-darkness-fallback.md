# ADR-0194: Native 160m Solar Darkness Fallback

- Status: Superseded
- Date: 2026-06-18
- Decision Origin: Design

## Context

Path p50 remains the authoritative live path predictor, but insufficient 160m
bucket evidence leaves a coverage gap when VOACAP has no usable 160m
current-hour fallback. The Phase 1 experiment compared exact analytic solar
path exposure against 7/9/11 point samples and archive slices. The result
favored exact analytic exposure for runtime use because it avoids sample
threshold aliasing and is cheaper than sample9 in the benchmarked path.

R/G propagation tags already evaluate path solar geometry, but their scientific
threshold is not the same as 160m darkness. R blackout gating should remain
sunlit-side geometry at horizon (`0` degrees), while 160m low-band opportunity
uses civil-darkness exposure (`-6` degrees) as a conservative proxy.

## Decision

Add a shared `internal/solarpath` package for exact great-circle solar exposure
and use it from both solar-weather daylight gates and native 160m fallback.

Add `path_reliability.native_160m_fallback` as YAML-owned experiment config.
When enabled, native 160m fallback evaluates only insufficient 160m p50 results
and only when VOACAP does not have a usable current-hour result with precedence.
It can emit only `LOW` or `UNLIKELY` from civil-dark path fraction thresholds.
It never emits `HIGH` or `MEDIUM`, never replaces sufficient p50, and does not
claim to model SNR or probability.

Add compact diagnostics (`n160|dNN`, `bn160|dNN`) and five-minute aggregate
counters so the on-air run can compare candidate volume, emissions, not-dark,
unknown, display-disabled, and fixed civil-darkness buckets.

Superseded note: ADR-0195 keeps the same p50 -> VOACAP -> native 160m
precedence but adds native 160m `CLOSED` for the low-darkness solar proxy
bucket.

## Alternatives considered

1. Use 9 path samples at runtime. This was simple and useful for experiments,
   but exact analytic exposure is cheaper and removes sample threshold aliasing.
2. Reuse solarweather R daylight directly for 160m. This conflates sunlit-side
   R blackout physics with low-band darkness opportunity and would use the
   wrong threshold.
3. Emit HIGH/MEDIUM classes from darkness. Rejected because darkness is not SNR
   and should not overstate confidence without calibrated on-air evidence.

## Consequences

### Benefits

- Gives 160m a bounded native fallback path when p50 and VOACAP cannot help.
- Keeps p50 authority intact.
- Shares exact solar geometry with R/G daylight gating.
- Produces aggregate evidence for the next calibration pass.

### Risks

- Civil darkness is only an opportunity proxy; it can be wrong for local noise,
  antenna pattern, absorption, geomagnetic disturbance, and path-specific
  effects.
- The experiment config intentionally displays native 160m LOW/UNLIKELY, so
  operators must treat it as weaker than p50-backed classes.
- R/G behavior for nonzero `sun.twilight_degrees` becomes more exact; the
  shipped `0` degree R threshold remains the intended current contract.

### Operational impact

- `Path predictions (5m)` gains `native160_low` and `native160_unlikely`.
- `Native 160m fallback (5m)` appears when native 160m candidates are evaluated.
- `SET DIAG PATH` can show `n160|dNN` or `bn160|dNN`.
- `native_160m_fallback.display_enabled: false` can switch to shadow-only
  counting without removing the evaluator.

## Links

- Related issues/PRs/commits: experiment branch `experiment/160m-native-heuristic`
- Related tests: `internal/solarpath/exposure_test.go`; `pathreliability/native160_fallback_test.go`; `telnet/server_prediction_stats_test.go`
- Related docs: `data/config/PATH_PREDICTIONS.md`; `pathreliability/README.md`; `docs/OPERATOR_GUIDE.md`; `customgpt/support-cards/path-reliability.md`
- Related TSRs: -
- Supersedes / superseded by: Superseded by ADR-0195
