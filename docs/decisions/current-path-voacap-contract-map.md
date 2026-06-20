# Current Path and VOACAP ADR Map

This map helps readers navigate the dense path-reliability and VOACAP decision
history. It is a pointer document, not a replacement runtime specification.
Current operator behavior remains in [`../../README.md`](../../README.md),
[`../OPERATOR_GUIDE.md`](../OPERATOR_GUIDE.md),
[`../../pathreliability/README.md`](../../pathreliability/README.md), and
current source/tests.

## How To Use

- Read the latest accepted ADRs in each chain first.
- Treat superseded ADRs as context for why the active design changed.
- Treat experiment ADRs as background unless a later accepted ADR makes their
  outcome part of runtime behavior.
- Re-check current source, tests, generated code maps, and operator docs before
  making implementation or support claims.

## Active Path Reliability Chain

- Active p50 scoring starts with
  [ADR-0126](ADR-0126-active-p50-path-scoring.md).
- Location-specific noise penalties are in
  [ADR-0127](ADR-0127-location-specific-path-noise-penalties.md).
- PATHP50 diagnostics and clamp config retirement are in
  [ADR-0128](ADR-0128-remove-pathp50-diagnostics-and-clamp-config.md).
- Receiver-cap capacity, decay, diversity gates, and cap-8 enforcement are in
  [ADR-0129](ADR-0129-path-receiver-capacity-and-enforcement.md),
  [ADR-0130](ADR-0130-path-receiver-cap-count-decay.md),
  [ADR-0134](ADR-0134-path-observation-floor-and-receiver-diversity-gate.md),
  and
  [ADR-0135](ADR-0135-path-cap8-enforcement-and-shadow-candidate-retirement.md).
- P50 midpoint and even-split display semantics are in
  [ADR-0131](ADR-0131-path-p50-midpoint-and-even-split-semantics.md).
- Active p50 histogram retention is in
  [ADR-0139](ADR-0139-active-path-p50-histogram-lane-retention.md).
- Fine/coarse scalar evidence is in
  [ADR-0170](ADR-0170-fine-coarse-union-scalar-evidence.md).
- Beacon receive-only prediction semantics are in
  [ADR-0174](ADR-0174-beacon-rx-only-path-prediction.md).
- Admission reuse between p50 path prediction and VOACAP sparse diagnostics is
  in [ADR-0176](ADR-0176-path-prediction-admission-reuse.md).
- Native 160m solar-darkness fallback for insufficient p50 and unavailable
  VOACAP coverage is currently in
  [ADR-0201](ADR-0201-native-160m-endpoint-daylight-gate.md), which makes
  endpoint daylight/twilight the first gate before whole-path darkness. It
  supersedes the whole-path-only classification order in
  [ADR-0197](ADR-0197-native-160m-closed-solar-proxy.md), which had already
  superseded the LOW/UNLIKELY-only experiment contract in
  [ADR-0196](ADR-0196-native-160m-solar-darkness-fallback.md).

## Active VOACAP Chain

- Early VOACAP work starts with the experiment records
  [ADR-0157](ADR-0157-voacap-ssn-moving-average-experiment.md),
  [ADR-0158](ADR-0158-voacap-process-wrapper-experiment.md),
  [ADR-0159](ADR-0159-voacap-yaml-owned-ssn-forecast-experiment.md), and
  [ADR-0160](ADR-0160-voacap-ft8-snr-output-contract.md).
- Runtime fallback, hourly cache, aligned sparse-p50 behavior, rolling forecast
  windows, and bidirectional/noise-aware semantics are in
  [ADR-0161](ADR-0161-voacap-closed-only-path-fallback.md),
  [ADR-0162](ADR-0162-voacap-hourly-forecast-window-cache.md),
  [ADR-0163](ADR-0163-voacap-aligned-sparse-p50-fallback.md),
  [ADR-0164](ADR-0164-voacap-rolling-hour-runtime-forecast-window.md), and
  [ADR-0169](ADR-0169-bidirectional-noise-aware-voacap-fallback.md).
- Ham median card template and distance-selected method behavior are in
  [ADR-0165](ADR-0165-voacap-ham-median-card-template.md) and
  [ADR-0177](ADR-0177-distance-selected-voacap-method.md).
- CLOSED filter semantics are in
  [ADR-0166](ADR-0166-closed-path-filter-token.md).
- Stage counters, cache comparison, sparse diagnostics, invalid-request reason
  splits, and open-row display are in
  [ADR-0167](ADR-0167-voacap-closed-sparse-p50-stage-counters.md),
  [ADR-0168](ADR-0168-voacap-p50-cache-comparison.md),
  [ADR-0175](ADR-0175-sparse-p50-voacap-outcome-diagnostics.md),
  [ADR-0178](ADR-0178-voacap-invalid-request-reason-diagnostics.md), and
  [ADR-0187](ADR-0187-show-prop-open-row-display.md).
- REL-gated open fallback is in
  [ADR-0173](ADR-0173-voacap-rel-gated-open-fallback.md).
- SHOW PROP worker refresh and glyph columns are in
  [ADR-0172](ADR-0172-show-prop-worker-refresh-and-glyph-columns.md).
- Runtime output cleanup, SSN persistence, forecast-cache persistence, shared
  Pebble open behavior, SSN overview visibility, and cache overview counters
  are in
  [ADR-0180](ADR-0180-voacap-runtime-output-cleanup.md),
  [ADR-0182](ADR-0182-voacap-runtime-ssn-state-persistence.md),
  [ADR-0184](ADR-0184-voacap-forecast-cache-persistence.md),
  [ADR-0185](ADR-0185-shared-pebble-directory-open-helper.md),
  [ADR-0190](ADR-0190-voacap-ssn-overview-visibility.md), and
  [ADR-0191](ADR-0191-voacap-cache-overview-counters.md).

## Historical Context

- [ADR-0122](ADR-0122-path-p50-shadow-diagnostics.md),
  [ADR-0124](ADR-0124-no-noise-penalty-path-evaluation.md), and
  [ADR-0125](ADR-0125-gated-pathp50-shadow-comparison.md) are earlier p50
  shadow/evaluation records superseded by the active p50 path.
- [ADR-0123](ADR-0123-dedicated-propagation-log.md) remains useful background
  for propagation logging, but later path and VOACAP ADRs own the current
  counter details.
- [ADR-0132](ADR-0132-path-receiver-cap-shadow-candidates.md) and
  [ADR-0133](ADR-0133-path-receiver-cap-p50-shadow-histograms.md) are
  superseded receiver-cap shadow records; read through ADR-0135 for the active
  shipped cap behavior.
- [ADR-0171](ADR-0171-show-prop-voacap-outlook.md) is superseded by ADR-0172
  for SHOW PROP behavior.
