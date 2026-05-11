# ADR-0133: Path Receiver Cap P50 Shadow Histograms

- Status: Superseded
- Date: 2026-05-11
- Decision Origin: Design

## Context

ADR-0132 added three receiver-cap shadow candidates, but those candidates were
gate-only. That answered whether a candidate cap would pass count and weight
floors, but not whether the candidate cap would materially change the p50 class
or glyph.

The operator needs to compare cap values under identical live traffic and know
whether a cap mostly changes insufficiency, mostly changes the p50 class, or has
little glyph-visible effect. The diagnostic must not change active glyphs or
PATH filters while the cap value is still being evaluated.

## Decision

Add `receiver_shadow_p50_enabled` to `path_reliability.yaml`. The key is
required and controls whether receiver-cap shadow candidates retain fixed SNR
histograms in addition to the ADR-0132 gate counters.

When enabled, each configured candidate count cap records a bounded fixed-bin
SNR histogram through the same fine/coarse sample selection and
receive/transmit merge path used by active p50 scoring. Five-minute propagation
logs add a separate `Path cap p50 shadow (5m)` line with:

- `capN_p50_pass_unlikely`, `capN_p50_pass_low`,
  `capN_p50_pass_medium`, and `capN_p50_pass_high`
- `capN_p50_same`, `capN_p50_stronger`, `capN_p50_weaker`, and
  `capN_p50_to_insufficient`

The existing `Path cap shadow (5m)` line remains gate-only and backward
compatible. Active glyphs, PATH filters, telnet command syntax, archive output,
peer wire format, and `SET DIAG PATH` output are unchanged.

## Alternatives considered

1. Derive candidate p50 outcomes from the active capped histogram. Rejected
   because candidate count caps can admit different per-receiver fractions and
   slot replacements.
2. Log every candidate histogram bin. Rejected because it would make routine
   propagation logs too large for the tuning question.
3. Enable candidate p50 shadow unconditionally. Rejected because operators may
   want gate-only cap shadow with lower retained state and hot-path work.

## Consequences

### Benefits

- A single run can show whether caps 5, 6, and 8 would change visible p50 class
  outcomes under the same traffic.
- Gate failures remain separate from p50 class movement.
- The daily propagation report can summarize candidate class movement without
  parsing verbose per-prediction diagnostics.

### Risks

- When enabled, candidate shadow state performs more fixed-array decay and merge
  work on path updates and predictions.
- The new line is diagnostic only; treating it as active behavior would
  misinterpret shadow-mode results.
- Historical logs without `Path cap p50 shadow (5m)` are not directly
  comparable for candidate glyph movement.

### Operational impact

- Startup now requires `receiver_shadow_p50_enabled` in
  `path_reliability.yaml`.
- The checked-in config enables candidate p50 shadow while receiver caps remain
  in `shadow` mode.
- The daily propagation report parser understands and summarizes
  `Path cap p50 shadow (5m)` lines.

## Links

- Related issues/PRs/commits:
- Related tests: `pathreliability/config_test.go`,
  `pathreliability/receiver_test.go`,
  `telnet/server_prediction_stats_test.go`,
  `internal/propreport/report_test.go`
- Related docs: `data/config/path_reliability.yaml`,
  `pathreliability/README.md`, `README.md`, `docs/OPERATOR_GUIDE.md`,
  `data/config/PATH_PREDICTIONS.md`, `customgpt/common-questions.md`
- Related TSRs: `docs/troubleshooting/TSR-0022-path-receiver-cap-lifetime-count.md`
- Supersedes / superseded by: extends ADR-0132; superseded by ADR-0135.
