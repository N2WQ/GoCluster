# ADR-0173: VOACAP REL-Gated Open Fallback

- Status: Accepted
- Date: 2026-06-11
- Decision Origin: Scope Ledger v1

## Context

ADR-0161 introduced VOACAP closed fallback for insufficient bucket evidence.
ADR-0163 then allowed a normal glyph when sparse bucket p50 and current-hour
VOACAP mapped to the same class. Live logs still showed many blank path glyphs
for paths where cached VOACAP had an open current-hour forecast, especially
`n0|none` cases with no sparse p50.

VOACAP method-30 output also includes optional `REL`, which reports reliability
against the deck's request SNR. That value can strengthen the decision to use a
cached VOACAP open forecast, but it is not a direct probability that the path is
HIGH, MEDIUM, LOW, or UNLIKELY.

## Decision

Keep sufficient bucket p50 authoritative. Cache-only p50-vs-VOACAP comparison
remains diagnostic and does not run VOACAP or alter glyphs.

Keep closed fallback based on the effective bidirectional FT8-equivalent SNR50
at or below `mode_thresholds.<mode>.closed`. Closed fallback does not require
REL.

Retain parsed VOACAP `REL` through hourly fallback records. For bidirectional
runtime fallback records, both receive and transmit decks must carry REL; the
effective request-SNR reliability is the lower directional value.

When the normal predictor returns insufficient and cached current-hour VOACAP is
open:

1. Same-class sparse p50 alignment continues to emit `voacap_aligned` without a
   REL gate.
2. Sparse p50 can be upgraded by one class only when VOACAP maps one class
   stronger and the configured REL gate for the VOACAP class passes.
3. Multi-tier sparse upgrades are counted but not emitted.
4. A no-p50 insufficient result can emit a normal VOACAP open glyph only when
   the configured REL gate for the VOACAP class passes.
5. Missing, malformed, or unretained REL blocks the new open fallback paths and
   is counted separately from below-floor REL.

The YAML-owned gates live under `voacap_fallback`:

```yaml
reliability_gated_open_enabled: true
reliability_sparse_upgrade_enabled: true
reliability_min_high: 0.90
reliability_min_medium: 0.80
reliability_min_low: 0.65
reliability_min_unlikely: 0.50
```

Higher displayed classes require higher REL thresholds. The gates are applied
to VOACAP's request-SNR reliability, not to class probability.

## Alternatives considered

1. Require REL for closed fallback.
   - Rejected because closed remains an SNR50 threshold decision and REL was
     introduced only to bound open VOACAP fallback.
2. Allow VOACAP to override sufficient p50.
   - Rejected because the observed bucket p50 contract remains authoritative.
3. Allow multi-tier sparse upgrades when REL is high.
   - Rejected for this slice because a multi-tier disagreement is exactly the
     calibration case that should be counted before it changes display.
4. Use a single REL threshold for all classes.
   - Rejected because HIGH and MEDIUM open fallbacks should require stronger
     model reliability than LOW or UNLIKELY.

## Consequences

### Benefits

- Reduces blank path glyphs for cached VOACAP paths without running additional
  VOACAP jobs.
- Adds a defensible path for `n0|none` candidates when the cached current-hour
  forecast is open and reliable enough.
- Keeps p50 authority, closed semantics, and same-class sparse alignment stable.
- Makes REL pass/fail behavior visible in propagation logs and diagnostics.

### Risks

- VOACAP REL is tied to the request-SNR deck contract, so the displayed class is
  still a threshold mapping rather than a calibrated class probability.
- REL availability depends on parsed VOACAP output in both directions for
  bidirectional fallback; missing REL will keep some candidates blank.
- The stage log is longer and requires support guidance to distinguish final
  emitted counters from fallback-stage explanations.

### Operational impact

- `Path predictions (5m)` adds `voacap_sparse_upgrade` and `voacap_open`.
- `VOACAP fallback (5m)` adds `sparse_upgrade`, `open_no_p50_rel`,
  `rel_missing`, `rel_below_floor`, and `rel_multi_tier`.
- `SET DIAG PATH` can show `vup|<p50>/<snr>r<rel>s<ssn>` and
  `vop|<snr>r<rel>h<hour>s<ssn>` for the new REL-gated outputs.
- Existing `vcap`, `valn`, PATH `CLOSED`, and sufficient-p50 behavior remain
  compatible.

## Links

- Related code: `pathreliability/voacap_fallback.go`,
  `pathreliability/config.go`, `pathreliability/predictor.go`,
  `telnet/server.go`, `internal/cluster/bootstrap.go`,
  `internal/propreport/report.go`
- Related config: `data/config/path_reliability.yaml`
- Related tests: `pathreliability/voacap_fallback_test.go`,
  `pathreliability/config_test.go`, `telnet/server_prediction_stats_test.go`,
  `telnet/path_settings_test.go`, `telnet/diag_command_test.go`,
  `internal/propreport/report_test.go`
- Related docs: `README.md`, `docs/OPERATOR_GUIDE.md`,
  `pathreliability/README.md`, `data/config/PATH_PREDICTIONS.md`,
  `customgpt/support-cards/path-reliability.md`
- Related TSRs: none
- Supersedes / superseded by: extends ADR-0161, ADR-0163, ADR-0167,
  ADR-0168, and ADR-0169 without superseding them
