# ADR-0167: VOACAP Closed Sparse-P50 Stage Counters

- Status: Accepted
- Date: 2026-06-10
- Decision Origin: Scope Ledger v1

## Context

ADR-0163 lets VOACAP closed fallback emit the configured closed glyph before
the sparse-p50 alignment branch runs. Live propagation logs showed many more
`voacap_closed` results than `voacap_aligned` results, which raised an
operator question: when VOACAP says closed, did the bucket predictor have no
sparse p50 evidence, or did VOACAP closed override sparse observed p50
evidence?

The existing `VOACAP fallback (5m)` stage line exposed only one aggregate
`closed` counter. That preserved final glyph counts but did not explain the
closed branch well enough for VOACAP calibration and support analysis.

## Decision

Keep the final `Path predictions (5m)` counters unchanged. `voacap_closed`
continues to count emitted closed glyphs, and `voacap_aligned` continues to
count emitted VOACAP-aligned sparse p50 glyphs.

Keep the aggregate `closed` stage counter in `VOACAP fallback (5m)` for
backward-compatible operator reading, and add closed-stage split counters:

```text
closed_no_p50=<n>
closed_with_sparse_p50=<n>
closed_with_sparse_p50_class_high=<n>
closed_with_sparse_p50_class_medium=<n>
closed_with_sparse_p50_class_low=<n>
closed_with_sparse_p50_class_unlikely=<n>
```

`closed_no_p50` counts closed VOACAP cache hits where the insufficient bucket
result had no sparse p50 value. `closed_with_sparse_p50` counts closed VOACAP
cache hits where the insufficient bucket result did have sparse p50 evidence.
The class-specific counters split that sparse p50 evidence by the same
`HIGH`/`MEDIUM`/`LOW`/`UNLIKELY` class mapping used for ordinary path glyphs.

This is observability only. It does not change VOACAP SNR conversion, p50
selection, fallback ordering, glyph choice, PATH filter semantics, queueing,
cache keys, or VOACAP deck generation.

## Alternatives considered

1. Replace `closed` with only the split counters.
   - Rejected because existing operators and scripts can continue reading the
     aggregate while newer analysis uses the split.
2. Add only `closed_no_p50` and `closed_with_sparse_p50`.
   - Rejected because the sparse-p50 class is the useful calibration clue when
     VOACAP closed overrides observed sparse evidence.
3. Change fallback ordering so sparse p50 can prevent closed glyphs.
   - Rejected for this scope because the request is diagnostic. Behavior
     changes require a separate decision after the new counters provide data.

## Consequences

### Benefits

- Operators can distinguish truly no-p50 closed fallbacks from closed fallbacks
  that had sparse observed p50 evidence.
- Calibration work can see whether VOACAP closed is overriding mostly weak
  sparse p50, or also overriding stronger sparse classes.
- Historical readers of `closed` remain compatible.

### Risks

- The fallback stage line is longer.
- Operators must continue to use `Path predictions (5m)` for final emitted
  glyph counts; the fallback line remains a stage explanation.
- The class split reflects sparse p50 class before the closed glyph is emitted,
  not a separate displayed glyph.

### Operational impact

No config or migration is required. New logs include the closed split counters
when VOACAP fallback activity is present. Older logs without the split remain
valid historical records.

## Links

- Related code: `telnet/server.go`, `internal/cluster/bootstrap.go`
- Related docs: `README.md`, `docs/OPERATOR_GUIDE.md`,
  `pathreliability/README.md`, `data/config/PATH_PREDICTIONS.md`,
  `customgpt/support-cards/path-reliability.md`
- Related tests: `telnet/server_prediction_stats_test.go`
- Related ADRs: ADR-0163, ADR-0164, ADR-0166
- Related TSRs: TSR-0028
- Supersedes / superseded by: extends ADR-0164's fallback-stage log contract
