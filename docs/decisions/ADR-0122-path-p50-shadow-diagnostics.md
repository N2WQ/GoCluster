# ADR-0122 - Path P50 Shadow Diagnostics

Status: Accepted
Date: 2026-05-08
Decision Makers: Founder, Codex
Technical Area: pathreliability, telnet diagnostics, config
Decision Origin: Design
Troubleshooting Record(s): none
Tags: path reliability, retained state, diagnostics, hot path

## Context

Path reliability currently uses decayed linear-power means for active glyphs
and PATH filters. That method can hide distribution shape: a few stronger
observations can raise the mean while the median-like experience remains lower.
Operators need a visible way to compare the current method with a p50 SNR view
before any enforcement or threshold retuning is considered.

The path store is hot, retained server-lifetime state. Any diagnostic method
must stay bounded in CPU, heap, and allocations, and it must not change current
glyph or filter behavior while the method is being evaluated.

## Decision

Add a shadow p50 SNR distribution to each path bucket and expose it through a
new telnet diagnostic mode, `SET DIAG PATHP50`.

The retained representation is fixed and inline:

- 50 fixed SNR bins: `< -24`, one-dB bins from `-24..-23` through `23..24`,
  and `>= 24`.
- one raw histogram lane and one capped histogram lane per bucket.
- each lane stores decayed effective weight by bin using a fixed `[50]float32`
  array.
- no per-bin receiver identity slots, no maps, no slices, and no side tables.

The active glyph and PATH filter path remains mean-based. The p50 model is
diagnostic only when `distribution_statistic_mode: shadow` is enabled.

`SET DIAG PATH` remains unchanged. `SET DIAG PATHP50` displays:

```text
p<db>d<delta>n<count>
```

where `p` is the p50 SNR bin, `d` is active mean SNR minus p50 SNR, and `n` is
the compact selected observation count for this prediction. PATHP50 deliberately
does not repeat the path glyph because the glyph remains visible in the normal
spot tail column, and it does not use the longer `n<capped>/r<raw>` form because
that can consume the fixed-width comment field. `SET DIAG PATH` remains the
place to inspect raw/capped count detail. Positive values omit a plus sign to
preserve comment space.

## Alternatives Considered

1. Replace `SET DIAG PATH` with p50 output.
   - Pros: one diagnostic command.
   - Cons: removes existing count/weight/age troubleshooting output and makes
     operational comparison harder.
2. Retain raw observations and calculate exact percentiles.
   - Pros: exact p50.
   - Cons: unbounded retained memory and higher CPU/allocation risk in a hot
     path.
3. Add per-mode or per-source histograms.
   - Pros: more detailed analysis.
   - Cons: multiplies retained heap and diagnostic complexity before the basic
     p50 signal has been validated.

## Consequences

- Positive outcomes:
  - Operators can compare p50 SNR against the current mean method per spot.
  - Existing path glyphs, PATH filters, and `SET DIAG PATH` output are
    unchanged.
  - Retained state growth is predictable and tied only to existing bucket
    count.
- Negative outcomes / risks:
  - p50 values are bin values, not exact raw SNRs.
  - Bucket size increases because two fixed 50-bin histograms are retained per
    bucket.
  - The elapsed-second update path now decays two fixed arrays when p50 shadow
    mode is enabled.
- Operational impact:
  - New operator config key: `distribution_statistic_mode: off|shadow`.
  - New diagnostic command: `SET DIAG PATHP50`.
  - No normal spot-line glyph or filter behavior changes.
- Follow-up work required:
  - Compare p50 and mean diagnostics in live logs and operator reports before
    considering enforcement, threshold changes, or additional distribution
    views.
  - The first comparison step is a diagnostic-observed shadow aggregate:
    record current mean glyph class versus p50 glyph class, same/different
    outcomes, sample-count, band, mode-family, source, and glyph-pair buckets
    in the propagation log. This does not change active glyphs or PATH filters.

## Validation

Required validation includes:

- config validation for `distribution_statistic_mode`.
- tests for bin boundaries, underflow, overflow, weighted p50, decay behavior,
  raw/capped divergence, stale purge coupling, and `PATHP50` formatting.
- benchmark reporting with `-benchmem` for update, receiver-cap update,
  elapsed-second decay, and prediction.
- retained heap reporting using `unsafe.Sizeof(bucket{})` before and after the
  histogram fields.

Evidence that would invalidate this decision:

- steady hot-path updates allocate.
- CPU cost is high enough to threaten ingest latency.
- retained heap growth is unacceptable at expected fine/coarse bucket counts.
- operator diagnostics show p50 is misleading or too coarse for useful
  evaluation.

## Rollout and Reversal

- Rollout plan:
  - Ship with `distribution_statistic_mode: shadow`.
  - Document `SET DIAG PATHP50` as a diagnostic comparison view only.
- Backward compatibility impact:
  - Existing `SET DIAG PATH`, glyphs, PATH filters, and spot tail columns keep
    their current behavior.
- Reversal plan:
  - Set `distribution_statistic_mode: off` to skip histogram work.
  - Remove the `PATHP50` command and histogram fields in a later binary if the
    diagnostic does not prove useful.

## References

- Issue(s): none
- PR(s): pending
- Commit(s): pending
- Related ADR(s): ADR-0084, ADR-0085, ADR-0095, ADR-0096, ADR-0123
- Troubleshooting Record(s): none
- Docs: `README.md`, `docs/OPERATOR_GUIDE.md`,
  `pathreliability/README.md`, `data/config/path_reliability.yaml`
