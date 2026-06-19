# Support Card: Path Reliability Troubleshooting

## Match

Use when a telnet user or node operator asks about blank or weak path glyphs,
path thresholds, `SET PATHSAMPLES`, `SET DIAG PATH`, receiver diversity, H3
tables, grid handling, noise penalties, or the `SHOW PROP` propagation outlook.

## First Safe Check

Treat threshold edits as a last step. First identify the observed symptom and
inspect user-visible diagnostics:

- confirm the user's grid with `SET GRID`
- inspect `SET PATHSAMPLES`
- use `SET DIAG PATH` when detailed path evidence is needed
- use `SHOW PROP <call|prefix|grid> [band] [mode]` when the question is "when
  can I work this path?"
- check effective path YAML and H3 table availability for operator-side issues

## Must Include

- A blank path glyph can mean insufficient, low-count, low-weight, stale, or
  capped evidence, not necessarily a bad path.
- `SET DIAG PATH` `w<weight>` is scalar evidence mass after decay and
  path selection. Fine/coarse blends use the larger overlapping layer for
  scalar weight rather than adding fine and coarse together; `a<age>` can
  reflect stale local fine evidence even when coarse regional evidence is
  fresher.
- `vcap` diagnostics are VOACAP closed fallback results; `valn` diagnostics
  are sparse bucket p50 aligned with current-hour VOACAP; `vup` diagnostics
  are REL-gated one-tier sparse p50 upgrades; `vop` diagnostics are REL-gated
  no-p50 open VOACAP fallbacks. The VOACAP SNR in these tags is the rounded
  bidirectional effective SNR after receive-side noise penalty, not one raw
  VOACAP direction.
- `n160c|dNN` means native 160m fallback classified an insufficient 160m result
  as `CLOSED` from civil-dark path fraction `NN`. It is a solar-darkness proxy,
  not a VOACAP SNR result. Beacon receive-only native 160m closed diagnostics
  use `bn160c|dNN`.
- `n160|dNN` means native 160m fallback filled an insufficient 160m result as
  `LOW` or `UNLIKELY` from civil-dark path fraction `NN`. It is not SNR.
  Beacon receive-only native 160m diagnostics use `bn160|dNN`.
- Insufficient sparse/no-p50 diagnostics can add compact `v*` suffixes such as
  `n0|none|vdly` or `n2|lown|vrel` to explain VOACAP state without changing the
  blank glyph: queued, delayed, inflight, unsupported band, empty/unknown band,
  invalid user/DX grid, invalid user/DX cell, missing SSN, missing current hour,
  queue full, worker not running, disabled, unavailable, REL/tier guard failure,
  or usable VOACAP that was not closed.
- `vssn` means the VOACAP fallback had no current rounded SSN generation. Check
  effective `voacap_fallback.ssn_state_path`, startup warnings about SSN state
  restore, and NOAA fetch errors before changing path thresholds. The SSN state
  file is per-node runtime state; do not share it between running processes.
  The local Overview page shows `VOACAP SSN: <integer|n/a>` for that rounded
  generation when `tview-v2` is enabled. Its Path Predictions panel also shows
  `VOACAP cache: <cache> (C) / <delay> (D) / <inflight> (I) / <queue> (Q)`
  from the existing fallback snapshot counters.
- Completed VOACAP forecast windows persist separately in the per-node Pebble
  cache at `voacap_fallback.forecast_cache_db_path`. Current restored records
  become ordinary memory cache hits and bypass `voacap_fallback.delay_seconds`;
  stale/malformed records are pruned and a missing/unavailable cache uses the
  normal warm-up delay and queue path.
- `brx` diagnostics mean the spot was marked as a beacon and path prediction
  used only the DX-to-user receive leg. Beacon VOACAP fallback diagnostics use
  `bvcap`, `bvaln`, `bvup`, or `bvop`; their SNR and REL fields are receive-leg
  values.
- VOACAP REL is treated as reliability of VOACAP's request SNR. It is not a
  direct probability that the path is HIGH, MEDIUM, LOW, or UNLIKELY.
- Runtime VOACAP fallback decks select Method 20 below 7000 km and Method 30 at
  and above 7000 km from the same Maidenhead grid-center endpoints used in the
  VOACAP circuit. Cached records still reuse the existing fine path-cell
  granularity, so near-threshold method reuse follows the res-2 cache boundary.
- Closed fallback spots can be filtered with `PASS/REJECT PATH CLOSED`;
  existing `PASS/REJECT PATH UNLIKELY` filters still include them for
  compatibility. Native 160m `CLOSED` is a low-darkness solar proxy; VOACAP
  `CLOSED` is an SNR threshold result.
- `Path predictions (5m)` counts final emitted `voacap_closed`,
  `voacap_aligned`, `voacap_sparse_upgrade`, `voacap_open`,
  `native160_closed`, `native160_low`, and `native160_unlikely` glyphs.
  Beacon final counters use `beacon_rx`, `beacon_rx_insufficient`,
  `beacon_rx_<reason>`, and `beacon_rx_voacap_*`.
  `VOACAP fallback (5m)` explains fallback stages such as queued, cache hit, no
  current hour, open without sparse p50, class mismatch, REL missing, REL below
  floor, and multi-tier sparse-upgrade candidates. Closed fallback stages split
  no-p50 from sparse-p50 cases and report the sparse p50 class when VOACAP
  closed overrides observed sparse evidence.
- `Sparse p50 VOACAP (5m)` focuses on the blank/sparse population and splits it
  by no-p50 versus very-low-count p50, cache/work state, invalid-request
  reason, closed/open/REL outcome, beacon RX-only provenance, and non-beacon
  provenance. It is diagnostic only.
- `Native 160m fallback (5m)` focuses on insufficient 160m candidates and
  splits candidates, emissions, CLOSED/LOW/UNLIKELY, not-dark, unknown,
  display-disabled, `dark_le_closed`, and fixed civil-darkness buckets.
- `VOACAP p50 compare (5m)` is an opportunistic cache-only comparison for
  sufficient p50 predictions. Cache misses do not run VOACAP; cache hits report
  class agreement, stronger/weaker effective SNR, closed-VOACAP versus p50
  class, and SNR-delta buckets.
- `SHOW PROP` defaults omitted mode to CW. `EFF`, `RX`, and `TX` are
  mode-specific glyphs for the merged bidirectional path, target-to-user leg
  after `SET NOISE`, and user-to-target leg. `REL` is the configured class for
  the merged path. It only shows rows whose `REL` prediction is `HIGH`,
  `MEDIUM`, or `LOW`; it does not show p50.
- Empty or partial single-band `SHOW PROP` cache results enqueue a refresh
  through the existing VOACAP fallback worker and may wait briefly. All-band
  requests show cached rows while refreshing missing or partial bands in the
  background.
- Thresholds mix operator policy and algorithm calibration; do not retune them
  as the first normal troubleshooting step.
- Effective YAML matters before suggesting config edits.

## Must Avoid

- Do not call a blank glyph a bad path.
- Do not call a `valn` glyph a pure VOACAP forecast; it requires sparse bucket
  p50 and VOACAP to agree.
- Do not call `vup` or `vop` a VOACAP probability result; REL is a gate on the
  request-SNR forecast.
- Do not tell users `SHOW PROP` always returns immediately or always waits for
  every band; single-band requests may wait briefly, while all-band requests do
  not wait for every missing band.
- Do not recommend changing thresholds before confirming symptom, diagnostics,
  and effective YAML.

## Sources

- `customgpt/troubleshooting-index.md`
- `README.md`
- `docs/OPERATOR_GUIDE.md`
- `pathreliability/README.md`
- `data/config/PATH_PREDICTIONS.md`
- `data/config/README.md`
