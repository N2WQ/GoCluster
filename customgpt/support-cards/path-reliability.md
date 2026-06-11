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
- Insufficient sparse/no-p50 diagnostics can add compact `v*` suffixes such as
  `n0|none|vdly` or `n2|lown|vrel` to explain VOACAP state without changing the
  blank glyph: queued, delayed, inflight, invalid request, missing SSN, missing
  current hour, queue full, worker not running, disabled, unavailable, REL/tier
  guard failure, or usable VOACAP that was not closed.
- `brx` diagnostics mean the spot was marked as a beacon and path prediction
  used only the DX-to-user receive leg. Beacon VOACAP fallback diagnostics use
  `bvcap`, `bvaln`, `bvup`, or `bvop`; their SNR and REL fields are receive-leg
  values.
- VOACAP REL is treated as reliability of VOACAP's request SNR. It is not a
  direct probability that the path is HIGH, MEDIUM, LOW, or UNLIKELY.
- Closed fallback spots can be filtered with `PASS/REJECT PATH CLOSED`;
  existing `PASS/REJECT PATH UNLIKELY` filters still include them for
  compatibility.
- `Path predictions (5m)` counts final emitted `voacap_closed`,
  `voacap_aligned`, `voacap_sparse_upgrade`, and `voacap_open` glyphs.
  Beacon final counters use `beacon_rx`, `beacon_rx_insufficient`,
  `beacon_rx_<reason>`, and `beacon_rx_voacap_*`.
  `VOACAP fallback (5m)` explains fallback stages such as queued, cache hit, no
  current hour, open without sparse p50, class mismatch, REL missing, REL below
  floor, and multi-tier sparse-upgrade candidates. Closed fallback stages split
  no-p50 from sparse-p50 cases and report the sparse p50 class when VOACAP
  closed overrides observed sparse evidence.
- `Sparse p50 VOACAP (5m)` focuses on the blank/sparse population and splits it
  by no-p50 versus very-low-count p50, cache/work state, closed/open/REL
  outcome, beacon RX-only provenance, and non-beacon provenance. It is
  diagnostic only.
- `VOACAP p50 compare (5m)` is an opportunistic cache-only comparison for
  sufficient p50 predictions. Cache misses do not run VOACAP; cache hits report
  class agreement, stronger/weaker effective SNR, closed-VOACAP versus p50
  class, and SNR-delta buckets.
- `SHOW PROP` defaults omitted mode to CW. `EFF`, `RX`, and `TX` are
  mode-specific glyphs for the merged bidirectional path, target-to-user leg
  after `SET NOISE`, and user-to-target leg. `REL` is the configured class for
  the merged path. It does not show p50.
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
