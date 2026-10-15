# Support Card: Path Reliability Troubleshooting

## Match

Use when a telnet user or node operator asks about blank or weak path glyphs,
path thresholds, `SET PATHSAMPLES`, `SET DIAG PATH`, receiver diversity, H3
tables, grid handling, noise penalties, path CPU/latency, or the `SHOW PROP`
propagation outlook.

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

- Minute stale purging and hourly path-map compaction retain their existing
  cadence. Compaction also handles contraction spread across small shards,
  preserving live buckets and prediction data. A lower retained Go heap does
  not imply immediate Windows working-set reduction. See ADR-0305.

- If path learning is missing reports, inspect `Path admission (5m)` counters:
  `future_clamped` counts observations normalized to arrival within the inclusive
  120-second future tolerance; `rejected_time` includes larger future offsets or
  missing arrival; `rejected_weight` counts nonfinite/nonpositive original
  weights. These are once-per-report counts, not fine/coarse write totals.
  Path-only dashboard `T` means timestamp drops. Path-report metrics use the
  accepted effective observation, while the shared spot timestamp is unchanged.
  Ordinary late reports can still rewind the separate hourly metrics window.
  See `docs/decisions/ADR-0300-path-observation-time-accounting.md` and
  `docs/troubleshooting/TSR-0092-path-timestamp-accounting.md` for the staged
  accounting contract and verified implementation status.
- Read the overview's UTC as-of date and unavailable/cached marker before
  interpreting path counts. `Path buckets`, `Path weight dist`, and `Path ge10
  variance` logs carry `available`, `has_asof`, and `asof_unix`; epoch zero with
  `has_asof=true` is valid. Modern propagation summaries omit unavailable or
  incomplete pairs and group valid pairs by sample time. Known zero bands are
  included. Historical logs retain their legacy log-time/zero assumptions.
  Offline repeated references replace each band only with a complete pair from
  one emission, in input order. Incomplete repeats preserve prior valid bands;
  complete known zeros replace them. Variance follows the same preservation
  rule and rejects both `max>0 deg=1` and `max=0 deg=0` as contradictory.
  Repeated or older samples do not extend the live variance window. Raw counts
  and newest-observation freshness include validated reports excluded from
  usable mass, so neither is a measurement of RF or QSO accuracy.
  Report JSON `ge10_variance_available=false` means unknown variance; its zero
  placeholders must not be interpreted as measured zero or degenerate evidence.
- A blank path glyph can mean insufficient, low-count, low-weight, stale, or
  capped evidence, not necessarily a bad path.
- The observed p50 uses exact comparison of the stored float32 bin masses.
  A tiny positive retained bin can distinguish a median crossing from a true
  50/50 tie. Only a true tie averages occupied representatives. Earlier decay,
  shift and merge rounding remain approximate. See ADR-0301 and TSR-0093 for
  the precision repair and its actual validation status.
- For higher path CPU, compare matched build/traffic profiles before assigning
  the increase to p50. The v6 speed experiment passed exactness and allocation
  checks but exceeded sparse/fallback regression limits, so the original exact
  median was initially restored. The owner subsequently requested restoration
  of the same v6 candidate on `path_relia` for long-running cluster profiling.
  Its dense speedup and synthetic profile shares do not establish production
  savings. Compare total cluster CPU for equivalent completed work; individual
  median timings are diagnostic evidence. See TSR-0094 for the measured limits.
- `SET DIAG PATH` `w<weight>` is scalar evidence mass after decay and
  path selection. Fine and coarse are selected at one common resolution,
  never added. `a<age>` reflects selected populations; excluded stale local
  evidence does not age a fresh selected regional population.
- `vcap` diagnostics are VOACAP closed fallback results; `valn` diagnostics
  are sparse bucket p50 aligned with current-hour VOACAP; `vup` diagnostics
  are REL-gated one-tier sparse p50 upgrades; `vop` diagnostics are REL-gated
  no-p50 open VOACAP fallbacks. The VOACAP SNR in these tags is the rounded
  bidirectional effective SNR after receive-side noise penalty, not one raw
  VOACAP direction.
- `n160c|uD|dNN` means native 160m fallback classified an insufficient 160m
  result as `CLOSED` because the user endpoint was in daylight. `xD` means DX
  endpoint daylight and `bD` means both endpoints. This is a solar-darkness
  proxy, not a VOACAP SNR result. Beacon receive-only native 160m diagnostics
  use the same suffixes with `bn160c`.
- `n160c|dNN` means no endpoint token was needed; native 160m `CLOSED` came
  from whole-path civil-dark fraction `NN`.
- `n160|xT|dNN` means native 160m fallback classified an insufficient 160m
  result as `UNLIKELY` because the DX endpoint was in civil twilight. `uT`
  means user endpoint twilight and `bT` means both endpoints.
- `n160|dNN` means native 160m fallback filled an insufficient 160m result as
  `LOW` or `UNLIKELY` from civil-dark path fraction `NN` after both endpoints
  were dark. It is not SNR. Beacon receive-only native 160m diagnostics use
  `bn160|dNN`.
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
  The separate `VOACAP disk index` line reports cached metadata `(M)`, retention `(R)` and
  eligibility `(E)` index counts, with `ready` and `dirty`. Successful maintenance
  makes these counts equal and bounded by `max_cache_entries`. `ready=false`
  means uncertified bookkeeping, so zero counts do not prove an empty database.
  `dirty=true` means a failed/uncertain disk operation needs recovery; inspect
  forecast-cache warnings. `VOACAP disk index: n/a` means no store is attached. Snapshot reads
  do not wait behind disk I/O. See ADR-0304.
- Runtime VOACAP execution is Windows-only. On Linux and other non-Windows
  builds, startup logs that the VOACAP fallback is disabled; `SHOW PROP`
  reports that VOACAP fallback is disabled, sparse/no-p50 diagnostics can show
  `vdis`, and the Overview VOACAP fields remain `n/a`.
- Completed VOACAP forecast windows persist separately in the per-node Pebble
  cache at `voacap_fallback.forecast_cache_db_path`. Current restored records
  become ordinary memory cache hits and bypass `voacap_fallback.delay_seconds`;
  stale/malformed records are pruned and a missing/unavailable cache uses the
  normal warm-up delay and queue path.
  Persisted capacity pruning retains newest storage timestamps, then lowest
  canonical cache keys for ties. Equal-time refreshes have no recency advantage;
  this does not change live-memory eviction.
  Ordinary completion batches its replacement and actual removals atomically;
  it maintains bounded indexes without scanning unrelated forecasts. Startup or
  dirty-state recovery still scans disk with bounded candidates/prune batches.
  Memory publication and waiter notification precede warning-only persistence;
  shutdown waits for persistence. No schema migration or firewall change is
  required.
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
  display-disabled, endpoint daylight/twilight outcomes, `dark_le_closed`, and
  fixed civil-darkness buckets.
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

- For S4 performance questions, consult `docs/path-reliability-first-wave-execution.md`
  and TSR-0097. Original Serial/off and repaired Serial/enforce tail guards
  failed. Same-binary enforce diagnostics also exceeded repeatability bounds;
  these do not prove production savings. Approved v4 accepts the unresolved
  S4 performance risk for development continuation, retaining safety checks
  and deferring S4 performance assessment until after S5-S7. ADR-0306 records
  that exception; failed results retain their historical status. Local
  qualification uses a separate frozen measurement protocol at the existing
  stable Windows executable paths, with no firewall changes.
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

- `docs/troubleshooting/TSR-0098-benchmark-completion-evidence.md`: S5's local
  measurement-checker repair, preserved first observation and timing limits.
- `pskreporter/README.md`: present numeric zero reports follow their configured
  normal or path-only route; missing/null reports remain excluded. Path-only
  reports contribute to learning without entering archive, telnet or peer output.
- `docs/path-reliability-first-wave-execution.md`
- `docs/troubleshooting/TSR-0097-path-sharding-measurement-clock.md`
- `customgpt/troubleshooting-index.md`
- `README.md`
- `docs/OPERATOR_GUIDE.md`
- `pathreliability/README.md`
- `data/config/PATH_PREDICTIONS.md`
- `data/config/README.md`

## Physical evidence and regional glyphs

Observed glyphs remain regional SNR summaries under the user's receive noise.
They do not claim a QSO probability. Ordinary paths use fine evidence only
when both distinct fresh directions reach the fine-only signal threshold;
otherwise available coarse directions are selected together. A fresh fine hint
is allowed only after all coarse candidates are unavailable. No fine/coarse
histograms are added. Shared regional evidence and receiver identities count
once; shared ordinary signal gets one single-direction discount, beacon signal
none. Required selected numerical failure is insufficient, without a layer
retry. See ADR-0308 and pathreliability/README.md for exact boundaries.
