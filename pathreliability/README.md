# Path Reliability

This directory owns path-reliability scoring, H3 grid mapping, decaying bucket storage, and the final class/glyph mapping used by telnet path display and PATH filters.

## Operator Summary

The main landing page [`../README.md`](../README.md) explains path reliability in operator language. This file records the implementation details behind that explanation.

## Data Sources

The predictor accepts only these ingest modes:

- `FT8`
- `FT4`
- `CW`
- `RTTY`
- `PSK`
- `WSPR`

`USB` and `LSB` are display-only. They can be classified with their own thresholds, but `BucketForIngest(...)` does not ingest them into path buckets.

Path-only PSKReporter modes, such as `WSPR`, can update the predictor without entering the normal dedup, archive, telnet, or peer paths.

## Storage Maintenance

The store retains per-shard growth peaks. Existing minute stale purging and
hourly compaction can rebuild map backing storage after substantial contraction,
including contraction spread across individually small shards. Maintenance
rechecks each shard under its lock and preserves the live bucket pointers.
It does not change active predictions or bucket contents. See ADR-0305.

### Local Performance Evidence

The owner accepted S4's unresolved performance risk under Approved v4, and its
retained safety checks passed before the local checkpoint. ADR-0306 records the
full-key selector and exception. The [first-wave execution record](../docs/path-reliability-first-wave-execution.md)
separates accepted slices, failed comparisons and diagnostic results. Do not use
contention-profile improvements to claim that a serial tail guard passed or
that production CPU fell.

Failed timing observations remain failed, and unrun comparisons remain unrun.
S4's remaining timing campaign and final matched profiles are deferred until
assessment after S5-S7. Correctness, race, memory, lifecycle and hourly-cadence
checks remain required; S5-S7 retain their own performance/resource gates.

The approved S4 measurement repair uses `testsupport.RunFixedBenchmark` with
explicit `-test.benchtime=1x`, a live outer test deadline and one complete fixed
workload. The supplied clock uses a primed `testing.B.Elapsed` domain. All role,
queue and maintenance owners must cancel and join before that domain ends;
benchmark cleanup cancellation cannot replace cancel-before-join defers.
Ordinary correctness tests and the real-cadence soak retain their original
clock domain. Never combine samples from these measurement protocols.

Use the existing stable native Windows test paths and shared runner lock for
these opt-in runs. No firewall rule change is part of qualification. The
measurement mechanism and remaining uncertainty are recorded in
[TSR-0097](../docs/troubleshooting/TSR-0097-path-sharding-measurement-clock.md).

## Grid To H3 Mapping

The implementation converts a Maidenhead grid to the center of the represented area:

- 4-character grid: center of the `2 x 1` degree square
- 6-character grid: center of the finer subsquare

That point is mapped into:

- H3 resolution 2 for fine cells
- H3 resolution 1 for coarse cells

The runtime uses stable `uint16` proxy IDs built from the precomputed H3
tables in [`../data/h3`](../data/h3). If a spot's grid is invalid, the cell
becomes invalid and prediction can fall back to insufficient. When path
reliability is enabled, missing, malformed, or wrong-sized H3 mapping tables
are startup failures reported in the system log because all path predictions
depend on those cells.

## FT8-Equivalent Conversion

Every accepted report is normalized to an FT8-equivalent dB value before entering the store.

The shipped config in [`../data/config/path_reliability.yaml`](../data/config/path_reliability.yaml) currently sets:

- `FT4: 0`
- `CW: -7`
- `RTTY: -7`
- `PSK: -19`
- `WSPR: 0`

After that conversion, the predictor stores the pre-clamp FT8-equivalent dB
value in fixed histogram bins. Active glyphs and PATH filters use the selected
p50 bin, so out-of-range observations remain visible as underflow or overflow
bins instead of being collapsed into one aggregate value.

## Report Timestamp Admission

Update callers provide `ObservationTimes` with separate source observation and
path-ingestion arrival. Arrival is required; a missing observation uses arrival.
Future observations up to 120 seconds ahead, inclusively, are clamped to arrival.
Larger future offsets are rejected from path learning and report metrics while
other applicable spot processing continues. The comparison uses full timestamp
precision; bucket accounting retains Unix-second resolution. `Spot.Time` is not
rewritten. Original report weights must be finite and positive before a beacon
weight limit is applied.

`ReportAdmission.Accepted()` describes input validation, independently of bucket
eligibility or receiver-cap admission. Its effective observation is authoritative
for path-report metrics. `Path admission (5m)` reports bounded interval counters
`future_clamped`, `rejected_time`, and `rejected_weight`; a report updating both
fine and coarse buckets is counted once. The path-only dashboard's `T` counter
records timestamp drops. Hourly report-window rewinding on ordinary late reports
remains a separate, deferred issue.

The accepted accounting contract and staged implementation are recorded in
[`ADR-0300`](../docs/decisions/ADR-0300-path-observation-time-accounting.md) and
[`TSR-0092`](../docs/troubleshooting/TSR-0092-path-timestamp-accounting.md).

## Bucket Storage

The predictor stores directional buckets keyed by:

- receiver cell
- sender cell
- band
- fine or coarse resolution

Each bucket stores:

- accumulated weight
- raw observation count
- recent raw usable report units, aged independently of signal mass
- capped receiver-attributed weight and effective observation count
- one fixed SNR histogram for the active p50 scoring lane
- fixed receiver contribution slots
- monotonic accounting reference and separate newest effective observation time

Updates age existing state to `R = max(previous reference, arrival)` using the
band's half-life. Incoming weight and effective count are aged from the validated
observation to that same R, including when the report arrived late. Advancing R
does not advance the newest observation.
The raw observation count is not decayed; it is a bounded diagnostic count of
eligible valid reports, including cap-rejected and numerically unusable reports.
Recent raw usable support increments only after raw numerical admission. One
fresh usable report adds one unit even with signal weight 0.01. Numerical
rejection adds zero; later cap rejection retains that raw unit. A 25% admitted
report adds one raw unit and 0.25 admitted units. Aging reduces both quantities.
Representable increments still count when histogram addition rounds them away.
Capped
receiver-attributed count is a decayed effective count, aligned with capped
weight in enforce mode, so old receiver evidence fades and makes room for newer
evidence.

Receiver contribution caps are bucket-owned and bounded by the bucket itself:
fine buckets track up to `receiver_fine_slots` identities and coarse buckets
track up to `receiver_coarse_slots` identities. The checked-in values are `6`
and `12`. One receiver can add at most `receiver_max_effective_count` decayed
effective observations and `receiver_max_effective_weight: 8.0` decayed
effective weight to a bucket's capped trust evidence. When a new report has
only partial remaining count or weight capacity, capped weight receives the
same fraction, and the active histogram receives that fraction when `enforce`
mode makes capped evidence the active p50 lane. When the slot set is full, the
weakest/oldest slot is reused; this is an approximation, not an unbounded exact
unique-receiver set.

Admission uses one fraction against the aged receiver's remaining count and
weight room: `min(1, count room / incoming aged count, weight room / incoming
aged weight)`. The same fraction scales capped weight, effective count, and the
active capped histogram. Arrival-order dependence remains an explicit
approximation.

Weighted incoming mass and cap-scaled mass must be positive and finite at
float32 histogram precision, with safe prospective writes. Representable
increments can be admitted even if addition rounds them away. Unusable raw
mass cannot allocate or replace a receiver; count and newest observation remain.
Usable raw mass survives cap-only underflow. Complete weighted products are
formed before rounding; an underflowed unit factor alone does not prove that
its weighted mass is absent.

`receiver_contribution_mode` controls how capped evidence is used:

- `shadow`: use raw selected evidence for active p50 glyphs and PATH filters,
  but expose whether the configured cap would have blocked the prediction.
- `enforce`: use recent cap-admitted reports for the support floor, plus capped
  receiver diversity and capped weight for receiver-cap trust gates.
- `off`: disable capped tracking and use raw evidence only.

The global `min_observation_count` is a recent report-support floor: raw usable
units in off/shadow, cap-admitted units in enforce. Compare fractional units
before directional display weights or the single-direction discount, without
integer rounding, epsilon promotion or lifetime-count fallback. Twenty-one
fresh unit reports meet 21; after one half-life 10.5 no longer do. Zero means zero.
Admitted support is explicitly unavailable while cap tracking is off.

Receiver concentration is checked by a separate derived gate:
the selected capped evidence must include at least
`ceil(min_observation_count / receiver_max_effective_count)` live attributed
receiver identities, capped by the selected physical populations' capacity.
Each tracked hash needs at least one accepted aged report unit. Sum the same
hash across distinct selected populations before qualification: 0.6+0.6 counts
as one receiver; shared 0.6 counts as none. Do not infer diversity from weight.
Slot rotation remains an approximation; report volume is not independent
samples or calibrated confidence. This keeps the
sample floor and receiver-diversity floor debuggable as separate causes.

The store retains one fixed SNR histogram per bucket for the active p50 lane.
In `off` and `shadow` mode that lane is raw selected evidence. In `enforce`
mode that lane is capped receiver-attributed evidence. Updates touch only the
selected bin unless time has advanced, and p50 scans the fixed array. Raw and
capped counts/weights remain separately retained for sample floors,
receiver-diversity gates, and `SET DIAG PATH`; the inactive histogram lane is
not retained during normal operation.

The SNR bins are fixed in code: `< -24`, one-dB bins from `-24..-23` through
`23..24`, and `>= 24`. Finite bins use their midpoint as the displayed p50
representative; underflow displays as `-24` and overflow displays as `24`.
When an exact 50/50 median boundary falls between two non-empty bins, p50 uses
the average of the two bin representatives so balanced bimodal evidence maps to
the typical middle rather than always choosing the weaker bin.

Sampling captures candidate values and receiver slots under their shard locks,
then evaluates all candidates at `T = max(query, captured references)`: four for
normal prediction, two for beacon receive-only prediction and public lookup.
Mass, effective counts, histogram decay, diversity, freshness, and expiry use T.
This ensures time coherence without an atomic event snapshot across buckets.
`Result.HasObservedSamplingTime` and `ObservedSamplingUnix` expose T;
`ObservedDistributionAvailable` describes the observed distribution independently
of any later synthetic fallback. Epoch zero is a valid time. If a needed
distribution becomes unsafe or entirely loses representable bins, the whole
observed median is unavailable; diagnostics remain available.

Directional bins retain their float32 materialization boundaries. Production
computes the final median once, before output count/weight/receiver floors.
The restored v6 candidate validates every bin before selection, compares one
or two occupied bins directly, and uses float64 sums for larger histograms only
when `24 + D + ceil(log2(n)) <= 53` certifies exact totals and prefixes. Here
`n` counts positive bins and `D` is their binary32 quantum-shift span, with
shift `max(E-1, 0)` for encoded exponent `E`. Other distributions use exact
integer arithmetic: five stack-local 64-bit words cover the full binary32
range. Selection compares twice the cumulative mass with the total; only true
equality interpolates between occupied representatives. Test-only distribution
probes also calculate individual quantiles. Earlier decay, shift and merge
rounding remain approximations.

`BenchmarkMedianSpeedPrimary`, `BenchmarkMedianSpeedFallbackMix` and
`BenchmarkMedianSpeedRuntime` exercise normal/beacon predictions and all
receiver modes with ingestion-backed synthetic fixtures. Preflights check final
materialized bins, rational-oracle outputs, allocations and retained owners.
Their input frequencies do not represent measured production traffic.
Primary/mix runs require a fixed iteration count divisible by their 12/30-call
cycles; use `-test.benchtime=600000x` and `-test.cpu=4` with a saved native test
binary. Default time-based calibration does not preserve those call fractions.
[TSR-0094](../docs/troubleshooting/TSR-0094-path-median-speed-retention.md)
records the failed v6 retention gate and its comparison method. The owner
subsequently requested restoration of that same candidate on `path_relia` for
long-running whole-cluster profiling. Restoration does not establish a cluster
CPU improvement.

Storewide reports discover their common reference during a provisional scan.
They publish it only if all references fit; otherwise they retry once at the
latest encountered reference. Continued advancement makes the attempt
unavailable. Cached counts preserve their prior date and values on failure;
`StatsSnapshot` copies totals and bands from one publication. Scratch and cache
storage are bounded by band count, with no per-bucket scan history.

The overview reads total and band counts from that one publication and shows
its UTC date. Before a publication exists it shows unavailable; a failed refresh
keeps the earlier dated numbers visibly marked unavailable/cached. Five-minute
`Path buckets`, `Path weight dist`, and `Path ge10 variance` lines carry
`available`, `has_asof`, and `asof_unix`. Epoch zero is valid when `has_asof=true`.
Available weight snapshots include every configured band, including known zeros.
The logger's one-hour variance window accepts only advancing available sample
references; repeated, older, or unavailable samples add no observation.

The offline propagation report requires complete count/weight pairs from one
emission at the same available reference. It groups them by that reference's
UTC date/hour and deduplicates equal references per band, rather than treating
log time as a new observation. The last complete pair in input encounter order
replaces that band's prior pair; missing, malformed or unavailable repeats
preserve earlier valid evidence. Complete known zeros replace prior values.
Variance follows the same per-band rule and requires `deg=1` exactly when
`max=0`; contradictory flags are unavailable evidence.
Historical metadata-free logs retain their emission-time and zero-weight
assumptions. Observed sampling metadata survives fallback replacement, while
request/reuse, forecast, and solar clocks retain their existing purposes.
Report JSON carries `ge10_variance_available`: when false, its variance bounds
are placeholders and do not support a zero-variance or degeneracy conclusion.

Five-minute propagation logs split insufficient path prediction outcomes into
`no_sample`, `low_count`, `low_receiver`, `low_weight`, and `stale`.
`low_count` maps to the recent usable/admitted report floor; `low_receiver` maps to
the receiver-diversity gate in enforce evaluation; `low_weight`
maps to the decayed effective weight floor. These aggregate lines are written
to `logging.propagation.dir`. VOACAP fallback outcomes are counted separately
as `voacap_closed`, `voacap_aligned`, `voacap_sparse_upgrade`, and
`voacap_open`. Native 160m darkness fallback emissions are counted as
`native160_closed`, `native160_low`, and `native160_unlikely`. Beacon paths also emit additive `beacon_rx`,
`beacon_rx_insufficient`, `beacon_rx_*` reason counters, and
`beacon_rx_voacap_*` fallback counters when the spot is marked `IsBeacon`.
When the fallback is active, a separate `VOACAP fallback (5m)` line reports
stage counters such as `queued`, `success`, `cache_hit`, `no_current_hour`,
`delay_wait`, `queue_full`, `closed`, `closed_no_p50`,
`closed_with_sparse_p50`, `closed_with_sparse_p50_class_*`, `aligned`,
`open_no_p50`, `class_mismatch`, `sparse_upgrade`, `open_no_p50_rel`,
`rel_missing`, `rel_below_floor`, and `rel_multi_tier`. The path-prediction
counters are final emitted glyphs; the fallback line explains why cached VOACAP
work did or did not emit a glyph. The closed sparse-p50 counters show when
VOACAP emitted a closed glyph despite sparse observed p50 evidence and which
p50 class that evidence would have mapped to.
A separate `VOACAP p50 compare (5m)` line may appear for sufficient p50
predictions when an existing current-hour VOACAP cache record is present. It
reports cache hit/miss counts, class agreement, stronger/weaker effective SNR,
closed-VOACAP versus p50 class splits, and absolute SNR-delta buckets. This
comparison is cache-only and does not enqueue VOACAP work or change emitted
glyphs. Beacon RX-only p50 results do not enter this bidirectional comparison.
When sparse or no-p50 candidates are present, a separate `Sparse p50 VOACAP
(5m)` line reports the diagnostic split for those candidates without changing
glyph decisions: `no_p50`, `very_low_count`, `beacon_rx`, `non_beacon`,
`cache_miss_total`, `cache_hit`, `queued`, `delayed`, `inflight`,
`invalid_request`, `invalid_unsupported_band`, `invalid_empty_unknown_band`,
`invalid_user_grid`, `invalid_dx_grid`, `invalid_user_cell`, `invalid_dx_cell`,
`ssn_unavailable`, `no_current_hour`, `queue_full`, `not_running`, `disabled`,
`unavailable`, `closed`, `aligned`, `sparse_upgrade`, `open_rel_pass`,
`open_rel_fail`, `not_closed`, `rel_missing`, `rel_below_floor`, and
`rel_multi_tier`.
`sparse_p50_diagnostic_max_observation_count` only controls the diagnostic
very-low-count bucket and does not relax prediction gates or enqueue additional
VOACAP work.
When native 160m fallback evaluates insufficient 160m candidates, a separate
`Native 160m fallback (5m)` line reports candidate, emitted, class, not-dark,
unknown, display-disabled, endpoint daylight/twilight outcome counters,
`dark_le_closed`, and fixed civil-darkness threshold buckets. Native 160m
fallback is an opportunity proxy: it can emit only `CLOSED`, `LOW`, or
`UNLIKELY`, never replaces sufficient p50, and yields to usable current-hour
VOACAP. Endpoint daylight emits `CLOSED`; endpoint civil twilight emits
`UNLIKELY`; whole-path civil-dark fraction is only the secondary score after
both endpoints are dark. `CLOSED` is a solar-darkness proxy bucket, not a VOACAP
SNR result.

When `voacap_fallback.enabled` is true on Windows, insufficient bucket results
may start a delayed VOACAP lookup. Linux and other non-Windows builds log that
runtime VOACAP execution is unsupported and leave the fallback provider
disabled at startup. The lookup is nonblocking in the telnet path. Cached
VOACAP output stores one hourly record per parsed forecast hour for the
requested band. Each cached hour carries both raw directions: DX-to-user receive
and user-to-DX transmit. Lookup blends those directions with the same
`merge_receive_weight` and `merge_transmit_weight` used by p50, then subtracts
the request user's receive-side `SET NOISE` penalty from the receive leg.
Runtime fallback decks start at the current rolling UTC forecast window, and
VOACAP output hour `24` is normalized to UTC hour `0`. Runtime decks select
Method 20 below 7000 km and Method 30 at and above 7000 km from the same
Maidenhead grid-center endpoints written to the VOACAP circuit. Cached records
still reuse the existing fine path-cell granularity, so near-threshold method
reuse follows the same res-2 cache boundary as other VOACAP fallback data.
Lookup selects the record matching the current UTC hour and re-evaluates the
effective blended SNR against the request mode, so the first mode or noise
class to populate the cache does not decide later requests. If the effective
VOACAP SNR is at or below
`mode_thresholds.<mode>.closed`, the fallback can return the configured closed
glyph. Closed remains based on effective FT8-equivalent SNR50 and does not
require VOACAP REL. Otherwise, it can return a normal `HIGH`, `MEDIUM`, `LOW`,
or `UNLIKELY` glyph when sparse p50 and current-hour VOACAP map to the same
class. It can also emit a no-p50 open VOACAP glyph, or upgrade sparse p50 by
one class, only when the cached VOACAP class is open and the configured
request-SNR REL gate passes. For bidirectional forecasts, the retained REL gate
uses the lower of the receive and transmit REL values. REL is reliability of
VOACAP's configured request SNR, not a direct probability of the displayed
class. It never overrides a sufficient bucket p50 result. For PATH filters, the
closed glyph is visible as `CLOSED` and remains compatible with `UNLIKELY`:
existing `PASS/REJECT PATH UNLIKELY` filters still include closed fallback
spots, while direct `PASS/REJECT PATH CLOSED` rules target only closed fallback
spots from VOACAP or native 160m low-darkness fallback.
For beacon spots, transmit evidence is not applicable. Beacon p50 prediction
uses the DX-to-user receive leg only, applies the user's receive-noise penalty,
does not apply `reverse_hint_discount`, and uses
`beacon_min_observation_count` plus the derived receiver gate. If beacon p50 is
insufficient, VOACAP fallback classifies and REL-gates on the receive leg
instead of the bidirectional effective SNR. Display glyphs and PATH filters use
the same beacon RX-only result.
`SET DIAG PATH` may append a compact sparse VOACAP suffix to insufficient
diagnostics, such as `n0/u0|none|vdly` or `n2/u1|lown|vrel`. The suffix explains the
VOACAP state for sparse/no-p50 candidates: queued, delayed, inflight,
unsupported band, empty/unknown band, invalid user/DX grid, invalid user/DX
cell, missing SSN, no current-hour cache record, queue full, worker not running,
disabled, unavailable, REL/tier guard failure, or usable VOACAP that did not
classify the candidate closed.

The runtime SSN monitor persists only its continuity state at
`voacap_fallback.ssn_state_path`: NOAA validators, last observed raw SSN, EWMA,
and current rounded SSN generation. Missing state cold-starts the SSN monitor;
malformed state is warned about and ignored so path reliability can still start.
Do not share the same state file between two running cluster processes.
The local Overview page displays that current rounded generation as
`VOACAP SSN: <integer|n/a>` when the interactive console is enabled.
The Overview Path Predictions panel also displays
`VOACAP cache: <cache> (C) / <delay> (D) / <inflight> (I) / <queue> (Q)` using
the existing in-memory fallback snapshot counters. A separate `VOACAP disk index`
line reports persisted-cache bookkeeping: metadata entries `(M)`, retention-heap entries `(R)`
and eligibility-heap entries `(E)`, plus `ready` and `dirty`. The three counts
converge to the same value, bounded by `max_cache_entries`, after a successful
operation. These are cached index counts, not a scan of disk or distinct paths.
`ready=false` means the database has not been certified; zero counts then do not
mean the database is empty. `dirty=true` means a failed or uncertain operation
requires recovery. `VOACAP disk index: n/a` means no store is attached. Reading the overview
does not wait for persistence.
On non-Windows builds those Overview VOACAP values stay `n/a` because the
fallback provider is not started.
Completed VOACAP hourly forecast-window records persist separately in the
per-node Pebble DB at `voacap_fallback.forecast_cache_db_path`. Startup hydrates
only records that still match the current cache schema, model generation,
rounded SSN generation, forecast month, TTL, and current UTC hour. Hydrated
records become ordinary memory cache hits and bypass
`voacap_fallback.delay_seconds`; stale or malformed records are pruned and a
missing/unavailable cache cold-starts normal delay/queue behavior.
When the persisted cache exceeds its capacity, it retains the newest storage
timestamps. Equal timestamps retain the lowest canonical cache keys; refreshing
an entry at the same timestamp gives it no extra retention priority. This
ordering applies to disk pruning and restart hydration, not live-memory eviction.
Ordinary completions maintain bounded metadata and two coupled indexed heaps;
they do not decode or sort unrelated persisted forecasts. Startup, recovery,
configuration/generation changes and backward clock movement rebuild from disk
with capacity-bounded candidates and 256-key prune batches. Replacement and its
ordinary removals share one atomic batch. Failed or uncertain disk operations discard
index trust; the next operation repairs from actual disk contents. Live memory
publication and waiter notification still precede warning-only persistence,
and shutdown waits for persistence before closing the database. See ADR-0304.

`SHOW PROP <call|prefix|grid> [band] [mode]` exposes those cached hourly
VOACAP records directly as a point-to-point outlook from the user's grid to the
target when the fallback provider is running. Omitted mode defaults to CW.
Empty or partial single-band command
lookups enqueue an immediate refresh through the existing fallback worker and
wait up to `voacap_fallback.show_prop_wait_milliseconds`; all-band requests
show cached rows immediately while refreshing missing or partial bands in the
background. Rows run from the current UTC hour through
`voacap_fallback.forecast_hours`, but only rows whose `REL` prediction is
`HIGH`, `MEDIUM`, or `LOW` are displayed. The displayed `RX` glyph is
DX-to-user after the requesting user's receive-noise penalty, `TX` is
user-to-DX, `EFF` is the configured receive/transmit merge glyph, and `REL` is
the configured path class for the requested mode and merged path. Bucket p50
remains internal to the live glyph decision and is not shown in this command.

The shipped config currently uses:

- half-lives ranging from `600s` on `160m` and `80m` down to `240s` on `12m`, `10m`, and `6m`
- `stale_after_half_life_multiplier: 3`
- `stale_after_seconds: 1800` as the fallback purge window
- `max_prediction_age_half_life_multiplier: 1.5` as a display/filter freshness gate
- `receiver_contribution_mode: enforce`
- `receiver_fine_slots: 6`
- `receiver_coarse_slots: 12`
- `receiver_max_effective_count: 8` decayed effective observations per receiver
- `receiver_max_effective_weight: 8.0`

## Sample Selection And Merge

Prediction uses two directions:

- receive sample: DX to user
- transmit sample: user to DX

A query captures each unique physical bucket once at one common accounting
reference, including references ahead of the request clock. Ordinary paths use
fine resolution only when both distinct directions are present, fresh, and
meet `fine_only_weight` (20, inclusive) in active signal weight. Otherwise they
use available coarse directions. A missing coarse direction is not filled with
fine evidence. Only when all coarse evidence is missing, expired or prediction-
stale may fresh fine evidence supply a hint below that threshold.

Fine and coarse populations are never added together. Their H3 mappings are
independent, so containment cannot be assumed. `min_fine_weight` remains a
required positive configuration value and lower bound for normalization of
`fine_only_weight`; it no longer directly shapes a blend.

Freshness is checked before selection using the common reference and newest
observation: age must be at most
`ceil(band_half_life * max_prediction_age_half_life_multiplier)`. Equality is
fresh (540 seconds on shipped 20m; 541 is stale). Zero disables this prediction
gate, while store aging and expiry remain active. Unselected stale evidence
cannot spoil a fresh chosen layer. Expired counts and signal mass are excluded; captured references still
participate in the common sampling time.

Availability is separate from distribution usability. Fresh count-only or
corrupt coarse evidence blocks a fine retry. A required selected distribution
that is empty, corrupt or numerically unusable produces INSUFFICIENT; it cannot
silently become a surviving-direction median. Excluded bad evidence does not
poison the selected layer.

A shared physical population is counted once as a receive hint: receive noise
is applied once, and ordinary signal weight and histogram mass receive the
single-direction discount once. Beacon receive-only evidence has no such
discount. Receiver identities shared by distinct selected populations count
once; configured capacity remains per distinct contributing bucket. This
regional SNR summary makes no QSO-probability or independent-sample claim.

The shipped config currently uses:

- `min_effective_weight: 0.5`
- `min_observation_count: 21`
- `beacon_min_observation_count: 11`
- `min_fine_weight: 5`
- `fine_only_weight: 20`
- `reverse_hint_discount: 0.5`
- `merge_receive_weight: 0.6`
- `merge_transmit_weight: 0.4`

If only one direction exists for a non-beacon path, the predictor still uses it,
but discounts the effective weight with `reverse_hint_discount`. Beacon paths
are the exception: a missing transmit direction is not weak evidence, so the
receive leg is used without that discount.

Telnet users can set a stricter personal observation floor with
`SET PATHSAMPLES <count>`. That setting is applied as
`max(min_observation_count, user setting)` and cannot lower the cluster default.
This floor uses recent usable/admitted report units for the configured mode.
In enforce mode, receiver concentration is evaluated separately from this user
sample floor.
For beacon spots, the session floor is
`max(beacon_min_observation_count, user setting)`, while receiver diversity is
derived from the configured beacon floor.

The receive-side noise table is resolved only by `SET NOISE` class. The same
location penalty applies on every band and is subtracted from DX-to-user p50
path evidence and the DX-to-user VOACAP fallback leg at prediction time.

| Class | Quiet | Rural | Suburban | Urban | Industrial |
| --- | ---: | ---: | ---: | ---: | ---: |
| Penalty dB | 0 | 4 | 12 | 17 | 20 |

## Class Mapping

Prediction returns either:

- `HIGH`
- `MEDIUM`
- `LOW`
- `UNLIKELY`
- `INSUFFICIENT`

`INSUFFICIENT` is returned when there is no usable sample, selected evidence is
too old for the freshness gate, selected recent report support is below
`min_observation_count`, receiver diversity is too low under enforce/candidate
cap evaluation, or the merged effective weight stays below
`min_effective_weight`.

The shipped glyph symbols are:

- `>` for `HIGH`
- `=` for `MEDIUM`
- `<` for `LOW`
- `-` for `UNLIKELY`
- space for `INSUFFICIENT`
- configured `glyph_symbols.closed` for `CLOSED` fallback when VOACAP predicts
  closed SNR or native 160m fallback emits the low-darkness solar proxy bucket

The shipped threshold table is:

| Mode | High | Medium | Low | Unlikely | Closed |
| --- | ---: | ---: | ---: | ---: | ---: |
| FT8 | -13 | -17 | -21 | -24 | -29 |
| FT4 | -5 | -10 | -14 | -17 | -22 |
| CW | -1 | -6 | -10 | -14 | -19 |
| RTTY | -1 | -6 | -10 | -14 | -19 |
| PSK | 5 | 0 | -4 | -7 | -12 |
| USB | 10 | 5 | 0 | -5 | -10 |
| LSB | 10 | 5 | 0 | -5 | -10 |

## Config Ownership

Runtime path reliability settings are owned by [`../data/config/path_reliability.yaml`](../data/config/path_reliability.yaml). Startup loads that file through the central config registry and fails if required settings are missing or malformed.

`DefaultConfig()` remains a package-local test/helper baseline for constructing in-memory fixtures. It is not a runtime fallback, and production behavior should be documented from YAML.

## Config Boundary

`enabled`, `display_enabled`, `glyph_symbols`, `allowed_bands`,
`min_observation_count`, and `receiver_contribution_mode` are operator policy.
Half-lives, stale/freshness multipliers, effective weight, geographic selection, directional merge,
reverse discount, mode thresholds, mode offsets, and noise tables are algorithm
calibration; do not retune them under normal operation without validation and
decision-memory handling.

## Solar Overrides

The predictor itself returns the normal class and glyph. Optional `R` and `G` solar-weather overrides are applied later by the telnet layer.

For user-facing behavior, see [`../README.md`](../README.md) and [`../telnet/README.md`](../telnet/README.md).
