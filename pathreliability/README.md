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

## Bucket Storage

The predictor stores directional buckets keyed by:

- receiver cell
- sender cell
- band
- fine or coarse resolution

Each bucket stores:

- accumulated weight
- raw observation count
- capped receiver-attributed weight and effective observation count
- one fixed SNR histogram for the active p50 scoring lane
- fixed receiver contribution slots
- last update time

Updates apply exponential decay using the band's half-life before adding the new sample.
The raw observation count is not decayed; it is a bounded diagnostic count of
how many reports contributed to the selected bucket evidence. Capped
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

`receiver_contribution_mode` controls how capped evidence is used:

- `shadow`: use raw selected evidence for active p50 glyphs and PATH filters,
  but expose whether the configured cap would have blocked the prediction.
- `enforce`: use raw selected count for the observation floor, plus capped
  receiver diversity and capped weight for receiver-cap trust gates.
- `off`: disable capped tracking and use raw evidence only.

In enforce mode, the global `min_observation_count` still means selected raw
observations. Receiver concentration is checked by a separate derived gate:
the selected capped evidence must include at least
`ceil(min_observation_count / receiver_max_effective_count)` live attributed
receiver slots, capped by the selected bucket's slot capacity. This keeps the
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

Five-minute propagation logs split insufficient path prediction outcomes into
`no_sample`, `low_count`, `low_receiver`, `low_weight`, and `stale`.
`low_count` maps to the raw selected observation floor; `low_receiver` maps to
the receiver-diversity gate in enforce evaluation; `low_weight`
maps to the decayed effective weight floor. These aggregate lines are written
to `logging.propagation.dir`. VOACAP fallback outcomes are counted separately
as `voacap_closed` and `voacap_aligned`.
When the fallback is active, a separate `VOACAP fallback (5m)` line reports
stage counters such as `queued`, `success`, `cache_hit`, `no_current_hour`,
`delay_wait`, `queue_full`, `closed`, `closed_no_p50`,
`closed_with_sparse_p50`, `closed_with_sparse_p50_class_*`, `aligned`,
`open_no_p50`, and `class_mismatch`. The path-prediction counters are final
emitted glyphs; the fallback line explains why cached VOACAP work did or did
not emit a glyph. The closed sparse-p50 counters show when VOACAP emitted a
closed glyph despite sparse observed p50 evidence and which p50 class that
evidence would have mapped to.
A separate `VOACAP p50 compare (5m)` line may appear for sufficient p50
predictions when an existing current-hour VOACAP cache record is present. It
reports cache hit/miss counts, class agreement, stronger/weaker effective SNR,
closed-VOACAP versus p50 class splits, and absolute SNR-delta buckets. This
comparison is cache-only and does not enqueue VOACAP work or change emitted
glyphs.

When `voacap_fallback.enabled` is true, insufficient bucket results may start a
delayed VOACAP lookup. The lookup is nonblocking in the telnet path. Cached
VOACAP output stores one hourly record per parsed forecast hour for the
requested band. Each cached hour carries both raw directions: DX-to-user receive
and user-to-DX transmit. Lookup blends those directions with the same
`merge_receive_weight` and `merge_transmit_weight` used by p50, then subtracts
the request user's receive-side `SET NOISE` penalty from the receive leg.
Runtime fallback decks start at the current rolling UTC forecast window, and
VOACAP output hour `24` is normalized to UTC hour `0`. Lookup selects the record
matching the current UTC hour and re-evaluates the effective blended SNR against
the request mode, so the first mode or noise class to populate the cache does
not decide later requests. If the effective VOACAP SNR is at or below
`mode_thresholds.<mode>.closed`, the fallback can return the configured closed
glyph. Otherwise, it can return a normal `HIGH`, `MEDIUM`, `LOW`, or
`UNLIKELY` glyph only when the insufficient bucket result still has sparse p50
evidence and that p50 class matches the effective VOACAP current-hour class. It
never overrides a sufficient bucket p50 result. For PATH filters, the closed glyph is
visible as `CLOSED` and remains compatible with `UNLIKELY`: existing
`PASS/REJECT PATH UNLIKELY` filters still include closed fallback spots, while
direct `PASS/REJECT PATH CLOSED` rules target only closed fallback spots.

The shipped config currently uses:

- half-lives ranging from `600s` on `160m` and `80m` down to `240s` on `12m`, `10m`, and `6m`
- `stale_after_half_life_multiplier: 3`
- `stale_after_seconds: 1800` as the fallback purge window
- `max_prediction_age_half_life_multiplier: 1.25` as a display/filter freshness gate
- `receiver_contribution_mode: enforce`
- `receiver_fine_slots: 6`
- `receiver_coarse_slots: 12`
- `receiver_max_effective_count: 8` decayed effective observations per receiver
- `receiver_max_effective_weight: 8.0`

## Sample Selection And Merge

Prediction uses two directions:

- receive sample: DX to user
- transmit sample: user to DX

For each direction, `SelectSample(...)` chooses between fine and coarse evidence:

- if fine is below `min_fine_weight`, coarse wins
- if fine is above `fine_only_weight`, fine wins outright
- otherwise, fine and coarse are blended by weight

When fine and coarse evidence are blended, the selected sample age is also a
weighted effective age. A small fresh sample therefore cannot hide a large stale
regional contribution.
The selected sample count uses the larger fine/coarse layer count instead of
summing both layers, because one report can update both resolutions.

After sample selection, the predictor applies the freshness gate. If selected
evidence is older than `ceil(band_half_life * max_prediction_age_half_life_multiplier)`,
that direction is discarded before receive/transmit merge. A value of `0`
disables this gate. Stale positive evidence returns `INSUFFICIENT`; it does not
fade through weaker glyph tiers just because it got older.

The shipped config currently uses:

- `min_effective_weight: 0.5`
- `min_observation_count: 21`
- `min_fine_weight: 5`
- `fine_only_weight: 20`
- `reverse_hint_discount: 0.5`
- `merge_receive_weight: 0.6`
- `merge_transmit_weight: 0.4`

If only one direction exists, the predictor still uses it, but discounts the effective weight with `reverse_hint_discount`.

Telnet users can set a stricter personal observation floor with
`SET PATHSAMPLES <count>`. That setting is applied as
`max(min_observation_count, user setting)` and cannot lower the cluster default.
This floor uses the raw selected observation count in all receiver-cap modes.
In enforce mode, receiver concentration is evaluated separately from this user
sample floor.

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
too old for the freshness gate, the selected raw observation count is below
`min_observation_count`, receiver diversity is too low under enforce/candidate
cap evaluation, or the merged effective weight stays below
`min_effective_weight`.

The shipped glyph symbols are:

- `>` for `HIGH`
- `=` for `MEDIUM`
- `<` for `LOW`
- `-` for `UNLIKELY`
- space for `INSUFFICIENT`
- configured `glyph_symbols.closed` for VOACAP `CLOSED` fallback when enabled
  and cached

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
Half-lives, stale/freshness multipliers, effective weight, fine/coarse merge,
reverse discount, mode thresholds, mode offsets, and noise tables are algorithm
calibration; do not retune them under normal operation without validation and
decision-memory handling.

## Solar Overrides

The predictor itself returns the normal class and glyph. Optional `R` and `G` solar-weather overrides are applied later by the telnet layer.

For user-facing behavior, see [`../README.md`](../README.md) and [`../telnet/README.md`](../telnet/README.md).
