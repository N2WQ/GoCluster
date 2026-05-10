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

The runtime uses stable `uint16` proxy IDs built from the precomputed H3 tables in [`../data/h3`](../data/h3). If the grid is invalid or the mapping tables are unavailable, the cell becomes invalid and prediction can fall back to insufficient.

## FT8-Equivalent Conversion

Every accepted report is normalized to an FT8-equivalent dB value before entering the store.

The shipped config in [`../data/config/path_reliability.yaml`](../data/config/path_reliability.yaml) currently sets:

- `FT4: 0`
- `CW: -7`
- `RTTY: -7`
- `PSK: -19`
- `WSPR: -26`

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
- capped receiver-attributed weight and observation count
- fixed raw and capped SNR histograms for active p50 scoring
- fixed receiver contribution slots
- last update time

Updates apply exponential decay using the band's half-life before adding the new sample.
The observation count is not decayed; it is a bounded diagnostic count of how
many reports contributed to the selected bucket evidence.

Receiver contribution caps are bucket-owned and bounded by the bucket itself:
fine buckets track up to `receiver_fine_slots` identities and coarse buckets
track up to `receiver_coarse_slots` identities. The shipped values are `4` and
`8`. One receiver can add at most `receiver_max_effective_count: 5` accepted
reports and `receiver_max_effective_weight: 5.0` accepted weight to a bucket's
capped trust evidence. When the slot set is full, the weakest/oldest slot is
reused; this is an approximation, not an unbounded exact unique-receiver set.

`receiver_contribution_mode` controls how capped evidence is used:

- `shadow`: use raw selected evidence for active p50 glyphs and PATH filters,
  but expose whether capped evidence would have blocked the prediction.
- `enforce`: use capped count and capped weight for the count/weight gates.
- `off`: disable capped tracking and use raw evidence only.

The store always retains fixed raw and capped SNR histograms. Updates touch only
the selected bin unless time has advanced, and p50 scans the fixed array. These
histograms are required for active glyphs and PATH filters.

The SNR bins are fixed in code: `< -24`, one-dB bins from `-24..-23` through
`23..24`, and `>= 24`. Displayed p50 values use the bin's compact lower-edge
value; underflow displays as `-24` and overflow displays as `24`.

Five-minute propagation logs split insufficient path prediction outcomes into
`no_sample`, `low_count`, `low_weight`, and `stale`. `low_count` maps to
`InsufficientLowCount`; `low_weight` maps to `InsufficientLowWeight`. These
aggregate lines are written to `logging.propagation.dir`.

The shipped config currently uses:

- half-lives ranging from `600s` on `160m` and `80m` down to `240s` on `12m`, `10m`, and `6m`
- `stale_after_half_life_multiplier: 3`
- `stale_after_seconds: 1800` as the fallback purge window
- `max_prediction_age_half_life_multiplier: 1.25` as a display/filter freshness gate
- `receiver_contribution_mode: shadow`
- `receiver_fine_slots: 4`
- `receiver_coarse_slots: 8`
- `receiver_max_effective_count: 5`
- `receiver_max_effective_weight: 5.0`

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

- `min_effective_weight: 0.6`
- `min_observation_count: 19`
- `min_fine_weight: 5`
- `fine_only_weight: 20`
- `reverse_hint_discount: 0.5`
- `merge_receive_weight: 0.6`
- `merge_transmit_weight: 0.4`

If only one direction exists, the predictor still uses it, but discounts the effective weight with `reverse_hint_discount`.

Telnet users can set a stricter personal observation floor with
`SET PATHSAMPLES <count>`. That setting is applied as
`max(min_observation_count, user setting)` and cannot lower the cluster default.
In `shadow` mode this floor still uses the raw selected observation count; in
`enforce` mode it uses the capped selected observation count.

The receive-side noise table is resolved only by `SET NOISE` class. The same
location penalty applies on every band and is subtracted from DX-to-user path
evidence at prediction time.

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
`min_observation_count`, or the merged effective weight stays below
`min_effective_weight`.

The shipped glyph symbols are:

- `>` for `HIGH`
- `=` for `MEDIUM`
- `<` for `LOW`
- `-` for `UNLIKELY`
- space for `INSUFFICIENT`

The shipped threshold table is:

| Mode | High | Medium | Low | Unlikely |
| --- | ---: | ---: | ---: | ---: |
| FT8 | -13 | -17 | -21 | -21 |
| FT4 | -5 | -10 | -14 | -17 |
| CW | 0 | -5 | -9 | -12 |
| RTTY | 12 | 4 | 0 | -3 |
| PSK | 5 | 0 | -4 | -7 |
| USB | 22 | 17 | 13 | 10 |
| LSB | 22 | 17 | 13 | 10 |

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
