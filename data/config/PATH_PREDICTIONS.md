# Understanding Path Predictions

## What Are Path Predictions?

When you connect to the cluster, you'll notice a single character appearing next to each spot. This character is a **path prediction glyph** - it's the cluster's educated guess about how good the radio path is between you and the DX station, based on real propagation data collected from thousands of stations worldwide.

Think of it as crowdsourced propagation forecasting. Every FT8, CW, PSK, and WSPR signal that gets decoded anywhere in the world feeds into a massive, constantly-updating database of what's actually working right now on each band.

## The Glyphs Explained

Here's what each symbol means:

- **`>`** (greater-than) = **High** - Excellent path. You should be able to work this station easily.
- **`=`** (equals) = **Medium** - Moderate path. Definitely workable with good technique.
- **`<`** (less-than) = **Low** - Weak path. It'll take patience, but it's possible.
- **`-`** (dash) = **Unlikely** - Very difficult path. Marginal conditions, but don't rule it out completely.
- **configured closed glyph** = **Closed** - Optional VOACAP fallback predicts the current UTC hour is at or below the closed threshold for this mode and path.
- **` `** (space) = **Insufficient data** - The system doesn't have enough information yet to make a prediction.

These symbols are mode-specific, meaning the same path might show `>` for FT8 but `=` for CW, because the thresholds are calibrated differently for each mode's sensitivity.

## How It Actually Works

### The Data Collection

Every time someone decodes a digital signal (FT8, FT4, CW from RBN, PSK, WSPR), the system captures:
- Who transmitted (and their grid square)
- Who received it (and their grid square)
- What band
- The signal strength (SNR)

All of these get normalized to "FT8-equivalent" values so CW, PSK, and other modes can be compared on the same scale. For example, a CW signal at -10 dB gets adjusted because CW decoding works at different SNR levels than FT8.

### Geographic Intelligence

Your location and the DX station's location get converted into hexagonal cells on a global grid (using something called H3). The system tracks propagation at two levels:

- **Fine resolution**: Pinpoint data for your specific area (a few kilometers across)
- **Coarse resolution**: Regional data for the broader area around you (tens of kilometers)

This dual-resolution approach is clever - if there isn't much fine-grained data for your exact location, the system falls back to what's happening in your general region. When you have lots of local data, it prioritizes that instead.

### Directional Awareness

Radio propagation isn't always symmetrical. A path might work great one direction but poorly the other way due to different noise levels, antenna patterns, or ionospheric tilt.

The system tracks paths **both directions**:
- **Receive path**: DX station transmitting → You receiving
- **Transmit path**: You transmitting → DX station receiving

It combines these intelligently: 60% weight on the receive direction (adjusted for your noise), 40% weight on transmit. If it only knows one direction, it uses that with a confidence penalty.

### Time Decay

Propagation changes constantly, so older data matters less. Each data point decays exponentially over time using a "half-life" - the time it takes for data to lose half its value.

The half-life is band-specific because different bands change at different rates:
- **Low bands (160m, 80m)**: 10-minute half-life - conditions change slowly
- **Mid bands (40m-20m)**: 6-8 minute half-life - moderate change rate
- **High bands (15m-6m)**: 4 minute half-life - conditions change rapidly

After about 3 half-lives (roughly 12-30 minutes depending on band), old data gets purged entirely to keep the predictions fresh.

Separately, selected prediction evidence has a freshness gate. The shipped value
is `max_prediction_age_half_life_multiplier: 1.5`, so selected evidence older
than about 1.5 band half-lives is treated as insufficient before the final
glyph is chosen. This is a hard cutoff: a strong old opening becomes a space
rather than fading from `>` to `=` to `<` because of age alone.

### Your Noise Environment

`SET NOISE` stores your local receive-noise class. The checked-in configuration
uses one receive-side penalty per location class:

| Class | Quiet | Rural | Suburban | Urban | Industrial |
| --- | ---: | ---: | ---: | ---: | ---: |
| Penalty dB | 0 | 4 | 12 | 17 | 20 |

The adjustment affects only the receive direction. Your transmit effectiveness
does not change based on local noise.

### The Final Calculation

When you see a glyph next to a spot, here's what happened behind the scenes:

1. **Lookup**: System finds all recent propagation data between your area and the DX station's area (both fine and coarse resolution, both directions).

2. **Decay**: Each data point gets weighted by how recent it is.

3. **Blend resolutions**: Fine and coarse data get combined. If you have
   strong local data (fine), it dominates. If not, regional data (coarse) fills
   in. Because fine reports also update the matching coarse bucket, scalar
   evidence mass uses the larger fine/coarse value instead of adding both. The
   p50 histogram still keeps the existing local-emphasis shape.

4. **Check freshness**: Selected receive/transmit evidence must be recent enough
   for the band. Blended fine/coarse age uses local fine evidence plus the
   coarse regional complement, so stale local evidence can make a direction old
   enough to discard even when the wider region was refreshed.

5. **Merge directions**: Receive and transmit paths combine (60/40 split), with the configured receive-side noise penalty applied when nonzero.
   For beacon spots, transmit evidence is not applicable. The system uses only
   the DX-to-user receive leg, still applies your receive-noise penalty, and
   does not apply the one-direction reverse hint discount.

6. **Apply receiver contribution caps**: The cluster tracks a bounded set of
   receiver identities per bucket. In `enforce` mode, receiver diversity and
   capped weight gate the path class separately from the raw observation floor.
   Old capped receiver evidence decays on the same clock as signal weight, so
   newer evidence can replace it. If an operator switches to `shadow`, the
   displayed glyph uses raw evidence while diagnostics and logs show where the
   capped evidence would have been stricter.

7. **Check evidence floor**: If the raw selected observation count is below the
   cluster minimum, the system shows a space (insufficient data). Users can make
   their own view stricter with `SET PATHSAMPLES <count>`, but cannot lower the
   cluster default. Five-minute propagation logs report this as `low_count`.

Optional VOACAP fallback needs a rounded SSN generation before it can run. The
runtime SSN monitor stores NOAA validators, the latest observation, EWMA, and
the current rounded SSN generation in `voacap_fallback.ssn_state_path`, so a
restart can reuse the SSN baseline. The path forecast cache itself remains
memory-only and is rebuilt lazily after restart.

8. **Check receiver diversity**: In receiver-cap enforcement and cap-shadow
   candidate evaluation, the selected capped evidence must include enough live
   attributed receiver slots for the configured sample floor and receiver cap.
   Five-minute propagation logs report this as `low_receiver`.

9. **Check confidence**: If the combined data weight is below the minimum
   threshold (default 0.5), the system shows a space (insufficient data) instead
   of making an unreliable prediction. Five-minute propagation logs report this
   as `low_weight`. After scalar union semantics, weight diagnostics can be
   lower than older releases that double-counted fine evidence inside coarse.

10. **Map to glyph**: The selected p50 signal strength gets compared against
   mode-specific thresholds to pick the right symbol. Fixed histogram bins use
   midpoint representatives, and exact 50/50 splits between two non-empty bins
   use the average of both representatives so balanced weak/strong evidence
   reflects the typical middle.

If the selected bucket evidence is insufficient and the optional
`voacap_fallback.enabled` setting is true, the cluster can start a delayed
nonblocking VOACAP lookup for the same grid-center endpoints. A cached VOACAP
result can replace an insufficient glyph in two cases: it can emit the
configured closed glyph when the current UTC hour's blended bidirectional
FT8-equivalent SNR, after the user's receive-side noise penalty, is at or below
the request mode's configured `mode_thresholds.<mode>.closed` threshold, or it
can emit a normal path glyph when sparse bucket p50 evidence exists and that
p50 class matches the cached VOACAP current-hour class. It can also emit a
normal no-p50 VOACAP open glyph, or upgrade sparse p50 by one class, only when
the cached VOACAP class is open and the configured request-SNR REL gate passes.
Closed fallback still uses the SNR50 closed threshold and does not require REL.
Cached VOACAP output retains the parsed hourly records for the requested band.
Runtime fallback decks start at the current rolling UTC window, and parsed
VOACAP hour `24` is stored as UTC hour `0`. Runtime decks select Method 20
below 7000 km and Method 30 at and above 7000 km using the same Maidenhead
grid-center endpoints written to the VOACAP circuit. Cached records reuse the
existing fine path-cell granularity, so near-threshold method reuse follows the
same res-2 cache boundary as other VOACAP fallback data. The fallback never
replaces a normal sufficient p50 bucket prediction.
For beacon spots, the same fallback uses the receive-leg VOACAP SNR and
receive-leg REL rather than the blended bidirectional effective SNR.
For `PASS/REJECT PATH`, the closed fallback is filter-visible as `CLOSED`.
`CLOSED` remains compatible with `UNLIKELY`, so existing `UNLIKELY` filters
still include closed fallback spots, while direct `CLOSED` filters target only
closed fallback spots.

Five-minute propagation logs keep final emit counters in `Path predictions
(5m)`: `voacap_closed`, `voacap_aligned`, `voacap_sparse_upgrade`, and
`voacap_open`. Beacon paths add `beacon_rx`, `beacon_rx_insufficient`,
`beacon_rx_<reason>`, and `beacon_rx_voacap_*` final counters. When fallback
work occurs, a separate `VOACAP fallback (5m)`
line reports stage counters such as `queued`, `success`, `cache_hit`,
`no_current_hour`, `closed`, `closed_no_p50`, `closed_with_sparse_p50`,
`closed_with_sparse_p50_class_*`, `open_no_p50`, `class_mismatch`,
`sparse_upgrade`, `open_no_p50_rel`, `rel_missing`, `rel_below_floor`, and
`rel_multi_tier`.
When sufficient p50 predictions can be compared with an existing current-hour
VOACAP cache record, `VOACAP p50 compare (5m)` reports cache hits/misses, class
agreement, stronger/weaker effective SNR, closed-VOACAP versus p50 class, and
SNR-delta buckets. That comparison is cache-only and does not run VOACAP on
cache misses.
When sparse or no-p50 candidates are present, `Sparse p50 VOACAP (5m)` reports
the diagnostic split for those candidates without changing glyph decisions:
`no_p50`, `very_low_count`, `beacon_rx`, `non_beacon`,
`cache_miss_total`, `cache_hit`, `queued`, `delayed`, `inflight`,
`invalid_request`, `invalid_unsupported_band`, `invalid_empty_unknown_band`,
`invalid_user_grid`, `invalid_dx_grid`, `invalid_user_cell`, `invalid_dx_cell`,
`ssn_unavailable`, `no_current_hour`, `queue_full`, `not_running`, `disabled`,
`unavailable`, `closed`, `aligned`, `sparse_upgrade`, `open_rel_pass`,
`open_rel_fail`, `not_closed`, `rel_missing`, `rel_below_floor`, and
`rel_multi_tier`.
The `sparse_p50_diagnostic_max_observation_count` setting only defines the
very-low-count diagnostic bucket; it does not relax p50 gates or start more
VOACAP work.

`SHOW PROP <call|prefix|grid> [band] [mode]` is the on-demand view of the same
rolling VOACAP horizon. It starts from your saved `SET GRID`, applies your
`SET NOISE` receive penalty to the RX leg, merges RX and TX into `EFF`, and
maps `EFF`, `RX`, and `TX` to mode-specific glyphs. `REL` remains the text class
for the requested mode and merged path. With no band, it shows each configured
VOACAP fallback band. With no mode, it uses CW. Empty or partial single-band
lookups enqueue a refresh through the existing VOACAP fallback worker and wait
up to `voacap_fallback.show_prop_wait_milliseconds`; all-band requests return
cached rows while refreshing missing or partial bands in the background. The row
count follows `voacap_fallback.forecast_hours`.

## How to Use This Information

### Making Quick Decisions

The glyphs help you prioritize. If you see:
- **`>` or `=`**: Go for it! These are solid opportunities.
- **`<`**: Worth trying, especially if you need that entity or grid.
- **`-`**: Probably not worth your time unless it's a rare one.
- **Closed glyph**: Optional VOACAP fallback thinks the band is closed for this
  mode, path, and receive-noise setting.
- **VOACAP-aligned normal glyph**: Bucket evidence was insufficient, but sparse
  bucket p50 and current-hour VOACAP mapped to the same path class.
- **REL-gated VOACAP normal glyph**: Bucket evidence was insufficient, but
  cached current-hour VOACAP mapped to an open class and passed the configured
  request-SNR REL gate. Sparse p50 can only upgrade by one class in this mode.
- **Space**: No prediction available - you're on your own. Could be good or bad.

### Understanding Limitations

**The system doesn't know everything about YOUR station**:
- Your antenna gain and pattern
- Your power level
- Your operating skill
- Interference at your specific location
- Specific propagation quirks (sporadic E, aurora, etc.)

It's giving you a statistical estimate based on what thousands of other stations are experiencing on similar paths. You might do better or worse depending on your setup.

**New paths take time**: If a band just opened to a new area, there might not be data yet. A space character doesn't mean the path is bad - it means the prediction system is still learning.

**You can require more samples**: `SET PATHSAMPLES 30` makes your session wait
for at least 30 selected observations before showing a path tag. Use
`SET PATHSAMPLES DEFAULT` to return to the cluster default.

**One receiver cannot carry a bucket by itself**: receiver contribution caps
limit one receiving station to the configured decayed effective observation and
weight caps per bucket. In `enforce` mode, caps gate the active path class. In
`shadow` mode, active glyphs still use raw selected evidence while the
prediction diagnostics can report whether the configured cap would have blocked
the prediction. Retired candidate-cap shadow lanes are not maintained by the
current runtime.

**Beacon RX-only paths**: The existing beacon flag covers source
class beacons, `/B` calls, known beacon calls, and beacon comment keywords.
Their default raw observation floor is `beacon_min_observation_count`, currently
11, with receiver diversity derived from that floor and the receiver cap.

**Sparse VOACAP suffixes**: `SET DIAG PATH` may append a `v*` suffix to an
insufficient sparse/no-p50 diagnostic, such as `n0|none|vdly` or
`n2|lown|vrel`. The suffix explains whether VOACAP was queued, delayed,
inflight, invalid by band/grid/cell reason, missing SSN, missing the current
hour, queue-full, not running, disabled, unavailable, blocked by REL/tier
guards, or usable but not closed.

### Noise Environment Setup

`SET NOISE` still stores your receive-noise class:

```
SET NOISE SUBURBAN
```

In the checked-in configuration, each class resolves to a scalar dB penalty.
Operators should set the class that best matches their receive environment.

### Band-Specific Behavior

- **Low bands (160m/80m)**: Predictions change slowly. A `>` will probably stick around for 15-20 minutes.
- **High bands (10m/6m)**: Predictions change rapidly. A `>` can disappear within a few minutes if no fresh supporting evidence arrives.

## Configuration Options

The system is highly configurable (see [path_reliability.yaml](path_reliability.yaml)). Operators don't normally need to touch these, but if you're curious:

- **Glyph symbols**: Can be customized (default: `>=<-` and space)
- **Half-life timings**: Per-band decay rates
- **Freshness gate**: Maximum selected evidence age as a multiple of band half-life
- **Noise penalties**: receive-side dB adjustments per environment type
- **Mode thresholds**: What signal strength qualifies as high/medium/low for each mode
- **Minimum observation count**: How many raw selected observations are needed before showing a prediction; receiver diversity is checked separately when receiver caps are enforced
- **Minimum weight**: How much data is needed before showing a prediction
- **Receiver contribution caps**: Whether capped receiver evidence is off, shadowed, or enforced, how many receiver slots are tracked in fine/coarse buckets, and how many decayed effective observations one receiver can contribute per bucket

## The Bottom Line

Path predictions give you a real-time, data-driven assessment of propagation conditions based on what's actually being heard worldwide right now. They're not perfect, but they're a lot better than guessing.

When you see a `>` next to a needed multiplier, don't hesitate - the data says the path is open. When you see a `-`, maybe wait for better conditions unless you really need it. And when you see a space, you're in uncharted territory - sometimes the best QSOs happen when the system says "I don't know yet."

Happy hunting!

---

*For technical implementation details, see the source code in `pathreliability/` and the configuration file `path_reliability.yaml`.*
