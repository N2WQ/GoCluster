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
is `max_prediction_age_half_life_multiplier: 1.25`, so selected evidence older
than about 1.25 band half-lives is treated as insufficient before the final
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

3. **Blend resolutions**: Fine and coarse data get combined. If you have strong local data (fine), it dominates. If not, regional data (coarse) fills in.

4. **Check freshness**: Selected receive/transmit evidence must be recent enough for the band. Stale selected evidence is discarded.

5. **Merge directions**: Receive and transmit paths combine (60/40 split), with the configured receive-side noise penalty applied when nonzero.

6. **Apply receiver contribution caps**: The cluster tracks a bounded set of
   receiver identities per bucket. In the shipped `enforce` mode, the count and
   weight gates use capped evidence. If an operator switches to `shadow`, the
   displayed glyph uses raw evidence while diagnostics and logs show where the
   capped evidence would have been stricter.

7. **Check evidence floor**: If the selected observation count is below the
   cluster minimum, the system shows a space (insufficient data). Users can make
   their own view stricter with `SET PATHSAMPLES <count>`, but cannot lower the
   cluster default. In `enforce` mode this check uses capped selected
   observations; in `shadow` mode it uses raw selected observations. Five-minute
   propagation logs report this as `low_count`.

8. **Check confidence**: If the combined data weight is below the minimum threshold (default 0.6), the system shows a space (insufficient data) instead of making an unreliable prediction. Five-minute propagation logs report this as `low_weight`.

9. **Map to glyph**: The final signal strength gets compared against mode-specific thresholds to pick the right symbol.

## How to Use This Information

### Making Quick Decisions

The glyphs help you prioritize. If you see:
- **`>` or `=`**: Go for it! These are solid opportunities.
- **`<`**: Worth trying, especially if you need that entity or grid.
- **`-`**: Probably not worth your time unless it's a rare one.
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
limit one receiving station to a shipped maximum of five accepted reports and
five accepted weight units per bucket. The shipped mode is `enforce`, so caps
gate the active path class.

**Beacons get capped**: The system limits how much any single beacon can dominate the data to prevent bias from loud beacons.

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
- **Minimum observation count**: How many selected observations are needed before showing a prediction; in `enforce` mode these are capped selected observations
- **Minimum weight**: How much data is needed before showing a prediction
- **Receiver contribution caps**: Whether capped receiver evidence is off, shadowed, or enforced, and how many receiver slots are tracked in fine/coarse buckets

## The Bottom Line

Path predictions give you a real-time, data-driven assessment of propagation conditions based on what's actually being heard worldwide right now. They're not perfect, but they're a lot better than guessing.

When you see a `>` next to a needed multiplier, don't hesitate - the data says the path is open. When you see a `-`, maybe wait for better conditions unless you really need it. And when you see a space, you're in uncharted territory - sometimes the best QSOs happen when the system says "I don't know yet."

Happy hunting!

---

*For technical implementation details, see the source code in `pathreliability/` and the configuration file `path_reliability.yaml`.*
