# Spot Confidence And Correction

This directory owns the canonical spot record, fixed-width spot formatting, call-correction helpers, harmonics, frequency averaging, recent-support stores, and custom SCP persistence.

Beacon state can come from source metadata, such as an RBN spot class of
`BEACON` or `NCDXF B`, or from the DX/comment beacon heuristics. Blank comments
on beacon spots display as `BEACON` in telnet and newly archived rows while
leaving the original peer-forwarded comment unchanged. `NCDXF B` source-class
spots use `NCDXF BEACON` as that blank-comment display/archive fallback.

## Confidence Paths

The runtime has two separate confidence paths.

### Resolver-capable modes

Resolver-capable modes are:

- `CW`
- `RTTY`
- `USB`
- `LSB`

The live mapping is shared through [`../internal/correctionflow/shared.go`](../internal/correctionflow/shared.go).

For resolver-capable modes:

- the emitted call gets a call-specific confidence percent
- weighted support is used when available
- plain reporter support is the fallback
- `FormatConfidence` maps:
  - one reporter to `?`
  - multi-reporter confidence `<= 50%` to `P`
  - multi-reporter confidence `>= 51%` to `V`

Contested resolver states are intentionally conservative:

- `split` or `uncertain` become `?` for one-reporter cases
- `split` or `uncertain` become `P` for multi-reporter cases

Support counts distinct eligible receivers. Neighborhood selection admits
comparable winning buckets, then keeps every candidate's receiver evidence in
those buckets, including dissent that is ineligible as a replacement. Repeated
receivers count once in the total and once for each candidate they support.
Overlapping candidate shares can therefore sum to more than 100%. Weights
900 and 950 supporting A against weight 1000 supporting B give A a displayed
64% share, regardless of repeated buckets.

These percentages describe retained receiver support, not calibrated correctness
probabilities. Eligibility uses the existing key and recency window; it does not
prove that all reports belong to the original transmission episode. Resolver
receiver evidence is published immutably; temporal scoring retains compact
tallies. See [ADR-0280](../docs/decisions/ADR-0280-resolver-receiver-evidence-ownership.md).

An exact split remains protected through neighborhood and temporal selection.
Temporal release filters the latest immutable rows at its current clock, keeps
the inclusive recency cutoff and excludes future observations. Saved proposals
carry no support authority. Current settled evidence and ordinary correction
gates authorize a changed identity; confirmed history counts accepted changes.
Fresh evidence in a key/window does not prove the same RF transmission episode.
The narrow ambiguity guard uses the closest eligible cross-candidate frequency
pair, with its other support, overlap and family conditions unchanged. Conflicting
equal-time frequency metadata causes abstention until superseded or expired.
The strict floating-point threshold has no tolerance: nominal boundary values
can differ slightly by band. See
[ADR-0021](../docs/decisions/ADR-0021-call-correction-ambiguity-guard-and-quality-rail.md).

### The `S` floor

`S` is not produced by `FormatConfidence`.

Instead, after the main confidence path runs, the runtime promotes `?` to `S` when:

- the call is in static support, or
- the call has recent on-band support

That floor only applies to confidence-capable modes and does not overwrite non-`?` results.

### Correction outcomes

Two glyphs represent correction outcomes rather than raw confidence:

- `C`: correction applied and validated
- `B`: correction attempted, but the candidate failed base-call or CTY validation, so the original call stayed on the spot

The percentage describes the call on that spot. A rejected replacement keeps
`B` and the retained original's support share; an accepted `C` uses the new call's
share. No eligible support means the percentage is unavailable (`--%` in DIAG
CONF). Equal weights with one receiver for the retained call and two for the
rejected candidate give 33%; unequal weights use the retained call's weighted
share. The glyph records the outcome separately from that support percentage.

### FT2, FT4, And FT8

FT modes use a separate corroboration stage in the main output pipeline.

The burst key is:

- normalized DX call
- exact FT mode
- canonical dial frequency

Burst timing is controlled by the `call_correction` FT knobs in the shipped config:

- `p_min_unique_spotters`
- `v_min_unique_spotters`
- per-mode quiet-gap seconds
- per-mode hard-cap seconds

In the shipped repo config these live under `call_correction` in [`../data/config/pipeline.yaml`](../data/config/pipeline.yaml):

- `call_correction.p_min_unique_spotters`
- `call_correction.v_min_unique_spotters`
- `call_correction.ft8_quiet_gap_seconds`
- `call_correction.ft8_hard_cap_seconds`
- `call_correction.ft4_quiet_gap_seconds`
- `call_correction.ft4_hard_cap_seconds`
- `call_correction.ft2_quiet_gap_seconds`
- `call_correction.ft2_hard_cap_seconds`

With the current shipped defaults:

- `P` = exactly 2 unique reporters
- `V` = 3 or more unique reporters
- FT8 = `6s` quiet gap, `12s` hard cap
- FT4 = `5s` quiet gap, `10s` hard cap
- FT2 = `3s` quiet gap, `6s` hard cap

The runtime can corroborate across PSKReporter and RBN-digital if the burst key matches and the spotter calls differ.

A burst expires at the earlier of the quiet-gap and hard deadline. A report
arriving exactly at expiry or later starts a new burst, even if timer draining
was delayed. It cannot promote spots in the expired burst. New singletons can
still receive the existing recent-band/static `S` floor. FT glyphs describe
distinct receiver corroboration; they do not carry a resolver percentage or
prove that receivers heard the same transmission episode.

## Config Boundary

The FT `p_min_unique_spotters`, `v_min_unique_spotters`, quiet-gap seconds, and
hard-cap seconds are explicit operator policy knobs. Resolver mutation gates,
distance models, Bayesian rails, family policy, stabilizer/temporal policy, and
custom SCP admission are algorithm calibration; change them only with
replay/validation and decision-memory handling.

### Special operator path

Local non-test `DX` self-spots are treated as operator-authoritative in the live runtime pipeline:

- they are forced to `V`
- they bypass resolver, temporal, and stabilizer delay in that runtime path

## Related Stores And Policy

- recent on-band support helps the `S` floor and some resolver gates
- custom SCP stores persistent support evidence
- custom SCP retention is bounded by `history_horizon_days`, `max_keys`, and `max_spotters_per_key`
- custom SCP static membership has its separate `static_horizon_days` retention horizon
- custom SCP admission is intentionally `V`-only to avoid reinforcement loops from weaker classes

### Historical receiver freshness

Resolver and stabilizer queries use their own `custom_scp` age-score and H3
cell minima against one canonical store. Each receiver qualification gate also
uses the greater of its consumer's receiver minimum and
`recent_band_record_min_unique_spotters`; the plus1 and edit-neighbor rails
add their own minima. Tightening the stabilizer does not raise resolver or FT
requirements. Partial counts remain visible to the Bayesian prior and diagnostics;
qualification gates still check their effective receiver minimum. Boolean store
queries preserve the request instead of lowering it to the storage cap.

The custom S floor uses its dedicated exact/family thresholds. The independent
FT recent floor uses the common receiver minimum without inheriting stabilizer
settings. Replay supplies both configured observation and static horizons;
its historical clock behavior remains unchanged.

For the custom S floor, exact-call support is checked first. A family fallback
qualifies when any one member independently meets every family age-score,
receiver and cell requirement. A higher-scoring member that fails a requirement
cannot hide another member's success. Counts and cells are never pooled across
members; static recognition remains a separate alternative. This rule does not
change output-pipeline family suppression.

Enabled historical receiver and cell requirements must fit
`custom_scp.max_spotters_per_key`. Loader, runtime and replay reject impossible
consumed requirements before creating history resources. Disabled unused gates
are ignored; the separate FT store is not constrained by this cap. Direct
CustomSCP boolean queries return false for an above-cap minimum instead of
lowering it. Count queries still expose eligible partial support.

Learned static membership qualifies only from the inclusive static-horizon cutoff
through the query's stored second. A future membership timestamp remains retained
but cannot supply static recognition or the S floor until the clock catches up.
`StaticContains` keeps its process-clock contract; explicit-clock eligible counts
are distinct from retained `StatsSnapshot().StaticCalls`. Independent historical
support can still qualify the S floor while static membership is future.

Custom SCP keeps each receiver's newest observation for its call, band and mode
bucket. Historical support counts that receiver only when its own observation is
within `history_horizon_days`, is not later than the query clock and meets the
lookup's minimum age score. Geographic diversity counts distinct nonzero H3
cells from those same qualifying observations. One refreshed receiver cannot
refresh other receivers or their cells.

The existing score tiers are unchanged:

| Observation age | Score |
| --- | ---: |
| Up to 12 hours | 10 |
| Over 12 hours, up to 48 hours | 7 |
| Over 48 hours, up to 7 days | 5 |
| Over 7 days, up to 30 days | 3 |
| Over 30 days, up to 90 days | 2 |
| Over 90 days, up to 180 days | 1 |
| Over 180 days | 0 |

Age and future exclusion use stored-second precision. Each upper age boundary
is inclusive; an observation at the query's stored second is not future. A
future observation stays retained but cannot qualify until that clock reaches
it. Because the store retains only the newest observation, a future newest
timestamp does not reveal an older replacement for that receiver during lookup.

For example, at minimum score 5, two receivers last observed 54 and 55 days ago
plus one observed a minute ago provide one qualifying receiver, not three. Only
the fresh receiver's cell contributes. The older observations may remain in
retained history under the configured horizon and qualify for a lower-score
lookup.

Qualification filtering alone does not delete still-retained observations that
fail a score gate or are future. Queries preserve existing observation-horizon
pruning, empty-entry removal and expiry maintenance; periodic cleanup remains
unchanged. Retained-state statistics and active-call counts therefore describe
a different population from qualifying support.
Historical recognition is also separate from the resolver's current receiver
consensus. Neither historical counts nor these scores are calibrated correctness
probabilities, and recency does not prove that reports belong to one RF episode.
See [ADR-0285](../docs/decisions/ADR-0285-individual-historical-receiver-freshness.md).

## Related Files

- `spot.go` for the spot record and fixed-width formatter
- `correction.go` for correction gates and confidence inputs
- `recent_band.go` for bounded recent-support state
- `custom_scp_store.go` for persistent support storage

For the main operator summary, see [`../README.md`](../README.md). For parser-specific input behavior, see [`../rbn/README.md`](../rbn/README.md) and [`../pskreporter/README.md`](../pskreporter/README.md).

## US And Canadian Address Metadata

`CallMetadata.State` holds the canonical two-letter US mailing state/territory or Canadian province/territory
or an empty string when unknown. It is distinct from CTY operating-location
metadata and Maidenhead grids. Central ingest fills the spotter value; final
output processing clears/recomputes the DX value after correction, including
delayed release. Immutable sink snapshots retain both values. State enrichment
is independent of license enforcement; no spot wire-format column is added.
`state.go` owns the fixed 73-code shared vocabulary, strict 60-code FCC vocabulary, 17 FCC ADIF entities and Canadian ADIF 1/211/252.

ISED province evidence prefers club information, otherwise personal addresses.
Exact active special calls use their listed trustee; temporary prefixes use an
assigned ordinary base. Dates are inclusive UTC; prefix matches establish
plausibility without proving event eligibility. See
[ADR-0254](../docs/decisions/ADR-0254-canadian-ised-license-and-state-reuse.md).
