# ADR-0347: VOACAP Flux Input and Daily Forecast Lifetime

- Status: Accepted
- Date: 2026-11-12
- Decision Origin: Design and troubleshooting; exact Approved v4

Current-contract note: [ADR-0350](ADR-0350-voacap-observation-count-input.md)
supersedes only the solar-input and monitor-continuity clauses below. Forecast
lifetime, renewal, geometry, migration, resource and glyph decisions remain
accepted. This record preserves the original flux experiment and its evidence.

## Context

The former runtime input used observatory counts, an eight-hour EWMA and a 12%
hold. That can suppress day-scale changes. An SSN-bearing cache key can strand
usable forecasts whenever input changes. An eight-hour window also requires
more frequent renewal. Qualification found a separate shared native CIRCUIT
column error that can change receiver longitude; old predictions cannot be
trusted merely because they remain within their prior lifetime.

The design must preserve nonblocking requests, one worker, bounded ownership,
and the existing p50/fallback classification rules. Deployment, general model
recalibration, CLOSED policy changes, all-band engine reuse, and the manual
Wolf experiment's input policy are outside this decision.

## Decision

### Solar input

Use NOAA SWPC `f107_cm_flux.json`, accepting only 2800 MHz `Noon` records. Keep
a floating-point recent flux EWMA using actual elapsed observation time and a
default half-life of two days. Blend it with the latest published 90-day mean
before applying the inverse of `F = 63.7 + 0.728R + 0.00089R²`. The recent
share is configured by `ssn_recent_flux_weight`, default `0.5`; valid endpoints
zero and one select a single component. Round only the final SSN. Valid
nonnegative blend values at or below 63.7 SFU produce SSN zero.

This is a daily model-input surrogate, not an official smoothed sunspot count
or a calibration claim. Every rounded SSN change can refresh requested paths;
the percentage hold is removed rather than replaced by an unmeasured dead band.

Track flux and mean independently. A valid new flux advances the recent EWMA
when its mean is absent, retaining the last accepted mean and its true timestamp.
The first late valid mean for an accepted Noon date can update input without
recounting flux. Already accepted component corrections are deferred. Cold
start replays accepted feed records chronologically. Do not reuse old
observatory-count state or its conditional validators.

Persist compatible bounded continuity (64 KiB maximum), including component
watermarks and the last accepted complete input. Compatibility includes source,
URL, units, conversion and smoothing parameters. Known contributing components
warn separately when their observation ages reach the default 48 hours, on
every poll outcome, including errors and HTTP 304. Warn/recover on transitions;
continue the last good input. A never-observed component is missing immediately
without an invented age or startup timer. A job copies one coherent SSN and
numeric provenance snapshot at dequeue.

### Geometry and migration

Serialize CIRCUIT fields to the native reader's fixed-column contract. Remove
the extra space before receiver longitude. Validate native body geometry as
well as input and parsed rows; an input echo alone cannot prove geometry.
See [TSR-0131](../troubleshooting/TSR-0131-voacap-receiver-longitude-columns.md).

Retire every model-generation-1 forecast. Never serve those predictions,
including after deletion failure or restart. Valid unexpired recognized legacy
keys may seed one-time immediate ordinary admission in the existing bounded
delay map. Eligibility uses the confirmed original eight-hour forecast window
and eight-hour cache lifetime, without extending either. Markers retain no
predictions and enqueue no startup work.
Successful queue admission consumes one; queue-full preserves it. Ordinary
failure cooldown applies after an attempted run. Lost or evicted markers
revert to normal cold-path warm-up.

### Forecast identity and lifetime

Use a stable path key: user cell, DX cell, band, frequency and direction.
Maintain one compact payload per key, with at most 24 unique hourly rows and
at most 128 additional bytes of validity/input metadata. No second forecast
cache, historical generations or migration registry is added.

The default forecast covers 24 absolute UTC hours from the hour containing
actual worker start. Coverage ends exactly 24 hours later; default TTL is
86400 seconds. Reads, replacements, failures and restarts never reset an old
forecast's coverage or expiry. Run canonical native `TIME 1..24` for each needed
calendar month in both directions, then select the owned absolute rows.
Ordinary windows need two engine runs; month/year crossings need four.

Keep a usable current-model forecast while replacement runs, including when
its SSN differs from current input. Ordinary requests with more than zero and
at most one hour left start renewal without initial warm-up, even if SSN is
unchanged. Failure cooldown remains. SSN-driven ordinary refresh outside that
final hour retains the ordinary delay. Cold ordinary paths retain the shipped
600-second delay. No timers or idle-path refresh are added. SHOW PROP retains
its empty/partial-horizon immediate refresh and bounded wait. Observation-only
lookups and diagnostics never enqueue.

### Ownership and observability

Retain one worker, queue capacity 1000, and 50,000-entry forecast and delay
limits; migration markers share the latter bound. One pending owner per path
coalesces requests. Requests during an active job share its frozen run; only a
later scheduling request can drive another replacement. Completion identity
and original validity prevent stale publication, without ordering by numeric
SSN magnitude.

Publish memory and notify waiters before persistence. Persist outside the
fallback mutex, keeping worker busy until persistence finishes. Completed
cycle count, sum and maximum include that whole cycle. Keep warmed hits free
of allocations and preserve the approved physical-heap and protected timing
guards; passing functional tests is insufficient evidence of performance.

Keep the cheap snapshot O(1). A dedicated, allocation-free bounded scan once
per overview uses one captured `(now, SSN, known)` input. Count only usable
forecasts with a represented current hour and original valid coverage/expiry.
Report the number using a different SSN as inventory, not backlog. Unknown SSN
remains unknown; zero is valid. Also show queue depth, oldest waiting age,
current worker busy time and completed cycle durations. SHOW PROP identifies
actual cached SSN and reports `ssn=mixed` with per-band SSN when needed.

## Alternatives considered

1. Add a small SSN dead band. No validated threshold exists; retain every
   rounded change and measure capacity first.
2. Preserve legacy forecasts within their eight-hour limits. Rejected after
   native geometry qualification showed their coordinates may be wrong.
3. Expire forecasts and wait for an ordinary cold restart. Requested final-hour
   renewal reduces avoidable gaps without idle work; failure can still cause gaps.
4. Queue every cached path at each solar update or use additional workers.
   Rejected in favour of request-driven bounded work and one worker.
5. Extend old expiry until replacement succeeds. Rejected because it would
   claim coverage beyond the forecast's original validity.

## Consequences

### Benefits

- Requested paths can follow daily solar-input changes while usable forecasts
  remain visible during replacement.
- One stable path payload bounds retained generations and preserves truthful
  input and expiry through restart.
- Native calendar and coordinate checks prevent plausible-looking output from
  masking the wrong circuit or month.

### Risks

- More hourly rows and daily replacements increase retained memory and work.
  Month crossings double native runs. Real worker demand must fit capacity.
- No available production demand trace establishes queue feasibility or glyph
  improvement. Native speed and local fixtures cannot certify production.
- Migration deliberately removes old predictions, so rebuild queueing can
  temporarily reduce forecast availability.
- Daily flux blending and adjacent-SSN comparisons do not establish physical
  forecast accuracy or a universally meaningful change threshold.

### Operational impact

New defaults are 172800 seconds EWMA half-life, `0.5` recent weight, 172800
seconds stale warning, 24 forecast hours and 86400 seconds TTL. The removed
`recompute_delta_percent` key is rejected. Update effective operator overlays.
Do not share state/DB paths between running nodes. This decision authorizes no
deployment, restart, merge or push. Protected timing controls, retained heap,
worker-load qualification and complete validation remain release evidence,
not assumptions implied by this accepted decision.

## Implementation and evidence map

| Approved concern | Current implementation/evidence surface |
| --- | --- |
| Flux conversion, replay, state and health | `internal/voacap/solar_flux*`, `solar_input.go`, `sunspot_monitor*` |
| Native CIRCUIT contract | `internal/voacap/deck.go`, `deck_circuit_test.go`; TSR-0131 |
| Absolute calendar rows | `pathreliability/voacap_forecaster*` and calendar ownership tests |
| Stable ownership, renewal and retirement | `pathreliability/voacap_fallback*`, forecast cache store, `voacap_v4_state_test.go` |
| Coherent overview and cached input labels | `internal/cluster/bootstrap.go`, overview tests, `telnet/show_prop*` |
| Resource/performance qualification | live-cache qualification and daily retained-heap fixtures; frozen baseline and paired controls required |

The production-builder native gate passed 36 isolated invocations, 192 rolling
windows and ten negative witnesses. Broader functional and performance evidence
must be reported separately; no blanket validation or production-capacity
verdict is recorded here.

## Links

- [Local implementation and qualification record](../voacap-flux-daily-execution.md).
- [Runtime path documentation](../../pathreliability/README.md),
  [operator guide](../OPERATOR_GUIDE.md), and
  [effective config guidance](../../data/config/README.md).
- [TSR-0131](../troubleshooting/TSR-0131-voacap-receiver-longitude-columns.md).
- Refines/supersedes affected clauses of ADR-0161/0162/0164 (input, key and
  lifetime), ADR-0182 (state identity), ADR-0184 (restore compatibility),
  ADR-0190/0191 (diagnostics), and ADR-0304 (validity metadata and migration).
  Their unrelated classification, persistence and observability decisions remain.
- Retains ADR-0173 REL/classification, ADR-0177 distance-selected method,
  ADR-0180 output cleanup and ADR-0303 deterministic persisted retention.
- The manual Wolf experiment policy in ADR-0157/0159 remains separate.
