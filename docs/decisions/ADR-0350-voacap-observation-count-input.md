# ADR-0350: VOACAP Observation-count Input

- Status: Accepted
- Date: 2026-11-12
- Decision Origin: Design; exact Approved v6

## Context

The owner selected the former NOAA observation source with a 48-hour exponential
half-life, while retaining the daily forecasting and geometry repairs from
[ADR-0347](ADR-0347-voacap-flux-and-daily-forecast.md). This changes the solar-input
experiment; it does not establish that either source produces more accurate
glyphs. The old experiment parser can combine stations and count duplicate rows,
so it is not the runtime ingestion contract.

## Decision

Poll NOAA SWPC `sunspot_report.json` every 1800 seconds by default. Reduce usable
rows into distinct groups within one observation timestamp, observatory and
station. Calculate the local index `R = 10 * groups + spots`. Do not combine
simultaneous competing stations, deduplicate by Region, infer zero from missing
rows, or invent revision precedence. Reject ambiguous/conflicting or malformed
representations; an identifiable bad aggregate does not discard unrelated usable
aggregates. Exact duplicate groups count once. Corrections present at first
ingestion may contribute when unambiguous; corrections to an already accepted
timestamp remain deferred.

Replay usable observations oldest first at cold start. Retain full precision:
`M_new = R + (M_old - R) * 2^(-elapsed / half_life)`, with default half-life
172800 seconds. Seed the first observation directly and round only the final
SSN sent to forecasts. Repeated bodies and HTTP 304 responses do not advance the
observation clock. There is no blend, flux conversion, or percentage hold.

The index describes validated reported rows. It is not official smoothed
International SSN, a calibrated station network, a completeness guarantee for
the solar disk, or a demonstrated predictor of glyph accuracy. The JSON feed's
explicit spotless-report encoding is not established. Absence is unavailable
input, not evidence of SSN zero. Manual Wolf experiment helpers remain separate
and unchanged.

Monitor continuity is version 3 with source/URL, count units, estimator and
reduction-policy version, and half-life identity. Incompatible versions 1 and 2
and their HTTP validators are discarded. One bounded state record, at most
64 KiB, retains the last compatible observation and EWMA, without history.
Continue using the last good compatible input after errors; warn at 48 hours
of observation age, including after unchanged or failed polls. Never-observed
input is missing immediately. On an upgrade outage, existing valid forecasts
can serve but new jobs wait for valid count input.

Forecast schema and model generation remain 2. Keep valid geometry-correct
legacy flux forecasts with their actual SSN, source-specific SFU provenance,
original coverage and expiry. New count provenance uses explicit count fields;
unknown provenance remains unknown. Source-tagged serialization rejects mixed
or unsupported encodings. The in-memory provenance remains pointer-free at
80 bytes, its SSN snapshot 88 bytes, within the existing forecast metadata budget.

All forecast and glyph contracts from ADR-0347 remain: a requested rounded SSN
change can start replacement through normal admission, an unchanged numerical
SSN across a source transition does not alone start a job, final-hour requested
renewal still runs, valid prior forecasts serve until their fixed expiry, and
idle paths create no work. One worker, queue 1000, both 50000-entry map caps,
stable path keys, at most 24 hourly rows, glyph thresholds and REL rules remain.

Remove `ssn_recent_flux_weight` from runtime configuration and reject it with
actionable migration guidance, including YAML merges and aliases. The removed
`recompute_delta_percent` remains rejected. No dual-source runtime selector is
introduced. Existing fetch, half-life, staleness, window and lifetime settings
remain operator-owned.

## Alternatives considered

1. Keep the flux blend. Rejected by the owner for this experiment.
2. Restore the old parser unchanged. Rejected because duplicate and mixed-station
   rows can change the count independently of the intended source selection.
3. Purge all saved forecasts on source change. Rejected: valid corrected-geometry
   forecasts can remain useful and their provenance can stay truthful.
4. Add a dead band or optimize engine throughput. Deferred; no validated numerical
   threshold or new performance claim is part of this change.

## Consequences

### Benefits

- Uses the selected observation source with a defined smoothing response.
- Preserves valid forecasts and current scheduling, resource and glyph contracts.
- Distinguishes observation counts from legacy flux in saved provenance.

### Risks

- Station differences, incomplete reports and deferred corrections can affect the
  local index. Conservative refusal can delay fresh input.
- Changed SSN can change VOACAP output and therefore fallback glyphs; neither
  functional tests nor this source choice prove physical forecast accuracy.
- Queue delays or engine failures can still leave gaps after fixed expiry.

### Operational impact

- Remove the retired blend key before startup. Monitor state reseeds from NOAA;
  do not relabel old state or share per-node state/cache paths between processes.
- A first-start NOAA outage prevents new forecasts until usable observations
  arrive. Existing valid generation-2 forecasts keep their original lifetime.
- Deployment, restart, publication and performance campaigns are outside this
  implementation authorization.

## Links

- [Approved scope and execution evidence](../voacap-sunspot-v6-execution.md).
- [Operator configuration](../../data/config/README.md).
- [Path-reliability contract](../../pathreliability/README.md).
- [NOAA JSON source](https://services.swpc.noaa.gov/json/sunspot_report.json).
- [NOAA-hosted AFMAN 15-124 report documentation](https://www.ngdc.noaa.gov/stp/space-weather/solar-data/solar-features/sunspot-regions/usaf_mwl/documentation/usaf_meteorological-codes_afman15-124.pdf).
  Its raw report format informs station/report identity; it is not a schema or
  completeness promise for the current JSON service.
- Supersedes only the solar-input and monitor-continuity clauses of
  [ADR-0347](ADR-0347-voacap-flux-and-daily-forecast.md); its forecast, geometry,
  resource, diagnostics and glyph contracts remain accepted.
