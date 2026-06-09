# ADR-0159: VOACAP YAML-Owned SSN Forecast Experiment

- Status: Accepted
- Date: 2026-06-08
- Decision Origin: Design

## Context

ADR-0157 added NOAA SSN parsing and moving-average primitives. ADR-0158 added
an isolated VOACAP process wrapper. The next experiment needs to combine those
pieces into an observer that can fetch SSN data, smooth fresh observations, and
run VOACAP when the smoothed value differs enough from the last successful
forecast.

The experiment still must not change production path-reliability behavior.
However, fetch cadence, EWMA half-life, recompute delta, forecast duration,
VOACAP timeout/home, state path, output naming, and tested center frequencies
are operationally important experiment values. Keeping them as command flags or
hidden Go defaults would make week-long observations hard to reproduce.

## Decision

Add `data/config/voacap_experiment.yaml` as optional tool config. The normal
server config loader recognizes the file name but does not merge it into
`config.Config`. The VOACAP experiment command loads it strictly at the tool
boundary and requires every experiment-owned key to be present and non-null.

Add an experiment-only state machine that:

- avoids EWMA updates for unchanged HTTP fetches and stale observations;
- updates the 8-hour half-life EWMA only for fresh observation timestamps;
- triggers the first forecast unconditionally;
- triggers later forecasts when EWMA delta reaches the YAML-owned percent;
- retries unchanged or stale input when no successful forecast baseline exists;
- updates the forecast SSN baseline only after a successful VOACAP run.

The experiment command builds a fixed Boston-to-Warsaw baseline deck using the
configured forecast hours and center frequencies. H3 endpoint selection,
VOACAP output parsing, forecast caching, and runtime path-reliability
integration remain out of scope.

## Alternatives considered

1. Keep all experiment values as command-line flags.
   - Rejected because a week-long observation should be reproducible from
     checked-in config rather than shell history.
2. Merge VOACAP experiment settings into server runtime config.
   - Rejected because the experiment is not production startup behavior.
3. Update the last forecast baseline before invoking VOACAP.
   - Rejected because a failed VOACAP run would hide the need to retry at the
     same smoothed SSN.

## Consequences

### Benefits

- Makes the SSN forecast experiment reproducible and reviewable from YAML.
- Preserves the startup config allowlist while keeping the file optional.
- Keeps failed VOACAP runs visible because the recompute baseline advances only
  after success.

### Risks

- The baseline deck uses fixed endpoint coordinates, so performance and output
  are useful for state-machine experimentation only.
- The `12%` default still uses a relative denominator; very low SSN behavior
  may need a later floor decision.
- Long-running evidence is still operator-observed rather than runtime
  confirmed in production.

### Operational impact

None for the live cluster. The new command is manual experiment tooling and is
not called by production path reliability.

## Links

- Related tests: `internal/voacap/config_test.go`,
  `internal/voacap/forecast_state_test.go`,
  `cmd/voacap_ssn_forecast_watch/main_test.go`
- Related docs: `data/config/README.md`
- Related ADRs: ADR-0157, ADR-0158, ADR-0067, ADR-0153
- Related TSRs: none
- Supersedes / superseded by: none
