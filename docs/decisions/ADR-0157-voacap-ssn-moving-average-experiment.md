# ADR-0157: VOACAP SSN Moving Average Experiment

- Status: Accepted
- Date: 2026-06-08
- Decision Origin: Design

## Context

VOACAP experimentation needs a near-real-time sunspot-number input before any
path-reliability integration is designed. The existing PowerShell experiment
uses NOAA SWPC `sunspot_report.json`, groups reports by observation timestamp,
computes a raw Wolf-number estimate as `10 * group_count + spot_count`, and
then computes an 8-hour moving average.

The repository does not currently contain SSN parsing or VOACAP launch logic.
Existing `solarweather` code handles GOES X-ray and Kp data only.

## Decision

Add an isolated Go experiment package and command for the SSN moving-average
slice only.

This decision intentionally does not add runtime polling, YAML configuration,
VOACAP launch behavior, H3 endpoint selection, caching, or path-reliability
fallback behavior. Those decisions require later ADRs because they affect
operator-visible behavior and path reliability semantics.

## Alternatives considered

1. Keep the PowerShell script outside the repo.
2. Add SSN polling directly to `solarweather.Manager`.
3. Add a broad VOACAP package with SSN, deck generation, launch, parsing, and
   path-reliability integration in one change.

## Consequences

### Benefits

- Gives the VOACAP work a tested Go calculation primitive inside the repo.
- Preserves deterministic fixture tests for NOAA parsing and 8-hour window
  semantics.
- Keeps runtime and operator contracts unchanged while experimentation
  continues.

### Risks

- NOAA JSON schema changes can make the manual command fail or drop malformed
  rows.
- The raw Wolf-number estimate remains a near-real-time approximation and is
  not an official corrected SSN.

### Operational impact

None for the live cluster. The new command is manual experiment tooling and
does not run in the production process.

## Links

- Related issues/PRs/commits: pending
- Related tests: `internal/voacap/sunspot_test.go`
- Related docs: none
- Related TSRs: none
- Supersedes / superseded by: none
