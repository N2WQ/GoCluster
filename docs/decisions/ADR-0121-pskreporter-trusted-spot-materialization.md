# ADR-0121: PSKReporter Trusted Spot Materialization

- Status: Accepted
- Date: 2026-05-07
- Decision Origin: Design

## Context
Post-deploy profiles after the telnet fan-out allocation patch showed that
PSKReporter spot materialization remained a dominant allocation and CPU surface.
The PSKReporter conversion path already normalizes and validates calls, mode,
frequency, grids, and timestamps before constructing a `spot.Spot`, but it then
used the general normalized constructor. That constructor still performed
general-purpose mode/callsign/default-time work that PSKReporter immediately
overrode or had already completed.

## Decision
Add a trusted normalized ingest constructor in `spot` for machine feeds that
already own canonical calls, canonical mode, observation time, and source
identity. Use it only in PSKReporter conversion for this change.

The constructor still preserves shared spot defaults that are part of the
runtime contract: 10 Hz frequency rounding, band derivation, normalized field
population, default TTL/report fields, UTC observation time, source identity,
human/skimmer classification, and one beacon-flag refresh.

## Alternatives considered
1. Keep using `NewSpotNormalized`.
   Rejected because it keeps repeated normalization/cache/time work in a
   high-volume path where the caller already owns those invariants.
2. Pool or reuse `spot.Spot` objects.
   Rejected because downstream ownership, delayed delivery, archive, dedupe,
   and fan-out lifetimes make pooling substantially higher risk.
3. Convert all `NewSpotNormalized` callers to the trusted constructor.
   Rejected because only PSKReporter was in the approved scope and each caller
   needs separate proof that its inputs are already canonical.

## Consequences
### Benefits
- PSKReporter conversion avoids repeated callsign/mode normalization and the
  constructor's current-time default before replacing it with source time.
- Source timestamp and machine-source classification are established at
  construction rather than corrected after construction.
- The narrow constructor gives future ingest optimizations an explicit contract
  instead of encouraging direct `spot.Spot` literals outside `spot`.

### Risks
- The trusted constructor relies on callers to pass canonical calls and mode.
  Misuse could bypass the safety provided by the general constructors.
- Allocation counts do not drop in the focused benchmark because the remaining
  allocations are the `Spot` object and normalized grid strings. This change
  is primarily duplicate-work removal, not object-lifetime reuse.

### Operational impact
- No telnet, peer, archive, HELP, config, filter, queue, or protocol behavior
  changes.
- Resource bounds are unchanged.
- If a future profile shows misuse or no useful benefit, PSKReporter can revert
  to `NewSpotNormalized` without data migration.

## Links
- Related issues/PRs/commits: current working tree
- Related tests:
  - `spot/spot_test.go`
  - `pskreporter/client_test.go`
- Related docs:
  - `docs/decisions/ADR-0117-hot-path-duplicate-work-removal.md`
- Related TSRs: none
- Supersedes / superseded by: none
