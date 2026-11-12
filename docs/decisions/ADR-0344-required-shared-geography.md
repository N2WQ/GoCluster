# ADR-0344: Required Shared Geography

- Status: Accepted
- Date: 2026-11-11
- Decision Origin: Troubleshooting chat

## Context

H3 initialization was gated by path prediction although GRID, NEARBY and
CustomSCP also consume its cells. Cold live startup with prediction disabled
left those consumers without mappings. Replay constructed CustomSCP without
initializing geography; prewarmed tests concealed that omission. ADR-0153 only
required tables when prediction was enabled.

## Decision

The owner selected required geography in Approved v6. Every live startup loads
both configured tables before downstream owners. Replay does so when call
correction and CustomSCP are enabled, before output directories, downloads or
scratch replacement. Its existing correction-disabled configuration error wins.

`InitRequiredH3Mappings` loads without generation, writing or alternate-directory
search. Each invocation reads each configured input once, limited to expected
bytes plus one: 6,737 for res1 and 47,057 for res2. It closes every opened handle,
reports read/close errors, and aggregates both table failures. Existing length
and count checks apply to these exact captured bytes. Both mappers build locally
before publication. No identity, uniqueness, sorting or checksum checks are added.

Validation precedes the existing process-wide Once on every call. Invalid inputs
do not consume Once; an earlier initialization/build failure remains sticky.
Different valid later directories do not replace established mappings. The
legacy generation-capable API remains available; production startup uses only
the strict API. Relative paths retain process-working-directory semantics.

Preserve H3 math, backend choice, file format/order, one-based proxy IDs, invalid
zero, persisted evidence and spot caches. Prediction off still disables learning,
maintenance, display and prediction commands. This decision does not repair
historical observations or claim full replay/live stabilizer parity.

## Alternatives considered

1. Warn and continue with prediction off: rejected by the owner because shared
   geography must be available rather than silently weakening its consumers.
2. Validate then call legacy initialization: rejected because cgo can regenerate
   a file removed between validation and the second read.
3. Lazy consumer initialization or a new geography package: unnecessary filesystem
   and lifecycle ownership expansion; existing startup and mapper owners suffice.

## Consequences

### Benefits

GRID, NEARBY and CustomSCP geography work without enabling path prediction.
Bounded reads and captured inputs give the same no-write behavior on all builds.

### Risks

Prediction-off deployments missing tables now fail startup. Operators must ship
both existing files. Existing count validation does not prove table identity or
geographic coverage. Fixed maps remain process-owned rather than reloadable.

### Operational impact

Repair the named configured files and restart after an initialization failure.
Use the explicit generator to prepare tables; startup never repairs them.

## Links

- Related tests: `pathreliability/h3required_test.go`, `internal/cluster/geography_startup_test.go`, `cmd/rbn_replay/geography_startup_test.go`.
- Related docs: `data/h3/README.md`, `data/config/README.md`, `docs/rbn_replay.md`.
- Related TSRs: [TSR-0128](../troubleshooting/TSR-0128-shared-geography-startup.md).
- Supersedes: ADR-0153 H3 enable-dependent startup requirement only.
