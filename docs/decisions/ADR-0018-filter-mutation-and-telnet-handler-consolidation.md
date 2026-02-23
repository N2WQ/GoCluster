# ADR-0018 - Filter Allow/Block Mutation Helper and Telnet Domain Handler Cleanup

Status: Accepted
Date: 2026-02-23
Decision Makers: Cluster maintainers
Technical Area: filter, telnet/filter command engine
Decision Origin: Design
Troubleshooting Record(s): none
Tags: refactor, filter, telnet, consistency

## Context
- The filter package repeated identical allow/block map mutation logic across multiple setters (band, mode, source, continents, zones, DXCC, confidence, path class, grid2).
- Duplicate mutation logic made it easy for `All*` and `BlockAll*` flag updates to drift.
- Telnet domain handler builders (`newContinentHandler`, `newZoneHandler`, `newDXCCHandler`, `newGrid2Handler`) still accepted snapshot callbacks that were no longer used.

## Decision
1. Add shared generic helpers in `filter` for map-backed allow/block updates:
   - map initialization
   - deny-first mutation
   - synchronized `All*` and `BlockAll*` maintenance.
2. Route all map-backed setter methods through the shared helper while keeping existing validation and normalization rules at each entrypoint.
3. Remove unused `snapshot` callback parameters from telnet domain handler builders and simplify call sites.
4. Add targeted tests covering key flag transitions to lock behavior.

## Alternatives considered
1. Keep per-setter duplicated logic.
   - Pros:
     - zero code movement.
   - Cons:
     - continued drift risk and repetitive maintenance.
2. Move shared helper to a cross-package utility.
   - Pros:
     - potential wider reuse.
   - Cons:
     - unnecessary API surface for a filter-internal invariant.
3. More aggressive telnet meta-builder consolidation.
   - Pros:
     - larger LOC reduction.
   - Cons:
     - higher risk of user-visible message/usage string regressions.

## Consequences
- Positive outcomes:
  - single mutation path for allow/block maps in `filter`.
  - lower chance of divergent `All*`/`BlockAll*` behavior.
  - simpler telnet handler registration by removing dead parameters.
- Risks:
  - helper bug could affect multiple filter domains at once.
- Operational impact:
  - no intended protocol or user-visible command response changes.
- Mitigation:
  - existing filter/telnet suites plus new targeted setter-flag tests.

## Links
- Related ADR(s): none
- Code:
  - `filter/filter.go`
  - `telnet/filter_commands.go`
- Tests:
  - `filter/filter_test.go`
  - `telnet/server_filter_test.go`
- Docs:
  - `docs/decision-log.md`
