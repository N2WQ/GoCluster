# ADR-0019 - Shared Spot Distance Core and Periodic Cleanup Runner

Status: Accepted
Date: 2026-02-23
Decision Makers: Cluster maintainers
Technical Area: spot/correction, spot cleanup lifecycle
Decision Origin: Design
Troubleshooting Record(s): none
Tags: spot, cleanup, concurrency, correction, weighted-distance

## Context
- `spot/correction` had duplicated CW/RTTY weighted edit-distance DP loops and duplicated weight-normalization configuration logic.
- Multiple spot components implemented near-identical periodic cleanup start/stop loops (`CorrectionIndex`, `FrequencyAverager`, `HarmonicDetector`, `CallCooldown`, `CallQualityStore`, `RecentBandStore`).
- Existing cleanup goroutines selected on struct fields that were also mutated during stop, creating avoidable race risk and lifecycle drift.

## Decision
1. Consolidate weighted distance internals in `spot/correction`:
   - shared rune-distance table lookup path
   - shared weighted pattern-cost DP
   - shared normalization of distance weight settings.
2. Introduce a shared spot-local periodic cleanup runner helper:
   - `startPeriodicCleanup` / `stopPeriodicCleanup`
   - captures the quit channel in the goroutine to avoid field-access races.
3. Keep existing cleanup semantics per component:
   - intervals, windows/TTLs, and lock ownership remain component-defined.
4. Keep call-correction model behavior unchanged (`plain`/`morse`/`baudot` selection and fallback cost semantics).

## Alternatives considered
1. Keep duplicated loops and lifecycle code.
   - Pros:
     - zero refactor risk.
   - Cons:
     - continued drift and repeated bug-fix effort.
2. Use a generic cleanup struct embedded in each component.
   - Pros:
     - stronger structural unification.
   - Cons:
     - broader API/field churn across multiple types.
3. Use function-pointer substitution callbacks in inner distance loops.
   - Pros:
     - very small code.
   - Cons:
     - avoidable per-cell call overhead in hot DP paths.

## Consequences
- Positive outcomes:
  - reduced duplication in call-distance and weight configuration code.
  - unified cleanup lifecycle behavior with safer stop semantics.
  - easier maintenance across spot bounded-memory components.
- Risks:
  - shared helper bugs affect multiple spot components.
- Operational impact:
  - no intended user-visible behavior changes.
  - cleanup cadence and bounded-resource policies remain unchanged.
- Mitigations:
  - added helper tests for start/stop idempotence and halt behavior.
  - full test/vet/static/race checker pass.

## Links
- Related ADR(s): none
- Code:
  - `spot/correction.go`
  - `spot/cleanup_runner.go`
  - `spot/frequency_averager.go`
  - `spot/harmonics.go`
  - `spot/cooldown.go`
  - `spot/quality.go`
  - `spot/recent_band.go`
- Tests:
  - `spot/cleanup_runner_test.go`
  - `spot/correction_test.go`
- Benchmarks:
  - `go test ./spot -run ^$ -bench BenchmarkSuggestCallCorrectionSlashPrecedence -benchmem`
- Docs:
  - `docs/decision-log.md`
