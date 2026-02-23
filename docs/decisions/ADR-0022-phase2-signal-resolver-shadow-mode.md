# ADR-0022 - Phase 2 Signal Resolver Shadow Mode

Status: Proposed
Date: 2026-02-23
Decision Makers: Cluster maintainers
Technical Area: spot/correction, main output pipeline, stats
Decision Origin: Troubleshooting chat
Troubleshooting Record(s): TSR-0003
Tags: call-correction, resolver, shadow-mode, bounded-resources, determinism

## Context

- Current correction decisions are per-spot and can produce locally reasonable but globally inconsistent outcomes under split evidence.
- Phase 1 added ambiguity guard and quality penalty safety rails, but core inference remains spot-level.
- We need low-risk architecture progression toward signal-level inference with no immediate behavior cutover.
- Resolver integration must preserve hot-path non-blocking contracts and bounded resources.

## Decision

- Introduce a new signal-level resolver in **shadow mode** for Phase 2.
- Resolver runs with a single-owner goroutine and bounded queue/state.
- Resolver ingest is non-blocking and fail-open (drop-on-full + metrics), with evidence snapshots captured pre-correction mutation.
- Evaluation cadence is event-driven with per-key rate limit plus periodic sweep.
- Resolver outputs are observational only in Phase 2:
  - no correction apply cutover,
  - no stabilizer cutover,
  - no confidence glyph semantic changes.
- Add disagreement telemetry between resolver and current correction path.

Detailed implementation constraints and defaults are defined in:
- `docs/call-correction-phase2-architecture-note.md`

## Alternatives Considered

1. Immediate cutover to resolver decisions in Phase 2
   - Pros:
     - Faster simplification.
   - Cons:
     - High regression risk without disagreement evidence.
2. Keep tuning current per-spot correction only
   - Pros:
     - Lowest short-term change risk.
   - Cons:
     - Does not address architectural decision-unit mismatch.
3. Timer-only resolver evaluation
   - Pros:
     - Simplified CPU envelope.
   - Cons:
     - Decision lag and weaker responsiveness.

## Consequences

- Positive outcomes:
  - Establishes bounded deterministic resolver foundation.
  - Produces data to evaluate safe Phase 3 cutover criteria.
  - Separates architecture migration from immediate user-visible changes.
- Negative outcomes / risks:
  - Additional shadow compute/storage overhead.
  - Temporary dual-model complexity during migration.
- Operational impact:
  - New shadow observability counters and disagreement metrics.
  - No protocol/wire behavior change in Phase 2.
- Follow-up work required:
  - Phase 3 cutover ADR based on disagreement and performance evidence.

## Validation

- Required checks for Phase 2 implementation:
  - `go test ./...`
  - `go vet ./...`
  - `staticcheck ./...`
  - `go test -race ./...`
- Required evidence:
  - resolver micro-bench,
  - hot-path benchmark guardrail,
  - disagreement metric summaries from replay/representative traffic.

## Rollout and Reversal

- Rollout plan:
  - Build resolver in shadow mode only, then monitor disagreement/perf.
- Backward compatibility impact:
  - No protocol or client behavior changes in Phase 2.
- Reversal plan:
  - Disable resolver ingest wiring and remove shadow metrics path.

## References

- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s):
  - `docs/decisions/ADR-0021-call-correction-ambiguity-guard-and-quality-rail.md`
- Troubleshooting Record(s):
  - `docs/troubleshooting/TSR-0003-phase2-signal-resolver-shadow-design.md`
- Docs:
  - `docs/call-correction-phase2-architecture-note.md`
  - `docs/call-correction-phase2-shadow-runbook.md`
