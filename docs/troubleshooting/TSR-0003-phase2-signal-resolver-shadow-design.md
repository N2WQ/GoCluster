# TSR-0003 - Phase 2 Signal Resolver Shadow-Mode Design and Validation Plan

Status: Open
Date Opened: 2026-02-23
Date Resolved: n/a
Owner: Cluster maintainers
Technical Area: spot/correction, main output pipeline
Trigger Source: Chat request
Led To ADR(s): ADR-0022
Tags: call-correction, resolver, shadow-mode, architecture

## Triggering Request

- Request date: 2026-02-23
- Request summary: Propose and prepare Phase 2 architecture for a signal-level resolver in shadow mode, including deterministic bounded execution and disagreement instrumentation.
- Request reference (chat/issue/link): Chat thread in this repository workspace.

## Symptoms and Impact

- What failed or looked wrong?
  - Per-spot correction decisions can appear contradictory under split evidence.
  - Tuning complexity increased from layered one-off rails.
- User/operator impact:
  - Reduced trust in correction determinism for edge clusters.
  - Slower troubleshooting due to coupled inference and delivery policy behavior.
- Scope and affected components:
  - `spot/correction.go`, `main.go`, stats/observability, decision docs.

## Timeline

1. 2026-02-23 - Phase 1 ambiguity guard and quality penalty rail accepted and implemented (ADR-0021).
2. 2026-02-23 - Phase 2 scope ledger proposed for shadow resolver.
3. 2026-02-23 - Architecture Note drafted for review (`docs/call-correction-phase2-architecture-note.md`).

## Hypotheses and Tests

1. Hypothesis A - Shadow resolver can be integrated without user-visible behavior change.
   - Evidence/commands: TBD during implementation.
   - Outcome: Inconclusive
2. Hypothesis B - Resolver disagreement telemetry can identify safe cutover criteria for Phase 3.
   - Evidence/commands: TBD during implementation.
   - Outcome: Inconclusive
3. Hypothesis C - Event-driven with per-key rate limit gives acceptable latency/cost balance.
   - Evidence/commands: TBD benchmark + production-like replay.
   - Outcome: Inconclusive

## Findings

- Root cause (or best current explanation):
  - TBD (active troubleshooting/design phase).
- Contributing factors:
  - TBD.
- Why this did or did not require a durable decision:
  - Expected to require a durable architecture decision due to shared hot-path impact and observability contract changes.

## Decision Linkage

- ADR created/updated:
  - `ADR-0022` (proposed, pending acceptance).
- Decision delta summary:
  - Introduce signal resolver in shadow mode with bounded resources and deterministic cadence.
- Contract/behavior changes (or `No contract changes`):
  - Planned for Phase 2: no user-visible behavior change (shadow-only).

## Verification and Monitoring

- Validation steps run:
  - TBD in implementation cycle.
- Signals to monitor (metrics/logs):
  - resolver queue drops, resolver state distribution, disagreement classes, agreement rate.
- Rollback triggers:
  - Any indication of hot-path blocking/regression from shadow integration.

## References

- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s):
  - `docs/decisions/ADR-0021-call-correction-ambiguity-guard-and-quality-rail.md`
  - `docs/decisions/ADR-0022-phase2-signal-resolver-shadow-mode.md`
- Related docs:
  - `docs/call-correction-phase2-architecture-note.md`
  - `docs/call-correction-phase2-shadow-runbook.md`
