# ADR-0026 - Resolver Primary Switchover Mode

Status: Accepted
Date: 2026-02-25
Decision Makers: Cluster maintainers
Technical Area: main output pipeline, spot/correction, config
Decision Origin: Troubleshooting chat
Troubleshooting Record(s): TSR-0004
Tags: call-correction, resolver, switchover, phase-3, rollback

## Context

- The resolver has been running in shadow mode and replay evidence is now used to drive phase-3 method selection.
- We need a production-safe way to make resolver the correction authority without removing rollback options.
- Switchover must preserve bounded-resource/non-blocking contracts and keep deterministic resolver semantics.

## Decision

- Add `call_correction.resolver_mode` with two values:
  - `shadow` (default): existing behavior; legacy correction applies, resolver stays observational.
  - `primary`: resolver applies corrections; legacy correction runs shadow-only for disagreement telemetry.
- In `primary` mode:
  - resolver states `split` and `uncertain` do not apply corrections,
  - confidence glyphs derive from resolver support (`?` for single/no support; otherwise `P`/`V`),
  - resolver-applied winner uses existing CTY/invalid-action rails (`C` on apply, `B` or suppress on invalid).
- Keep resolver ingest non-blocking/fail-open with existing bounded queue/cap contracts.

## Alternatives considered

1. Immediate hard cutover with no legacy shadow path
   - Pros: lower CPU and simpler runtime path.
   - Cons: reduced rollback visibility during rollout/canary windows.
2. Keep resolver shadow-only until default flip
   - Pros: zero production behavior change risk.
   - Cons: blocks operational cutover and does not validate live primary behavior.
3. Add multi-stage hybrid weighting between methods
   - Pros: potentially smoother transitions.
   - Cons: higher complexity, weaker determinism, and harder operator reasoning.

## Consequences

- Positive outcomes:
  - Enables controlled resolver cutover behind a single explicit mode switch.
  - Preserves default behavior (`shadow`) for existing deployments.
  - Maintains rollback by config revert to `shadow`.
- Risks:
  - `primary` mode adds CPU overhead when legacy shadow comparison is enabled.
  - New operator mode requires clear rollout discipline.
- Operational impact:
  - New config surface: `call_correction.resolver_mode`.
  - Existing resolver summary/disagreement telemetry remains usable.

## Links

- Troubleshooting:
  - `docs/troubleshooting/TSR-0004-resolver-primary-switchover-mode.md`
- Related ADRs:
  - `docs/decisions/ADR-0022-phase2-signal-resolver-shadow-mode.md`
  - `docs/decisions/ADR-0024-replay-dual-method-stability-metrics.md`
  - `docs/decisions/ADR-0025-replay-run-history-and-local-comparison.md`
- Related docs:
  - `docs/call-correction-phase3-switchover-ledger-v2.md`
