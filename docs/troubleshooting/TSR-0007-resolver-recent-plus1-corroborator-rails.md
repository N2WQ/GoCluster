# TSR-0007 - Resolver-Primary One-Short Misses and Conservative Recent +1 Corroborator Rail

Status: Resolved
Date Opened: 2026-02-26
Date Resolved: 2026-02-26
Owner: Cluster maintainers
Technical Area: main output pipeline, spot/correction, internal/correctionflow, cmd/rbn_replay, config
Trigger Source: Chat request
Led To ADR(s): ADR-0034
Tags: resolver-primary, recent-band, one-short, replay-parity

## Triggering Request

- Request date: 2026-02-26
- Request summary: revisit deferred non-port item N5 and design a resolver-forward, conservative recent-on-band corroborator bonus; approved as scope ledger v6.
- Request reference (chat/issue/link): repository chat thread.

## Symptoms and Impact

- What failed or looked wrong?
  - Resolver-primary still rejected some likely-correct winners that were exactly one report short of `min_reports` despite strong recent-on-band corroboration.
  - Similar calls could still leak when strict min-report gating under-admitted near-threshold winners.
- User/operator impact:
  - Correct calls could remain uncorrected in one-short situations, reducing perceived recall and consistency.
  - Operators needed explicit reason counters to separate safe recent corroboration from over-aggressive score inflation.
- Scope and affected components:
  - resolver-primary gate evaluation in `spot/correction.go`,
  - runtime apply reason wiring in `main.go`,
  - replay policy parity and AB metrics in `cmd/rbn_replay/*`,
  - config schema/defaults in `config/config.go` and pipeline config.

## Timeline

1. 2026-02-26 - User requested reconsideration of N5 (recent-band bonus policy) and asked for conservative design.
2. 2026-02-26 - Scope ledger v6 approved with resolver-native recent `+1` approach and guard rails.
3. 2026-02-26 - Implemented runtime/replay parity, config knobs/defaults, reason telemetry, tests, and docs.

## Hypotheses and Tests

1. Hypothesis A - A bounded one-short `+1` corroborator can improve recall without reintroducing legacy heuristic drift.
   - Evidence/commands:
     - Implemented one-short-only `recent_plus1` gate in `EvaluateResolverPrimaryGates(...)`.
     - Added apply/reject reason tests in `main_test.go`.
   - Outcome: Supported.
2. Hypothesis B - Safety rails (distance/family, min recent winner support, subject weaker, contested-neighbor disallow) are required to avoid over-correction.
   - Evidence/commands:
     - Implemented reject rails and disallow hook.
     - Added reject-path tests and replay reject-counter breakdown tests.
   - Outcome: Supported.
3. Hypothesis C - Replay must classify the same gate policy to keep rollout evidence trustworthy.
   - Evidence/commands:
     - Added shared replay gate evaluation path and recent-plus1 replay counters.
     - Updated replay docs/compare output and validated tests.
   - Outcome: Supported.

## Findings

- Root cause (or best current explanation):
  - Strict min-reports-only admission in resolver-primary left no bounded mechanism to use recent corroboration for one-short winners.
- Contributing factors:
  - Prior policy intentionally deferred legacy recent-band bonus due score-inflation risk, leaving a recall gap for safe one-short cases.
- Why this did or did not require a durable decision:
  - Required a durable decision because it changes resolver-primary admission semantics, config contracts, and replay/runtime observability.

## Decision Linkage

- ADR created/updated:
  - `docs/decisions/ADR-0034-resolver-recent-plus1-corroborator-rail.md`
- Decision delta summary:
  - Added conservative resolver recent `+1` corroborator rail with strict rails and explicit reject reasons.
  - Added config knobs/defaults and replay/runtime parity instrumentation.
- Contract/behavior changes (or `No contract changes`):
  - Config contract changed (new `call_correction.resolver_recent_plus1_*` keys).
  - Runtime decision reasons and replay counters extended.
  - User-visible correction behavior can change when one-short winners satisfy corroboration rails.

## Verification and Monitoring

- Validation steps run:
  - `go test ./... -count=1`
  - `go vet ./...`
  - `staticcheck ./...`
  - `go test -race ./... -count=1`
- Signals to monitor (metrics/logs):
  - runtime decision reasons:
    - `resolver_applied_recent_plus1`
    - `resolver_applied_neighbor_recent_plus1`
    - `resolver_recent_plus1_reject_*`
  - replay AB counters:
    - `resolver.recent_plus1_applied`
    - `resolver.recent_plus1_rejected`
    - `resolver.recent_plus1_reject_*` breakdown.
- Rollback triggers:
  - sustained increase in likely false corrections or contested-neighbor instability after enabling/keeping defaults.

## References

- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s):
  - `docs/decisions/ADR-0026-resolver-primary-switchover-mode.md`
  - `docs/decisions/ADR-0032-resolver-primary-family-gate-parity-and-conservative-contested-glyphs.md`
  - `docs/decisions/ADR-0033-resolver-neighborhood-and-contested-edit-neighbor-rails.md`
  - `docs/decisions/ADR-0034-resolver-recent-plus1-corroborator-rail.md`
- Related docs:
  - `docs/call-correction-resolver-scope-ledger-v6.md`
  - `docs/rbn_replay.md`
  - `docs/troubleshooting-log.md`
  - `docs/decision-log.md`
