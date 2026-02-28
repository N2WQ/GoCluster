# ADR-0044 - Resolver Bayesian Capped Gate Bonus for Distance-1/2 Near-Threshold Winners

Status: Accepted
Date: 2026-02-28
Decision Makers: Cluster maintainers
Technical Area: spot/correction, main output pipeline, cmd/rbn_replay, config
Decision Origin: Design
Troubleshooting Record(s): none
Tags: resolver-primary, recall, conservative-rails, replay-parity

## Context
- Resolver-primary already uses conservative hard gates and reliability-weighted support, but still misses some likely-correct 1-2 character differences near thresholds.
- Existing `resolver_recent_plus1` helps one-short min-reports cases, but does not address all tie-like advantage misses.
- Any recall improvement must preserve deterministic behavior, high accuracy, bounded resources, and runtime/replay parity.
- Custom SCP admission is V-only (ADR-0043), so recent-support priors should not create self-reinforcing loops through non-V admission.

## Decision
- Add an optional, default-off `call_correction.bayes_bonus` rail evaluated inside `EvaluateResolverPrimaryGates(...)`.
- Scope is limited to distance-1 and distance-2 pairs only.
- Compute a bounded Bayesian-style score from:
  - weighted-support ratio term (with smoothing and cap), and
  - recent-support ratio term (with smoothing, distance-weight scaling, and cap).
- Apply conservative caps:
  - report-gate bonus: at most `+1` effective support (min-reports gate only),
  - advantage-gate bonus: at most `+1` effective winner support, tie-break only.
- Keep all existing safety rails active:
  - max-edit/family rails, confidence rails, CTY/base-call validation, neighborhood conflict gates, and contested edit-neighbor disallow.
- Do not change custom SCP persistence schema in v1; use existing `RecentSupportCount` evidence.
- Add explicit runtime/replay reason labels and replay AB counters for Bayesian considered/applied/rejected outcomes.

## Alternatives Considered
1. Raise global recall by lowering base gates (`min_reports`, `min_confidence`, `min_advantage`)
   - Pros: simple.
   - Cons: broad accuracy risk across all candidate distances and modes.
2. Expand existing recent `+1` rail only
   - Pros: minimal change.
   - Cons: does not address advantage tie-break misses and remains less expressive than weighted+recent evidence fusion.
3. Add uncapped Bayesian score directly to ranking/gates
   - Pros: higher potential recall lift.
   - Cons: harder to reason about, higher over-correction risk, weaker operational predictability.

## Consequences
- Positive outcomes:
  - Improves recall for conservative 1-2 character near-threshold misses.
  - Preserves deterministic bounded behavior through strict caps and rails.
  - Provides clearer observability with dedicated reject/apply reasons and replay counters.
- Negative outcomes / risks:
  - Additional configuration and operator complexity.
  - Potential precision degradation if thresholds are tuned too aggressively.
- Operational impact:
  - New config block `call_correction.bayes_bonus.*` (default disabled).
  - New reason labels:
    - `resolver_applied_bayes_*`
    - `resolver_bayes_report_reject_*`
    - `resolver_bayes_advantage_reject_*`
  - Replay AB metrics now include `resolver.bayes_report_*` and `resolver.bayes_advantage_*`.
- Follow-up work required:
  - Validate on replay A/B before enabling in production.

## Validation
- Added/updated runtime + replay parity tests for:
  - Bayesian report bonus one-short admission,
  - Bayesian report reject (`score_below_threshold`),
  - Bayesian advantage tie-break admission,
  - AB metric bucket accounting.
- Added config default/override/sanitization coverage for `bayes_bonus`.
- Evidence that would invalidate this decision:
  - sustained precision regressions or unstable correction reason mix when enabled with conservative defaults.

## Rollout and Reversal
- Rollout plan:
  - Keep `call_correction.bayes_bonus.enabled=false` by default.
  - Evaluate with replay comparison first, then controlled runtime enablement.
- Backward compatibility impact:
  - None when disabled (default behavior preserved).
  - Config contract is extended with new optional keys.
- Reversal plan:
  - Set `call_correction.bayes_bonus.enabled=false`.
  - If superseded later, publish a new ADR and mark this one superseded.

## References
- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s):
  - `docs/decisions/ADR-0034-resolver-recent-plus1-corroborator-rail.md`
  - `docs/decisions/ADR-0036-resolver-confusion-tiebreak-runtime-replay-parity.md`
  - `docs/decisions/ADR-0039-custom-scp-runtime-evidence-and-shared-pebble-resilience.md`
  - `docs/decisions/ADR-0043-custom-scp-v-only-admission.md`
- Troubleshooting Record(s): none
- Docs:
  - `README.md`
  - `docs/OPERATOR_GUIDE.md`
  - `docs/rbn_replay.md`
  - `data/config/pipeline.yaml`
