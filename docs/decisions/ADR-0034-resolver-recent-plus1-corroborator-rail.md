# ADR-0034 - Resolver-Primary Conservative Recent-On-Band +1 Corroborator Rail

Status: Accepted
Date: 2026-02-26
Decision Makers: Cluster maintainers
Technical Area: main output pipeline, spot/correction, internal/correctionflow, cmd/rbn_replay, config
Decision Origin: Troubleshooting chat
Troubleshooting Record(s): TSR-0007
Tags: resolver-primary, recent-band, gating, replay-parity

## Context

- Resolver-primary recall and stability are better than legacy consensus, but one-short cases still appear where the resolver winner misses `min_reports` by exactly one vote even when recent-on-band corroboration is strong.
- Directly porting legacy recent-band score inflation would reintroduce heuristic coupling and can over-correct contested signals.
- Any recent-on-band assist must preserve deterministic resolver behavior, bounded runtime cost, and replay/runtime policy parity.

## Decision

- Add a resolver-primary conservative recent-on-band `+1` corroborator rail in `EvaluateResolverPrimaryGates(...)`.
- Apply the rail only when winner effective support is exactly one short of `min_reports` (`effective_support == min_reports - 1`) after existing truncation-family length bonus logic.
- Apply `+1` only to the min-reports admission check. Do not change winner support used for advantage checks and do not relax confidence thresholds.
- Gate `+1` behind deterministic rails:
  - Enabled only when `resolver_recent_plus1_enabled` is true.
  - Winner/subject pair must pass proximity rails: mode-aware distance `<= resolver_recent_plus1_max_distance` or truncation-family relation when `resolver_recent_plus1_allow_truncation_family` is true.
  - Winner must have at least `resolver_recent_plus1_min_unique_winner` recent unique spotters on same band/mode.
  - When `resolver_recent_plus1_require_subject_weaker` is true, winner recent support must be strictly greater than subject recent support.
  - Disable `+1` for contested edit-neighbor winner contexts via `ResolverPrimaryGateOptions.RecentPlus1DisallowReason=edit_neighbor_contested`.
- Keep main/replay parity by evaluating the same resolver-primary gate helper and exporting the same reject/apply reasons into runtime and replay telemetry.
- Default rollout policy for v6:
  - `resolver_recent_plus1_enabled: true`
  - `resolver_recent_plus1_min_unique_winner: 3`
  - `resolver_recent_plus1_require_subject_weaker: true`
  - `resolver_recent_plus1_max_distance: 1`
  - `resolver_recent_plus1_allow_truncation_family: true`

## Alternatives Considered

1. Keep strict resolver gate without recent-on-band corroboration
   - Pros:
     - Simplest and lowest policy surface area.
   - Cons:
     - Leaves one-short misses unresolved even with strong corroborative recent evidence.
2. Port legacy recent-band bonus behavior as a broad score boost
   - Pros:
     - Higher immediate recall lift potential.
   - Cons:
     - Higher false-correction risk and tighter coupling to legacy heuristics; conflicts with resolver-only simplification.
3. Allow unconditional `+1` when one-short
   - Pros:
     - Very high recall impact.
   - Cons:
     - Unsafe for contested neighbors and weak-recent scenarios; likely over-apply behavior.

## Consequences

- Positive outcomes:
  - Improves resolver-primary recall in bounded one-short scenarios where recent corroboration is strong.
  - Keeps decision logic deterministic and explicit with reject reasons.
  - Preserves replay/runtime parity for rollout analysis.
- Negative outcomes / risks:
  - More policy knobs increase operator tuning complexity.
  - Mis-tuned thresholds can still over-apply or under-apply in edge conditions.
- Operational impact:
  - New config surface under `call_correction`.
  - New runtime decision reasons and replay counters for `recent_plus1` applied/rejected paths.
- Follow-up work required:
  - Use replay and burn-in windows to decide whether to keep defaults or tighten rails further.
  - Complete planned legacy-shadow retirement gates in phase P3.

## Validation

- Added/updated tests:
  - `main_test.go`:
    - `TestEvaluateResolverPrimaryGatesAppliesRecentPlus1OneShort`
    - `TestEvaluateResolverPrimaryGatesRejectsRecentPlus1WhenSubjectNotWeaker`
    - `TestMaybeApplyResolverCorrectionUsesRecentPlus1AppliedReason`
    - `TestMaybeApplyResolverCorrectionRejectsRecentPlus1WhenSubjectNotWeaker`
  - `config/call_correction_stabilizer_test.go`: defaults/overrides/sanitization for recent-plus1 knobs.
  - `cmd/rbn_replay/ab_metrics_test.go`: recent-plus1 replay counter breakdowns.
- Checker evidence:
  - `go test ./... -count=1`
  - `go vet ./...`
  - `staticcheck ./...`
  - `go test -race ./... -count=1`

## Rollout and Reversal

- Rollout plan:
  - Keep v6 defaults enabled as approved; verify via replay deltas and live decision-reason counters.
  - Watch for shifts in correction apply/reject mix and contested-neighbor behavior.
- Backward compatibility impact:
  - No protocol wire format changes.
  - Config schema extended with optional keys (defaults preserve deterministic behavior).
  - User-visible correction behavior can change (more one-short resolver corrections admitted under rails).
- Reversal plan:
  - Set `resolver_recent_plus1_enabled: false` to disable the rail immediately.
  - Tighten thresholds (`min_unique_winner`, `max_distance`, `require_subject_weaker`) if partial rollback is preferred.

## References

- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s):
  - `docs/decisions/ADR-0026-resolver-primary-switchover-mode.md`
  - `docs/decisions/ADR-0028-shared-correctionflow-main-replay.md`
  - `docs/decisions/ADR-0030-shared-stabilizer-policy-parity-main-replay.md`
  - `docs/decisions/ADR-0032-resolver-primary-family-gate-parity-and-conservative-contested-glyphs.md`
  - `docs/decisions/ADR-0033-resolver-neighborhood-and-contested-edit-neighbor-rails.md`
- Troubleshooting Record(s):
  - `docs/troubleshooting/TSR-0007-resolver-recent-plus1-corroborator-rails.md`
- Docs:
  - `docs/call-correction-resolver-scope-ledger-v6.md`
  - `docs/rbn_replay.md`
  - `docs/decision-log.md`
  - `docs/troubleshooting-log.md`
