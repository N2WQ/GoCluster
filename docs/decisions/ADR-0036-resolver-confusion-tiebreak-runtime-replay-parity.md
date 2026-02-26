# ADR-0036 - Resolver Confusion Tie-Break and Runtime/Replay Reliability-Parity Wiring

Status: Accepted
Date: 2026-02-26
Decision Makers: Cluster maintainers
Technical Area: main output pipeline, cmd/rbn_replay, cmd/callcorr_reveng_rebuilt, internal/correctionflow, spot/signal_resolver
Decision Origin: Troubleshooting chat
Troubleshooting Record(s): TSR-0009
Tags: resolver-primary, replay-parity, confusion-model, reliability

## Context

- Resolver-primary paths are required to stay parity-aligned between runtime and replay so replay evidence remains rollout-valid.
- User-reported regression indicated prior reliability/confusion inputs were missing from resolver paths after refactor.
- Existing confusion modeling already informed legacy correction scoring, but resolver winner selection still used only weighted support and deterministic fallback ordering.
- Resolver changes must preserve bounded behavior and deterministic fallback when confusion evidence is disabled.

## Decision

- Adopt a phased decision for resolver-primary behavior:

1. Phase 1 parity wiring:
   - Restore propagation of spotter reliability maps and confusion model into resolver-primary correction settings in:
     - `main.go` runtime resolver apply path
     - `cmd/rbn_replay` replay resolver gate path
     - `cmd/callcorr_reveng_rebuilt` replay-equivalent resolver gate path
   - Preserve report/SNR evidence by extending resolver evidence payload with `Report` and wiring it from spot snapshots.

2. Phase 2 resolver winner tie-break:
   - Extend `spot.SignalResolver` ranking to apply confusion-model score only within the top tie cohort where:
     - weighted support is equal, and
     - support count is equal.
   - Use `confusion_model_weight` as the blend coefficient for tie resolution.
   - Keep deterministic fallback order unchanged when confusion model is absent or weight is zero.
   - Keep confidence-state classification based on weighted support contracts from ADR-0027.

## Alternatives Considered

1. Keep parity wiring only; no resolver confusion tie-break
   - Pros:
     - minimal behavioral change risk.
   - Cons:
     - unresolved recall opportunities in resolver ties.
2. Apply confusion bonus across all candidates (not tie-limited)
   - Pros:
     - stronger influence from confusion likelihood.
   - Cons:
     - higher behavior shift risk and lower interpretability.
3. Keep legacy deterministic fallback ordering only
   - Pros:
     - simplest deterministic behavior.
   - Cons:
     - ignores available model evidence in ambiguous top-tier cases.

## Consequences

- Positive outcomes:
  - Runtime and replay now consume the same reliability/confusion inputs for resolver-primary gating.
  - Resolver can break tied weighted/support cohorts using model-informed likelihood, improving recall potential in ambiguous cases.
  - Existing deterministic fallback remains active when confusion model is disabled.
- Negative outcomes / risks:
  - Non-zero confusion weight changes tie outcomes and may increase winner flips if weight is overtuned.
  - Additional ranking logic complexity in resolver core.
- Operational impact:
  - No config schema additions; existing reliability/confusion settings are now parity-enforced in resolver paths.
  - Operators should evaluate replay recall/stability impact before increasing confusion weight in production.
- Follow-up work required:
  - Continue replay shadow comparisons to tune confusion weight safely.

## Validation

- Added/updated tests:
  - `spot/signal_resolver_test.go`
    - `TestSignalResolverConfusionTieBreakEnabled`
    - `TestSignalResolverConfusionTieBreakDisabledUsesFallbackOrder`
  - `main_test.go` resolver apply-path signature and call-path coverage updates.
- Full checker evidence:
  - `go test . -count=1`
  - `go test ./...`
  - `go vet ./...`
  - `staticcheck ./...`
  - `go test -race ./...`

## Rollout and Reversal

- Rollout plan:
  - Keep confusion weight at conservative values and validate replay/runtime parity deltas before wider rollout.
- Backward compatibility impact:
  - No wire/protocol changes.
  - No config-key changes.
  - Resolver tie outcomes can differ when confusion model is enabled.
- Reversal plan:
  - Set `call_correction.confusion_model_weight` to `0` (or disable confusion model) to return to prior deterministic tie fallback.

## References

- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s):
  - `docs/decisions/ADR-0006-confusion-model-tie-break-ranking.md`
  - `docs/decisions/ADR-0027-resolver-reliability-weighted-voting.md`
  - `docs/decisions/ADR-0028-shared-correctionflow-main-replay.md`
  - `docs/decisions/ADR-0035-resolver-neighborhood-anchor-comparability-rails.md`
- Troubleshooting Record(s):
  - `docs/troubleshooting/TSR-0009-resolver-reliability-confusion-parity-gap.md`
- Docs:
  - `docs/decision-log.md`
  - `docs/troubleshooting-log.md`
