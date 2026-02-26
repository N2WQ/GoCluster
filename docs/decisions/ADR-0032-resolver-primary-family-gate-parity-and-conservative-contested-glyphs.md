# ADR-0032 - Resolver-Primary Family-Gate Parity and Conservative Contested Confidence Glyphs

Status: Accepted
Date: 2026-02-26
Decision Makers: Cluster maintainers
Technical Area: main output pipeline, spot/correction
Decision Origin: Troubleshooting chat
Troubleshooting Record(s): TSR-0005
Tags: resolver-primary, call-correction, truncation, confidence

## Context
- Resolver-primary correction admission used a reduced gate set compared with legacy `SuggestCallCorrection` family-policy rails.
- Operators reported misses in family-truncation cases (for example shorter vs more-specific call variants).
- Resolver `split/uncertain` states could still render as `V`, overstating confidence for contested variants.
- Changes must preserve bounded resolver contracts and deterministic behavior.

## Decision
- Add a shared, pure evaluator in `spot/correction.go`:
  - `EvaluateResolverPrimaryGates(...)`.
- Resolver-primary final admission now applies family-sensitive rails aligned with legacy correction semantics:
  - truncation advantage relaxation (with validation rails),
  - truncation length bonus for min-reports,
  - truncation delta-2 validation/confidence rails.
- Keep resolver split/freq ambiguity handling unchanged; no queue/cap/hysteresis contract changes.
- Change resolver confidence glyph mapping for contested states:
  - `split` or `uncertain` now map conservatively to `P` (or `?` for single/no reporter), never `V`.

## Alternatives Considered
1. Keep current resolver-primary gate set and tune thresholds only
   - Pros:
     - Minimal code change.
   - Cons:
     - Leaves structural drift between resolver-primary and legacy family rails.
2. Re-run full legacy correction inside resolver-primary apply path
   - Pros:
     - Maximum parity.
   - Cons:
     - Duplicative work in hot path and larger complexity/observability ambiguity.
3. Defer changes until a broader resolver policy redesign
   - Pros:
     - Potentially cleaner long-term architecture.
   - Cons:
     - Leaves known user-visible misses and contested-confidence confusion unaddressed.

## Consequences
- Positive outcomes:
  - Better recall for resolver-primary truncation-family edge cases without broad threshold weakening.
  - More conservative and deterministic confidence display for contested evidence.
  - Reduced semantic drift between legacy and resolver-primary admission rails for family cases.
- Negative outcomes / risks:
  - Slight behavior change in resolver-primary apply decisions and confidence glyph distribution.
  - More gate complexity in resolver-primary apply path.
- Operational impact:
  - No config schema/protocol changes.
  - Expected increase in `P` under contested states where `V` previously appeared.
- Follow-up work required:
  - Monitor reason/glyph distributions after deployment and revisit broader parity scope if needed.

## Validation
- Added/updated tests:
  - `TestResolverConfidenceGlyphFromSnapshot`
  - `TestMaybeApplyResolverCorrectionAppliesWinner`
  - `TestMaybeApplyResolverCorrectionHonorsMaxEditDistance`
  - `TestMaybeApplyResolverCorrectionHonorsDistance3ExtraRails`
  - `TestMaybeApplyResolverCorrectionAppliesTruncationLengthBonusParity`
  - `TestMaybeApplyResolverCorrectionHonorsTruncationDelta2ValidationRail`
- Checker evidence:
  - `go test ./...`
  - `go vet ./...`
  - `staticcheck ./...`
  - `go test -race ./...`

## Rollout and Reversal
- Rollout plan:
  - Ship with existing config defaults and monitor resolver state/glyph distributions and correction reason mix.
- Backward compatibility impact:
  - No wire/protocol compatibility impact.
  - User-visible confidence semantics and some resolver-primary correction outcomes change.
- Reversal plan:
  - Revert resolver-primary gate helper wiring and contested-state glyph downgrade in `main.go`/`spot/correction.go`.

## References
- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s):
  - `docs/decisions/ADR-0016-call-correction-family-aware-recent-and-delta2-rails.md`
  - `docs/decisions/ADR-0026-resolver-primary-switchover-mode.md`
  - `docs/decisions/ADR-0028-shared-correctionflow-main-replay.md`
- Troubleshooting Record(s):
  - `docs/troubleshooting/TSR-0005-resolver-primary-family-recall-and-contested-glyph.md`
- Docs:
  - `docs/troubleshooting-log.md`
  - `docs/decision-log.md`
