# TSR-0005 - Resolver-Primary Recall Gap on Truncation Families and Overstated V Glyph in Contested States

Status: Resolved
Date Opened: 2026-02-26
Date Resolved: 2026-02-26
Owner: Cluster maintainers
Technical Area: main output pipeline, spot/correction, resolver-primary behavior
Trigger Source: Chat request
Led To ADR(s): ADR-0032
Tags: call-correction, resolver-primary, truncation, confidence-glyph

## Triggering Request

- Request date: 2026-02-26
- Request summary: improve ineffective call correction with examples (`VE3NNT` vs `UA3NNT` / `VE3NN`) and address contested variants both showing `V` (`K8AR` vs `K8AM`).
- Request reference (chat/issue/link): repository chat thread.

## Symptoms and Impact

- What failed or looked wrong?
  - Resolver-primary path missed family-truncation corrections that legacy correction rails could admit.
  - Resolver `split/uncertain` snapshots could still present `V`, overstating certainty for contested variants.
- User/operator impact:
  - Lower correction recall for short-vs-long family variants.
  - Ambiguous evidence could appear over-confident in operator-facing output.
- Scope and affected components:
  - `main.go` resolver-primary apply path and confidence glyph mapping.
  - `spot/correction.go` family-policy gate logic reuse.

## Timeline

1. 2026-02-26 - User reported missed corrections and contested dual-`V` observations.
2. 2026-02-26 - Root cause isolated to resolver-primary gate drift vs legacy family rails and split/uncertain glyph semantics.
3. 2026-02-26 - Resolver-primary family-gate parity helper + conservative glyph downgrade implemented and validated.

## Hypotheses and Tests

1. Hypothesis A - Resolver-primary omits truncation-family rails used by legacy correction.
   - Evidence/commands: inspected `main.go` resolver path and `spot/correction.go`; added `EvaluateResolverPrimaryGates` parity helper and tests.
   - Outcome: Supported.
2. Hypothesis B - Contested resolver states can surface as `V` and mislead certainty.
   - Evidence/commands: `resolverConfidenceGlyph` behavior inspection and test update (`TestResolverConfidenceGlyphFromSnapshot`).
   - Outcome: Supported.
3. Hypothesis C - Fix can be delivered without changing bounded-resource resolver contracts.
   - Evidence/commands: no resolver queue/state-cap changes; full checker suite pass.
   - Outcome: Supported.

## Findings

- Root cause (or best current explanation):
  - Resolver-primary final gate logic drifted from family-policy rails implemented in legacy correction (truncation advantage/bonus/delta-2 safeguards).
  - Confidence glyph mapping did not conservatively treat `split/uncertain` as contested evidence.
- Contributing factors:
  - Resolver-primary and legacy paths had different gate semantics at final admission time.
- Why this did or did not require a durable decision:
  - Required a durable decision due user-visible correction behavior and confidence semantics changes in resolver-primary mode.

## Decision Linkage

- ADR created/updated:
  - `docs/decisions/ADR-0032-resolver-primary-family-gate-parity-and-conservative-contested-glyphs.md`
- Decision delta summary:
  - Added shared resolver-primary family-sensitive gate evaluator.
  - Downgraded `split/uncertain` resolver confidence glyph outcomes to conservative `P/?`.
- Contract/behavior changes (or `No contract changes`):
  - User-visible behavior change in resolver-primary confidence semantics and family-truncation correction admission.

## Verification and Monitoring

- Validation steps run:
  - `go test ./... -run "TestResolverConfidenceGlyphFromSnapshot|TestMaybeApplyResolverCorrectionAppliesWinner|TestMaybeApplyResolverCorrectionHonorsMaxEditDistance|TestMaybeApplyResolverCorrectionHonorsDistance3ExtraRails|TestMaybeApplyResolverCorrectionAppliesTruncationLengthBonusParity|TestMaybeApplyResolverCorrectionHonorsTruncationDelta2ValidationRail"`
  - `go test ./...`
  - `go vet ./...`
  - `staticcheck ./...`
  - `go test -race ./...`
- Signals to monitor (metrics/logs):
  - resolver state distribution (`split`, `uncertain`, `probable`, `confident`),
  - correction apply/reject reason mix,
  - operator reports of contested variants and truncation misses.
- Rollback triggers:
  - sustained correction recall regression or unexpected correction pressure increase in resolver-primary mode.

## References

- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s):
  - `docs/decisions/ADR-0016-call-correction-family-aware-recent-and-delta2-rails.md`
  - `docs/decisions/ADR-0026-resolver-primary-switchover-mode.md`
  - `docs/decisions/ADR-0028-shared-correctionflow-main-replay.md`
- Related docs:
  - `docs/troubleshooting-log.md`
  - `docs/decision-log.md`
