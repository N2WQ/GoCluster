# TSR-0008 - Resolver Neighborhood Forced-Split Regression From Unrelated Adjacent Buckets

Status: Resolved
Date Opened: 2026-02-26
Date Resolved: 2026-02-26
Owner: Cluster maintainers
Technical Area: internal/correctionflow, main output pipeline, cmd/rbn_replay, config
Trigger Source: Chat request
Led To ADR(s): ADR-0035
Tags: resolver-primary, neighborhood, replay-regression, split-gating

## Triggering Request

- Request date: 2026-02-26
- Request summary: replay performance declined after recent resolver-neighborhood changes; map replay runs to code deltas and propose a fix that keeps neighborhood benefits but avoids unrelated adjacent-bucket split forcing.
- Request reference (chat/issue/link): repository chat thread.

## Symptoms and Impact

- What failed or looked wrong?
  - Replay stability/recall regressed after neighborhood competition rollout.
  - `neighborhood_conflict_split` increased in scenarios where adjacent buckets held unrelated winners.
- User/operator impact:
  - Resolver-primary rejected otherwise-correct corrections due to forced split states.
  - Operators observed more duplicated/competing calls in output and lower trust in resolver outcomes.
- Scope and affected components:
  - `internal/correctionflow.SelectResolverPrimarySnapshot(...)`
  - main resolver-primary apply path (`main.go`)
  - replay policy mirror and comparison tooling (`cmd/rbn_replay/*`)
  - `call_correction` config defaults/sanitization

## Timeline

1. 2026-02-26 - Replay regressions reported for Feb 22 replays after neighborhood rollout.
2. 2026-02-26 - Root cause isolated to neighborhood competition admitting unrelated adjacent winners into split arbitration.
3. 2026-02-26 - Implemented anchor-scoped comparability rails, fail-closed override behavior, and replay observability for exclusions.

## Hypotheses and Tests

1. Hypothesis A - Neighborhood split arbitration is over-broad because unrelated winners are allowed to compete.
   - Evidence/commands: code inspection of `internal/correctionflow/shared.go` (neighborhood aggregation + split path) and replay deltas showing split growth.
   - Outcome: Supported.
2. Hypothesis B - Restricting neighborhood candidates to calls comparable to the subject preserves useful boundary merges while removing unrelated splits.
   - Evidence/commands: added call-anchored selection API and comparability rails; validated with new tests in `internal/correctionflow/shared_test.go`.
   - Outcome: Supported.
3. Hypothesis C - Replay comparability requires explicit exclusion counters to explain behavior shifts.
   - Evidence/commands: added replay AB metrics/tests and compare script columns for neighborhood exclusion reasons.
   - Outcome: Supported.

## Findings

- Root cause (or best current explanation):
  - Neighborhood competition was allowed to include winners that were not comparable to the subject signal identity; subsequent runner-up ratio checks could mark `split` even when the conflict was unrelated.
- Contributing factors:
  - Missing anchor contract between emitted subject call and neighborhood candidate admission.
  - Missing replay counters for why neighborhood candidates were excluded (unrelated/distance/anchor-missing).
- Why this did or did not require a durable decision:
  - Required a durable decision because resolver-neighborhood arbitration semantics, config contracts, and replay observability changed.

## Decision Linkage

- ADR created/updated:
  - `docs/decisions/ADR-0035-resolver-neighborhood-anchor-comparability-rails.md`
- Decision delta summary:
  - Neighborhood arbitration is now subject-anchored and comparability-gated.
  - Unrelated adjacent winners are excluded from winner override/split arbitration.
  - Replay/comparer now expose neighborhood exclusion reasons.
- Contract/behavior changes (or `No contract changes`):
  - Config contract changed with two neighborhood knobs:
    - `call_correction.resolver_neighborhood_max_distance`
    - `call_correction.resolver_neighborhood_allow_truncation_family`
  - Replay AB metrics schema extended with neighborhood exclusion counters.

## Verification and Monitoring

- Validation steps run:
  - `go test ./internal/correctionflow ./config ./cmd/rbn_replay -count=1`
  - `go test . -count=1`
  - `go test ./... -count=1`
  - `go vet ./...`
  - `staticcheck ./...`
  - `go test -race ./... -count=1`
- Signals to monitor (metrics/logs):
  - replay `resolver.neighborhood_conflict_split`
  - replay `resolver.neighborhood_excluded_unrelated`
  - replay `resolver.neighborhood_excluded_distance`
  - replay `resolver.neighborhood_excluded_anchor_missing`
  - runtime resolver rejection reason rates (`resolver_neighbor_conflict`, `resolver_state_split`)
- Rollback triggers:
  - if neighborhood override collapses materially while split counts remain high, or if correction apply-rate regresses without a corresponding precision gain.

## References

- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s):
  - `docs/decisions/ADR-0033-resolver-neighborhood-and-contested-edit-neighbor-rails.md`
  - `docs/decisions/ADR-0034-resolver-recent-plus1-corroborator-rail.md`
- Related docs:
  - `docs/call-correction-resolver-scope-ledger-v7.md`
  - `docs/rbn_replay.md`
  - `docs/troubleshooting-log.md`
  - `docs/decision-log.md`
