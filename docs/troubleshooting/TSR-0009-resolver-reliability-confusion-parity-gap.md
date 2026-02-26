# TSR-0009 - Resolver Reliability/Confusion Parity Gap and Winner Tie-Break Integration

Status: Resolved
Date Opened: 2026-02-26
Date Resolved: 2026-02-26
Owner: Cluster maintainers
Technical Area: main output pipeline, cmd/rbn_replay, cmd/callcorr_reveng_rebuilt, internal/correctionflow, spot/signal_resolver
Trigger Source: Chat request
Led To ADR(s): ADR-0036
Tags: resolver-primary, replay-parity, spotter-reliability, confusion-model

## Triggering Request

- Request date: 2026-02-26
- Request summary: user reported resolver code no longer included spotter reliability and confusion-model data that existed previously; requested phased restoration where phase 1 restores parity wiring and phase 2 adds confusion model to resolver winner selection with runtime/replay parity.
- Request reference (chat/issue/link): repository chat thread.

## Symptoms and Impact

- What failed or looked wrong?
  - Resolver-primary gating was no longer consistently wired with spotter reliability maps and confusion model in all execution paths.
  - Resolver winner selection did not consume confusion-model evidence during top-tier ties.
- User/operator impact:
  - Runtime/replay behavioral drift risk increased for correction scoring rails.
  - Recall opportunities were missed in tie scenarios where confusion likelihood could resolve ambiguity.
- Scope and affected components:
  - `main.go` resolver-primary apply path
  - `cmd/rbn_replay` resolver-primary gate evaluation
  - `cmd/callcorr_reveng_rebuilt` replay-equivalent resolver gate path
  - `internal/correctionflow.BuildResolverEvidenceSnapshot`
  - `spot/signal_resolver.go`

## Timeline

1. 2026-02-26 - User reported parity regression and requested phased restoration.
2. 2026-02-26 - Code inspection confirmed missing reliability/confusion wiring and missing resolver confusion tie-break.
3. 2026-02-26 - Implemented parity wiring and resolver tie-break integration; validated with full checker baseline plus race.

## Hypotheses and Tests

1. Hypothesis A - Resolver-primary settings in runtime/replay are missing reliability/confusion inputs after recent refactors.
   - Evidence/commands:
     - inspected `main.go`, `cmd/rbn_replay/main.go`, `cmd/rbn_replay/pipeline.go`, `cmd/callcorr_reveng_rebuilt/main.go`, `internal/correctionflow/shared.go`.
   - Outcome: Supported.
2. Hypothesis B - Adding parity wiring alone restores data availability but does not change resolver winner selection behavior.
   - Evidence/commands:
     - wired reliability/confusion through resolver-primary settings builders and replay/runtime call sites.
     - verified build and test consistency with `go test ./...`.
   - Outcome: Supported.
3. Hypothesis C - Resolver tie scenarios can be resolved deterministically by applying confusion-model score only within top tie cohort.
   - Evidence/commands:
     - implemented tie-cohort confusion scoring in `spot/signal_resolver.go`.
     - added tests `TestSignalResolverConfusionTieBreakEnabled` and `TestSignalResolverConfusionTieBreakDisabledUsesFallbackOrder`.
   - Outcome: Supported.

## Findings

- Root cause (or best current explanation):
  - Resolver-primary flow refactors preserved core resolver operation but lost complete parity wiring for reliability/confusion inputs in some resolver paths, and resolver ranking lacked confusion-informed tie resolution.
- Contributing factors:
  - Shared-flow migration touched multiple entry points (runtime, replay, rebuilt replay) and argument surfaces expanded.
  - Existing confusion logic was present in legacy correction scoring but not in resolver winner selection.
- Why this did or did not require a durable decision:
  - Required a durable decision because winner-selection semantics changed and parity guarantees between runtime and replay were codified.

## Decision Linkage

- ADR created/updated:
  - `docs/decisions/ADR-0036-resolver-confusion-tiebreak-runtime-replay-parity.md`
- Decision delta summary:
  - Phase 1: restore resolver parity wiring for reliability/confusion inputs across runtime and replay.
  - Phase 2: apply confusion-model tie-break to resolver top tie cohort during winner selection, with deterministic fallback when disabled.
- Contract/behavior changes (or `No contract changes`):
  - No protocol/schema contract changes.
  - Resolver winner selection behavior changes in weighted/support tie cases when confusion model is enabled and weighted by config.

## Verification and Monitoring

- Validation steps run:
  - `go test . -count=1`
  - `go test ./...`
  - `go vet ./...`
  - `staticcheck ./...`
  - `go test -race ./...`
- Signals to monitor (metrics/logs):
  - resolver winner transitions in contested/tie scenarios
  - resolver rejected/applied reason counters in runtime and replay comparison outputs
  - replay recall/stability delta after enabling non-zero confusion weight
- Rollback triggers:
  - unexpected split/flip growth or confidence instability after enabling confusion weight beyond baseline.

## References

- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s):
  - `docs/decisions/ADR-0006-confusion-model-tie-break-ranking.md`
  - `docs/decisions/ADR-0027-resolver-reliability-weighted-voting.md`
  - `docs/decisions/ADR-0028-shared-correctionflow-main-replay.md`
- Related docs:
  - `docs/decision-log.md`
  - `docs/troubleshooting-log.md`
