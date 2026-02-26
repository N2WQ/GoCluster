# ADR-0028 - Shared Call-Correction Flow for Main and Replay

Status: Accepted
Date: 2026-02-26
Decision Origin: Design

## Context

- The live pipeline (`main.go`) and replay pipeline (`cmd/rbn_replay`) had duplicated call-correction runtime/settings/application logic.
- Shared `spot.SuggestCallCorrection` was already used, but duplicated orchestration created drift risk when correction rails and settings evolve.
- Replay is used for method selection and regression evidence, so behavioral parity with the live correction path is required.

## Decision

- Introduce a shared helper package: `internal/correctionflow`.
- Centralize these behaviors in the shared package:
  - runtime parameter resolution (`ResolveRuntimeSettings`)
  - settings construction (`BuildCorrectionSettings`)
  - confidence mapping (`FormatConfidence`)
  - correction application rails (`ApplyConsensusCorrection`)
  - resolver evidence/observation helpers used by replay/live parity paths
- Keep context-specific side effects outside the shared core:
  - live path keeps dashboard/log output and trace logging wiring
  - replay keeps AB metrics/legacy-confidence comparisons

## Alternatives considered

1. Keep duplicate logic and sync manually
   - Pros: no refactor cost.
   - Cons: high drift risk and replay/live mismatch over time.
2. Make replay call live `main` helpers directly
   - Pros: less new code.
   - Cons: poor package boundaries and coupling to runtime/UI concerns.
3. Move all side effects into one shared flow
   - Pros: maximal consolidation.
   - Cons: mixes replay analytics concerns with live operator behavior.

## Consequences

- Benefits:
  - Replay now tracks live correction method updates through one shared flow.
  - Future correction-rail changes require one implementation update.
  - Reduced parity bugs between replay evidence and production behavior.
- Risks:
  - Shared helper regressions impact both live and replay paths.
  - Requires discipline to keep wrappers thin and side effects outside shared core.
- Operational impact:
  - No config contract changes.
  - No protocol/ordering/drop contract changes.

## Links

- Decision index: `docs/decision-log.md`
- Code:
  - `internal/correctionflow/shared.go`
  - `main.go`
  - `cmd/rbn_replay/pipeline.go`
- Tests:
  - `internal/correctionflow/shared_test.go`
  - `cmd/rbn_replay/pipeline_test.go`
