# ADR-0006: Confusion-Model Tie-Break Ranking for Call Correction

- Status: Accepted
- Date: 2026-02-13

## Context

Call correction now has stronger anchor/consensus gating, but tied top-support
candidates can still be ambiguous in noisy CW/RTTY conditions. We already
generate RBN analytics artifacts (`confusion_model.json`) that encode observed
character confusions by mode and SNR band. The requirement is to use that data
for better ranking precision without loosening any safety gates.

Constraints:
- No new hard-coded tuning in code paths; operators must control behavior via
  `pipeline.yaml`.
- No changes to existing hard gates (min reports, advantage, confidence,
  freq-guard, cooldown, edit distance).
- Keep runtime bounded and deterministic on the hot path.

## Decision

Integrate confusion-model scoring as an optional ranking signal only:

1. Add config controls:
   - `call_correction.confusion_model_enabled`
   - `call_correction.confusion_model_file`
   - `call_correction.confusion_model_weight`
2. Load and validate `confusion_model.json` at startup when enabled.
3. During correction, apply confusion scoring only to tied top-support
   candidates, then re-rank that tie with:
   - `support + confusion_weight * confusion_score`
4. Keep all existing hard gates unchanged and evaluated exactly as before.
5. Extend decision tracing/storage with confusion fields for auditability:
   - `confusion_weight`
   - `winner_confusion_score`
   - `runner_up_confusion_score`

## Alternatives considered

1. Use confusion score as an absolute gate (accept/reject).
- Rejected: would change safety semantics and increase risk of false positives.

2. Replace consensus support counts with probabilistic scoring.
- Rejected: too invasive for this iteration and hard to reason about operationally.

3. Keep confusion model offline-only (no runtime use).
- Rejected: leaves precision gains on the table for tie situations.

## Consequences

### Benefits

- Better disambiguation when top-support candidates are tied.
- Uses offline analytics directly in production ranking.
- No contract change to correction hard-gate behavior.

### Risks

- Misconfigured model path or malformed JSON disables the feature at runtime.
- Poorly trained confusion data can bias tie ordering; mitigated by keeping hard
  gates unchanged and allowing `confusion_model_weight: 0`.

### Operational impact

- Adds three operator-facing config knobs.
- Adds one startup artifact dependency when enabled.
- Slight CPU overhead only when a top-support tie exists.

## Links

- Code:
  - `spot/confusion_model.go`
  - `spot/correction.go`
  - `spot/decision_logger.go`
  - `main.go`
  - `config/config.go`
- Tests:
  - `spot/confusion_model_test.go`
  - `spot/correction_test.go`
  - `main_test.go`
- Config/docs:
  - `data/config/pipeline.yaml`
  - `README.md`
  - `docs/decision-log.md`

