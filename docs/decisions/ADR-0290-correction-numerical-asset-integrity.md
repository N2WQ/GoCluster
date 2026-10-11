# ADR-0290: Correction Numerical Asset Integrity

- Status: Accepted
- Date: 2026-10-11
- Decision Origin: Design

## Context

Nonfinite reliability values could be clamped or silently skipped, and model
counts could be negative or overflow their int64 totals. Live startup and the
offline production-gate evaluator warned and continued without invalid configured
assets. Such fallback changes the evidence population and ranking inputs.

## Decision

Reject nonfinite reliability values, including numeric parsing overflow, before
clamping or skipping parse errors. Preserve finite clamping, duplicate-line
accounting and unrelated malformed-line skipping. A selected nonfinite direct
receiver-map value refuses that receiver before conversion; a present invalid
mode override cannot fall back to a valid global value.

Validate every model edge as finite and strictly ascending, every count as
nonnegative, and each substitution row/deletion/insertion sum as representable
int64. Preserve shapes, symbols, mode validation, add-one arithmetic and accepted
finite ranking results. Representable counts above binary64's exact-integer range
retain their existing floating-point precision limit.

Live startup, historical replay and the offline evaluator fail on invalid
applicable configured assets with the responsible configuration key and path.
Nonblank reliability paths are loaded regardless of global correction enablement;
the model's own enabled flag controls its load, including at weight zero. Existing
YAML presence/nonempty requirements remain unchanged. Disabled models ignore
their asset path. No automatic asset paths or replacement models are introduced.

Check active raw scalar floors and model weights before normalization. The direct
resolver retains its pointer-returning constructor and refuses evidence when
active numerical parameters are nonfinite. A model weight without a model is
unused. Loaded models and borrowed receiver maps are immutable during use.

## Alternatives considered

- Clamping infinity or substituting unit weights manufactures a different input.
- Ignoring an invalid override hides a configured defect behind fallback.
- Checking only totals can conceal negative individual counts.
- Replacing the constructor API or adding a shared validation framework expands
  a bounded repair without providing a necessary contract.

## Consequences

Invalid configured assets now prevent applicable initialization or evaluation.
Live failure precedes new resolver/history/output ownership, and existing close
owns cancellation and joining of earlier licensed-call data workers. This is
numerical and failure-accounting integrity, not evidence of training validity,
incremental field benefit or calibrated correctness probabilities. SNR presence
and domain handling are a separate patch; RF episode association remains deferred.

## Links

- Approved scope: `docs/call-correction-evidence-scope-ledger-v2.md`, B2.1.
- Validation: `docs/call-correction-evidence-execution.md`.
- Operator contract: [spot confidence](../../spot/README.md).
