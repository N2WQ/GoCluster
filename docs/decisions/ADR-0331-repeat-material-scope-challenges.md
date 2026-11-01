# ADR-0331: Repeat Material Scope Challenges

- Status: Accepted
- Date: 2026-11-01
- Decision Origin: Workflow review

## Context

The Codex workflow requires an initial scope challenge and material findings to
be dispositioned. It did not explicitly require another challenge after a
material revision, allowing an incomplete revised ledger to be presented for
approval without a further review.

## Decision

Challenge the initial Non-trivial scope before approval. After a material
revision, challenge the revised scope again before requesting approval. Focus
on affected areas unless broader assumptions changed. Repeat until every
material finding is resolved or explicitly accepted as a residual risk.
Unresolved product decisions and unaccepted material risks block an
approval-ready ledger; the lead cannot accept risk for the owner. Specialist
and independent-review triggers remain risk-based.

## Alternatives considered

1. Rely on the existing general challenge rule: rejected because its treatment
   of revised scope was insufficiently explicit.
2. Repeat the entire investigation after every revision: rejected because a
   targeted challenge is sufficient when wider assumptions remain unchanged.

## Consequences

The approval boundary covers material revisions as well as initial proposals.
Review can still miss defects; static checks cannot prove review quality or
conversational approval. Materiality remains an engineering judgment.

## Operational impact

This changes Codex workflow only; it does not change product runtime or Fable
workflow. No measured improvement in reasoning or engineering quality is claimed.

## Links

- Refines [ADR-0224](ADR-0224-evidence-before-scope-and-material-reapproval.md).
- Contracts: `AGENTS.md`, `docs/change-workflow.md`, and
  `.agents/skills/scope-ledger-adversarial-review/SKILL.md`.
- Checks: `scripts/check-workflow-contract.ps1` and
  `scripts/test-workflow-contract.ps1`.
