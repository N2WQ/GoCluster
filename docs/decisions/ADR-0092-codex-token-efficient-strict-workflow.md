# ADR-0092: Codex Token-Efficient Strict Workflow

- Status: Superseded
- Date: 2026-04-30
- Decision Origin: Design

## Context
The gocluster Codex workflow had accumulated repeated reporting requirements
across `AGENTS.md`, `docs/change-workflow.md`,
`docs/templates/non-trivial-change-template.md`, `docs/review-checklist.md`,
and `VALIDATION.md`.

The duplicated prose consumed tokens without improving rigor. The user also
required that any optimized workflow remain strict enough that Codex cannot
skip gates, and that the wording target Codex executing in this repository
rather than generic AI agents.

## Decision
Keep the same development rigor and approval discipline, but make the reporting
shape compact and marker-driven.

`AGENTS.md` remains the always-loaded Codex execution contract. It now requires
strict evidence markers for Non-trivial work:

- `GATE`
- `DISCOVERY`
- `SCOPE`
- `PREFLIGHT`
- `DESIGN`
- `IMPLEMENTATION`
- `REVIEW`
- `SELF-AUDIT`
- `CLOSEOUT`
- `TRACEABILITY`
- `VALIDATION`

Codex must treat each required marker as an execution gate. If a required marker
cannot be completed from inspected workspace evidence, Codex must stop and
report the missing evidence instead of continuing.

Token efficiency changes reporting shape only. It does not reduce required
discovery, approval, dependency rigor, validation, review, ADR handling, or
traceability.

## Alternatives considered
1. Keep the existing verbose workflow unchanged.
2. Make Non-trivial reporting discretionary and rely on Codex judgment.
3. Keep the full workflow but move all detail out of `AGENTS.md`.

## Consequences
### Benefits
- Reduces repeated workflow narration in Codex responses.
- Keeps required evidence mechanically visible through named markers.
- Makes the workflow explicitly Codex-specific.
- Preserves exact approval, validation, review, audit, ADR, and traceability
  obligations.

### Risks
- Over-compression could hide incomplete work if future edits weaken marker
  requirements.
- Workflow docs can drift if future changes reintroduce competing output
  shapes.

### Operational impact
- No runtime behavior changes.
- Codex closeouts should be shorter, but missing required evidence remains a
  workflow failure.
- `VALIDATION.md` remains a rubric, not a narrative response template.

## Links
- Related issues/PRs/commits:
- Related tests:
- Related docs: `AGENTS.md`, `docs/change-workflow.md`, `docs/templates/non-trivial-change-template.md`, `docs/review-checklist.md`, `VALIDATION.md`, `docs/WORKING_WITH_CODEX.md`
- Related TSRs:
- Supersedes / superseded by: superseded by ADR-0221
