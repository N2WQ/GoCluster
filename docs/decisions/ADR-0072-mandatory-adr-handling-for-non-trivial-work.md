# ADR-0072: Mandatory ADR Handling for Non-trivial Work

- Status: Accepted
- Date: 2026-04-23
- Decision Origin: Design
- Supersession note: ADR-0221 supersedes this decision's mandatory no-change
  ADR application for Codex only. Fable's existing application remains active.

## Context
The workflow now requires durable decision memory for every Non-trivial task.
Some Non-trivial tasks do not introduce a new durable architectural or
operational decision, but the repo still wants a consistent closeout path with
an ADR reference instead of ad hoc `Decision refs: none`.

## Decision
Every Non-trivial task must end with ADR handling:
- a new ADR
- an updated ADR
- or a lightweight ADR stub recording `No durable decision change`

TSRs remain separate and are still required only for troubleshooting-origin
work. When troubleshooting is Non-trivial but does not change a durable
decision, the task still gets the lightweight ADR stub.

## Alternatives considered
1. Keep ADRs optional for Non-trivial work.
2. Require a full ADR for every Non-trivial task.
3. Keep `Decision refs: none` as the no-change path.

## Consequences
### Benefits
- Every Non-trivial task ends with durable decision-memory handling.
- Final summaries always have ADR-backed decision refs.
- The lightweight stub path keeps the policy consistent without forcing a full
  essay for every docs-only or narrow Non-trivial task.

### Risks
- More repo churn from small ADR entries.
- The ADR log can grow with low-decision stubs if the lightweight path is
  overused.

### Operational impact
- Non-trivial closeout now always includes ADR handling.
- Operators and future tool runs can rely on ADR references being present.

## Links
- Related issues/PRs/commits:
- Related tests:
- Related docs: `AGENTS.md`, `docs/decision-memory.md`, `docs/change-workflow.md`, `docs/templates/non-trivial-change-template.md`, `docs/review-checklist.md`
- Related TSRs:
- Supersedes / superseded by: Codex application selectively superseded by ADR-0221
