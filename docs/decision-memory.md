# Decision Memory

ADRs preserve durable architecture, operations, scientific, and workflow
decisions. TSRs preserve durable troubleshooting evidence and incident
learning. They are not task-administration logs.

## Canonical Locations

- ADRs: `docs/decisions/`
- ADR index: `docs/decision-log.md`
- TSRs: `docs/troubleshooting/`
- TSR index: `docs/troubleshooting-log.md`
- Templates: `docs/templates/adr-template.md` and
  `docs/troubleshooting/TSR-TEMPLATE.md`

Search the indexes for the affected component or decision chain and open the
relevant records. Do not read every ADR and TSR for unrelated work. Current
source, tests, runtime contracts, and operator documentation remain the final
evidence for current behavior.

## Codex Application

Every Non-trivial Codex closeout considers and states the decision disposition.
Create or update an ADR only when a durable decision changes. Create or update a
TSR when troubleshooting produces durable evidence, root cause, or operational
learning. When neither applies, record the disposition in the closeout without
creating a file solely to document the task.

## Fable Application

Fable continues to follow `CLAUDE.md`: every Non-trivial Fable task uses a new
ADR, updated ADR, or lightweight ADR stub. This Codex workflow change does not
alter that requirement. Fable retains its existing templates, indexes, review,
validation, and reporting semantics until separately approved.

## Durable ADR Triggers

Create or supersede an ADR for durable decisions involving:

- protocol, parser, compatibility, or operator-visible behavior;
- concurrency, lifecycle, shutdown, deadlines, retry, backpressure, queues,
  drops, or disconnect policy;
- resource bounds, persistence, shared-component behavior, or observability
  contracts;
- scientific/model semantics or supportable claims;
- operational mode selection; or
- repository workflow authority and validation policy.

Use `docs/templates/adr-template.md`. Include context, decision, alternatives,
consequences, operational impact, links, and supersession. Preserve accepted
history: reverse or replace it through a new ADR and link both directions.

## TSR Triggers

Create or update a TSR when work originates in production triage, debugging,
hypothesis testing, root-cause analysis, or durable failure learning. If the
troubleshooting changes a durable decision, create or update the TSR first,
then the ADR, and cross-link them.

Preserve earlier hypotheses and evidence that disproved them. Use
`docs/troubleshooting/TSR-TEMPLATE.md` and run
`scripts/check-troubleshooting-records.ps1` when a TSR, its template, or its
index changes.

## Index And Closeout

Add new records to the applicable index using its existing newest-first
convention. Keep status, date, area, links, and supersession current.

Codex closeout links applicable ADRs and TSRs to the approved item they govern.
No fixed `Decision refs` label or empty decision field is required. Fable keeps
the exact reporting required by its own contract.
