# docs/decision-memory.md

This document defines how to preserve durable architectural and troubleshooting knowledge.

## Purpose
Use ADRs and TSRs to preserve decisions, tradeoffs, reversals, and incident-driven learning so that future work does not re-debate settled issues or erase important context.

## Canonical locations
- ADR files: `docs/decisions/`
- ADR index: `docs/decision-log.md`
- Troubleshooting records: `docs/troubleshooting/`
- Troubleshooting index: `docs/troubleshooting-log.md`

## Mandatory pre-read
For every Non-trivial task and every troubleshooting task:
1. read `docs/decision-log.md`
2. read `docs/troubleshooting-log.md`
3. open the ADRs/TSRs relevant to the affected component
4. if none apply, state:
   - `No relevant ADR found`
   - and/or `No relevant TSR found`

## ADR handling for Non-trivial work
Every Non-trivial task must end with one of:
- a new ADR
- an updated ADR
- a lightweight ADR stub recording `No durable decision change`

This makes ADR handling mandatory even when the task does not introduce a new
durable design decision.

## When a full ADR is required
Create or update a full ADR when a Non-trivial change affects any of:
- protocol or compatibility
- parser behavior
- concurrency model
- backpressure, queue, drop, or disconnect policy
- deadlines, retries, or shutdown behavior
- resource bounds
- reliability or observability contracts
- shared component behavior used by multiple packages
- operational mode selection with user-visible or operator-visible impact

If none of those changed, create a lightweight ADR stub instead of a full ADR.
The stub should record:
- task/date/scope
- summary of what changed
- `No durable decision change`
- links to relevant code/tests/docs when applicable

Keep lightweight no-durable-change stubs short. They preserve traceability; they
are not a substitute for a full ADR when a durable decision changed.

## When a TSR is required
Create or update a TSR when:
- the task originates from debugging, production triage, or failure analysis
- a bug or incident required hypothesis testing or root-cause analysis
- troubleshooting produced durable system insight even before a final fix landed

If troubleshooting leads to a durable decision change:
1. create/update the TSR first
2. then create/update the ADR
3. cross-link both

If troubleshooting ends without a durable decision change:
- record the troubleshooting outcome in the TSR
- still create the lightweight ADR stub required for the Non-trivial task

## Naming convention
Suggested filenames:
- `docs/decisions/ADR-0001-short-title.md`
- `docs/troubleshooting/TSR-0001-short-title.md`

Use zero-padded numeric IDs and keep titles short.

## ADR required fields
- Title
- Status: Proposed | Accepted | Superseded | Deprecated
- Date
- Decision Origin: Design | Incident | Troubleshooting chat
- Context
- Decision
- Alternatives considered
- Consequences
- Links

Use `docs/templates/adr-template.md`.

## TSR required fields
- Title
- Status: Open | Resolved | Superseded
- Date opened
- Date resolved or current status date
- RCA Summary:
  - What happened
  - Why
  - What fixed it
  - How we know
  - Operator/support answer
- Trigger
- Symptoms and impact
- Hypotheses tested
- Evidence
- Root cause or best current explanation
- Fix or mitigation
- Why an ADR was or was not required
- Links

Use `docs/templates/tsr-template.md`.

## Immutability and supersession
- Do not rewrite the history of accepted ADRs.
- If direction changes, write a new ADR and mark the old one `Superseded`.
- Link old and new records both ways.
- TSRs may be updated as evidence improves, but preserve earlier hypotheses and what disproved them.

## Traceability requirements
Every Non-trivial final summary must include:
- `Decision refs: ADR-XXXX`

If troubleshooting originated the durable decision, include both:
- `Decision refs: ADR-XXXX; TSR-XXXX`

Scope-to-Code Traceability must include decision refs for affected items.

## Index maintenance
Whenever a new ADR or TSR is created:
- add it to the relevant log file
- keep the newest entries at the top unless the repo already uses a different convention
- include status, date, component, and one-line summary
- for TSRs, run `scripts/check-troubleshooting-records.ps1` after adding or
  editing a troubleshooting record, the TSR template, or the troubleshooting
  index
