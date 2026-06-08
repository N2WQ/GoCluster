---
name: decision-memory-audit
description: "Use for every Non-trivial gocluster task, troubleshooting-origin task, ADR/TSR update, decision-log or troubleshooting-log edit, durable architecture or operations decision, no-durable-decision stub choice, final Decision refs, and Scope-to-Code Traceability review."
---

# Decision Memory Audit

## Overview

Use this skill to keep gocluster's ADR and TSR record complete, current, and
traceable. It applies before implementation planning and again during closeout
for Non-trivial and troubleshooting tasks.

## Workflow

1. Perform the mandatory pre-read.
   - Read `docs/decision-log.md`.
   - Read `docs/troubleshooting-log.md`.
   - Open component-relevant ADRs and TSRs.
   - If none apply, state `No relevant ADR found` and/or
     `No relevant TSR found`.

2. Classify the task origin.
   - Design or workflow change
   - Troubleshooting, incident, or production failure analysis
   - Follow-up implementation of an accepted decision
   - Read-only audit or explanation

3. Choose the required record.
   - Create or update a full ADR when the task changes a durable decision about
     protocol, parser behavior, concurrency model, backpressure, queue, drop or
     disconnect policy, deadlines, retries, shutdown behavior, resource bounds,
     observability contracts, shared component behavior, or operator-visible
     mode selection.
   - Create a lightweight ADR stub when a Non-trivial task does not change a
     durable decision.
   - Create or update a TSR when troubleshooting, hypothesis testing,
     root-cause analysis, or durable failure insight is involved.
   - A final `Decision refs: none` closeout is not valid for Non-trivial work.

4. Preserve decision history.
   - Do not rewrite accepted ADR history.
   - Use a new ADR to supersede or reverse a prior accepted decision.
   - TSRs may accumulate evidence, but preserve earlier hypotheses and the
     evidence that disproved them.

5. Maintain indexes and links.
   - Add new ADRs to `docs/decision-log.md`.
   - Add new TSRs to `docs/troubleshooting-log.md`.
   - Keep newest entries at the top unless the existing file shows a different
     convention.
   - Cross-link ADRs and TSRs when troubleshooting produced the decision.

6. Close with traceability.
   - Include `Decision refs: ADR-XXXX` in the final closeout.
   - Include `Decision refs: ADR-XXXX; TSR-XXXX` when troubleshooting
     originated the durable decision.
   - Map approved scope items to changed files, tests/checks, docs/comments,
     support-agent docs, and decision refs in Scope-to-Code Traceability.

## Output Expectations

- Include a `Decision-memory audit` section when this skill triggers.
- Report relevant ADR/TSR pre-read, record type chosen, index maintenance,
  closeout decision refs, and any missing decision-memory evidence.
- State whether a full ADR, updated ADR, lightweight ADR stub, TSR, or no
  write applies. For Non-trivial implementation work, no write applies only if
  the approved scope is explicitly read-only and no durable repo decision is
  being committed.
- Treat missing ADR/TSR evidence as a workflow gap, not a style issue.
