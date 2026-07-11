---
name: decision-memory-audit
description: "Use when a Codex task may change a durable architecture, operations, scientific, or workflow decision; originates in troubleshooting; or edits ADR/TSR records and indexes. Do not trigger merely because work is Non-trivial or require a no-change ADR."
---

# Decision Memory Audit

## Purpose

Preserve durable decisions and troubleshooting learning without filling the ADR
index with task-administration records.

## Method

1. Search `docs/decision-log.md` and `docs/troubleshooting-log.md` for the
   affected component or decision chain. Open the relevant records; do not read
   every record by default.
2. Classify the origin as design, follow-up implementation, troubleshooting,
   incident analysis, or read-only review.
3. Create or update a full ADR only when a durable decision changes. Create or
   update a TSR for durable troubleshooting evidence, hypotheses, root cause,
   or operational learning.
4. Preserve accepted history. Reverse or replace an accepted decision through a
   new superseding ADR and link both directions.
5. Maintain the applicable index and cross-links when a record changes.
6. Record a concise decision disposition at closeout. If no durable decision
   changed, do not create a file solely to say so.

Fable continues to follow its own mandatory ADR-handling rule through
`CLAUDE.md`; this skill defines Codex behavior only.

## Reporting

Report the relevant records inspected, record choice, index/link updates, and
any missing durable evidence. Add ADR/TSR references to traceability only when
they apply. No fixed heading or no-change artifact is required.
