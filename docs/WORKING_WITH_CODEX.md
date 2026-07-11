# Working With Codex In gocluster

`AGENTS.md` is the Codex execution contract. `CLAUDE.md` separately governs
Fable. They share codebase engineering standards but do not share planning,
reporting, or agent-orchestration requirements.

## Read-only Requests

Explanation, review, audit, diagnosis, prioritization, and recommendations can
proceed without a change Scope Ledger. Codex still inspects current relevant
evidence and labels unknowns. If you later request edits, Codex enters the Small
or Non-trivial change route first.

## Small Changes

Small work is localized and low blast radius, with no protocol, compatibility,
concurrency, lifecycle, queue, shutdown, shared-interface, or material
user-visible behavior change. Runtime config/schema/default/sentinel semantics,
parsers, authentication/admission, persisted state, scientific models, hot
paths, shared contracts, material operator behavior, and durable decisions are
Non-trivial. Uncertain work is Non-trivial.

## Non-trivial Approval

Codex first inspects the current affected state and presents a bounded
`Proposed Scope Ledger vN`. It challenges that scope for missing dependencies
and edge cases. Mutation begins only after you reply with the exact matching
token:

```text
Approved vN
```

Only agreed scope is executable. New required work stops for a revised ledger
and exact reapproval.

Decompose work when real rollback, ownership, uncertainty, or validation
boundaries exist. A bounded coherent change may remain one slice. Broad
refactor-shaped scope is not approval-ready; no fixed slice-field schema is
required.

## Specialists And Independent Review

Codex loads a specialist only when a concrete risk triggers its unique method.
Non-trivial work does not automatically require multiple specialists, parallel
discovery, or independent agents.

The repository owner provides standing authorization for subagent use when the
active platform permits it. Authorization does not make subagents mandatory or
expand their scope.

Examples include unresolved semantics, scientific/model authority, a genuine
design fork, unclear test falsifiability, uncertain blast radius, substantial
Go implementation, lifecycle, leaks, retained state, config, or hot paths.

Localized specialist work may remain lead-owned. When context partitioning
materially improves a triggered investigation, Codex uses a bounded subagent
when supported. A separate non-steered context is required only when evidence
credibility depends on independence; a fresh lead pass is not independent
review. Pre-approval agents remain read-only, post-approval workers need
approved disjoint scope and stop conditions, and Codex retains final ownership.
High-risk closeout still receives a fresh verification pass, which may be
lead-owned.

## Validation And Closeout

Approval rigor follows change risk; validation follows the touched surface.
Markdown-only workflow changes do not require Go tests. Codex runs targeted
checks while working and one final selected lane, then reruns only evidence
invalidated by review fixes.

Closeout reports the outcome, material findings or gaps, substantive validation
results, compact approved-item traceability, and durable decision references
when applicable. It does not require numeric scores, fixed marker order,
process score rows, repeated agent envelopes, or visible irrelevant categories.

For commands, use `docs/dev-runbook.md`. For engineering rules, use only the
applicable sections of `docs/code-quality.md`. Fable continues to follow its
own workflow, review, validation, and template documents until separately
changed.
