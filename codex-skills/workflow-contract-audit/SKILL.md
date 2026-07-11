---
name: workflow-contract-audit
description: "Use when Codex workflow authority, approval, validation routing, review rules, templates, repo-managed skills, or enforcing scripts change. Do not trigger for ordinary product documentation or runtime work whose workflow contract is unchanged."
---

# Workflow Contract Audit

## Purpose

Prevent contradiction, unreachable rules, duplicated ownership, and checkers
that confuse response formatting with engineering compliance.

## Method

1. Identify the changed Codex contract surfaces and any shared documents used
   by Fable.
2. Preserve authority routes: Read-only/Small/Non-trivial separation, exact
   `Approved vN`, agreed scope, reapproval on expansion, current evidence, and
   touched-surface validation.
3. Confirm each detailed rule has one natural owner and remains reachable from
   `AGENTS.md` or a positive specialist trigger.
4. Check validation commands, review expectations, skill descriptions,
   metadata, templates, and scripts for contradiction.
5. Preserve each specialist's unique engineering method while allowing trigger
   narrowing and reporting simplification.
6. Verify repo-skill frontmatter, names, referenced assets, and absence of
   user-level installation paths.
7. Review shared-document changes for cross-executor semantic drift. Do not
   alter Fable-owned files in a Codex-only change.
8. Run the workflow checker, its named negative fixtures, and the repo-skill
   verifier after edits.

## Static Boundary

Static checks may establish text, ownership, references, positive and negative
trigger representation, and supplied changed-path boundaries. They cannot
prove conversational approval, classification, discovery sufficiency,
specialist necessity, validation adequacy, genuine independence, durable
decision judgment, or engineering quality.

## Reporting

Report material contradictions, checker results, cross-executor risks, and
remaining gaps without a mandatory heading or fixed field set.
