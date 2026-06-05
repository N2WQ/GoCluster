---
name: go-code-walk
description: "Use when Codex needs to understand unfamiliar or cross-package Go behavior before planning or changing code. Walk current source using package docs, crawler-entry comments, semantic navigation, tests, ADR/TSR history, and support routing; report inspected evidence and unknowns before proposing changes."
---

# Go Code Walk

## Overview

Use this skill to build current-state understanding from the repository before
planning, explaining, reviewing, or editing non-trivial Go behavior. It is for
facts and call paths, not for implementation.

## Workflow

1. Anchor the question.
   - Identify the package, command, config key, log/event, protocol path, or
     user-visible behavior under discussion.
   - Prefer package READMEs, crawler-entry comments, and selected/open files as
     the first routing layer.
   - If the anchor is ambiguous, search docs and source before asking the user.

2. Walk one material level up and down.
   - Read the defining file and the nearest caller/callee where behavior is
     material.
   - Use `gopls definition`, `gopls references -d`, `gopls implementation`,
     `gopls call_hierarchy`, and `gopls symbols` when a symbol location is
     known.
   - Use `rg`, `fd`, `bat`, `go list`, and `go test -list` to find source,
     tests, package ownership, and named behavior.

3. Use whole-program tools when local navigation is insufficient.
   - Use `callgraph -algo rta -test ./...` or `callgraph -algo vta -test ./...`
     for possible whole-program call edges.
   - Treat callgraph output as a conservative aid, not proof that a path runs
     in the concrete scenario.
   - Optional tools such as Graphviz `dot`, `goda`, or `go-callvis` can improve
     package or graph visualization when installed, but their absence is not a
     blocker for ordinary code walking.

4. Check decision and support context.
   - Read `docs/decision-log.md` and `docs/troubleshooting-log.md` for
     component-relevant ADRs/TSRs when behavior may have durable history.
   - For operator/support surfaces, check `customgpt/source-map.md`,
     `customgpt/developer-guide-index.md`, and the relevant package docs.

5. Report evidence before conclusions.
   - Name inspected files, commands, and tests.
   - Separate facts, assumptions, and unknowns.
   - If a fact is not established from inspected source, say
     `Unknown from inspected code` and name the next file/tool that would
     resolve it.

## Output Expectations

- Include a `Code-walk evidence` section when this skill is used for
  Non-trivial workflow discovery.
- Keep command output summarized; do not paste large call graphs.
- Do not propose changes until current behavior and unknowns are clear enough
  to support a Scope Ledger or an explanation.
