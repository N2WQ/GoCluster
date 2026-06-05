---
name: go-blast-radius-audit
description: "Use before planning, approving, reviewing, or implementing Go changes with uncertain blast radius, shared interfaces, semantic call/reference impact, package dependency impact, tests/docs/support routing impact, or cross-package behavior. Classify required and optional analysis tools separately."
---

# Go Blast Radius Audit

## Overview

Use this skill to map what a proposed Go change can affect before scope is
approved or implementation continues. It complements dependency rigor in
`docs/change-workflow.md`.

## Required Tool Boundary

- Required baseline tools are the repo workflow tools: Go toolchain, `gopls`
  when semantic symbol data is needed, `rg`, `go list`, `jq` for JSON package
  analysis, and the standard validation commands.
- Optional tools such as Graphviz `dot`, `goda`, `go-callvis`, `semgrep`,
  `ast-grep`, and Sysinternals can improve evidence for specific
  investigations.
- Missing optional tools are reported as conditional evidence gaps only for the
  workflow that needed them. Their absence does not block ordinary Go
  implementation, review, or validation.

## Workflow

1. Classify the affected surface.
   - Package-local implementation
   - Exported/shared API
   - Interface or method set
   - Parser/protocol/config/schema
   - Concurrency/lifecycle/queue/timer/shutdown path
   - User/operator-visible behavior
   - Docs/support-agent routing

2. Map semantic callers and callees.
   - Use `gopls references -d <file:line:col>` for symbol references.
   - Use `gopls implementation <file:line:col>` for interface impact.
   - Use `gopls call_hierarchy <file:line:col>` for function-level callers and
     callees.
   - Use `callgraph -algo rta -test ./...` or `callgraph -algo vta -test ./...`
     only when whole-program call edges are useful; treat output as possible
     behavior, not concrete runtime proof.

3. Map package and test impact.
   - Use `go list -deps -test -json ./...` with `jq` to inspect import and test
     dependencies.
   - Identify targeted package tests, broader package suites, and required full
     validation from `docs/dev-runbook.md`.
   - If optional Graphviz `dot` and `goda` are installed, use them for rendered
     reverse-import or graph views when package impact remains unclear.
   - When graph evidence should be useful to the custom GPT support agent,
     write a small Markdown code map under `docs/code-maps/` that names the
     command, important edges, inspected source/tests, and limits.

4. Search non-Go contracts.
   - Use `rg` and `yq` for YAML keys, command names, HELP text, log/event
     names, docs, support routes, ADRs, TSRs, generated artifacts, and fixtures.
   - Use optional structural search (`semgrep` or `ast-grep`) only when text
     search cannot reliably find the pattern.

5. Classify the result.
   - Covered by current scope
   - Requires revised Scope Ledger
   - Explicitly out of scope
   - Validation-only follow-up
   - Documentation/support routing follow-up

## Output Expectations

- Include a `Blast-radius audit` section for Non-trivial work when this skill
  triggers.
- Record commands/steps used and reviewed files/packages in the existing
  `Dependency scan evidence` line when Full rigor applies.
- State optional missing tools separately from required missing tools.
- Do not claim exhaustive blast-radius coverage from text search alone when
  semantic callers, interfaces, config, or support docs are material.
- Do not treat rendered graph output as support-agent evidence unless a
  retrievable Markdown summary captures the conclusion and source follow-ups.
