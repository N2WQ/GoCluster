# Code Maps

This directory is for support-agent-readable code-map summaries. Use it when a
dependency or package graph will remain useful beyond one Codex turn.

The custom GPT support agent can retrieve Markdown source files, but it cannot
run local tools such as `gopls`, `goda`, `callgraph`, or Graphviz `dot`.
Therefore durable graph evidence belongs in small Markdown summaries that name
the commands, packages, files, assumptions, and limits. Rendered SVG/PNG files
may be useful locally, but they are not the support agent's authoritative input.

## When To Add A Code Map

Add a code map only when one of these is true:
- a subsystem dependency shape is repeatedly useful for support or developer
  routing
- a Non-trivial blast-radius audit found package impact that is hard to explain
  from source paths alone
- a generated graph materially clarifies a durable architecture decision

Do not check in broad whole-repo graphs by default. They are noisy, drift
quickly, and can make the support agent over-trust stale topology.

## Required Summary Shape

Each checked-in code map should be a focused Markdown file with:
- scope: packages, commands, or subsystem covered
- generated: date and command line used
- tools: `goda`, Graphviz `dot`, `go list`, `gopls`, `callgraph`, or other
  evidence tools used
- entry points: package READMEs, crawler-entry comments, source files, and tests
  inspected
- graph summary: the small set of edges or clusters that matter
- limits: what the graph does not prove, including interface dispatch,
  build-tag/test-only edges, runtime feature flags, and concrete traffic paths
- follow-up: source files or tests the support agent should retrieve next

## Local Commands

Examples:

```powershell
goda graph ./internal/cluster ./telnet | dot -Tsvg -o .\tmp\cluster-telnet.svg
goda tree ./internal/cluster
go list -deps -test -json ./... | jq <filter>
callgraph -algo rta -test ./...
```

Use rendered graphs to guide source review, not to replace it. For concrete
behavior, verify the relevant source, tests, config, and decision records.
