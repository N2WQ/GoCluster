# GoCluster Scripts

Tracked PowerShell scripts in this directory are operational tooling for local
builds, release packaging, profiling, console setup, workflow checks, and Codex
skill installation.

## Operational Helpers

- `watch-voacap-ssn.ps1` runs the repo-local lightweight SSN watcher
  (`cmd/voacap_sunspot_watch`) to watch NOAA fetches, raw SSN, rounded EWMA
  SSN, recompute delta, and recompute markers without launching VOACAP
  forecasts. Example:

  ```powershell
  .\scripts\watch-voacap-ssn.ps1
  ```

## Workflow Checkers

- `measure-codex-workflow-context.ps1` reports declared Codex instruction-path
  scenarios at immutable Git revisions using deterministic words, characters,
  and UTF-8 bytes. Results are informational context-footprint proxies, not
  adoption gates, model-token evidence, or quality proof.
- `test-measure-codex-workflow-context.ps1` exercises the measurement script in
  a disposable Git repository and proves dirty worktree bytes cannot alter a
  pinned candidate comparison.

- `check-workflow-contract.ps1` verifies mechanically representable Codex
  authority routes, positive and negative risk routing, retired Codex-only
  requirements, references, optional changed-path exclusions, and the
  repository's push-CI, Codex-contract-CI, nightly-race, conditional-check,
  workflow-permission, and Actionlint invariants. It explicitly disclaims
  conversational, hosted-run, and engineering proof.
- `test-workflow-contract.ps1` runs positive and named negative fixtures. Each
  negative case asserts its invariant-specific failure so an unrelated checker
  error cannot create a false green.
- `check-yaml-doc-rigor.ps1` checks first-party runtime YAML headers and
  comment-only YAML scope.
- `check-go-crawler-entry-comments.ps1` checks changed support-critical Go files
  for package/file entry comments. It is a mechanical review aid; source-aware
  review still decides whether comments explain useful intent and why.
- `update-code-maps.ps1` regenerates checked-in Markdown code maps from Go
  package metadata and ADR records.
- `check-code-maps.ps1` verifies checked-in Markdown code maps are fresh without
  modifying files. Use this in CI and release freshness gates.
- `check-troubleshooting-records.ps1` verifies troubleshooting-log rows and
  `docs/troubleshooting/TSR-*.md` records stay link-complete, status-aligned,
  ADR-linked, and readable through the required `RCA Summary` block.
- `check-support-agent.ps1` verifies the custom GPT support-agent deployment
  bundle, support-route contracts, bounded support search, routing docs, local
  Worker behavior, and optional deployed Worker health without printing bearer
  tokens.
- `evaluate-support-agent.ps1` runs the local support-agent eval harness against
  `docs/support-agent-eval-cases.json`, using the checked-in Worker and current
  workspace files to validate retrieval/source coverage and optionally score
  pasted or live-generated answers.
- `verify-agentic-tools.ps1` checks required repo workflow tools and required
  semantic/navigation helpers, then reports recommended or optional
  investigation helpers separately so missing optional tools do not block
  ordinary Go work. It also reports optional dependency-visualization helpers
  such as Graphviz `dot` and `goda`; summarize durable graph findings in
  `docs/code-maps/` when the custom GPT support agent should use them later.

## Header Standard

Every tracked first-party `.ps1` script should start with PowerShell
comment-based help before executable statements:

```powershell
<#
.SYNOPSIS
  One-line purpose.

.DESCRIPTION
  What the script does, when to use it, and what it changes.

.PARAMETER Name
  Parameter meaning and default.

.NOTES
  Prerequisites: required tools, auth, binaries, logs, or environment.
  Side effects: files created, processes started, releases published, or state changed.
  Safety: dirty-worktree behavior, secret handling, generated artifacts, or production cautions.
#>
```

Use the header as local context for operators, support agents, and developers.
The script body remains authoritative for actual behavior. Header-only updates
must not change parameters, commands, generated paths, process launch behavior,
release publishing behavior, profiling cadence, or local Codex skill state.
