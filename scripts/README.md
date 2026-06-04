# GoCluster Scripts

Tracked PowerShell scripts in this directory are operational tooling for local
builds, release packaging, profiling, console setup, workflow checks, and Codex
skill installation.

## Workflow Checkers

- `check-yaml-doc-rigor.ps1` checks first-party runtime YAML headers and
  comment-only YAML scope.
- `check-go-crawler-entry-comments.ps1` checks changed support-critical Go files
  for package/file entry comments. It is a mechanical review aid; source-aware
  review still decides whether comments explain useful intent and why.
- `verify-agentic-tools.ps1` checks required repo workflow tools and required
  semantic/navigation helpers, then reports recommended or optional
  investigation helpers separately so missing optional tools do not block
  ordinary Go work.

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
