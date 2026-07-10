# Codex Workflow Checks

This component contains Codex-specific checks for the workflow/skill-doc lane.
It does not select a validation lane; `docs/dev-runbook.md` remains authoritative.

## Required Codex checks

- audit drift across `AGENTS.md`, `docs/change-workflow.md`, `VALIDATION.md`,
  `docs/review-checklist.md`, the Non-trivial template, and `codex-skills/**`
- run `scripts/check-workflow-contract.ps1` and its fixture suite when Codex
  workflow semantics or enforcement change
- run `scripts/verify-codex-skills.ps1` after repo-managed skill edits
- review skill metadata/body synchronization when metadata changes
- use fresh read-only conformance review for changed independent-review roles
- use a fresh verifier for high-risk workflow changes when supported and
  authorized; otherwise report the canonical status and waiver disposition

Repo skill metadata YAML uses metadata/body and manifest consistency checks;
the runtime-config header standard is `N/A` unless a stricter local standard
applies.
