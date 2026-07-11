# Codex Workflow Checks

This component lists Codex-specific checks for the workflow/skill-doc lane. It
does not select a validation lane; `docs/dev-runbook.md` remains authoritative.

Run only checks triggered by the changed surface:

- `scripts/check-workflow-contract.ps1 -BaselineRevision <approved-baseline>`
  for Codex authority, routing, retired-requirement, reference, and protected-
  path invariants;
- `scripts/test-workflow-contract.ps1` when the contract checker or its owned
  semantics change;
- `scripts/verify-codex-skills.ps1` after repo-managed skill or metadata edits;
- `scripts/test-measure-codex-workflow-context.ps1` when informational context
  measurement changes;
- PowerShell parser checks for changed scripts;
- targeted cross-reference and metadata/body checks;
- cross-executor semantic review for shared documents;
- final diff review and `git diff --check`.

Static checks do not prove conversational approval, classification, discovery,
validation sufficiency, genuine independence, or engineering quality. Do not
run Go validation solely because Codex workflow Markdown, skill metadata, or
workflow-checker scripts changed.
