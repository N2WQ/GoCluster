# TSR-0033 - Fable Workflow Checker CRLF Self-Inconsistency

Status: Resolved
Date Opened: 2026-07-11
Date Resolved: 2026-07-11
Owner: Fable
Technical Area: scripts/check-fable-workflow-contract.ps1
Trigger Source: Chat request
Led To ADR(s): none
Tags: workflow, checker, Fable, CRLF

## RCA Summary
- What happened: `scripts/check-fable-workflow-contract.ps1` and
  `scripts/test-fable-workflow-contract.ps1` failed against a known-correct
  repo state, reporting `docs/fable-validation.md` and
  `docs/templates/fable-non-trivial-change-template.md` as "missing exact
  workflow text: Validation Score: X/6" even though that text was present
  verbatim in both files.
- Why: `Get-RepoText` normalizes target-file content from CRLF to LF before
  comparison, but the checker's own `$validationBlock` here-string literal
  (built from the checker script's own on-disk content) was never
  normalized. On a checkout with `core.autocrlf=true`, the checker script
  itself has CRLF line endings, so `$validationBlock` retained embedded
  `\r\n` while the normalized target text used `\n` only — a substring
  match that could never succeed regardless of target-file correctness.
- What fixed it: added `.Replace("`r`n", "`n")` to `$validationBlock`'s
  construction (`scripts/check-fable-workflow-contract.ps1:138`), matching
  the same normalization already applied to target-file content.
- How we know: confirmed root cause via a non-mutating diagnostic copy of
  the checker with the same normalization applied, which passed against the
  live repo; confirmed the committed (`HEAD`) version of the checker's
  here-string was already LF-only and the CRLF only appeared in this local
  working-tree checkout; after the one-line fix, both
  `scripts/check-fable-workflow-contract.ps1` and
  `scripts/test-fable-workflow-contract.ps1` (all positive and negative
  fixtures) pass cleanly.
- Operator/support answer: not operator-facing; internal Fable workflow
  tooling only.

## Triggering Request
- Request date: 2026-07-11
- Request summary: discovered while validating an unrelated Non-trivial
  Fable workflow-contract change (`ADR-0226`) — running the required
  workflow-contract-lane checker unexpectedly failed on content that was
  independently confirmed correct by direct inspection.
- Request reference (chat/issue/link): same session as `ADR-0226`.

## Symptoms and Impact
- What failed or looked wrong? The checker reported a false failure on
  correct file content, blocking a clean validation-lane pass.
- User/operator impact: none directly; affects Fable agents' ability to
  self-validate workflow-contract changes on any checkout with
  `core.autocrlf=true` (the common case on Windows).
- Scope and affected components: `scripts/check-fable-workflow-contract.ps1`
  only. Not caused by, and unrelated to, the content of `ADR-0226`'s 9
  changed files.

## Timeline
1. 2026-07-11 - `scripts/check-fable-workflow-contract.ps1` run as part of
   `ADR-0226`'s workflow-contract-lane validation; failed unexpectedly.
2. 2026-07-11 - direct `grep` confirmed the flagged text was present
   verbatim in both target files, ruling out a content defect.
3. 2026-07-11 - byte-level inspection of the checker script confirmed CRLF
   line endings in its `$validationBlock` here-string.
4. 2026-07-11 - non-mutating diagnostic copy with normalization added
   passed cleanly, confirming the hypothesis.
5. 2026-07-11 - one-line fix applied to the checker script; full test suite
   (`scripts/test-fable-workflow-contract.ps1`) passes.

## Hypotheses and Tests
1. Hypothesis A - The target files are genuinely missing the required text.
   - Evidence/commands: `grep -n "Validation Score: X/6" -A2
     docs/fable-validation.md docs/templates/fable-non-trivial-change-template.md`
   - Outcome: Rejected - text present verbatim in both files.
2. Hypothesis B - `ADR-0226`'s edits accidentally altered the validation
   block region.
   - Evidence/commands: `git diff --unified=1` restricted to the validation
     block lines in both files.
   - Outcome: Rejected - no diff hunks touch that region.
3. Hypothesis C - CRLF/LF mismatch between the checker's own here-string
   literal and the normalized target-file content.
   - Evidence/commands: byte dump of the checker script's here-string
     (`0D 0A` present between lines); non-mutating diagnostic copy with
     `-replace "`r`n","`n"` added to `$validationBlock`, run with explicit
     `-RepoRoot`, passed cleanly.
   - Outcome: Supported.

## Findings
- Root cause (or best current explanation): missing CRLF normalization on
  the checker's own embedded here-string literal, asymmetric with the
  normalization already applied to inspected target-file content.
- Contributing factors: `core.autocrlf=true` on this checkout converts the
  checker script itself to CRLF on write, which PowerShell's here-string
  parsing preserves literally.
- Why this did or did not require a durable decision: no ADR needed - this
  is a mechanical checker self-consistency bug with a direct, narrow fix
  that restores the checker's own already-stated normalization intent; it
  does not change any workflow-contract rule, evidence requirement, or
  reporting shape.

## Decision Linkage
- ADR created/updated: none
- Decision delta summary: n/a
- Contract/behavior changes (or `No contract changes`): No contract changes
  - the checker's required text and structure are unchanged; only its
  internal comparison correctness was fixed.

## Verification and Monitoring
- Validation steps run: `pwsh -File scripts/check-fable-workflow-contract.ps1`
  (PASS after fix); `pwsh -File scripts/test-fable-workflow-contract.ps1`
  (all positive and negative fixtures PASS after fix).
- Signals to monitor (metrics/logs): none - one-time mechanical fix.
- Rollback triggers: none identified; the fix only removes a false-failure
  condition and does not change what the checker accepts as valid content.

## References
- Issue(s): none
- PR(s): none
- Commit(s): pending (uncommitted at time of writing)
- Related ADR(s): ADR-0226
- Related docs: `scripts/check-fable-workflow-contract.ps1`,
  `scripts/test-fable-workflow-contract.ps1`
