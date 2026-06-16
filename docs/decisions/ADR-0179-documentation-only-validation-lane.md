# ADR-0179: Documentation-Only Validation Lane

- Status: Accepted
- Date: 2026-06-15
- Decision Origin: Design

## Context
The Codex workflow used strict Non-trivial validation gates to protect runtime
correctness, but the runbook treated Go validation as the default closeout path
even when a change only edited Markdown documentation. That made documentation
maintenance expensive and consumed review tokens on Go suites that could not
validate Markdown wording.

The repository still needs strict approval, review, ADR handling, and
traceability for Non-trivial workflow and operator documentation changes. The
gap is validation proportionality, not workflow rigor.

## Decision
Add a documentation-only Markdown validation lane.

When a diff changes only Markdown documentation and does not touch code, config,
generated artifacts, scripts, CI, schemas, protocol/runtime contracts, or
checked-in data consumed by the runtime, Codex validates with documentation
review, targeted text checks, workflow/support drift checks when applicable, and
`git diff --check`.

Code, mixed, config, YAML, script, CI, generated-artifact, parser/protocol,
runtime-contract, concurrency, lifecycle, queue, hot-path, and runtime-data
changes continue to use their existing targeted or full Go validation paths.
If a documentation-only task expands beyond Markdown, Codex must switch to the
appropriate validation lane before closeout.

## Alternatives considered
1. Keep full Go validation for all Non-trivial work.
   - Rejected because Go suites do not validate Markdown-only wording and add
     unnecessary token/runtime cost.
2. Let agents waive Go validation ad hoc for documentation tasks.
   - Rejected because waivers make validation inconsistent and easy to misuse.
3. Make all documentation work Small.
   - Rejected because workflow, ADR, operator, support-agent, and contract docs
     can be Non-trivial even when no code changes.

## Consequences
### Benefits
- Documentation-only Markdown changes get validation that matches the changed
  artifact.
- Runtime and mixed changes keep the existing code-validation rigor.
- Workflow closeouts can remain strict without spending Go-suite validation on
  Markdown-only edits.

### Risks
- A mixed diff could be misclassified as documentation-only.
- Documentation can describe runtime behavior incorrectly even when Markdown
  checks pass.

### Operational impact
- No runtime, config, protocol, parser, queue, telnet, peer, archive, CI, or
  release behavior changes.
- Future documentation-only Markdown tasks should report the selected validation
  lane and must switch lanes if code, config, generated artifacts, scripts, CI,
  schemas, protocol/runtime contracts, or runtime-consumed data become involved.

## Links
- Related issues/PRs/commits:
- Related tests: targeted workflow text checks, `git diff --check`
- Related docs: `AGENTS.md`, `docs/change-workflow.md`, `docs/dev-runbook.md`,
  `VALIDATION.md`, `docs/templates/non-trivial-change-template.md`,
  `docs/review-checklist.md`, `.github/pull_request_template.md`
- Related TSRs:
- Supersedes / superseded by:
