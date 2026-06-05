# ADR-0144: Slice-Shaped Scope Ledgers

- Status: Accepted
- Date: 2026-06-05
- Decision Origin: Design

## Context
The repository already required Scope Ledgers, approval gates, current-state
discovery, incremental validation, and implementation slices. The weak point was
that a proposed Scope Ledger could still describe broad refactor-shaped work and
defer slicing until after approval.

That left too much room for large diffs, late blast-radius discovery, and
validation only after multiple uncertain changes had already been combined.

## Decision
Make slice-shaped Scope Ledgers a hard gate for Non-trivial work.

Every approved implementation slice must be independently codeable, testable,
and reviewable. Each slice must state:

- objective
- bounded files, packages, or docs expected to change
- blast-radius boundary and explicit out-of-slice work
- production-safe stopping point
- targeted checks to run before the next slice starts

Broad entries such as "refactor the parser", "clean up telnet", or "rewrite
config handling" are not approval-ready until decomposed. A mechanical migration
may remain one slice only when its target set is bounded, the transformation is
uniform, and the validation path is narrow and explicit.

## Consequences
### Benefits
- Reduces implementation blast radius by default.
- Forces reviewable stopping points before code starts.
- Makes per-slice validation evidence mandatory instead of aspirational.
- Gives humans and support-agent routing a clear reason to reject broad
  refactor ledgers.

### Risks
- Some tasks require more up-front planning before approval.
- Over-slicing could add ceremony for purely mechanical migrations; the
  bounded mechanical-migration exception keeps those cases practical.

### Operational impact
- No runtime, config, protocol, parser, queue, telnet, peer, archive, or
  user-visible behavior changes.
- Future Non-trivial tasks must provide slice-level evidence in Phase A and
  Phase B closeout.

## Links
- Related issues/PRs/commits:
- Related tests:
  - workflow text checks for slice-shaped Scope Ledger hard gate
  - `git diff --check`
- Related docs: `AGENTS.md`, `docs/change-workflow.md`,
  `docs/templates/non-trivial-change-template.md`, `VALIDATION.md`,
  `docs/WORKING_WITH_CODEX.md`, `customgpt/source-map.md`,
  `customgpt/developer-guide-index.md`, `customgpt/common-questions.md`
- Related TSRs:
- Supersedes / superseded by:
