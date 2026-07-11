# ADR-0225: Remove Codex Target Reasoning Recommendation

- Status: Accepted
- Date: 2026-07-11
- Decision Origin: Design

## Context

ADR-0222 Decision 1 required every Non-trivial Codex Scope Ledger to estimate
and present the lowest sufficient target reasoning level, a rationale, an
escalation condition, and a user override. The recommendation does not itself
change the already-selected reasoning setting for the active Codex session.
The field therefore adds planning narration and can anchor deliberation toward
a lower setting without changing authority, evidence, validation, or
execution.

The earlier model-specific workflow evaluations did not establish a quality or
token-efficiency benefit for reasoning-level recommendations. The repository
still needs explicit scope, risks, unknowns, validation, exact approval, and
material reapproval, but those controls do not depend on a model-effort field.

## Decision

1. This decision selectively supersedes ADR-0222 Decision 1.
2. Codex Scope Ledgers no longer estimate, recommend, or present a target
   reasoning level or effort, rationale, escalation condition, or user
   override.
3. No replacement model-effort field or alternate reasoning-budget terminology
   is added to the Codex workflow.
4. Codex may discuss reasoning effort when the user explicitly asks; that
   discussion is not a Scope Ledger requirement or authority signal.
5. Active Codex contract surfaces and static fixtures reject reintroduction of
   a mandatory target-reasoning field. Accepted ADR history remains intact.
6. This decision does not change current-evidence discovery, material unknown
   handling, Scope Ledger versioning, exact `Approved vN` authority, agreed
   scope, material reapproval, specialist triggers, validation, review,
   decision memory, or closeout.
7. Fable's separate reasoning-budget policy and Fable-owned workflow remain
   unchanged.

## Alternatives considered

1. Keep the mandatory recommendation.
   - Rejected because it adds an unexecuted model-setting estimate to every
     Non-trivial approval packet without demonstrated workflow benefit.
2. Replace the field with a different effort taxonomy.
   - Rejected because renaming preserves the same narration and anchoring.
3. Forbid all reasoning-effort discussion.
   - Rejected because user-requested model-setting advice remains legitimate;
     only the mandatory workflow field is removed.

## Consequences

### Benefits

- Scope Ledgers focus on authority, boundaries, risks, unknowns, and evidence.
- Codex no longer recommends a setting it cannot apply to the current session.
- The removal reduces planning narration without weakening approval or
  validation.

### Risks

- Users no longer receive unsolicited model-effort advice in every
  Non-trivial plan.
- Historical references to the superseded requirement can be mistaken for
  current policy unless reciprocal decision links remain clear.

### Operational impact

- No Go runtime, config, protocol, parser, queue, lifecycle, deployment, or
  operator behavior changes.
- Codex workflow Markdown, its checker fixtures, and decision memory change.
- No Fable-owned workflow changes.

## Links

- Related issues/PRs/commits:
- Related tests: `scripts/check-workflow-contract.ps1`,
  `scripts/test-workflow-contract.ps1`
- Related docs: `AGENTS.md`, `docs/change-workflow.md`,
  `docs/WORKING_WITH_CODEX.md`,
  `docs/templates/non-trivial-change-template.md`
- Related TSRs: none
- Supersedes / superseded by: selectively supersedes ADR-0222 Decision 1; not
  superseded
