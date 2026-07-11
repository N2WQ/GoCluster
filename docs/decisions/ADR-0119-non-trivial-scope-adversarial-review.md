# ADR-0119: Non-trivial Scope Adversarial Review

- Status: Superseded
- Date: 2026-05-06
- Decision Origin: Design

## Context
Repeated Scope Ledger review passes exposed a workflow gap: the existing
Non-trivial approval gate required current-state discovery and exact
`Approved vN` approval, but it did not require Codex to adversarially test the
ledger itself before asking for approval.

The workflow already had a `Requirements & Edge Cases Note`, but that note ran
after approval as part of implementation planning. That timing allowed an
unsafe or incomplete ledger to be approved before edge cases were forced into
scope, deferred, or rejected explicitly.

## Decision
Add a mandatory pre-approval `SCOPE ADVERSARIAL REVIEW` marker for every
Non-trivial Scope Ledger.

Before presenting the exact approval token, Codex must ask what edge case would
make the proposed scope unsafe or incomplete. The review checks applicable
operational and workflow edge areas, classifies any material issue as covered,
out of scope, or requiring a revised ledger, and repeats with a new ledger
version until the disposition is `nothing material found`.

The rule is enforced in the always-loaded contract, the detailed workflow, the
Non-trivial template, and the validation rubric.

## Alternatives considered
1. Rely on the post-approval `Requirements & Edge Cases Note`.
   - Rejected because that note runs after approval and cannot protect the
     approval boundary from incomplete scope.
2. Add the review only to the template.
   - Rejected because template-only rules are easier to miss than a rule also
     present in `AGENTS.md`, `docs/change-workflow.md`, and `VALIDATION.md`.
3. Rely on the user to request repeated ledger reviews manually.
   - Rejected because edge-case discovery is Codex's responsibility under the
     repository execution contract.

## Consequences
### Benefits
- Forces edge-case review before the user is asked to approve Non-trivial
  scope.
- Makes scope revisions explicit through new ledger versions.
- Keeps exact `Approved vN` discipline intact.

### Risks
- Adds a small amount of Phase A workflow overhead.
- If written too broadly in future edits, the marker could become checklist
  noise instead of a focused scope-safety review.

### Operational impact
- No runtime, telnet, config, archive, protocol, or support-agent behavior
  changes.
- Future Non-trivial work must include the new marker before approval.

## Links
- Related issues/PRs/commits: current working tree
- Related tests:
  - `VALIDATION.md`
  - `docs/templates/non-trivial-change-template.md`
- Related docs:
  - `AGENTS.md`
  - `docs/change-workflow.md`
- Related TSRs: none
- Supersedes / superseded by: superseded by ADR-0221
