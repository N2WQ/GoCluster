# ADR-0199: Subagent Delegation Boundaries

- Status: Accepted
- Date: 2026-06-19
- Decision Origin: Design

## Context
GoCluster already had strict Codex workflow gates for Scope Ledgers,
`Approved vN`, pre-approval `SCOPE ADVERSARIAL REVIEW`, slice-shaped
implementation, fresh verification, claim evidence, decision memory, and
documentation-only validation.

The active Codex environment can support delegated or parallel subagents. That
creates a useful opportunity for independent evidence gathering and verification,
but also a workflow risk: a subagent could be misread as replacing the lead
agent's approval-gate responsibility, or a worker could edit outside an approved
slice.

The existing docs allowed an independent verifier when environment and user
authorization support it, but did not define general boundaries for
pre-approval explorers, adversarial-review explorers, post-approval workers, or
fresh-verifier explorers.

## Decision
Add an explicit subagent-use contract to the Codex workflow.

Subagents may be used only when the active environment and user authorization
support delegated or parallel agent work.

Before exact `Approved vN`, subagents are read-only explorers. They may gather
evidence or challenge a proposed scope, including independent adversarial review
of `Proposed Scope Ledger vN`, but they must not edit files, propose diffs, run
formatters, create generated artifacts, or run full validation suites.

After exact `Approved vN`, worker subagents are allowed only for approved,
disjoint implementation slices. A worker assignment must name the approved
scope version, slice objective, base revision or integration point, allowed
paths, forbidden paths, production-safe stopping point, targeted checks,
expected output, and stop conditions for hidden blast radius, overlap, failed
assumptions, or scope uncertainty.

For high-risk closeout, prefer a read-only fresh-verifier explorer when the
environment and user authorization support it. The verifier reports findings
only.

Subagent output is evidence, not transferred authority. The lead Codex agent
owns Scope Ledger disposition, `SCOPE ADVERSARIAL REVIEW`, integration, final
Review Pass, validation claims, ADR/TSR handling, Scope-to-Code Traceability,
and the final response.

## Alternatives considered
1. Continue using only the existing fresh-verifier wording.
   - Rejected because it did not define pre-approval explorers, adversarial
     review, or post-approval workers.
2. Make subagent use mandatory for high-risk work.
   - Rejected because tool availability and user authorization vary. The
     workflow must preserve a single-agent fallback.
3. Allow worker subagents for any approved task.
   - Rejected because overlapping docs or code ownership can weaken review and
     hide scope expansion. Workers require disjoint approved slices.
4. Add a new repo-managed skill for subagent orchestration.
   - Rejected for now because the behavior can be routed through the existing
     workflow docs, templates, and review rules without adding another skill.

## Consequences
### Benefits
- Independent explorers can improve Current-State Discovery and Scope Ledger
  adversarial review without weakening the approval gate.
- Post-approval workers have explicit isolation and stop conditions.
- Fresh verification can use a read-only explorer when available while keeping
  final claims lead-owned.
- The support-agent routing layer can answer developer workflow questions about
  subagents by pointing to authoritative workflow docs.

### Risks
- Subagent language can be overused for small tasks where coordination costs
  exceed the value.
- Parallel workers remain risky on tightly coupled files; the lead agent must
  avoid delegation when write scopes are not cleanly disjoint.
- Parallel full-suite validation can create Go cache or export-data failures
  unless isolated; final validation remains lead-owned and normally sequential.

### Operational impact
- No runtime, config, protocol, parser, telnet, peer, archive, queue,
  persistence, connection, CI, generated-artifact, or operator command behavior
  changes.
- Future Codex workflow responses should report subagent authorization, phase,
  allowed actions, findings used as evidence, and lead-owned disposition when
  subagents are used.
- Documentation-only validation remains the correct lane when this policy is
  changed without touching code, config, CI, generated artifacts, schemas,
  protocol/runtime contracts, or runtime-consumed data.

## Links
- Related issues/PRs/commits:
- Related tests:
  - targeted workflow text checks
  - support-agent routing checks when `customgpt/` routing changes
  - `git diff --check`
- Related docs:
  - `AGENTS.md`
  - `docs/change-workflow.md`
  - `docs/templates/non-trivial-change-template.md`
  - `docs/review-checklist.md`
  - `docs/dev-runbook.md`
  - `VALIDATION.md`
  - `docs/WORKING_WITH_CODEX.md`
  - `customgpt/source-map.md`
  - `customgpt/developer-guide-index.md`
  - `customgpt/common-questions.md`
- Related ADRs: ADR-0202, ADR-0205
- Related TSRs: none
- Supersedes / superseded by: none
