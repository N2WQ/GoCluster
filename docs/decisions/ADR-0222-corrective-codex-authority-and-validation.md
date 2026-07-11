# ADR-0222: Corrective Codex Authority And Validation

- Status: Accepted
- Date: 2026-07-10
- Decision Origin: Design

## Context

ADR-0221 established a substantially smaller Codex authority-and-evidence
workflow. Review of its implementation found bounded defects rather than a
failure of that architecture: the reasoning recommendation was prohibited,
sensitive work could enter Small, production-Go final validation became
conditional, shared documents changed Fable obligations, standing subagent
authorization disappeared, specialist routes referenced retired artifacts,
and the multiple-package review trigger was broader than necessary.

The correction must restore rigor and cross-executor boundaries without
restoring reporting choreography, default specialists, repeated suites,
scorecards, or no-change decision records.

## Decision

Selectively supersede ADR-0221 only as follows:

1. Every Non-trivial Scope Ledger presents the lowest sufficient target
   reasoning level once before approval, with a concise rationale, escalation
   condition, and user override. No exact heading or repeated reporting is
   required.
2. Runtime config/schema/default/sentinel semantics, parser or protocol
   behavior, authentication/admission, persisted state, scientific/model
   semantics, hot paths, shared contracts, material operator behavior, and
   durable decisions cannot use the Small route.
3. Every Non-trivial production-Go change runs targeted tests while working,
   then `go test ./...`, `go vet ./...`, `staticcheck ./...`, and the configured
   linter once on the final relevant state. Triggered race, fuzz, benchmark,
   profile, config, and comment checks remain additive.
4. Shared code-quality and validation documents preserve Fable's existing
   obligations. Codex-only methods belong in Codex workflow documents,
   runbooks, or triggered skills unless a separate Fable scope approves them.
5. The repository owner's standing authorization permits subagent use when
   active platform policy permits. It does not require use, expand scope,
   bypass approval, authorize pre-approval edits, or transfer lead authority.
6. Specialist integrations reference current command evidence, approved scope,
   implementation design, and relevant discovery/validation evidence rather
   than retired markers or report sections.
7. Multiple production packages trigger independent Go review when shared
   behavior, ownership, interfaces, contracts, or meaningful cross-package
   uncertainty are affected. High-risk classification, shared/exported
   interfaces, material algorithms or state machines, substantial rewrites,
   and residual uncertainty remain independent triggers.
8. Static fixtures preserve these concepts and their canonical ownership, not
   headings, field order, sentence structure, or repeated narration. They do
   not prove live classification or engineering judgment.

ADR-0221 remains Accepted and authoritative for its unaffected simplification.
ADR-0211 remains historical; this ADR independently restates the retained
standing-authorization principle.

## Alternatives Considered

1. Revert ADR-0221 wholesale.
   - Rejected because its authority-and-evidence architecture remains sound and
     materially reduces workflow context and narration.
2. Rewrite ADR-0221 in place.
   - Rejected because accepted decision history must remain intact and
     reciprocally linked.
3. Keep production-Go checks discretionary.
   - Rejected because efficiency should come from one final suite, not weaker
     final evidence.

## Consequences

### Benefits

- Restores approval and validation rigor while preserving token-efficient
  reporting and risk-triggered specialization.
- Makes shared-executor boundaries explicit.
- Avoids independent review for mechanical multi-package edits without
  weakening review of shared impact.

### Risks

- Static wording checks may be mistaken for proof of live compliance; checker
  output must retain its explicit limitation.
- Shared-document reorganization can drift again unless Fable obligations are
  reviewed whenever those documents change.

### Operational Impact

- No Go runtime, config, protocol, parser, queue, lifecycle, deployment, or
  operator-command behavior changes.
- This workflow-only implementation does not run Go tests, Fable checkers,
  live/model evaluations, or context measurement.

## Links

- Related docs: `AGENTS.md`, `docs/change-workflow.md`,
  `docs/WORKING_WITH_CODEX.md`, `docs/code-quality.md`,
  `docs/dev-runbook.md`, `docs/review-checklist.md`,
  `docs/templates/non-trivial-change-template.md`, `codex-skills/`
- Related checks: `scripts/check-workflow-contract.ps1`,
  `scripts/test-workflow-contract.ps1`, `scripts/verify-codex-skills.ps1`
- Related TSRs: none
- Supersedes / superseded by: selectively supersedes ADR-0221
