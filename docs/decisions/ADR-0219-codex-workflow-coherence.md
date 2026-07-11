# ADR-0219: Codex Workflow Coherence

- Status: Superseded
- Date: 2026-07-10
- Decision Origin: Design

## Context

The Codex workflow preserved strict approval and validation rigor but developed
contradictions around non-mutating reviews, validation-lane precedence, Scope
Ledger status authority, exact skill markers, and independent-agent evidence
states. Presence-only checker fixtures could not detect several of those
contradictions.

The repository owner requires token efficiency without reducing discovery,
approval, evidence, independent review, validation, decision memory, or
traceability.

## Decision

Extend the existing canonical Codex workflow instead of creating a second
review contract or template.

1. Non-mutating explanation, review, audit, diagnosis, prioritization, and
   requested recommendations use a first-class read-only route. They retain
   current-source, claim-evidence, and applicable independent-review duties but
   do not use change approval, implementation markers, change-validation
   lanes, traceability, or the Non-trivial validation score. Any later mutation
   enters the applicable change gate first.
2. Task size controls approval rigor; touched surface controls validation.
3. Only `Agreed` Scope Ledger items are approval-eligible, executable, and
   traceable. `Pending` blocks approval; `Rejected` and `Deferred` are excluded.
4. Emit exactly one standalone skill marker per assistant turn.
   Independent-agent report fields use one canonical status enum, with role outcome,
   waiver, and explanatory detail represented separately.
5. Mechanical checks enforce representable fields and exact contract lines.
   Human workflow-drift review remains responsible for conversational timing,
   genuine independence, approval, and higher-order semantics.

## Alternatives considered

1. Add a standalone read-only workflow document and template.
   - Rejected because it creates duplicate evidence and status owners.
2. Treat broad read-only work as a Non-trivial change.
   - Rejected because it requires fictitious approval and implementation
     evidence for a non-mutating request.
3. Leave lane, scope-status, and agent-status interpretation to Codex.
   - Rejected because those interpretations affect authority and validation.
4. Replace the checker with a generalized workflow manifest.
   - Deferred outside P0; targeted enforcement is sufficient for these rules.

## Consequences

### Benefits

- Read-only work remains rigorous without change-only ceremony.
- Approval and traceability become fail-closed and single-valued.
- Validation selection no longer depends on task-size wording.
- Exact report fields can be checked without banning ordinary prose.

### Risks

- Agents could misuse the read-only route to avoid a change gate.
- Static checks could be mistaken for proof of conversational compliance.
- Shared runbook wording could drift from the Fable contract.

The transition boundary, checker disclaimers, negative fixtures, and
cross-executor review mitigate these risks.

### Operational impact

- No Go runtime, protocol, parser, config, telnet, queue, persistence, or
  operator-command behavior changes.
- Codex developer workflow, PowerShell workflow checks, repo skills, human
  guidance, and developer-support routing change.
- No measured token-saving, development-speed, or model-quality claim is made.

## Links

- Related issues/PRs/commits: none
- Related tests: `scripts/check-workflow-contract.ps1`,
  `scripts/test-workflow-contract.ps1`, `docs/workflow-eval-cases.md`
- Related docs: `AGENTS.md`, `VALIDATION.md`, `docs/change-workflow.md`,
  `docs/review-checklist.md`, `docs/dev-runbook.md`,
  `docs/templates/non-trivial-change-template.md`,
  `docs/WORKING_WITH_CODEX.md`, `codex-skills/`, `customgpt/`
- Related ADRs: ADR-0092, ADR-0119, ADR-0144, ADR-0179, ADR-0194, ADR-0199,
  ADR-0202, ADR-0203, ADR-0204, ADR-0210, ADR-0211, ADR-0213, ADR-0216
- Related TSRs: none
- Supersedes / superseded by: superseded by ADR-0221
