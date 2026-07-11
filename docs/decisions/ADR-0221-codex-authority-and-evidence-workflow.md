# ADR-0221: Codex Authority And Evidence Workflow

- Status: Accepted
- Date: 2026-07-10
- Decision Origin: Design

## Context

The Codex workflow accumulated strong engineering guidance alongside mandatory
markers, numeric scorecards, repeated evidence fields, default specialist and
independent-agent stages, forced slice schemas, no-change ADRs, and hard
context-reduction gates. Repository review found that this reporting and
orchestration layer increased reasoning and output without demonstrated quality
benefit, while also producing contradictory validation-lane and ownership
rules.

The repository owner requires lower token and coordination cost without
weakening exact approval, current evidence, bounded scope, code quality,
touched-surface validation, final review, durable decisions, or the unique
methods of domain specialists.

## Decision

Adopt a Codex workflow that constrains authority, evidence, scope, and claims
without prescribing universal narration.

1. Keep a short always-loaded contract covering Read-only, Small, and
   Non-trivial routes; exact `Approved vN`; agreed scope; reapproval on
   expansion; evidence honesty; touched-surface validation; and final review.
2. Treat Standard versus High-risk as internal routing. Load specialists and
   independent review only for concrete positive risks.
3. Preserve every retained specialist's unique engineering method, including
   ambiguity registers, normative model derivation and independent golden
   vectors, design comparison, falsifiability analysis, code walking,
   blast-radius analysis, and domain-specific engineering checks.
4. Remove mandatory skill markers, reasoning-budget fields, ledger-status
   echoes, marker order, agent envelopes, Self-Audit taxonomies, numeric
   validation scores, visible irrelevant categories, and exact closeout blocks.
5. Decompose work when real rollback, ownership, uncertainty, or validation
   boundaries exist. A bounded coherent change may remain one slice. Broad
   refactor-shaped scope is not approval-ready; no fixed slice schema is
   required.
6. Run targeted checks while working and one complete final touched-surface
   lane. Review fixes rerun affected checks and rerun the full lane only when
   broader evidence can be invalidated.
7. Codex creates or updates ADRs only for durable decisions and TSRs for
   durable troubleshooting learning. Fable retains its existing mandatory ADR
   handling until separately changed.
8. Static checkers enforce only representable text, ownership, reference,
   trigger, legacy-absence, and supplied path invariants. They explicitly do
   not prove conversational compliance or engineering quality.
9. Context measurement preserves immutable revision reads and declared
   scenarios but is informational only. It is not an adoption or quality gate.

## Prior Decision Disposition

| Prior ADR | Disposition |
| --- | --- |
| ADR-0092, ADR-0119, ADR-0144 | Superseded; exact approval, lead scope challenge, and meaningful decomposition are restated without marker or field schemas |
| ADR-0199, ADR-0202, ADR-0203, ADR-0209, ADR-0211, ADR-0216 | Superseded; when-used phase boundaries and lead ownership remain, while default agents and evidence choreography are removed |
| ADR-0204 | Superseded; minimal current evidence and privacy safeguards remain without a prescribed report location |
| ADR-0213, ADR-0219, ADR-0220 | Superseded; retained authority and ownership controls are restated without scorecards or hard context gates |
| ADR-0072 | Codex-only mandatory no-change ADR application superseded; Fable application remains accepted |
| ADR-0156 | Universal routing and mandatory template evidence fields selectively superseded; repo-managed skills, concrete triggers, and unique methods remain |
| ADR-0155, ADR-0179, ADR-0194, ADR-0210 | Retained: repo skill authority, touched-surface documentation lane, current claim evidence and anti-speculation, and executor-aware validation boundaries |
| ADR-0094, ADR-0103, ADR-0105, ADR-0106 | Substantive support, YAML, and Go-comment duties retained; mandatory output fields retired |

Fable-owned decisions and workflow files are unchanged.

## Alternatives Considered

1. Implement the v6/v7 seven-outcome scorecard.
   - Rejected because it preserved numeric compliance narration under a smaller
     taxonomy.
2. Remove domain specialist guidance.
   - Rejected because its concrete engineering methods prevent real config,
     lifecycle, retained-state, performance, and model failures.
3. Split code-quality guidance into multiple files.
   - Deferred because one canonical document with independently readable
     headings has lower routing and drift risk.
4. Use context reduction as an adoption gate.
   - Rejected because file size cannot establish engineering quality.

## Consequences

### Benefits

- Ordinary planning and closeout load and report substantially less process.
- Exact authority, evidence honesty, validation, and engineering standards
  remain explicit.
- Specialists address concrete risks rather than becoming workflow stages.
- Static checkers no longer claim to prove conversational or semantic quality.

### Risks

- Over-compression could make a specialist hard to discover.
- A checker could still mistake keyword presence for semantic compliance.
- Shared document edits could indirectly change Fable behavior.

Positive and negative trigger fixtures, method-preservation checks, explicit
checker disclaimers, and cross-executor shared-document review mitigate these
risks.

### Operational Impact

- No Go runtime, config, protocol, parser, queue, lifecycle, deployment, or
  operator-command behavior changes.
- No Go validation or live model evaluation is part of this decision.
- The release-script correction at baseline `820bc54` remains untouched.

## Links

- Related docs: `AGENTS.md`, `VALIDATION.md`, `docs/change-workflow.md`,
  `docs/code-quality.md`, `docs/review-checklist.md`, `docs/dev-runbook.md`,
  `docs/decision-memory.md`, `docs/templates/non-trivial-change-template.md`,
  `codex-skills/`
- Related checks: `scripts/check-workflow-contract.ps1`,
  `scripts/test-workflow-contract.ps1`, `scripts/verify-codex-skills.ps1`,
  `scripts/measure-codex-workflow-context.ps1`,
  `scripts/test-measure-codex-workflow-context.ps1`
- Related TSRs: none
- Supersedes / superseded by: supersedes the prior ADRs as classified above

