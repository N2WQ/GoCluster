# ADR-0223: Bounded Specialist Context And Independent Evidence

- Status: Accepted
- Date: 2026-07-11
- Decision Origin: Design

## Context

ADR-0221 made Codex specialists and independent review risk-triggered rather
than default workflow stages. ADR-0222 restored standing subagent authorization
and described broad Go-review triggers. The resulting contract conflated three
different decisions: whether a specialist method applies, whether a bounded
subagent would partition context usefully, and whether credible evidence
requires reasoning outside the lead's accumulated assumptions.

That conflation also produced a contradiction. High-risk work required a fresh
final pass, independent Go review was described as required for broad triggers,
and a fresh lead pass was simultaneously accepted as a universal substitute for
independence.

## Decision

1. Keep specialist triggering risk-based. Non-trivial or High-risk status alone
   does not create a default subagent stage.
2. Localized, already-bounded specialist work may remain lead-owned. When a
   triggered investigation materially benefits from context partitioning, use
   a bounded subagent when supported.
3. Require a separate non-steered context only when same-context reasoning would
   compromise the credibility of the evidence. A fresh lead pass is useful
   verification but is not independent review.
4. High-risk work retains mandatory fresh final verification, which may be
   lead-owned. Go risk triggers the Go code-quality review method; the evidence
   need determines whether that method also requires an independent context.
5. When preferred delegation is unavailable, a disclosed fresh lead pass is
   valid. When required independent context is unavailable, pause or proceed
   only with explicit user approval and clearly limit the affected claim.
6. Independent findings are evidence, not transferred authority. The lead
   verifies material evidence, dispositions findings, resolves conflicts, and
   owns scope, approval, integration, validation, and final claims without
   duplicating the complete delegated investigation.
7. Static enforcement is limited to representable routing and authority
   relationships. It cannot prove that a separate context was used, that a
   briefing was neutral, or that reasoning was competent.

This decision selectively refines ADR-0221 Decision 2 by separating specialist
triggering, context selection, and independent-evidence requirements. It
selectively supersedes ADR-0222 Decision 7's interpretation that every listed Go
method trigger itself requires a separate independent reviewer. All unrelated
ADR-0221 and ADR-0222 decisions remain accepted.

## Alternatives Considered

1. Require independent agents for every Non-trivial or High-risk task.
   - Rejected because it restores default orchestration without an evidentiary
     need.
2. Require independent review before approval and after implementation for
   every task.
   - Rejected because review phase follows the triggered risk; two universal
     gates add ceremony and duplicate work.
3. Treat a fresh lead pass as independent review.
   - Rejected because the lead retains its accumulated assumptions and cannot
     supply evidence from a separate reasoning context.
4. Add mandatory execution-mode fields, agent envelopes, or panels.
   - Rejected because the routing rule does not require new reporting
     choreography.

## Consequences

### Benefits

- Broad specialist evidence can be partitioned without loading the complete
  investigation into the lead context.
- Independent evidence is reserved for cases where separation affects
  credibility.
- Fresh High-risk verification, lead authority, and ordinary Non-trivial flow
  remain intact.

### Risks

- Context-partitioning value and evidentiary independence require judgment that
  static tooling cannot prove.
- Over-triggering can recreate orchestration cost; under-triggering can leave
  shared assumptions unchallenged.
- Subagents can inspect the wrong baseline or return unsupported findings, so
  lead verification remains required.

### Operational Impact

- Codex workflow routing, review wording, specialist integration, decision
  memory, and workflow checker fixtures change.
- No Go runtime, config, protocol, parser, scientific/model, deployment, or
  operator-command behavior changes.
- Fable-owned workflow files and obligations remain unchanged.

## Links

- Related docs: `AGENTS.md`, `VALIDATION.md`, `docs/change-workflow.md`,
  `docs/review-checklist.md`, `docs/WORKING_WITH_CODEX.md`, `codex-skills/`
- Related checks: `scripts/check-workflow-contract.ps1`,
  `scripts/test-workflow-contract.ps1`, `scripts/verify-codex-skills.ps1`
- Related TSRs: none
- Supersedes / superseded by: selectively refines ADR-0221 Decision 2 and
  selectively supersedes ADR-0222 Decision 7
