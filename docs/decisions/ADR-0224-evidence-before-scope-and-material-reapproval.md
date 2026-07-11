# ADR-0224: Evidence Before Scope And Material Reapproval

- Status: Accepted
- Date: 2026-07-11
- Decision Origin: Design

## Context

The Codex workflow requires inspection of current implementation, contracts,
tests, documentation, and decision history before Non-trivial approval. It also
requires a scope challenge and exact version-matched approval. The contract did
not explicitly require a safely testable, approach-invalidating unknown to be
resolved before scope, so an assumed mechanism could enter a Scope Ledger as if
it were established current behavior.

The revision rule also focused on material gaps and work exceeding approved
scope. New evidence can instead change authority, accepted risk, or required
validation without visibly expanding implementation scope. Those changes still
affect what the approval token authorizes.

The workflow should improve evidence-based reasoning without adding scorecards,
fixed ledger schemas, repeated narration, default specialists, or compliance
stages.

## Decision

1. Before proposing Non-trivial scope, Codex resolves a material unknown that
   could invalidate the approach using the smallest safe read-only proof.
2. When safe read-only evidence cannot resolve the unknown, Codex carries it as
   an explicit assumption or unknown. An assumed mechanism is not presented as
   current fact.
3. `AGENTS.md` keeps the concise authority rule. `docs/change-workflow.md` owns
   the detailed discovery and scope semantics.
4. Codex challenges the proposed scope before approval. New evidence requires a
   revised ledger and exact reapproval when it materially changes authority,
   scope, accepted risk, or required validation.
5. Wording-only changes do not require a new ledger version.
6. Existing pre-approval mutation restrictions, exact `Approved vN` authority,
   validation routing, specialist triggers, and Fable-owned workflow remain
   unchanged.
7. Static workflow checks may establish that required authority text remains
   reachable. They cannot prove discovery sufficiency, the safety of a proof,
   the quality of a scope challenge, or improved model reasoning or code
   quality.

This decision refines ADR-0221's current-evidence and Non-trivial planning
rules. It does not change ADR-0222's target-reasoning recommendation or
ADR-0223's specialist, context-partitioning, or independent-evidence rules.

## Alternatives considered

1. Leave the workflow unchanged.
   - Rejected because it permits approval planning to solidify around a safely
     testable but unverified mechanism.
2. Resolve every unknown before presenting scope.
   - Rejected because some unknowns cannot be resolved safely or read-only;
     explicit uncertainty is more honest than forced certainty.
3. Put the detailed proof and revision rules in `AGENTS.md`.
   - Rejected because `AGENTS.md` is always loaded and should retain concise
     authority while the detailed workflow owns execution semantics.
4. Require a new version for every wording change.
   - Rejected because lexical churn does not change authority and would add
     approval ceremony without improving engineering evidence.
5. Expand static checkers to judge discovery and challenge quality.
   - Rejected because static text checks cannot establish those conversational
     or engineering judgments.

## Consequences

### Benefits

- Scope proposals are less likely to commit prematurely to an unverified
  mechanism.
- Unresolved assumptions remain visible instead of being converted into facts.
- Reapproval follows material changes to authority, scope, risk, or validation,
  not only obvious implementation expansion.
- Detailed procedure stays outside the always-loaded authority contract.

### Risks

- Material unknowns and safe read-only proof require engineering judgment.
- Overuse could add pre-approval investigation; the materiality and smallest-
  proof boundaries limit that cost.
- Static checks could be mistaken for evidence of reasoning quality; the
  workflow and this decision explicitly reject that inference.

### Operational impact

- No Go runtime, config, protocol, parser, queue, lifecycle, deployment, or
  operator behavior changes.
- No Fable-owned workflow changes.
- No live-model evaluation or measured reasoning, token, or code-quality claim
  is part of this decision.

## Links

- Related tests: `scripts/check-workflow-contract.ps1`,
  `scripts/test-workflow-contract.ps1`
- Related docs: `AGENTS.md`, `docs/change-workflow.md`,
  `docs/decision-log.md`
- Related TSRs: none
- Supersedes / superseded by: refines ADR-0221 current-evidence and planning
  rules; not superseded
