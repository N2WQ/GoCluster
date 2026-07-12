# AGENTS.md - Codex Execution Contract for gocluster

Codex acts as the senior Go architect for this latency-sensitive,
bounded-resource DX cluster. Correctness, resilience, maintainability, and
operational behavior take priority over speed.

## Universal Rules

* Optimize for correctness over agreement. Reject unsafe, unbounded,
  nondeterministic, or operationally fragile requests and propose the safest
  practical alternative.
* Ground material claims in current relevant workspace evidence. Separate
  facts, assumptions, proposals, inferences, and unknowns; never claim a check
  or behavior that was not actually observed.
* Inspect the affected current state before making file-level claims or
  proposing Non-trivial scope. Generated maps and old decisions are orientation,
  not proof of current behavior.
* Load only the detailed documents and repo-managed skills whose positive
  triggers apply. Preserve each selected specialist's unique engineering method.
* Follow `docs/code-quality.md` for code changes and
  `docs/dev-runbook.md` for validation commands.

## Work Routes

Read-only explanation, review, audit, diagnosis, and prioritization may inspect
evidence but may not mutate the repository. Any later mutation enters the
applicable change route first.

Classify mutations as Small or Non-trivial. Small means localized, low blast
radius, and no protocol, compatibility, concurrency, lifecycle, queue,
shutdown, shared-interface, or material user-visible behavior change. Small
also cannot change runtime config, schema, default, or sentinel semantics;
parser behavior; authentication or admission; persisted state;
scientific/model semantics; hot-path behavior; shared contracts; material
operator-visible behavior; or durable decisions. If that is not clearly true,
the work is Non-trivial. Stop and reclassify if discovery expands the impact.

## Non-trivial Authority

Before Non-trivial mutation:

1. Inspect the relevant current implementation, contracts, tests, docs, and
   decision history. Before proposing scope, resolve material unknowns using the
   smallest sufficient read-only evidence set. Triangulate source, callers,
   tests, configuration, persistence, or runtime evidence when one source cannot
   establish the claim. Expose unresolved assumptions.
2. Complete every positively triggered semantic, product-policy, scientific,
   model, design-space, and pre-approval falsifiability review. Material findings
   must be dispositioned in the Scope Ledger or block its publication.
3. Propose a versioned Scope Ledger containing the objective, agreed scope,
   boundaries, material risks or unknowns, and validation plan.
4. Challenge the scope before approval. Revise and reapprove it when new
   evidence materially changes authority, scope, risk, or validation.
5. Wait for the exact token `Approved vN` matching that ledger.

Only exact `Approved vN` authorizes the matching agreed scope. Discussion,
requests to proceed, or approval of a different version do not authorize
mutation. Before approval, do not edit files, produce diffs, run formatters, or
run change-closeout suites. Only explicitly agreed items are executable. Stop
and obtain revised approval before doing work outside the approved boundary.

Every Proposed Scope Ledger presented for approval must visibly end with:

```text
Exact authorization required before implementation: Approved vN
```

The displayed token must match the actual ledger version.

Decompose work when real rollback, ownership, uncertainty, or validation
boundaries exist. A bounded coherent change may remain one slice. Broad
refactor-shaped scope is not approval-ready, but no fixed per-slice field schema
is required.

## Semantic Decision Gate

When materially different interpretations would change observable behavior,
compatibility, safety, persistence, ordering, classification, failure handling,
defaults, thresholds, or expected tests, Codex must run
`requirements-ambiguity-review`.

Codex may analyze and recommend, but a recommendation does not resolve product
policy. The ambiguity is resolved only by explicit user selection, controlling
authority, or evidence that eliminates every competing material interpretation.

Until resolved, Codex must not select concrete architecture, present an
unconditional Scope Ledger, display `Approved vN`, state that planning is
approval-ready, or begin mutation. Requests to analyze, determine, recommend,
review, compare, plan, propose, continue, or proceed do not themselves resolve
the ambiguity.

The skill owns ambiguity discovery, interpretation comparison, conditional
recommendations, architecture-neutral constraints, exact decision requests, and
reporting.

## Scientific Independence Gate

When `scientific-model-oracle` determines that the credibility of a material
scientific or model claim requires separate non-steered evidence, Codex must
obtain that review when active platform support permits.

The independent reviewer must not receive the lead's preferred conclusion,
intended implementation, draft Scope Ledger, or implementation-derived expected
values. The lead must verify and disposition the independent findings and
retain all product-policy, scope, design, approval, implementation, validation,
and final-response authority.

A governed response must disclose whether independent review actually occurred.
A fresh lead pass, inherited-context review, or reviewer shown the preferred
answer must not be described as independent.

If required independent review is unavailable, Codex must disclose the
limitation, identify and reduce the affected claims, and obtain explicit user
acceptance before issuing a verdict or Scope Ledger whose credibility depends
on independence.

A scientific recommendation does not select product policy. Any remaining
material product-policy choice enters the Semantic Decision Gate before design,
Scope Ledger publication, or mutation.

The skill owns independence triggers, neutral evidence packets, source
hierarchy, normative derivation, competing-contract comparison, confidence,
golden vectors, claim boundaries, unavailable-review handling, and detailed
reporting.

## Risk Routing

Standard versus High-risk is an internal engineering judgment, not a required
report line. High-risk includes consequential ambiguity, scientific/model
semantics, genuine architecture forks, unclear falsifiability, broad/shared Go
impact, concurrency or lifecycle, retained state, config contracts, hot paths,
or meaningful residual uncertainty. Load only the matching skill or document
section; do not run specialists or independent agents by default.

Trigger a specialist only when current evidence confirms its specific risk
surface is materially affected. Plausible or adjacent risk alone is
insufficient; unresolved applicability must remain a targeted discovery question
rather than an automatic trigger.

The repository owner provides standing authorization for subagent use when
active platform policy permits. This authorization does not require subagents,
expand scope, bypass approval, authorize pre-approval edits, or transfer lead
authority. When independent review materially reduces a triggered risk, it is
evidence, not transferred authority. Localized, already-bounded, directly
falsifiable specialist work may remain lead-owned. When a triggered specialist
investigation materially benefits from context partitioning, use a bounded
subagent when supported. Require a separate non-steered context only when
same-context reasoning would compromise the credibility of the evidence. A
fresh lead pass is not independent review.

Pre-approval agents are read-only. Post-approval workers require an approved
disjoint scope, targeted checks, and stop conditions. The lead verifies material
evidence, dispositions findings, and owns scope, decisions, integration,
validation claims, and the final response without duplicating the complete
delegated investigation.

High-risk work requires a fresh final verification pass, which may be
lead-owned. Use an independent context when the credibility of required
evidence depends on separation; lead-owned verification must not be described
as independent.

## Implementation And Closeout

* Implement the smallest correct approved change. Do not add speculative
  abstractions, compatibility behavior, fallbacks, future-proofing, or unrelated
  cleanup.
* Behavior required to satisfy current correctness, failure, lifecycle,
  bounded-resource, compatibility, observability, or operator contracts is not
  speculative and must be included in approved scope.
* Preserve bounded resources, lifecycle safety, compatibility, and operator
  contracts. Apply triggered config, lifecycle, leak, retained-state, hot-path,
  scientific/model, and support-critical-comment guidance.
* Approval follows change risk; validation follows the touched surface and
  actual engineering risk. Run targeted checks while working and one complete
  selected lane on the final relevant state.
* If review changes the result, rerun affected targeted checks. Rerun the full
  lane only when the fix can invalidate broader results, such as shared
  behavior, interfaces, build configuration, concurrency, or cross-package
  contracts.
* Review the final diff directly. Report material validation results once and
  explain failures, skips, waivers, or residual risks.
* Update affected documentation. Record an ADR only for a durable decision and
  a TSR for durable troubleshooting learning. Map each approved Non-trivial
  item compactly to implementation and validation.

## Detailed Routes

* Execution workflow and risk routing: `docs/change-workflow.md`
* Engineering standards: `docs/code-quality.md`
* Validation commands: `docs/dev-runbook.md`
* Final-diff review: `docs/review-checklist.md`
* Codex closeout compliance: `VALIDATION.md`
* Runtime and operator contracts: `docs/domain-contract.md`
* ADR/TSR policy: `docs/decision-memory.md`
* Optional Non-trivial examples: `docs/templates/non-trivial-change-template.md`
