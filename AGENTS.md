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
   decision history. Before proposing scope, resolve material unknowns that
   could invalidate the approach using the smallest safe read-only proof.
   Expose unresolved assumptions.
2. Resolve every material semantic or product-policy ambiguity through the
   Semantic Decision Gate below.
3. Resolve every triggered scientific or model question through the Scientific
   Independence Gate below.
4. Propose a versioned Scope Ledger containing the objective, agreed scope,
   boundaries, material risks or unknowns, and validation plan.
5. Challenge the scope before approval. Revise and reapprove it when new
   evidence materially changes authority, scope, risk, or validation.
6. Wait for the exact token `Approved vN` matching that ledger.

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

A material semantic or product-policy ambiguity exists when two or more
plausible interpretations would change user-visible or operator-visible
behavior, compatibility, safety, ordering, persistence, failure handling,
classification, defaults, thresholds, or expected test results.

Codex may analyze the interpretations, explain consequences, and recommend one
under explicit assumptions. A recommendation does not resolve the ambiguity.

A material semantic ambiguity is resolved only when:

1. the user explicitly selects or approves an interpretation;
2. a current authoritative repository contract unambiguously selects it;
3. a governing normative authority selects it; or
4. current evidence disproves every competing material interpretation.

Requests to analyze, determine, recommend, review, compare, plan, or propose do
not constitute policy selection.

These examples are advice requests, not semantic authorization:

* `Determine what this should mean.`
* `Recommend the best behavior.`
* `Plan the change.`
* `What would you do?`
* `Continue.`

These examples explicitly resolve policy:

* `I choose interpretation A.`
* `Use session-authority ordering.`
* `Approved: replacement invalidates every old save not yet committed.`
* `Define newer as the currently authoritative registered session.`

When material semantics remain unresolved, Codex must:

* report the competing interpretations and consequences;
* provide any conditional recommendation;
* state architecture-neutral invariants only;
* identify the user or authority that owns the decision;
* end with the exact decision required from the user.

While the ambiguity remains unresolved, Codex must not:

* call one interpretation correct, selected, decided, required, or the contract;
* select or recommend a concrete architecture or mechanism;
* draft an unconditional Proposed Scope Ledger;
* display an `Approved vN` token;
* state that planning is approval-ready;
* begin mutation.

After explicit semantic resolution, Codex records the selected interpretation
and its authority source, then performs any triggered design challenge and
falsifiability analysis before presenting a Proposed Scope Ledger.

## Scientific Independence Gate

A scientific or model review requires separate non-steered evidence when the
credibility of the result materially depends on deriving the normative contract
outside the lead's implementation context.

This gate applies when the `scientific-model-oracle` determines that separate
review is required, including consequential questions involving:

* scientific or mathematical model semantics;
* operational prediction;
* classification or ranking;
* thresholds and boundaries;
* confidence or probability claims;
* calibration or tolerances;
* correction or scoring behavior;
* externally visible scientific claims;
* implementation-derived expected values;
* disputed or materially uncertain normative sources.

When this gate applies, Codex must use a separate non-steered read-only reviewer
when active platform support permits.

The reviewer must receive a neutral evidence packet containing only what is
needed to derive the contract independently, such as:

* the neutral scientific or model question;
* confirmed product objectives;
* operating domain and units;
* authoritative source candidates;
* current behavior only when clearly labeled as observational evidence;
* unresolved questions and known constraints.

The neutral evidence packet must not include:

* the lead's preferred answer;
* the intended implementation;
* a draft Scope Ledger;
* implementation-derived expected values;
* golden vectors copied from current tests or fixtures;
* wording designed to steer the reviewer toward the lead's conclusion.

The lead must verify the independent review's material evidence, resolve
conflicts, and retain authority over final synthesis. Independent findings do
not transfer product-policy, scope, design, approval, implementation, or
validation authority.

### Required Independence Disclosure

Any scientific or model response governed by this gate must explicitly state:

* whether a separate non-steered reviewer was actually used;
* whether the result is independent, separately reviewed, or lead-owned;
* the neutral question and material evidence supplied to the reviewer;
* whether the reviewer saw the lead's preferred conclusion or intended design;
* the reviewer's material findings;
* material agreement or disagreement with the lead;
* how the lead dispositioned disagreements;
* remaining uncertainty and confidence limits.

Do not describe a fresh lead pass, a second pass in the same inherited context,
or a reviewer shown the preferred answer as independent.

### When Independent Review Is Unavailable

If separate non-steered review is required but unavailable, Codex must:

* disclose that limitation before issuing a verdict;
* identify which claims are affected by correlated reasoning risk;
* perform only a clearly labeled lead-owned derivation;
* reduce confidence where independence could materially change the result;
* avoid presenting the result as an independently established normative
  contract;
* state what separate evidence is still required.

Codex must not issue an unqualified final scientific verdict, calibrated
threshold recommendation, confidence claim, normative contract, or
scientifically grounded Scope Ledger unless one of the following occurs:

1. the required separate review is completed;
2. authoritative evidence makes independent derivation unnecessary because no
   material interpretation remains open; or
3. the user explicitly accepts a limited lead-owned analysis after being told
   the independence limitation and affected claims.

Examples of explicit user acceptance include:

* `Proceed with a lead-owned scientific analysis despite the missing independent review.`
* `I accept the independence limitation for this review.`
* `Use the lead-owned result as provisional evidence only.`

A request to continue, analyze, recommend, review, or plan does not constitute
acceptance of the independence limitation.

### Scientific Recommendation Versus Product Policy

The scientific reviewer may:

* rank competing scientific or model contracts;
* recommend the strongest evidence-supported contract;
* reject unsupported scientific claims;
* narrow confidence, accuracy, probability, or operational-prediction claims;
* define independent golden vectors and tolerances;
* identify calibration or normative evidence still required.

A scientific recommendation does not itself select product policy.

When scientific evidence is clear but product behavior may intentionally differ,
Codex must state both separately:

```text
Scientific recommendation: <evidence-supported contract or claim>

Product-policy decision required: <whether to adopt it or intentionally use a
different behavior with documented limitations>
```

If product policy remains materially open, route the decision through the
Semantic Decision Gate before design or Scope Ledger publication.

### Scientific Scope Gate

While required independent review remains incomplete or materially unresolved,
Codex must not:

* present an independently established scientific verdict;
* state that thresholds, classifications, probabilities, tolerances, confidence,
  or model outputs are scientifically validated;
* derive implementation expectations solely from current code or tests;
* produce an unconditional Scope Ledger whose behavior depends on the unresolved
  scientific contract;
* display an implementation approval token for that unresolved contract;
* begin mutation.

Codex may still report:

* current observed behavior;
* source hierarchy;
* preliminary lead-owned analysis;
* uncertainty and evidence gaps;
* provisional golden vectors clearly labeled as non-independent;
* the exact independent evidence or user acceptance required to proceed.

After the gate is satisfied, Codex records:

* the independence status;
* the controlling normative evidence;
* the established scientific contract and bounded uncertainty;
* any remaining product-policy decision;
* the authority source for each resolved element.

Triggered requirements ambiguity, design challenge, falsifiability analysis, and
Scope Ledger work may then proceed.

## Risk Routing

Standard versus High-risk is an internal engineering judgment, not a required
report line. High-risk includes consequential ambiguity, scientific/model
semantics, genuine architecture forks, unclear falsifiability, broad/shared Go
impact, concurrency or lifecycle, retained state, config contracts, hot paths,
or meaningful residual uncertainty. Load only the matching skill or document
section; do not run specialists or independent agents by default.

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
