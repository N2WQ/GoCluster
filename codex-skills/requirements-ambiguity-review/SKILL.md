---
name: requirements-ambiguity-review
description: "Use after current-state discovery when material product, operator, compatibility, failure, classification, default, threshold, ordering, precedence, persistence, or test-oracle semantics still admit more than one plausible interpretation. Compare interpretations and make conditional recommendations, but unresolved material semantics remain user-owned and block concrete design selection and an unconditional Scope Ledger until explicitly resolved by the user, controlling authority, or evidence that eliminates every competing interpretation. Do not trigger merely because requirements are detailed or work is Non-trivial."
---

# Requirements Ambiguity Review

## Purpose

Expose unresolved semantic forks before they harden into architecture, scope, or
implementation.

Provide evidence, consequences, architecture-neutral constraints, and
conditional recommendations that help the user make a requirements decision.

The governing invariant is: do not choose product policy.

Produce findings for the lead agent. The lead agent and user retain authority
over product policy, scope, design, approval, implementation, validation, and
final disposition.

## Phase And Trigger

Run after Current-State Discovery and applicable scientific or normative review,
but before concrete design selection or publication of a Proposed Scope Ledger.

Trigger only when current evidence still permits materially different semantics.

Do not trigger merely because:

* requirements are detailed;
* the task is important;
* the work is Non-trivial;
* only implementation choices remain;
* wording is imperfect but controlling behavior is settled.

## Reference Loading

Always load:

* `references/resolution-authority.md`
  to determine whether policy is resolved and who owns the decision.

Load only when needed:

* `references/ambiguity-analysis.md`
  when two or more material interpretations require structured comparison.
* `references/architecture-boundary.md`
  when the analysis risks entering mechanisms, implementation structures, or
  concrete design.
* `references/scope-ledger-gate.md`
  when the request involves planning, scope, implementation, approval,
  authorization, or mutation.

Do not load every reference by default.

## Authority Boundary

The reviewer may:

* identify and compare competing interpretations;
* explain observable, compatibility, safety, persistence, ordering, failure,
  classification, and test consequences;
* make a conditional recommendation under explicit assumptions;
* identify architecture-neutral invariants;
* identify validation obligations and test oracles;
* describe common work valid under every interpretation.

The reviewer must not:

* call an unresolved interpretation correct, selected, decided, required, or
  the contract;
* treat technical preference as requirements authority;
* infer policy approval from requests to analyze, determine, recommend, plan,
  compare, review, propose, continue, or proceed;
* select or recommend a concrete architecture;
* publish an unconditional Proposed Scope Ledger while semantics remain
  unresolved;
* display `Approved vN` or an authorization instruction before resolution;
* approve scope or authorize implementation.

## Workflow

### 1. State the unresolved question

Separate:

* confirmed facts;
* current behavior;
* documented intent;
* scientific or normative evidence;
* assumptions;
* proposals;
* unknowns.

Treat current code and tests as evidence of current behavior, not automatic proof
of intended semantics.

### 2. Determine whether the ambiguity is material

A material ambiguity exists when competing answers would change observable
behavior, compatibility, safety, persistence, ordering, classification, failure
handling, defaults, thresholds, or expected tests.

If the uncertainty is only about implementation mechanism, classify it as
implementation uncertainty and hand it to design after semantics are settled.

### 3. Determine resolution status

Apply `references/resolution-authority.md`.

A recommendation alone never resolves policy.

Record the decision owner and authority source.

### 4. Compare interpretations when required

When multiple material interpretations remain, apply
`references/ambiguity-analysis.md`.

At minimum:

1. state each interpretation neutrally;
2. include one concrete edge case or interleaving that distinguishes them;
3. compare observable and validation consequences;
4. state assumptions and rejection criteria;
5. identify architecture-neutral constraints;
6. state what would change the recommendation.

### 5. Make a conditional recommendation

Recommend an interpretation when it improves decision quality.

Every recommendation must:

* identify assumptions and decision criteria;
* cite supporting evidence;
* identify the decision owner;
* state what would change the recommendation;
* state that the recommendation is not yet policy;
* end with the exact explicit decision required.

### 6. Enforce design and scope boundaries

Load `references/architecture-boundary.md` only if mechanisms or concrete design
enter the discussion.

Load `references/scope-ledger-gate.md` only if planning, scope, approval,
implementation, or mutation is in play.

While semantics remain unresolved, do not present:

* a concrete mechanism;
* an unconditional Scope Ledger;
* `Approved vN`;
* an authorization instruction;
* a claim that planning is approval-ready;
* mutation steps.

## ambiguity register

For each material ambiguity, report:

* unresolved question;
* confirmed facts and evidence;
* competing interpretations;
* distinguishing edge case or interleaving;
* consequences;
* conditional recommendation, if any;
* architecture-neutral constraints;
* affected contracts and tests;
* decision owner;
* exact resolution required;
* status and authority source.

No fixed table is required.

## Relationships

The scientific oracle answers:

> What does the strongest scientific, mathematical, statistical, or normative
> evidence support?

This reviewer answers:

> Given that evidence, what product semantics remain open to user choice?

The design challenger answers:

> Given explicitly resolved semantics, what architecture should implement them?

The test-strategy adversary determines whether planned tests can falsify the
resolved behavior.

## Stopping Condition

Block concrete design and unconditional Scope Ledger publication when unresolved
semantics could materially change observable behavior or expected tests.

The block is removed only when:

* the user explicitly selects an interpretation;
* controlling authority unambiguously selects it;
* confirmed evidence eliminates every competing material interpretation.

After resolution:

1. record the selected semantics and authority source;
2. route materially open architectures through `design-challenger`;
3. complete applicable falsifiability analysis;
4. only then may the lead present a Proposed Scope Ledger.

## Completion Gate

Before responding, confirm that:

* the unresolved question is explicit;
* material and non-material uncertainties are separated;
* interpretations are stated neutrally;
* a distinguishing edge case is included when needed;
* any recommendation is conditional;
* the decision owner is identified;
* the exact decision required is stated;
* only architecture-neutral constraints are provided;
* unresolved semantics were not converted into architecture, scope, approval, or
  mutation authority.

## Output

Report the material result without a mandatory score or fixed envelope.

When material semantics remain unresolved, finish with the explicit user
decision required.

Do not present a concrete design or unconditional Scope Ledger.
