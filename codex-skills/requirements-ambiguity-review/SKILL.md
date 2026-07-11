---
name: requirements-ambiguity-review
description: "Use after current-state discovery when material product, operator, compatibility, failure, classification, default, threshold, ordering, precedence, persistence, or test-oracle semantics still admit more than one plausible interpretation. Compare interpretations and make conditional recommendations, but unresolved material semantics remain user-owned and block concrete design selection and an unconditional Scope Ledger until the user or authoritative contract explicitly resolves them. Do not trigger merely because requirements are detailed or work is Non-trivial."
---

# Requirements Ambiguity Review

## Purpose

Expose unresolved semantic forks before they harden into architecture, scope, or
implementation.

Provide evidence, consequences, architecture-neutral constraints, and
conditional recommendations that help the user make a requirements decision.

The governing invariant is: do not choose product policy. Do not select concrete
architecture, approve scope, authorize mutation, or convert a recommendation
into an approved requirement.

Produce findings and recommendations for the lead agent. The lead agent and user
retain authority over product policy, scope, design, approval, implementation,
validation, and final disposition.

## Phase

Run after Current-State Discovery and applicable scientific or normative review,
but before concrete design selection or publication of a Proposed Scope Ledger.

Trigger only when current evidence still permits materially different semantics.

Do not trigger merely because:

* requirements are detailed;
* the task is important;
* the work is Non-trivial;
* implementation choices remain open after behavior is already explicit;
* documentation wording is imperfect but controlling behavior is settled.

## Required References

Load only the references applicable to the ambiguity being reviewed:

* `references/resolution-authority.md`
  for deciding whether policy is resolved and who owns the decision.
* `references/ambiguity-analysis.md`
  for finding, classifying, comparing, and reporting semantic forks.
* `references/architecture-boundary.md`
  for separating behavioral semantics from implementation mechanisms.
* `references/scope-ledger-gate.md`
  for determining what planning output is allowed before and after resolution.

Do not load a reference merely because it exists. Load it when its method is
needed for the actual ambiguity.

## Authority Boundary

The reviewer may:

* identify and compare competing interpretations;
* explain user, operator, compatibility, safety, persistence, ordering, failure,
  classification, and test consequences;
* state which interpretation the evidence favors;
* recommend one interpretation under explicit assumptions and decision
  criteria;
* identify architecture-neutral invariants;
* identify validation obligations and test oracles;
* describe common work valid under every interpretation;
* prepare conditional branches clearly labeled as non-authorizing.

The reviewer must not:

* call an unresolved interpretation correct, selected, decided, required, or
  the contract;
* treat technical preference as requirements authority;
* infer policy approval from a request to analyze, determine, recommend, plan,
  compare, review, propose, continue, or proceed;
* select, rank, reject, or recommend a concrete implementation architecture;
* publish an unconditional Proposed Scope Ledger while material semantics remain
  unresolved;
* display `Approved vN` or an implementation authorization instruction before
  explicit resolution;
* approve or expand scope;
* authorize implementation;
* invent compatibility or migration obligations unsupported by evidence.

## Core Workflow

### 1. Confirm the unresolved question

State the semantic question neutrally.

Separate:

* confirmed facts;
* current observed behavior;
* documented intended behavior;
* scientific or normative evidence;
* assumptions;
* proposals;
* unknowns.

Treat current code and tests as evidence of current behavior, not automatic proof
of intended semantics.

### 2. Determine whether the ambiguity is material

Apply `references/ambiguity-analysis.md`.

An ambiguity is material when competing answers would change observable
behavior, compatibility, safety, persistence, ordering, classification, failure
handling, defaults, thresholds, or expected test results.

Classify non-material uncertainty separately as implementation uncertainty,
documentation ambiguity, or an evidence gap.

### 3. Determine authority and resolution status

Apply `references/resolution-authority.md`.

A recommendation alone never resolves policy.

Record the decision owner and authority source for every interpretation marked
resolved.

### 4. Compare competing interpretations

For every material ambiguity:

1. state each plausible interpretation neutrally;
2. provide at least one concrete edge case or interleaving that distinguishes
   them;
3. compare observable behavior, compatibility, safety, persistence, ordering,
   failure handling, classification, and test-oracle consequences;
4. identify assumptions and rejection criteria;
5. state architecture-neutral constraints;
6. identify evidence or objectives that would change the recommendation.

Do not manufacture an implementation mechanism during semantic analysis.

### 5. Make a conditional recommendation

Recommend an interpretation when it improves decision quality.

Every recommendation must:

* identify its assumptions and decision criteria;
* cite the supporting evidence;
* identify the decision owner;
* state what would change the recommendation;
* state that the recommendation is not yet policy;
* end with the exact explicit decision required from the user.

Use forms such as:

* `Evidence favors A, but explicit user approval is required.`
* `Recommended if the intended objective is X.`
* `Prefer A over B under these assumptions.`
* `No recommendation because the choice is policy-dependent.`

### 6. Enforce architecture and scope boundaries

Apply `references/architecture-boundary.md` and
`references/scope-ledger-gate.md`.

While material semantics remain unresolved, provide only:

* the ambiguity register;
* conditional recommendations;
* architecture-neutral invariants;
* conditional behavioral branches;
* common work valid under every interpretation;
* validation consequences;
* the exact decision required.

Do not present a concrete design, unconditional Scope Ledger, approval token, or
claim that planning is approval-ready.

### 7. Record the ambiguity register

Maintain one entry per material candidate containing:

* unresolved question;
* confirmed facts and evidence;
* competing interpretations;
* distinguishing edge case or interleaving;
* consequences of each interpretation;
* conditional recommendation, if any;
* architecture-neutral constraints;
* affected contracts and tests;
* decision owner;
* exact resolution required;
* status and authority source.

No fixed table format is required when another structure is clearer.

## Relationship to Other Skills

The scientific oracle answers:

> What does the strongest scientific, mathematical, statistical, or normative
> evidence support?

This reviewer answers:

> Given that evidence, what product semantics remain open to user choice?

The design challenger answers:

> Given explicitly resolved semantics, what architecture should implement them?

The test-strategy adversary determines whether planned tests can falsify the
resolved behavior.

Do not collapse scientific uncertainty, product-policy ambiguity, architecture,
and falsifiability into one decision.

## Stopping Condition

Block concrete design selection and unconditional Scope Ledger publication when
any unresolved semantic issue could materially change:

* observable behavior;
* compatibility or migration;
* safety;
* ownership or ordering;
* persistence or retained state;
* failure or recovery behavior;
* classification, default, threshold, or sentinel meaning;
* expected test results or oracle behavior.

The block is removed only when:

* the user explicitly selects an interpretation;
* controlling repository or external authority unambiguously selects it;
* confirmed evidence eliminates every competing material interpretation.

After resolution:

1. record the selected semantics and authority source;
2. route materially open architectures through `design-challenger`;
3. complete applicable falsifiability analysis;
4. only then may the lead present a Proposed Scope Ledger.

## Completion Gate

Before issuing the result, confirm every applicable item:

* the unresolved question is explicit;
* material and non-material uncertainties are separated;
* competing interpretations are stated neutrally;
* at least one distinguishing edge case or interleaving is included;
* observable and validation consequences are compared;
* any recommendation is conditional and evidence-backed;
* the decision owner is identified;
* the exact explicit decision required is stated;
* only architecture-neutral constraints are provided;
* unresolved semantics have not been converted into architecture, scope,
  approval, or mutation authority.

## Output

Report the material result without a mandatory heading, score, or fixed result
envelope.

Include, when applicable:

* the ambiguity register;
* blocking unresolved semantics;
* conditional recommendations and assumptions;
* architecture-neutral invariants;
* implementation-only uncertainties delegated to design;
* inspected evidence and remaining unknowns;
* decision owner and exact explicit decision required;
* conditional validation consequences.

When material semantics remain unresolved, finish with the explicit user
decision required.

Do not present a concrete design or unconditional Scope Ledger.
