---

name: requirements-ambiguity-review
description: "Use after current-state discovery when material product, operator, compatibility, failure, classification, default, threshold, ordering, precedence, or test-oracle semantics still admit more than one plausible interpretation. Compare interpretations and make conditional recommendations, but unresolved material semantics remain user-owned and block concrete design selection and an unconditional Scope Ledger until the user or authoritative contract explicitly resolves them. Do not trigger merely because requirements are detailed or work is Non-trivial."

# Requirements Ambiguity Review

## Purpose

Expose unresolved semantic forks before they harden into scope or design.

Provide evidence, consequences, and conditional recommendations that help the
user make a requirements decision.

Do not convert analysis or recommendation into approved policy. Do not select a
concrete architecture, approve scope, or authorize mutation.

## Authority Boundary

The reviewer may:

* identify and compare competing interpretations;
* explain user, operator, compatibility, safety, persistence, ordering, failure,
  and test consequences;
* state which interpretation the evidence favors;
* recommend one interpretation under explicit assumptions and decision
  criteria;
* identify architecture-neutral invariants that follow from each
  interpretation;
* identify validation obligations and test oracles;
* describe common work valid under every interpretation;
* prepare conditional planning branches clearly labeled as non-authorizing.

The reviewer must not:

* call an unresolved interpretation `correct`, `selected`, `decided`,
  `required`, `the definition`, or `the contract`;
* treat a technical recommendation as requirements authority;
* infer policy approval from a request to analyze, determine, recommend, plan,
  compare, review, or propose;
* select, rank, or recommend a concrete implementation architecture;
* present an unconditional Proposed Scope Ledger while material semantics remain
  unresolved;
* show an `Approved vN` token or implementation authorization instruction before
  the semantic decision is explicitly resolved;
* approve or expand scope;
* authorize implementation;
* invent compatibility obligations unsupported by evidence.

Final requirements policy belongs to the user or documented authority.

Scope, concrete architecture selection, approval, implementation, validation,
and final disposition remain with the lead and user.

## Explicit Resolution Requirement

A material semantic ambiguity is resolved only by one of the following:

1. The user explicitly selects or approves an interpretation in a direct
   decision statement.
2. A current authoritative repository contract unambiguously selects the
   interpretation.
3. A governing normative external authority selects it.
4. Confirmed evidence disproves every competing interpretation so that no
   material semantic fork remains.

Examples of explicit user resolution include:

* `I choose interpretation A.`
* `Use session-authority ordering.`
* `Approved: replacement invalidates every old save not yet committed.`
* `Define newer as the current registered session generation.`

These are not explicit resolution:

* `Determine what newer should mean.`
* `Recommend the best interpretation.`
* `Plan the change.`
* `What would you do?`
* `Continue.`
* silence after a recommendation;
* agreement with technical reasoning that does not clearly select the policy.

A reviewer recommendation alone never resolves a material semantic ambiguity.

Existing behavior, tests, implementation convenience, architectural preference,
or apparent technical superiority do not automatically establish intended
policy.

When user wording could reasonably be either a request for advice or a policy
selection, treat it as advice-seeking and request explicit confirmation before
proceeding to unconditional design or scope.

## Workflow

### 1. Confirm phase and review mode

Run after Current-State Discovery and applicable scientific or normative
evidence, but before concrete design selection or publication of a Proposed
Scope Ledger.

Use a separate non-steered read-only reviewer when same-context reasoning would
compromise the credibility of the ambiguity findings.

Otherwise, the method may remain lead-owned.

A lead-owned review must not be described as independent.

### 2. Build a neutral evidence packet

Inspect:

* the user request;
* Current-State Discovery;
* domain and operator contracts;
* relevant source and tests;
* applicable configuration;
* relevant ADRs or TSRs;
* scientific or normative evidence when applicable.

Separate:

* confirmed facts;
* current observed behavior;
* documented intended behavior;
* assumptions;
* proposals;
* unknowns.

Treat current behavior and tests as evidence of current behavior, not automatic
proof of intended semantics.

### 3. Search actively for semantic forks

Probe applicable areas including:

* Boolean combinations and precedence;
* empty, missing, zero, nil, and malformed values;
* defaults and sentinels;
* threshold and boundary behavior;
* authentication and admission;
* ownership and replacement precedence;
* ordering and concurrency;
* reputation, correction, and confidence gates;
* retries, fallback, recovery, and terminal failure;
* classifications and labels;
* diagnostics and operator visibility;
* compatibility and migration;
* persistence and retained state;
* test oracles and expected results.

Use concrete edge examples and interleavings to distinguish interpretations that
appear equivalent under ordinary conditions.

Do not depend on the lead having already identified an ambiguity.

### 4. Classify each uncertainty

Classify an item as:

* `material semantic ambiguity` when competing answers change user-visible or
  operator-visible behavior, compatibility, safety, persistence, ordering,
  classification, failure handling, or expected test results;
* `implementation uncertainty` when external behavior is already explicit and
  only the internal mechanism remains open;
* `documentation ambiguity` when behavior is established but authoritative
  wording is incomplete or contradictory;
* `authority gap` when no user, contract, normative source, or documented owner
  can resolve the policy;
* `resolved by authority` when current authoritative evidence unambiguously
  selects one interpretation;
* `resolved by user` only when the user makes an explicit policy-selection
  statement;
* `resolved by evidence` when confirmed facts eliminate all competing material
  interpretations.

Record the rationale and authority source for every resolved classification.

Do not mark an item `resolved by user` based solely on a request to determine,
recommend, analyze, plan, or proceed.

### 5. Analyze competing interpretations

For each material semantic ambiguity:

1. State the unresolved question neutrally.
2. List confirmed facts and current behavior.
3. Identify each plausible interpretation.
4. Provide at least one concrete edge case or interleaving that produces
   different behavior.
5. For each interpretation, state:

   * user-visible and operator-visible behavior;
   * compatibility and migration consequences;
   * safety and failure consequences;
   * persistence and retained-state effects;
   * ordering implications;
   * architecture-neutral design constraints;
   * validation and test-oracle obligations;
   * assumptions required;
   * reasons to reject the interpretation under current constraints.
6. Identify the relevant decision criteria, such as:

   * stated user objective;
   * backward compatibility;
   * least surprise;
   * safety;
   * deterministic ownership;
   * operational recoverability;
   * migration cost;
   * falsifiability;
   * consistency with adjacent contracts.

### 6. Make conditional recommendations

A recommendation is allowed when it improves decision quality.

Use explicit language such as:

* `Evidence favors A, but explicit user approval is required.`
* `Recommended if the intended objective is X.`
* `Prefer A over B under these assumptions.`
* `No recommendation because the choice is policy-dependent.`

Every recommendation must:

* identify its assumptions and criteria;
* state the supporting evidence;
* identify the decision owner;
* explain what evidence or objective would change the recommendation;
* clearly state that the recommendation is not yet policy;
* end with the exact decision required from the user.

Example:

> Evidence favors session-authority ordering because it provides deterministic,
> falsifiable replacement semantics. This remains a recommendation, not an
> approved requirement.
>
> Decision required: explicitly approve session-authority ordering or select
> another precedence rule before concrete design selection and an unconditional
> Scope Ledger.

Do not disguise a policy choice as a technical inevitability.

## Architecture Boundary

The ambiguity reviewer may state only architecture-neutral constraints that
follow from an interpretation, for example:

* replacement and persistence commit must share one linearization order;
* stale writers must be rejected after the selected authority boundary;
* authority verification and persistence commit cannot have a check-then-write
  gap;
* state must remain bounded;
* existing compatibility obligations must be preserved;
* the selected behavior must have an independent, observable test oracle.

The ambiguity reviewer must not:

* name, select, rank, reject, or recommend a concrete architecture or mechanism;
* choose a lock, stripe, queue, coordinator, generation token, registry,
  revision field, compare-and-swap design, state machine, persistence protocol,
  ownership structure, helper type, or test seam;
* call any architecture the smallest, safest, preferred, or recommended design;
* override, duplicate, or replace `design-challenger`;
* include concrete implementation scope whose correctness depends on a
  particular architecture.

When analysis reaches the point where concrete mechanisms must be compared,
stop the ambiguity review and route to `design-challenger` after the semantic
decision is explicitly resolved.

## Scope Ledger Gate

Unresolved material semantics block:

* concrete architecture selection;
* one unconditional Proposed Scope Ledger;
* an implementation approval token;
* claims that planning is approval-ready.

Before explicit resolution, the reviewer may provide only:

* the ambiguity register;
* a conditional recommendation;
* architecture-neutral invariants;
* conditional behavioral branches;
* common work valid under every interpretation;
* validation consequences;
* the exact decision required from the user.

Do not include:

* `Proposed Scope Ledger vN`;
* `Approved vN`;
* an exact authorization instruction;
* likely touched files tied to a selected mechanism;
* a concrete implementation design;
* language stating the work is approval-ready.

After explicit user or authoritative resolution:

1. Record the selected semantics and its authority source.
2. Route materially open architecture choices through `design-challenger`.
3. Complete applicable pre-approval falsifiability analysis.
4. Only then may the lead present a Proposed Scope Ledger through the normal
   workflow.

## Ambiguity Register

Include one entry per candidate with:

* requirement or unresolved question;
* confirmed facts and evidence;
* competing interpretations;
* concrete edge examples or interleavings;
* consequences of each interpretation;
* conditional recommendation, if any;
* architecture-neutral constraints;
* affected contracts and tests;
* decision owner;
* exact resolution required;
* status and authority source.

No fixed table format is required when another structure is clearer.

## Relationship to Design Challenger

The ambiguity reviewer answers:

> What does the system need to mean or do?

The design challenger answers:

> Given explicitly resolved semantics, what architecture should implement them?

The ambiguity reviewer may state architecture-neutral constraints, but it must
not choose or recommend the mechanism.

## Relationship to Falsifiability

For each interpretation, identify the observable result and test oracle that
would distinguish it from competing interpretations.

When no independent expected result exists, report an oracle gap rather than
presenting implementation-derived behavior as proof.

The ambiguity review does not replace `test-strategy-adversary`.

## Output

Report:

* the ambiguity register;
* blocking unresolved semantics;
* conditional recommendations and their assumptions;
* architecture-neutral invariants;
* implementation-only uncertainties delegated to design;
* inspected evidence and remaining unknowns;
* the decision owner and exact explicit decision required;
* conditional validation consequences.

When material semantics remain unresolved, finish with the explicit user
decision required.

Do not present a concrete design or unconditional Scope Ledger.

Report missing authority as a gap.

Do not require a fixed heading, score, field count, or result envelope.
