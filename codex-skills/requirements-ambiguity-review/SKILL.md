---

name: requirements-ambiguity-review
description: "Use after current-state discovery when material product, operator, compatibility, failure, classification, default, threshold, ordering, precedence, or test-oracle semantics still admit more than one plausible interpretation. The reviewer may compare interpretations and make conditional recommendations, but unresolved material semantics remain user-owned and block an unconditional Scope Ledger until explicitly resolved. Do not trigger merely because requirements are detailed or work is Non-trivial."

# Requirements Ambiguity Review

## Purpose

Expose unresolved semantic forks before they harden into scope or design.

Provide evidence, consequences, and conditional recommendations that help the
user make a requirements decision.

Do not convert a recommendation into approved policy, select architecture,
approve scope, or authorize mutation.

## Authority Boundary

The reviewer may:

* identify and compare competing interpretations;
* explain the observable behavior, compatibility, safety, operator, persistence,
  ordering, and test consequences of each interpretation;
* state which interpretation the evidence favors;
* recommend one interpretation under explicit assumptions or decision criteria;
* explain conditional design and validation implications;
* identify common work valid under every interpretation;
* prepare conditional planning branches when useful.

The reviewer must not:

* call a materially unresolved interpretation “correct,” “selected,” “decided,”
  “required,” or “the contract” unless current documented authority or the user
  has explicitly resolved it;
* treat technical preference as requirements authority;
* present a recommended interpretation as approved policy;
* produce one unconditional Proposed Scope Ledger whose behavior depends on an
  unresolved semantic choice;
* independently select among materially different architectures;
* approve or expand scope;
* authorize implementation;
* invent compatibility obligations unsupported by evidence.

Final requirements policy belongs to the user or documented authority.

Scope, architecture selection, approval, implementation, validation, and final
disposition remain with the lead and user.

## Resolution Standard

A material semantic ambiguity is resolved only by one of the following:

1. The user explicitly selects an interpretation.
2. A current authoritative contract unambiguously selects it.
3. A normative external authority governs the behavior.
4. The competing interpretation is disproven by current evidence and is no
   longer plausible.

A reviewer recommendation alone does not resolve the ambiguity.

Existing behavior, tests, implementation convenience, architectural preference,
or apparent technical superiority do not automatically establish intended
policy.

## Workflow

### 1. Confirm phase and review mode

Run after Current-State Discovery and applicable scientific or normative
evidence, but before publishing a Proposed Scope Ledger.

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
* `implementation uncertainty` when required external behavior is already
  explicit and only the internal mechanism remains open;
* `documentation ambiguity` when behavior is established but authoritative
  wording is incomplete or contradictory;
* `authority gap` when no user, contract, normative source, or documented owner
  can resolve the policy;
* `resolved by authority` when current authoritative evidence unambiguously
  selects one interpretation;
* `resolved by user` when the user explicitly selects an interpretation;
* `resolved by evidence` when competing interpretations are no longer plausible
  under confirmed facts.

Record the rationale and authority source for every resolved classification.

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
   * conditional design and ownership implications;
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

* `Evidence favors A, but user approval is required.`
* `Recommended if the intended objective is X.`
* `Prefer A over B under the following assumptions.`
* `No recommendation because the choice is policy-dependent.`

Every recommendation must:

* identify the assumptions and criteria;
* state the evidence supporting it;
* identify the user or authority that must resolve it;
* explain what evidence or objective would change the recommendation;
* avoid wording that implies the recommendation is already approved.

The reviewer must end every unresolved material recommendation with a direct
decision request, for example:

> Decision required: choose A or B before an unconditional Scope Ledger can be
> presented.

Do not disguise a policy choice as a technical inevitability.

## Architecture Boundary

The ambiguity reviewer may explain architecture implications conditionally:

* `If interpretation A is selected, the design must preserve invariant X.`
* `If interpretation B is selected, architecture Y becomes necessary.`

It must not:

* select a concrete architecture;
* choose a lock, queue, registry, schema, state machine, or ownership mechanism;
* override or replace `design-challenger`;
* silently turn semantic analysis into a design decision.

When materially different architectures remain viable after semantics are
resolved, route to `design-challenger`.

## Scope Ledger Gate

Unresolved material semantics block publication of one unconditional Proposed
Scope Ledger when competing interpretations would authorize materially different
behavior.

Before user or authoritative resolution, the reviewer may provide only:

* the ambiguity register;
* a conditional recommendation;
* conditional design implications;
* conditional scope branches clearly labeled as non-authorizing;
* common work valid under every interpretation;
* the exact decision required from the user.

Do not include:

* `Proposed Scope Ledger vN`;
* `Approved vN`;
* an exact authorization instruction;
* language stating that planning is approval-ready.

After the user or documented authority resolves the ambiguity, the lead may
proceed to design analysis and later present a Scope Ledger through the normal
workflow.

## Ambiguity Register

Include one entry per candidate with:

* requirement or unresolved question;
* confirmed facts and evidence;
* competing interpretations;
* concrete edge examples or interleavings;
* consequences of each interpretation;
* conditional recommendation, if any;
* affected contracts, surfaces, and tests;
* decision owner;
* exact resolution required;
* status and authority source.

No fixed table format is required when another structure is clearer.

## Relationship to Design Challenger

The ambiguity reviewer answers:

> What does the system need to mean or do?

The design challenger answers:

> Given resolved semantics, what architecture should implement them?

The ambiguity reviewer may state design constraints that follow conditionally
from an interpretation, but it must not select the implementation architecture.

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
* implementation-only uncertainties delegated to design;
* inspected evidence and remaining unknowns;
* the decision owner and exact decision required;
* conditional scope or validation consequences.

When material semantics remain unresolved, finish with the explicit user
decision required. Do not present an unconditional Scope Ledger.

Report missing authority as a gap.

Do not require a fixed heading, score, field count, or result envelope.
