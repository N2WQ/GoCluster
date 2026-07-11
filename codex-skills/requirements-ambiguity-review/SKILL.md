---

name: requirements-ambiguity-review
description: "Use after current-state discovery when material product, operator, compatibility, failure, classification, default, threshold, or test-oracle semantics still admit more than one plausible interpretation. The reviewer may make conditional recommendations under explicit assumptions while leaving final policy, scope, and design authority with the user and lead. Do not trigger merely because requirements are detailed or work is Non-trivial."

# Requirements Ambiguity Review

## Purpose

Actively expose unresolved semantic forks before they harden into scope or
design.

Produce evidence and conditional decision support for the lead and user.

Do not silently choose product policy, approve scope, authorize mutation, or
replace design and validation ownership.

## Authority Boundary

The reviewer may:

* identify and compare competing interpretations;
* explain the observable behavior, compatibility, safety, operator, and test
  consequences of each interpretation;
* recommend one interpretation under explicit assumptions or stated decision
  criteria;
* rank interpretations conditionally;
* identify design and validation implications that follow from each
  interpretation;
* prepare conditional scope branches when that helps the user make the policy
  decision.

The reviewer must not:

* present a recommendation as an approved requirement;
* silently convert an assumption into product policy;
* select unresolved policy on behalf of the user;
* approve or expand scope;
* authorize implementation;
* invent compatibility obligations unsupported by evidence;
* collapse materially different interpretations into one ambiguous ledger.

Final requirements policy belongs to the user or documented authority. Scope,
design selection, approval, implementation, validation, and final disposition
remain with the lead and user.

## Workflow

### 1. Confirm phase and review mode

Run after Current-State Discovery and applicable scientific or model evidence,
but before publishing a Proposed Scope Ledger.

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

Treat existing behavior and tests as evidence of current behavior, not automatic
proof of intended semantics.

### 3. Search actively for semantic forks

Probe applicable areas including:

* Boolean combinations and precedence;
* empty, missing, zero, nil, and malformed values;
* defaults and sentinels;
* threshold and boundary behavior;
* authentication and admission;
* reputation, correction, and confidence gates;
* retries, fallback, recovery, and terminal failure;
* ordering and concurrency;
* classifications and labels;
* diagnostics and operator visibility;
* compatibility and migration;
* persistence and retained state;
* test oracles and expected results.

Use concrete edge examples to distinguish interpretations that appear equivalent
under ordinary conditions.

Do not depend on the lead having already identified an ambiguity.

### 4. Classify each uncertainty

Classify an item as:

* `material semantic ambiguity` when competing answers change user-visible or
  operator-visible behavior, compatibility, safety, persistence, classification,
  failure handling, or the expected test result;
* `implementation uncertainty` when required external behavior is already
  explicit and only the internal mechanism remains open;
* `documentation ambiguity` when behavior is established but authoritative
  wording is incomplete or contradictory;
* `authority gap` when no user, contract, normative source, or documented owner
  can resolve the policy;
* `resolved by evidence` when current authoritative evidence eliminates the
  competing interpretations.

Record the rationale for the classification.

### 5. Analyze competing interpretations

For each material semantic ambiguity:

1. State the unresolved question.
2. List confirmed facts and current behavior.
3. Identify each plausible interpretation.
4. Provide at least one concrete edge case that produces different behavior.
5. For each interpretation, state:

   * user-visible and operator-visible behavior;
   * compatibility and migration consequences;
   * safety and failure consequences;
   * persistence or retained-state effects;
   * design and ownership implications;
   * validation and test-oracle obligations;
   * assumptions required;
   * reasons to reject the interpretation under current constraints.
6. Identify which decision criteria matter, such as:

   * user intent;
   * backward compatibility;
   * least surprise;
   * safety;
   * operational recoverability;
   * deterministic behavior;
   * migration cost;
   * falsifiability;
   * consistency with adjacent contracts.

### 6. Make conditional recommendations

A recommendation is allowed when it improves decision quality.

Use one of these forms:

* `Recommended if <assumption or objective>`:
  choose interpretation A because of the stated criteria.
* `Prefer A over B under current evidence`:
  explain the evidence and residual uncertainty.
* `No recommendation`:
  use when the choice is genuinely policy-dependent or evidence is insufficient.
* `Evidence favors A, authority still required`:
  use when one interpretation is better supported but not yet authorized.

Every recommendation must:

* identify the assumptions and criteria;
* state what evidence supports it;
* state what remains user-owned;
* explain what would change the recommendation.

Do not disguise a policy choice as a technical inevitability.

### 7. Produce an ambiguity register

Include one row per candidate with:

* requirement or unresolved question;
* confirmed facts and evidence;
* competing interpretations;
* concrete edge examples;
* consequences of each interpretation;
* conditional recommendation, if any;
* affected contracts, surfaces, and tests;
* decision owner;
* resolution and status.

No fixed table format is required if another structure is clearer.

### 8. Enforce the stopping condition

Unresolved material semantics block publication of a single unconditional Scope
Ledger when the competing interpretations would authorize materially different
behavior.

The reviewer may still provide:

* a conditional recommendation;
* conditional design implications;
* conditional Scope Ledger branches;
* common work that is valid under every interpretation;
* the exact user decision required to unblock planning.

Resolved items and implementation-only uncertainties must be dispositioned
explicitly so the lead can carry them into design and validation.

Leave official requirements decisions, scope, design selection, validation,
approval, and final disposition with the lead and user.

## Relationship to Design Challenger

The ambiguity reviewer answers:

> What does the system need to mean or do?

The design challenger answers:

> Given resolved semantics, what architecture should implement them?

The ambiguity reviewer may identify conditional design implications, but it
does not replace full design comparison when materially different architectures
remain viable.

Do not recommend architecture independently of an explicit semantic assumption.

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
* any conditional scope or validation consequences.

Report missing authority as a gap.

Do not require a fixed heading, score, field count, or result envelope.
