---
name: test-strategy-adversary
description: "Use before approval when testability or oracle quality could materially affect scope, behavior, architecture, observability, or validation planning, and after approval before implementation when the detailed planned evidence may not falsify a broken design. Apply to unclear parser, protocol, config, lifecycle, retained-state, performance, scientific/model, security, workflow-checker, or other material oracles. Do not trigger when accepted checks directly prove a localized mechanical change."
---

# Test Strategy Adversary

## Purpose

Determine whether the proposed or planned evidence can falsify a broken design
or implementation rather than merely confirm its own assumptions.

Use the method in two phases:

* a lightweight pre-approval falsifiability probe when testability could affect
  scope, behavior, design, observability, or validation planning;
* a detailed post-approval contract-to-test review when implementation design is
  sufficiently concrete.

Treat all results as findings-only evidence. Leave product policy, scope,
approval, implementation, test ownership, final execution, validation claims,
and disposition with the lead and user.

## Phase Selection

### Pre-approval Falsifiability Probe

Use before publishing or approving a Scope Ledger when any of the following is
materially uncertain:

* the intended behavior cannot yet be stated as an observable result;
* multiple plausible implementations could satisfy superficial checks;
* testability may require a seam, interface, diagnostic, fixture, fault
  injection point, or architectural change;
* the planned validation may copy the implementation's assumptions;
* a parser, protocol, config, lifecycle, retained-state, security,
  performance, scientific/model, or workflow claim lacks an independent oracle;
* a broken design could plausibly pass the proposed checks;
* the evidence needed to prove the behavior may materially change scope,
  accepted risk, or validation.

Do not require an approved Scope Ledger or detailed implementation design for
this phase.

The pre-approval probe must remain lightweight and decision-focused. It should
not attempt to design every test or produce a complete test matrix.

### Post-approval Detailed Review

Use after exact approval and before implementation when the design is detailed
enough to derive concrete falsifiable checks and the planned evidence still
requires adversarial review.

This phase preserves the detailed contract-to-test matrix and blocks
implementation until material findings are dispositioned.

## Independence

Use a separate non-steered read-only reviewer when credible falsifiability
evidence depends on reasoning outside the lead's accumulated design assumptions.

Otherwise, the method may remain lead-owned.

A lead-owned pass must not be described as independent. State remaining oracle
or correlated-reasoning risk directly.

## Pre-approval Method

1. Identify the material behavior, invariant, or claim the proposed change is
   intended to establish.
2. State the observable result that would demonstrate success.
3. Identify at least one plausible broken design or implementation that could
   still pass the currently proposed evidence.
4. Determine what evidence would distinguish the intended behavior from that
   false-green case.
5. Check whether obtaining that evidence requires:

   * different scope;
   * a different ownership boundary or architecture;
   * additional observability;
   * a test seam, interface, fixture, or fault injection point;
   * an independent oracle or normative source;
   * a different validation level;
   * an explicit assumption or accepted residual risk.
6. Classify the result:

   * `adequate for scope`: the proposed validation can falsify the material
     behavior without changing scope or design;
   * `validation-plan refinement`: the planned evidence should change, but the
     proposed behavior and implementation authority do not;
   * `design or observability impact`: testability requires a material design,
     ownership, interface, diagnostic, or observability change before approval;
   * `scope or behavior gap`: the proposed scope omits behavior or failure
     handling required for a falsifiable result;
   * `oracle or normative gap`: no credible independent expected result is
     available yet.
7. Require every material finding to be dispositioned before the Scope Ledger is
   presented for approval.

The pre-approval probe may recommend validation obligations and conditional
design implications. It must not:

* select unresolved product policy;
* approve scope;
* write tests;
* create implementation diffs;
* claim validation was executed;
* replace the detailed post-approval review when that review remains triggered.

## Post-approval Gate

1. Require exact approval of the current Scope Ledger and an implementation
   design detailed enough to derive falsifiable checks.
2. Run before any implementation slice begins.
3. Provide the approved ledger, detailed design, normative contracts, relevant
   ADRs or TSRs, ambiguity and oracle results, current tests, and proposed
   validation plan.
4. Do not provide a completed implementation or intended test answers when a
   non-steered review is required.
5. Lead-owned falsifiability analysis is valid when independence is not
   triggered; state remaining oracle risk directly.

## Build the Contract-to-Test Matrix

1. Trace every material design contract and invariant to evidence that could
   fail when the implementation is wrong.

2. Include one row per distinct failure mechanism with these fields:

   | Field                    | Required content                                                                          |
   | ------------------------ | ----------------------------------------------------------------------------------------- |
   | Contract or invariant    | Behavior or bounded property that must hold                                               |
   | Failure or boundary case | Concrete way the design could violate it                                                  |
   | Stimulus or fault        | Input, timing, state, load, or injected failure needed                                    |
   | Observable result        | Result that proves or disproves the contract                                              |
   | Evidence level           | Unit, consumer, integration, fuzz, race, benchmark, profile, runtime, or workflow fixture |
   | False-green risk         | How the planned evidence could pass despite the defect                                    |
   | Exact checker            | Specific command, test, fixture, or captured evidence                                     |
   | Owner                    | Package, test, script, or lead-owned evidence surface                                     |

3. Challenge common false-green paths:

   * tests copying the production algorithm or implementation-derived golden
     values;
   * assertions that accept fallbacks, defaults, skips, partial output, or
     unreachable branches;
   * fixtures that omit malformed, zero, nil, empty, threshold, ordering,
     timeout, overload, cancellation, shutdown, and recovery cases;
   * race checks without concurrent stimuli;
   * fuzzing without useful oracles;
   * benchmarks without correctness guards;
   * profiles without a comparable baseline;
   * checker fixtures that prove keywords exist but not phase ordering,
     refusal, failure, or blocking behavior;
   * scientific tests whose expected values come only from the implementation
     under test.

4. Require the evidence level implied by the risk:

   * fuzzing for parser or protocol changes when malformed or adversarial input
     is material;
   * race and lifecycle checks for concurrent or shared-state work;
   * benchmarks and profiles for performance claims;
   * independent vectors and normative evidence for scientific or model claims;
   * behavioral fixtures rather than keyword-only checks for workflow claims.

Do not claim those checks ran unless they were actually executed and observed.

## Post-approval Disposition and Blocking

Classify every detailed-review finding:

* `covered` by a falsifiable planned check;
* `checker-only refinement` that may update implementation detail or planned
  checks without changing approved behavior or scope;
* `material scope or behavior gap` requiring a revised Scope Ledger, repeated
  applicable scope review, and exact reapproval;
* `normative evidence conflict` that blocks implementation pending resolution.

Block the first implementation slice until the lead dispositions all material
findings and the matrix is adequate.

Repeat the detailed review after material matrix or design changes.

Checker-only refinements may update the design or evidence plan, but must not
smuggle new behavior into the approved scope.

## Relationship Between Phases

A successful pre-approval probe does not automatically eliminate the
post-approval detailed review.

Skip the detailed phase only when:

* the change is localized and mechanical;
* the accepted checks directly prove the behavior;
* no material false-green path remains;
* implementation detail does not introduce a new oracle, lifecycle, ordering,
  concurrency, performance, scientific, security, or workflow risk.

When the detailed phase runs, reuse pre-approval findings rather than
duplicating the analysis.

## Reporting

For a pre-approval probe, report only:

* the material claim or invariant;
* the principal false-green case;
* the evidence needed to distinguish it;
* any scope, design, observability, oracle, or validation impact;
* the required disposition.

For a post-approval detailed review, report:

* evidence inspected;
* the contract-to-test matrix;
* findings and unknowns;
* required lead actions.

Do not require a fixed heading, score, or result envelope.

## Boundaries

Do not:

* write tests;
* implement production changes;
* execute final validation;
* review final implementation quality;
* replace the post-implementation code-quality reviewer;
* replace final-state verification;
* claim that a design or implementation is correct merely because a validation
  plan exists.
