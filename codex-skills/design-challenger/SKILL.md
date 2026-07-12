---
name: design-challenger
description: "Use after neutral discovery and before Scope Ledger drafting when consequential design uncertainty, ownership placement, lifecycle/state responsibility, compatibility, migration, algorithm choice, operability, or reversibility could admit a materially different safe approach. Begin with a lightweight design-space probe when the lead currently sees one obvious design but an alternative may plausibly exist. Run the full comparison when the probe identifies two or more viable approaches or when choosing incorrectly would be materially consequential. Do not trigger when settled constraints clearly force one safe design."
---

# Design Challenger

## Purpose

Prevent premature convergence on the first plausible design.

Use the method in two stages:

* a lightweight design-space probe that tests whether the apparent design is
  actually forced by the evidence;
* a full neutral comparison when two or more viable approaches remain or when
  the cost of choosing incorrectly is materially consequential.

Treat the result as findings-only evidence.

Leave product policy, scope, design selection, approval, implementation,
validation, and final disposition with the lead and user.

## Stage Selection

### Lightweight Design-Space Probe

Use after neutral current-state discovery and before drafting the Proposed Scope
Ledger when any of the following is true:

* the lead currently sees one obvious design, but ownership could plausibly sit
  at another layer;
* lifecycle, state, persistence, compatibility, migration, or operability could
  be handled through a materially different mechanism;
* the proposed design introduces new shared state, synchronization, retained
  state, queues, registries, interfaces, background work, or ownership;
* the design depends on a local patch around a broader invariant or ownership
  boundary;
* failure, overload, recovery, shutdown, rollback, or migration behavior may
  favor a different architecture;
* choosing incorrectly would be difficult to reverse or could create material
  operational or compatibility cost;
* current evidence does not yet prove that the apparent design is the only safe
  design.

Do not require the lead to identify two viable architectures before this probe
runs.

The probe must remain lightweight. Its purpose is to determine whether a full
comparison is warranted, not to generate alternatives for their own sake.

### Full Design Comparison

Run the full comparison when:

* the probe identifies two or more viable approaches with materially different
  ownership, lifecycle, state, compatibility, migration, algorithms,
  reversibility, or operability;
* the user or current evidence already presents a genuine design fork;
* the apparent alternatives differ materially in failure behavior, resource
  bounds, observability, rollback, or validation;
* the consequence of selecting the wrong design justifies independent
  comparison even if one approach currently appears preferable.

Do not run the full comparison when settled requirements and current evidence
clearly eliminate all but one safe design.

## Establish Neutrality

Use a separate non-steered read-only reviewer when credible design comparison
depends on reasoning outside the lead's accumulated assumptions.

Otherwise, the method may remain lead-owned.

For a separate reviewer:

1. Run after the lead prepares a neutral fact-and-constraint packet from
   current-state discovery and any triggered scientific-model,
   requirements-ambiguity, or falsifiability work.
2. Provide confirmed requirements, constraints, current-state evidence,
   relevant ADRs or TSRs, and resolved semantics.
3. Withhold the lead's preferred solution, draft plan, and intended diff.
4. If inherited context exposes the preferred design, state the anchoring risk
   rather than claiming neutral comparison.

A lead-owned probe or comparison must not be described as independent.

## Lightweight Probe Method

1. State the apparent design and the constraints that seem to support it.
2. Ask whether each material responsibility could plausibly belong elsewhere:

   * ownership;
   * state;
   * synchronization;
   * persistence;
   * lifecycle;
   * validation;
   * operator control;
   * compatibility or migration.
3. Identify at least one credible alternative mechanism or ownership placement
   when the evidence supports one.
4. Test whether the alternative is eliminated by confirmed constraints or only
   by the lead's current preference.
5. Compare the apparent design and alternative at a high level across:

   * invariant preservation;
   * bounded resources;
   * normal and failure behavior;
   * shutdown and cleanup;
   * compatibility and migration;
   * observability;
   * rollback;
   * falsifiability.
6. Classify the result:

   * `single design established`: confirmed constraints eliminate credible
     alternatives;
   * `local implementation choice`: alternatives exist but do not materially
     change ownership, lifecycle, compatibility, risk, or validation;
   * `material design fork`: two or more viable approaches differ materially
     and require full comparison;
   * `design evidence gap`: the available evidence is insufficient to determine
     whether an alternative is viable;
   * `requirements or policy blocker`: the design cannot be compared until a
     user-owned semantic or policy choice is resolved.
7. Require material findings to be dispositioned before drafting the Scope
   Ledger.

The probe may recommend whether a full comparison is needed. It must not select
unresolved product policy or authorize implementation.

## Full Design Comparison Method

1. Verify material facts against cited source, tests, configuration, and
   decision records.
2. State unknowns instead of filling gaps with assumptions.
3. Identify at least two viable approaches when the evidence supports them.
4. If only one safe approach remains, explain which confirmed constraints
   eliminate each alternative.
5. For each viable approach, report:

   * mechanism and ownership boundaries;
   * invariants and bounded-resource properties;
   * state and lifecycle ownership;
   * normal, overload, failure, recovery, and shutdown behavior;
   * failure modes and edge conditions;
   * protocol, schema, persistence, and compatibility consequences;
   * migration and rollback consequences;
   * observability and operator effects;
   * validation obligations capable of disproving the approach;
   * assumptions and user-owned decisions;
   * reasons to reject the approach under the current constraints.
6. Compare the approaches using criteria derived from the actual problem rather
   than a generic preference for minimal code or minimal change.
7. Identify the smallest robust approach supported by the evidence.
8. Present the recommendation for lead disposition, not as an approved or
   automatically selected design.
9. Surface unresolved semantic or product-policy choices as blockers.

## Recommendation Rules

The challenger may:

* recommend one approach over another;
* make a conditional recommendation under explicit assumptions;
* explain why an apparently larger structural change is safer than a smaller
  patch;
* recommend retaining the current design when alternatives are materially
  worse;
* recommend additional evidence before selection.

The challenger must not:

* choose unresolved product policy;
* approve scope;
* expand mutation authority;
* create implementation diffs;
* treat the first apparent design as correct merely because it is smaller;
* invent alternatives unsupported by the system's actual constraints.

## Relationship to Falsifiability

Use pre-approval falsifiability findings as design evidence.

A design fork is material when the approaches differ in whether the intended
behavior can be independently disproved, observed, fault-injected, benchmarked,
or validated under realistic failure conditions.

The design challenger does not replace `test-strategy-adversary`.

## Relationship to Scope Review

The design challenger asks:

> What materially different design or ownership model could satisfy the same
> requirements, and which is best supported by the evidence?

The later `scope-ledger-adversarial-review` asks:

> Does the selected Scope Ledger safely and completely authorize the chosen
> work?

Do not approve scope or replace the scope adversary.

Require the lead to disposition every material design finding before drafting
the ledger.

## Reporting

For the lightweight probe, report only:

* the apparent design;
* the credible alternative, if any;
* the confirmed constraint that eliminates it, or the reason it remains viable;
* the classification;
* the required lead disposition.

For the full comparison, report:

* evidence inspected and unknowns;
* compared approaches;
* material tradeoffs;
* recommendation and conditions;
* user-owned choices;
* validation obligations;
* constraints the eventual Scope Ledger must preserve.

Do not require a fixed heading, score, field count, or result envelope.
