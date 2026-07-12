---
name: workflow-contract-audit
description: "Use when reviewing or changing Codex workflow authority, approval, validation routing, review rules, templates, repo-managed skills, or enforcing scripts. Select conformance mode when checking implementation against accepted workflow policy. Select effectiveness mode when the user explicitly asks whether the workflow, its rules, or its architecture should be retained, changed, consolidated, replaced, or removed. Do not trigger for ordinary product documentation or runtime work whose workflow contract is unchanged."
---

# Workflow Contract Audit

## Purpose

Support two distinct forms of workflow review without confusing policy
conformance with policy effectiveness:

* Conformance mode prevents contradiction, unreachable rules, duplicated
  ownership, and checkers that confuse response formatting with engineering
  compliance.
* Effectiveness mode evaluates whether the workflow supports sound reasoning,
  engineering quality, efficiency, user authority, and repository safety.

## Mode Selection

Select the mode from the user's intent before applying the method.

### Conformance Mode

Use conformance mode when implementing or reviewing a change within accepted
workflow policy, checking contract drift, or validating consistency among
workflow documents, skills, templates, decisions, and scripts.

In conformance mode, preserve accepted authority routes and specialist methods
unless the approved change explicitly modifies them.

### Effectiveness Mode

Use effectiveness mode when the user explicitly asks whether the Codex
workflow, its architecture, rules, approval mechanisms, classifications,
specialist boundaries, triggers, templates, checker assertions, or accepted
workflow decisions limit reasoning or should be improved.

In effectiveness mode:

* Treat all current Codex workflow policy as evidence and an object of
  evaluation, not as a conclusion that must be preserved.
* The review may recommend retaining, modifying, consolidating, replacing, or
  removing any Codex workflow policy, specialist identity, trigger, template,
  checker assertion, approval mechanism, classification, or workflow decision.
* Do not apply a preservation requirement from conformance mode to a mechanism
  whose effectiveness is being evaluated.
* Distinguish stable repository-safety principles from revisable workflow
  policy.
* Permit conditional recommendations while leaving product policy, mutation
  authority, and final workflow decisions with the user.
* Do not claim improved reasoning, engineering quality, or efficiency without
  comparative evidence. Identify unmeasured benefits as hypotheses.
* Checker success establishes contract consistency only. It does not establish
  workflow effectiveness, reasoning quality, correct classification, sufficient
  discovery, or engineering quality.

Effectiveness mode remains read-only unless the user separately authorizes a
workflow change through the applicable mutation route.

## Stable Constraints

Both modes preserve these constraints:

* User authority over repository mutation and approved scope.
* Current repository evidence for material claims.
* Clear separation of observed facts, inferences, assumptions, proposals, and
  unknowns.
* Preservation of unrelated work and executor ownership boundaries.
* Honest reporting of checks, runtime behavior, performance, scientific claims,
  and independent review.
* Validation appropriate to the affected behavior and actual engineering risk.
* Bounded resources, lifecycle safety, compatibility, and operator contracts.
* No mutation of Fable-owned files through a Codex-only change.

These constraints govern how workflow policy is reviewed or changed. They do
not require preservation of every current workflow mechanism.

## Conformance Method

1. Identify the changed Codex contract surfaces and any shared documents used
   by Fable.
2. Preserve accepted authority routes, agreed scope, reapproval requirements,
   current-evidence requirements, and touched-surface validation except where
   the approved change explicitly modifies them.
3. Confirm each detailed rule has one natural owner and remains reachable from
   `AGENTS.md` or a positive specialist trigger.
4. Check validation commands, review expectations, skill descriptions,
   metadata, templates, and scripts for contradiction.
5. Preserve each specialist's applicable engineering method except where the
   approved change explicitly modifies, consolidates, or replaces it.
6. Verify repo-skill frontmatter, names, referenced assets, and absence of
   user-level installation paths.
7. Review shared-document changes for cross-executor semantic drift.
8. Run the workflow checker, its named negative fixtures, and the repo-skill
   verifier after edits.

## Effectiveness Method

1. Identify the user's intended outcomes and the current workflow mechanisms
   that influence them.
2. Map any self-referential rule that restricts which mechanisms may be
   questioned, which evidence may be gathered, which recommendations may be
   made, or how deeply findings may be reported.
3. Separate stable repository-safety principles from revisable workflow policy.
4. Evaluate each material mechanism against observable benefits, failure modes,
   governance effects, reasoning effects, engineering consequences, and
   operational cost.
5. Search for credible alternatives rather than assuming the current
   architecture or terminology is the correct comparison baseline.
6. Allow specialists or reviewers to make conditional recommendations under
   explicit assumptions without transferring user or lead authority.
7. Distinguish structural evidence, historical rationale, measured outcomes,
   and unsupported hypotheses.
8. Recommend how material claims could be falsified or compared before treating
   a workflow redesign as proven.
9. When proposing changes, preserve stable constraints and identify any
   governance, rigor, code-quality, ownership, or validation risk introduced by
   the proposal.
10. Do not run consistency checkers merely to support an effectiveness claim.
    Run them after an authorized edit to verify that the resulting contract is
    internally coherent.

## Static Boundary

Static checks may establish text, ownership, references, positive and negative
trigger representation, supplied changed-path boundaries, and consistency with
the policy they encode.

They cannot prove conversational approval, correct classification, discovery
sufficiency, specialist necessity, validation adequacy, genuine independence,
durable decision judgment, improved reasoning, engineering quality, or
workflow effectiveness.

## Reporting

In conformance mode, report material contradictions, checker results,
cross-executor risks, and remaining gaps.

In effectiveness mode, report material self-referential constraints, retained
safety principles, revisable policies, alternative mechanisms, evidence limits,
and risks of the recommendation.

Do not require a fixed heading, score, field count, or result envelope in either
mode.
