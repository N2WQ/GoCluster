# Semantic Resolution Authority

Use this reference to determine whether a material semantic ambiguity has been
resolved and who owns the decision.

## Valid Resolution Sources

A material ambiguity is resolved only by one of these:

1. the user explicitly selects or approves an interpretation in a direct
   decision statement;
2. a current authoritative repository contract unambiguously selects it;
3. a governing normative external authority selects it;
4. confirmed evidence disproves every competing material interpretation.

Record the authority source for every resolved item.

## Explicit User Resolution

Examples of explicit resolution:

* `I choose interpretation A.`
* `Use session-authority ordering.`
* `Replacement invalidates every old save not yet committed.`
* `Define newer as the currently authoritative registered session.`

The statement must select the policy, not merely approve the reasoning process.

## Not Resolution

These do not resolve policy:

* `Determine what this should mean.`
* `Recommend the best interpretation.`
* `Plan the change.`
* `What would you do?`
* `Continue.`
* `Proceed.`
* silence after a recommendation;
* agreement with technical reasoning that does not clearly select the policy;
* existing implementation convenience;
* current tests that encode one behavior;
* an architecture that makes one interpretation easier;
* a recommendation from the ambiguity reviewer.

When wording could reasonably be advice-seeking or policy selection, treat it as
advice-seeking and request explicit selection.

## Authority Gaps

Classify an authority gap when no user, contract, normative source, or confirmed
evidence can resolve the policy.

For an authority gap:

* identify the missing decision owner or authority;
* state the consequences of leaving the issue unresolved;
* do not infer intent from implementation convenience;
* ask for the exact decision required.

## Evidence Versus Policy

Current behavior and tests may establish what the system does today.

They do not automatically establish what it should mean, especially when the
review exists because current semantics are disputed.

A stronger scientific or normative source may narrow the available policy
choices without selecting among remaining product options.

## Required Output

For each material ambiguity, state:

* status: unresolved, resolved by user, resolved by authority, resolved by
  evidence, or authority gap;
* decision owner;
* authority source;
* exact decision still required, if unresolved.
