# Semantic Ambiguity Analysis

Use this reference to discover, classify, compare, and report material semantic
forks.

## Active Search Areas

Probe applicable areas including:

* Boolean combinations and precedence;
* empty, missing, zero, nil, and malformed values;
* defaults and sentinels;
* thresholds and exact-boundary behavior;
* authentication and admission;
* ownership and replacement precedence;
* ordering and concurrency;
* reputation, correction, confidence, and classification gates;
* retries, fallback, recovery, and terminal failure;
* diagnostics and operator visibility;
* compatibility and migration;
* persistence and retained state;
* test oracles and expected results.

Do not depend on the lead having already identified the ambiguity.

## Classification

Classify each uncertainty as one of:

* `material semantic ambiguity` — competing answers change observable behavior,
  compatibility, safety, persistence, ordering, classification, failure
  handling, defaults, thresholds, or expected tests;
* `implementation uncertainty` — external behavior is explicit and only the
  mechanism remains open;
* `documentation ambiguity` — behavior is established but authoritative wording
  is incomplete or contradictory;
* `authority gap` — no user, contract, normative source, or documented owner can
  resolve the policy;
* `resolved by authority` — controlling evidence selects one interpretation;
* `resolved by user` — the user explicitly selects one interpretation;
* `resolved by evidence` — confirmed facts eliminate every competing material
  interpretation.

Record the rationale and authority source.

## Distinguishing Cases

Use concrete edge cases and interleavings that produce different observable
results.

Useful probes include:

* exact threshold and one unit on either side;
* empty versus missing;
* zero versus unset;
* simultaneous replacement and persistence;
* stale writer versus current owner;
* retry after partial success;
* fallback after malformed input;
* conflicting precedence rules;
* migration from legacy to new semantics;
* insufficient evidence versus adverse classification.

An ambiguity is not fully analyzed until at least one case distinguishes the
material interpretations.

## Interpretation Comparison

For each interpretation, state:

* user-visible behavior;
* operator-visible behavior;
* compatibility and migration consequences;
* safety and failure consequences;
* persistence and retained-state effects;
* ordering implications;
* architecture-neutral constraints;
* validation and test-oracle obligations;
* assumptions;
* reasons to reject it under current constraints.

## Decision Criteria

Use applicable criteria such as:

* stated user objective;
* backward compatibility;
* least surprise;
* safety;
* deterministic ownership;
* operational recoverability;
* migration cost;
* falsifiability;
* consistency with adjacent contracts.

Do not disguise a product-policy choice as a technical inevitability.

## Conditional Recommendations

A recommendation should identify:

* favored interpretation;
* supporting evidence;
* assumptions;
* decision criteria;
* decision owner;
* what evidence or objective would change the recommendation;
* exact explicit decision required.

A recommendation remains non-authorizing until valid authority resolves it.

## Required Output

The ambiguity register should allow the lead and user to understand:

> what is unresolved → why the alternatives differ → what each alternative
> changes → which is conditionally favored → who must decide → what exact
> decision is required
