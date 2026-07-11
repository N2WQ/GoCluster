---

name: initial-review
description: Deliver a concise initial orientation to existing code without implementing changes. Use only when the user explicitly asks for a concise, brief, quick, high-level, or initial overview and no more specific repo-managed review, audit, diagnostic, domain, or engineering skill applies. Do not compose this skill with a specialist skill or use its output format to constrain specialist analysis.
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Initial Review

## Purpose

Provide a brief, evidence-grounded orientation to existing code when the user
explicitly requests a quick or initial overview and no more specific
repo-managed skill applies.

This skill is a standalone orientation route. It is not a generic formatting
layer for specialist reviews.

## Routing Boundary

Use this skill only when all of the following are true:

1. The user explicitly requests a concise, brief, quick, high-level, or initial
   overview.
2. The request is read-only and does not ask for implementation.
3. No more specific repo-managed review, audit, diagnostic, domain, security,
   lifecycle, concurrency, performance, scientific, configuration, protocol,
   blast-radius, or engineering skill applies.

Examples that trigger this skill:

* “Give me a quick overview of this package.”
* “Briefly explain what this file does.”
* “Give me a high-level orientation to this component.”
* “Summarize this code in five bullets.”
* “Start with a concise initial review.”

Do not use this skill for:

* thorough, comprehensive, detailed, or deep reviews;
* audits, diagnosis, or root-cause investigation;
* architecture, correctness, risk, security, performance, concurrency,
  lifecycle, resource, configuration, protocol, persistence, or scientific
  analysis;
* requests covered by a more specific repo-managed skill;
* change planning or implementation.

Route ordinary code explanations to `explain-code`.

Route specialist requests to the applicable specialist skill.

## Composition Rule

Do not compose this skill with another repo-managed review, audit, diagnostic,
domain, or engineering skill.

When another skill has a positive trigger:

* use that skill’s engineering method;
* use that skill’s depth and output expectations;
* do not apply this skill’s five-bullet structure;
* do not apply this skill’s word target;
* do not apply this skill’s risk or unknown limit;
* do not add a clarifying question merely because this skill normally permits
  one.

A specialist skill may produce a concise answer when the evidence and user
request justify concision. That concision must not omit specialist-required
analysis.

## Process

1. Resolve the requested code scope from the user’s prompt.
2. If the scope is materially ambiguous and cannot be resolved from the
   repository, ask one targeted question.
3. Read the relevant current code before making behavioral claims.
4. Inspect enough of the immediate execution path to identify entry points,
   primary flow, material state changes, side effects, and key invariants.
5. Produce a concise orientation without proposing or implementing changes.

## Output Contract

When this skill is the sole applicable repo-managed skill, target 250 words or
fewer and return exactly five bullets in this order:

1. **Entry points** - relevant files, functions, methods, or types.
2. **Execution flow** - the primary runtime path.
3. **State and side effects** - I/O, shared state, storage, logging, metrics, or
   external interactions.
4. **Inputs and invariants** - assumptions, limits, validation, and rules the
   code relies on.
5. **Risks or unknowns** - up to three material items, with exact next-read
   pointers where additional inspection is required.

This output contract applies only when `initial-review` is the sole applicable
skill.

## Evidence Rules

* Ground material claims in inspected repository source.
* Include file references with line numbers when available. If exact lines are
  unavailable, cite the nearest function, method, type, or block.
* Separate observed behavior from unknowns.
* Do not infer uninspected behavior from typical implementations.
* Do not claim tests, runtime behavior, or validation results unless they were
  actually observed.
* If a conclusion requires a specialist method or broader inspection, route to
  that skill rather than presenting an incomplete orientation as definitive.

## Change Boundary

* Do not propose refactors, optimizations, implementation details, edit
  locations, or patches.
* Do not modify repository files.
* Do not enter the Small or Non-trivial mutation route unless the user later
  requests a change.

## Clarifying Questions

Ask a clarifying question only when it is necessary to identify the requested
code scope or distinguish materially different runtime behavior.

Do not add a mandatory question when current repository evidence is sufficient.
