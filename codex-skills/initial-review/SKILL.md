---

name: initial-review
description: Deliver a concise initial orientation to existing code without implementing changes. Use only when the user explicitly asks for a concise, brief, quick, high-level, or initial overview. Do not use for thorough reviews, deep explanations, audits, diagnosis, architecture assessment, correctness analysis, risk analysis, or change planning.

# Initial Review

Provide a brief initial orientation to the user-provided code scope.

This skill is intentionally narrow. It provides fast orientation, not deep
analysis.

## Routing Boundary

Use this skill only when the user explicitly requests a concise, brief, quick,
high-level, or initial overview.

Examples that trigger this skill:

* “Give me a quick overview of this package.”
* “Briefly explain what this file does.”
* “Give me a high-level orientation to this component.”
* “Summarize this code in five bullets.”
* “Start with a concise initial review.”

Do not use this skill for:

* thorough or comprehensive reviews;
* deep explanations or full execution traces;
* audits or correctness analysis;
* diagnosis or root-cause investigation;
* architecture, concurrency, lifecycle, performance, or security assessment;
* risk analysis;
* change planning or implementation requests.

Route broader read-only code-understanding requests to `explain-code`.

Do not compress a broader request into this format merely because a concise
summary is possible.

## Process

1. Resolve the requested scope from the user’s prompt.
2. If the scope is materially ambiguous and cannot be resolved from the
   repository, ask one targeted question.
3. Read the relevant code before making behavioral claims.
4. Inspect enough of the immediate execution path to identify entry points,
   primary flow, material state changes, side effects, and key invariants.
5. Produce a concise orientation without proposing or implementing changes.

## Output Contract

Target 250 words or fewer and return exactly five bullets in this order:

1. **Entry points** - relevant files, functions, methods, or types.
2. **Execution flow** - the primary runtime path.
3. **State and side effects** - I/O, shared state, storage, logging, metrics, or
   external interactions.
4. **Inputs and invariants** - assumptions, limits, validation, and rules the
   code relies on.
5. **Risks or unknowns** - up to three material items, with exact next-read
   pointers where additional inspection is required.

## Evidence Rules

* Ground material claims in inspected repository source.
* Include file references with line numbers when available. If exact lines are
  unavailable, cite the nearest function, method, type, or block.
* Separate observed behavior from unknowns.
* Do not infer uninspected behavior from typical implementations.
* Do not claim tests, runtime behavior, or validation results unless they were
  actually observed.
* If a conclusion requires broader inspection than this skill permits, state
  that the request should continue through `explain-code` rather than presenting
  an incomplete conclusion as definitive.

## Change Boundary

* Do not propose refactors, optimizations, implementation details, edit
  locations, or patches.
* Do not modify repository files.
* Do not enter the Small or Non-trivial mutation route unless the user later
  requests a change.

## Clarifying Questions

Ask a clarifying question only when it is necessary to identify the code scope
or distinguish materially different runtime behavior.

Do not add a mandatory question when the request can be answered from current
repository evidence.
