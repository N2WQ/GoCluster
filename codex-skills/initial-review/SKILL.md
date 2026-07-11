---

name: initial-review
description: Deliver a concise initial orientation to existing code without implementing changes. Use only when the user explicitly asks for a concise, brief, quick, or initial overview. Do not use for thorough reviews, deep explanations, audits, diagnosis, architecture assessment, correctness analysis, or change planning. Produce a deterministic 5-bullet summary with file references and targeted clarifying questions.

# Initial Review

Provide a brief initial orientation to the user-provided scope.

## Process

1. Resolve scope from user arguments; if scope is missing or ambiguous, ask for the path/component first.
2. Read code only. Use read/search tools to gather entry points, execution flow, state mutations, and side effects.
3. Synthesize a concise understanding-first review. Do not propose or implement code changes.

## Routing Boundary

* Use this skill only when the user explicitly requests concise or initial orientation.
* Route broader code-understanding requests to `explain-code`.
* Do not compress a thorough review, audit, diagnosis, architecture assessment, correctness analysis, or risk review into this format.

## Output Contract

Target 250 words or fewer and return exactly 5 bullets in this order:

1. **Entry points** - files and functions.
2. **Execution flow** - happy path.
3. **State + side effects** - I/O, globals, storage.
4. **Inputs/invariants** - assumptions, limits, and rules the code relies on.
5. **Risks/unknowns** - maximum 3, with exact next-read pointers when needed.

## Rules

* Include file references with line numbers when available; if exact lines are unavailable, cite the nearest function or block.
* Keep explanations short and concrete; do not repeat requirements from the user's prompt.
* Do not suggest implementation details, edit hooks, or apply edits.
* End with 1-3 targeted clarifying questions; default to 1 unless ambiguity is high.
