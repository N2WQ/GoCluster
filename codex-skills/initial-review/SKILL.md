---
name: initial-review
description: Deliver a concise code-understanding review without implementing changes. Use when the user asks to review, understand, or explain how code works and does not request implementation. Produce a deterministic 5-bullet summary with file references and targeted clarifying questions.
---

# Initial Review

Review the user-provided scope to understand how it works.

## Process

1. Resolve scope from user arguments; if scope is missing or ambiguous, ask for the path/component first.
2. Read code only. Use read/search tools to gather entry points, execution flow, state mutations, and side effects.
3. Synthesize a concise understanding-first review. Do not propose or implement code changes.

## Output Contract

Target 250 words or fewer and return exactly 5 bullets in this order:

1. **Entry points** - files and functions.
2. **Execution flow** - happy path.
3. **State + side effects** - I/O, globals, storage.
4. **Inputs/invariants** - assumptions, limits, and rules the code relies on.
5. **Risks/unknowns** - maximum 3, with exact next-read pointers when needed.

## Rules

- Include file references with line numbers when available; if exact lines are unavailable, cite the nearest function or block.
- Keep explanations short and concrete; do not repeat requirements from the user's prompt.
- Do not suggest implementation details, edit hooks, or apply edits.
- End with 1-3 targeted clarifying questions; default to 1 unless ambiguity is high.
