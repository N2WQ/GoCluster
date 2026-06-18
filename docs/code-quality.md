# docs/code-quality.md

This document owns the code-quality rules referenced by `AGENTS.md`.

## Core Standard
Commercial-grade from the first draft. Do not write simple code that needs hardening later.

- Prefer the smallest correct change that satisfies the approved scope, preserves bounded-resource contracts, and has validation proportional to risk.
- Do not add speculative features, abstractions, or refactors that are not required by the approved scope.
- Do not add compatibility shims, fallback paths, feature flags, generic helper
  layers, or future-proof hooks unless they are required by the approved scope
  and validated at the same risk level as the behavior they affect.
- Correctness over speed. No races, leaks, unbounded resources, or silent contract drift.
- Use context cancellation plus explicit deadlines and idle/stall timeouts on all long-lived network I/O.
- For non-trivial changes, define architecture before code: concurrency model, backpressure strategy, failure/recovery modes, resource bounds, and shutdown sequencing.
- Maintain comments on non-trivial code covering invariants,
  ownership/lifetime, concurrency contracts, drop policy, and why.
- Be concise in responses. Skip ceremony for truly small edits; use the full workflow for non-trivial work.
- No placeholders. Do not leave `TODO`, `...`, stubs, partial handlers, or omitted error paths in touched files.

## Reviewability
Keep code reviewable in one sitting.

- Functions and methods: soft target <= 80 lines.
- Review trigger: > 120 lines requires a short justification.
- Avoid introducing new functions > 200 lines unless clearly justified by linear parsing/state-machine flow, generated code, or table-driven structure.
- Files: soft review trigger > 500 lines for hand-written source files.
- If a file grows past 500 lines, explain why splitting would reduce clarity or worsen cohesion.
- Prefer cohesive helpers over monolithic routines, but do not fragment code so aggressively that control flow becomes harder to follow.

## Go Comment Intent
Go comments on support-critical code should help a human or agent answer
operational questions from source: why this boundary exists, what it owns, and
how to troubleshoot surprising behavior.

Required intent coverage when touching relevant code:
- crawler-entry comments on package entry files, subsystem integration files,
  support-critical leaf files, and replay/tool entry points where a human or
  agent must know why the file matters before reading implementation detail
- ownership and lifetime for goroutines, timers, channels, queues, retained
  state, file handles, and background workers
- resource bounds, eviction, expiry, drop, delay, overflow, or fail-open policy
- invariants that would not be obvious from local control flow
- operator/support meaning of logs, metrics, replay artifacts, confidence
  glyphs, filters, gates, or diagnostics
- config, ADR, or runtime boundaries when nearby code depends on them

Avoid comment noise:
- do not restate obvious assignments, branches, or simple booleans
- do not comment every repeated row/branch when the first meaningful occurrence
  or a field guide explains the schema
- do not add file headers to every helper solely to satisfy a pattern; use them
  where they improve discovery of ownership, boundaries, related docs/tests, or
  troubleshooting routes
- do not use comments as proof of behavior when code, tests, docs, or ADRs
  disagree
- keep comments updated when behavior changes; stale comments are defects

## Hot Paths
On hot paths, generic helper reuse is subordinate to runtime shape.

- If the path is dominated by single-item overflow or single-item correction, default to in-place single-victim logic unless measurements justify a more abstract design.
- Any new shared helper introduced on a hot path must prove zero or near-zero allocation with targeted benchmarks before it is considered acceptable.
- Performance claims require measurements. Do not infer success from code shape alone.

## Scientific And Model Claims
Path reliability, p50, propagation, VOACAP, call-correction, confidence, and
contest-utility behavior must stay scientifically grounded.

- Name the model assumption or contract being changed or relied on.
- Tie behavior claims to current source, tests, replay/evaluation evidence,
  benchmark/profile data, runtime captures, or accepted ADR/TSR records.
- State remaining uncertainty when evidence is indirect, sampled, stale, or
  missing.
- Do not present plausible reasoning, comments, old ADRs, or generated maps as
  proof of current runtime behavior without checking the current source and
  relevant validation.
- Do not claim lower latency, lower allocation, better p99, better prediction
  quality, or better call accuracy without the matching measurements or
  evaluation evidence.

## Bounded Retained State
Bounded retained state is mandatory.

Any new or modified server-lifetime `map`, `sync.Map`, heap/index, cache, pool, interner, retained slice, or side table must explicitly document and validate one of:
- a hard cardinality cap
- a time/window expiry rule
- ownership-coupled deletion
- reference-counted reclamation
- a clear proof that its lifetime and cardinality are bounded by another structure

Soft optimization caches are still resources. Interners, dedupe helpers, scratch caches, and memoization maps may not grow for process lifetime unless their maximum size and eviction/reset behavior are proven.

When deleting or evicting a primary object, review every secondary index, cache, intern table, active counter, and diagnostics structure that can retain derived state. Primary bounds do not imply secondary bounds unless deletion coupling or cardinality coupling is explicit.

Use `go-retained-state-audit` before implementing retained-state changes when available.
