# Code Quality

This is the canonical codebase-level engineering standard shared by Codex and
Fable. Read the universal kernel for code changes, then only the risk sections
that match the touched surface. Executor-specific methods and reporting remain
in each executor's workflow and skills.

## Universal Code-quality Kernel

- Commercial-grade from the first draft; do not write simple code that needs
  hardening later.
- Prefer the smallest correct change that satisfies approved scope, preserves
  bounded-resource contracts, and has validation proportional to risk.
- Do not add speculative features, abstractions, refactors, compatibility
  shims, fallback paths, feature flags, generic helper layers, or future-proof
  hooks unless approved and validated at the same risk level.
- Correctness over speed: no races, leaks, unbounded resources, or silent
  contract drift.
- For Non-trivial changes, define architecture before code: concurrency model,
  backpressure, failure/recovery, resource bounds, and shutdown sequencing.
- Maintain comments on Non-trivial code covering invariants,
  ownership/lifetime, concurrency contracts, drop policy, and why.
- No placeholders: do not leave `TODO`, `...`, stubs, partial handlers, or
  omitted error paths in touched files.
- Keep code reviewable in one sitting. Functions and methods have a soft target
  of 80 lines; more than 120 lines requires justification. Avoid new functions
  over 200 lines unless linear parsing/state-machine flow, generated code, or
  table-driven structure is clearer. A hand-written file over 500 lines is a
  review trigger; explain why splitting would reduce clarity or cohesion.
- Prefer cohesive helpers over monoliths without fragmenting control flow.

## Network And Lifecycle

Use context cancellation plus explicit deadlines and idle/stall timeouts on all
long-lived network I/O. Executor-specific lifecycle audits own deeper
connection-state, recovery, diagnostic, and leak methods.

## Bounded Retained State

Bounded retained state is mandatory. Any new or modified server-lifetime map,
`sync.Map`, heap/index, cache, pool, interner, retained slice, or side table
must explicitly document and validate one of:

- a hard cardinality cap;
- a time/window expiry rule;
- ownership-coupled deletion;
- reference-counted reclamation; or
- a clear proof that lifetime and cardinality are bounded by another structure.

Soft optimization caches are still resources. Interners, dedupe helpers,
scratch caches, and memoization maps may not grow for process lifetime unless
their maximum size and eviction/reset behavior are proven.

When deleting or evicting a primary object, review every secondary index,
cache, intern table, active counter, and diagnostics structure that can retain
derived state. Primary bounds do not imply secondary bounds unless deletion or
cardinality coupling is explicit.

Use the executor's retained-state audit before implementing retained-state
changes when available.

## Hot Paths And Performance

On hot paths, generic helper reuse is subordinate to runtime shape.

- If a path is dominated by single-item overflow or correction, default to
  in-place single-victim logic unless measurements justify more abstraction.
- A new shared helper on a hot path must prove zero or near-zero allocation with
  targeted benchmarks before it is acceptable.
- Performance claims require measurements; do not infer success from code
  shape alone.

## Scientific And Model Claims

Path reliability, p50, propagation, VOACAP, call correction, confidence, and
contest-utility behavior must remain scientifically grounded.

- Name the model assumption or contract being changed or relied on.
- Establish definitions, units, domains, boundaries, interpolation, rounding,
  tolerances, sentinels, and classifications from appropriate authority.
- Use provenance-independent golden vectors when model correctness is material.
- Tie behavior claims to current source, tests, replay/evaluation evidence,
  benchmark/profile data, runtime captures, or accepted ADR/TSR records.
- State remaining uncertainty when evidence is indirect, sampled, stale, or
  missing.
- Do not present plausible reasoning, comments, old ADRs, or generated maps as
  proof of current behavior without checking current source and validation.
- Do not claim lower latency, allocation, better p99, prediction quality, or
  call accuracy without matching measurements or evaluation evidence.

## Support-critical Comments (Go Comment Intent)

Go comments on support-critical code should help a human or agent answer why a
boundary exists, what it owns, and how to troubleshoot surprising behavior.

Cover when relevant:

- crawler-entry purpose on package entry, subsystem integration,
  support-critical leaf, replay, and tool entry files;
- ownership and lifetime for goroutines, timers, channels, queues, retained
  state, file handles, and background workers;
- resource bounds, eviction, expiry, drop, delay, overflow, or fail-open policy;
- invariants not obvious from local control flow;
- operator/support meaning of logs, metrics, replay artifacts, confidence
  glyphs, filters, gates, and diagnostics; and
- config, ADR, or runtime boundaries on which nearby code depends.

Do not restate obvious assignments, branches, or simple booleans. Do not
comment every repeated row or branch when one meaningful occurrence or field
guide explains the schema. Add file headers only when they improve discovery
of ownership, boundaries, related docs/tests, or troubleshooting routes.
Comments are not proof when code, tests, docs, or ADRs disagree, and stale
comments are defects.
