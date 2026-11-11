---
title: Code Quality
description: Shared GoCluster engineering standards and proportionate permanent testing.
---

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

## Proportionate Permanent Testing

- Inspect and extend existing tests, fixtures and runners before adding parallel
  infrastructure. Organize test files by cohesive behavior, not development task.
- Test behavior primarily in its owning component. Add integration coverage
  when it proves a distinct wiring, transport, persistence or lifecycle risk
  that owner-level tests cannot establish. Do not repeat a semantic matrix at
  every layer without a distinguishing failure it can detect.
- Treat new permanent runners, cross-process handoffs, receipt formats and
  miniature protocol services as exceptional. Explain the concrete evidence
  gap, why existing tools cannot close it, and the maintenance cost in the
  existing change plan or review. This is not a separate approval ceremony.
- Separate development experiments from permanent regression protection.
  Retain investigative code only when it provides meaningful, repeatable
  protection or is needed for an accepted qualification obligation.
- Review the whole testing change, including helpers, scripts, fixtures,
  duplicated scenarios and CI, for complexity, execution cost and maintenance.
  Ask whether a smaller suite would detect the same material failures.
- File counts and test-to-production line ratios are diagnostic signals, not
  acceptance targets. Do not combine files or remove unique protection merely
  to improve those numbers. Review expected behavior as well as coverage;
  passing tests can enforce an incorrect expectation.

Keep risk-driven race, performance, scientific and lifecycle evidence required
by the affected contract. These rules govern how evidence is obtained, not
whether necessary evidence may be waived. Existing execution and qualification
obligations remain in force; reducing them requires separately approved scope.
See the [testing architecture](runbooks/testing-architecture.md) and
[ADR-0316](decisions/ADR-0316-proportionate-permanent-testing.md).

## Network And Lifecycle

Use context cancellation plus explicit deadlines and idle/stall timeouts on all
long-lived network I/O. Executor-specific lifecycle audits own deeper
connection-state, recovery, diagnostic, and leak methods.

## Bounded Retained State

Bounded retained state is mandatory. Any new or modified server-lifetime map,
`sync.Map`, heap/index, cache, pool, interner, retained slice, or side table
must document and validate a defensible bound on simultaneous entries and
retained bytes. Include retained backing storage and secondary state, not just
visible lengths or primary counters. State the enforcing paths, assumptions
and evidence; distinguish a proven bound from an estimate or sampled trend.

A proof may combine a hard cap, a fixed key universe, bounded admission,
maximum entry/backing size, expiry, ownership-coupled deletion, reference
counting or another explicitly bounded owner. Account for admission bursts,
cleanup delay, stale derived entries and transient overlap when they affect
the claimed maximum. A cardinality cap alone does not bound variable payloads
or retained slice capacity.

Expiry, ownership-coupled deletion and reference counting alone are not proof
of simultaneous size bounds. Expiry limits retention age only when cleanup
timing is enforced; its peak size also depends on admission or a bounded key
universe and entry size. Reclamation requires a bound on simultaneously live
owners/references and their retained state.

Executor audits may name a primary reclamation mechanism, but that
classification does not replace the simultaneous-size proof. Use supplementary
or combined constraints where needed; do not require a new capacity setting
when existing enforced limits establish the bound. A bound on retained data
does not by itself establish a process RSS or working-set ceiling; label
allocator/runtime overhead estimates and unproved assumptions honestly.

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
