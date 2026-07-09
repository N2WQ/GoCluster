---
name: "go-retained-state-audit"
description: "Use when designing, reviewing, or modifying Go server-lifetime retained state: maps, sync.Maps, heaps, indexes, caches, pools, interners, retained slices, cleanup/eviction paths, or memory optimizations that add side tables. Require explicit ownership, bounds, eviction coupling, churn tests, and cardinality observability."
---

# Go Retained-State Audit

Use this skill before implementing or approving changes that add or modify
long-lived retained state in the Go cluster. Fable-native counterpart to
`codex-skills/go-retained-state-audit/SKILL.md`.

## Required Workflow

1. Inventory retained state.
   - List every new or modified `map`, `sync.Map`, heap/index, cache, pool,
     interner, retained slice, queue, or side table.
   - Identify whether it is process-lifetime, component-lifetime,
     request-lifetime, or scratch-only.
   - Treat soft optimization structures as real retained resources.

2. Prove the bound.
   - For each retained structure, name exactly one primary bound:
     - hard cardinality cap
     - time/window expiry
     - ownership-coupled deletion
     - reference-counted reclamation
     - bounded by another explicitly bounded owner
   - If the bound depends on another structure, name the owner and the code
     path that keeps them coupled.
   - Reject process-lifetime growth based only on "low expected
     cardinality."

3. Audit deletion and eviction coupling.
   - For every primary delete, eviction, cleanup, load-prune, overflow
     trim, and shutdown path, identify secondary structures that can
     retain derived state.
   - Verify indexes, heaps, interners, counters, active sets, and
     diagnostics converge after the primary object is removed.
   - If secondary state intentionally outlives the primary object, it must
     have its own independent bound.

4. Define tests before code.
   - Add or update tests that insert beyond the cap/window and assert
     retained cardinality stays bounded.
   - Add churn tests: insert, evict/delete, insert new unique values, then
     assert side structures do not grow with total historical input.
   - Add stale-index tests where heap/index entries are updated repeatedly
     before cleanup.
   - Add restart/load tests when the retained state is rebuilt from disk.

5. Define observability.
   - Expose or log cardinality for every new server-lifetime retained
     structure unless an existing metric already covers it.
   - For memory investigations, include enough counters to distinguish
     active primary state from secondary/index/cache retention.

6. Review performance claims separately.
   - Do not accept a memory optimization that reduces duplicate allocation
     by adding an unbounded lifetime cache.
   - Separate `alloc_space` wins from `inuse_space` retention risk.
   - If live pprof confirmation is missing, say the change is locally
     validated, not runtime-confirmed.

## Output Expectations

- Include a `Retained-State Audit` section before implementation.
- State `No retained-state changes` if the audit was triggered but no
  long-lived state is actually affected.
- Name the exact code paths that enforce the bound and the tests that prove
  it.
- Call out any structure whose bound is not executable or observable.

## Default Red Flags

- `map[string]string` interners without a cap, generation reset, or
  reference counting.
- `sync.Map.Store` on repeated evaluations with no delete path.
- Heap/index maps that mirror primary maps but are not cleaned when primary
  entries are removed.
- `sync.Pool` used to hide allocation without a benchmark and without safe
  object lifetime.
- Cleanup paths that prune primary entries but leave side tables, active
  counters, or interned strings behind.
