# Code Quality

This is the canonical codebase-level engineering standard shared by Codex and
Fable. Restructuring this file does not change Fable workflow or reporting.
Read the universal kernel for code changes, then only the risk sections that
match the touched surface.

## Universal Code-quality Kernel

- Implement the smallest approved correct change.
- Do not add unapproved abstractions, refactors, compatibility behavior,
  fallback paths, feature flags, future-proofing, or unrelated cleanup.
- Do not introduce races, leaks, unbounded resources, silent contract drift,
  ignored errors, incomplete paths, placeholders, stubs, or task-related
  `TODO`s.
- Preserve clear ownership, error behavior, compatibility, determinism, and
  reviewability. Validate touched behavior and material claims proportionally
  to risk.
- Define material architecture before code, including applicable concurrency,
  backpressure, failure/recovery, resource-bound, and shutdown decisions.
- Keep functions cohesive and reviewable. Functions and methods have a soft
  target of 80 lines; more than 120 lines requires justification. Avoid new
  functions over 200 lines unless a linear parser/state machine, generated
  code, or table-driven structure is clearer. A hand-written source file
  growing past 500 lines is a review trigger; explain why splitting would
  reduce clarity or cohesion. Avoid both monoliths and fragmented control flow.

## Network And Lifecycle

Long-lived network paths require an explicit, tested liveness, cancellation,
recovery, and shutdown contract appropriate to the protocol. Determine:

- who owns the connection, goroutines, timers, channels, and cancellation;
- how initial failure, EOF, mid-stream drop, silent stall, and zero-data states
  are detected;
- which deadlines, keepalives, idle/stall timers, retries, and backoff rules
  apply, including any deliberately persistent behavior;
- how shutdown interrupts blocking work and releases every owned resource;
- how overload, retry storms, and repeated failure remain bounded; and
- which logs or metrics distinguish connecting, active, idle, retrying,
  disabled, and stopped states.

Use context cancellation plus explicit deadlines and idle/stall timeouts on all
long-lived network I/O, with values and reset behavior appropriate to the
protocol.

Do not treat a keepalive, retry loop, or context alone as proof of recovery or
clean shutdown. Use `go-connection-lifecycle-audit` and compose
`go-leak-detection` when this surface changes.

## Bounded Retained State

Any new or modified server-lifetime map, `sync.Map`, heap, index, cache, pool,
interner, retained slice, or side table must have an executable bound through
at least one of:

- a hard cardinality cap;
- time or window expiry;
- ownership-coupled deletion;
- reference-counted reclamation; or
- a demonstrated bound inherited from another structure.

Soft caches are still resources. Document ownership, insertion paths, cleanup
triggers, eviction coupling, and observable cardinality. When a primary object
is removed, inspect every secondary index, counter, cache, and intern table that
can retain derived state. Validate churn and high-cardinality behavior, not only
steady state. Use `go-retained-state-audit` when this surface changes.

## Hot Paths And Performance

Runtime shape outranks abstract helper reuse on parsing, fan-out, queue,
correction, and other measured hot paths.

- Establish the baseline and workload before optimizing.
- Preserve correctness guards in benchmarks and profiles.
- Review allocations, CPU, retained heap, contention, latency, and p99 according
  to the claim being made.
- If a path is dominated by single-item overflow or correction, default to
  in-place single-victim logic unless measurements justify more abstraction.
- A new shared helper on a hot path must prove zero or near-zero allocation with
  targeted benchmarks and an explicit non-regression criterion; code shape
  alone is not evidence.
- Distinguish cold, warm, transition, allocation-space, and in-use evidence.

Do not claim lower latency, allocation, CPU, heap, or better p99 without the
matching measurement. Use `go-hotpath-design` before material hot-path design
and `pprof-impact-review` when comparing profile bundles.

## Scientific And Model Claims

Path reliability, p50, propagation, VOACAP, call correction, confidence, and
contest-utility behavior must remain scientifically grounded.

- Name the model assumption or normative contract being changed or relied on.
- Establish definitions, units, domains, boundaries, interpolation, rounding,
  tolerances, sentinels, and classifications from appropriate authority.
- Use golden vectors independent of the implementation when model correctness
  is material.
- Tie claims to current source, tests, replay/evaluation evidence,
  benchmark/profile data, runtime captures, or accepted current decisions.
- State uncertainty when evidence is indirect, sampled, stale, calibrated, or
  missing.

Implementation code, comments, generated maps, and tests derived only from that
implementation are not independent normative proof. Use
`scientific-model-oracle` when model semantics or claims change.

## Support-critical Comments

Comments on support-critical Go should explain why a boundary exists, what it
owns, and how surprising behavior is diagnosed. Cover, when material:

- package or subsystem entry purpose and important source/test/doc routes;
- ownership and lifetime of goroutines, timers, channels, queues, retained
  state, files, and background workers;
- resource bounds, eviction, expiry, overflow, drop, delay, and fail-open or
  fail-closed policy;
- non-obvious invariants and concurrency contracts;
- operator meaning of logs, metrics, artifacts, filters, gates, diagnostics,
  and classifications; and
- config, ADR, or runtime boundaries on which nearby code depends.

Do not restate obvious assignments or add headers mechanically. Prefer the
first meaningful explanation or a field guide over repetitive comments. Stale
comments are defects; comments never override current code, tests, runtime
contracts, or decisions.
