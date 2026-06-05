# ADR-0143: Custom SCP Support Lookup Duplicate Work Removal

- Status: Accepted
- Date: 2026-06-05
- Decision Origin: Incident

## Context

Follow-up profiling after the p50 merge showed Custom SCP was not the CPU root
cause, but it still contributed avoidable allocation and write work. The live
allocation profile attributed about 319 MiB to
`CustomSCPStore.RecentSupportCount -> snapshotFor -> NormalizeCallsign`, driven
by edit-neighbor stabilizer probes generating many synthetic calls that missed
the global callsign normalization cache. The same path also allocated the full
edit-neighbor variant slice before scanning it.

The fix must not change active p50 calculations, Custom SCP admission,
Custom SCP scoring, evidence windows, YAML defaults, retained-state bounds, or
operator-visible output.

## Decision

Add normalized support-count lookup methods on the existing recent-support
stores without changing the `RecentSupportStore` interface. The correctionflow
stabilizer uses the optional normalized method only after it has generated or
derived correction-normalized keys.

Stream edit-neighbor substitution variants through a callback so the stabilizer
can stop on the first contested neighbor without retaining the entire generated
variant slice.

Have Custom SCP static retention report whether the static timestamp actually
advanced. Live observation writes now refresh static expiry and persist static
membership only when the retained timestamp changed.

## Alternatives considered

1. Keep using `RecentSupportCount` for all generated variants.
   Rejected because it continues to populate the global callsign normalization
   cache with one-off synthetic misses.
2. Add methods to the shared `RecentSupportStore` interface.
   Rejected because existing callers do not need the fast path, and widening
   the interface would increase blast radius without changing semantics.
3. Batch or delay Custom SCP persistence.
   Rejected because crash-recovery convergence is more important than a broader
   write-coalescing optimization in this scope.

## Consequences

### Benefits

- Synthetic edit-neighbor lookups avoid the global callsign normalization cache.
- Full variant traversal avoids the retained output slice allocation.
- Custom SCP skips Pebble static membership writes when the static timestamp did
  not advance.
- p50 calculation, Custom SCP scoring, and retained evidence semantics remain
  unchanged.

### Risks

- Normalized lookup callers must only pass correction-normalized call keys.
- Edit-neighbor variant streaming must preserve the previous deterministic
  order and early-match semantics.
- Static persistence must still write every new or newer static timestamp.

### Operational impact

- No telnet command, HELP, YAML, p50, glyph, PATH filter, archive, or protocol
  behavior changes.
- The change reduces allocation and write churn inside existing bounded stores.

## Links

- Related issues/PRs/commits: current working tree
- Related tests: `spot/custom_scp_store_test.go`,
  `internal/correctionflow/shared_test.go`,
  `internal/correctionflow/stabilizer_test.go`
- Related docs: `docs/decisions/ADR-0080-custom-scp-retained-heap-layout.md`,
  `docs/decisions/ADR-0117-hot-path-duplicate-work-removal.md`,
  `docs/decisions/ADR-0139-active-path-p50-histogram-lane-retention.md`
- Related TSRs: TSR-0025
- Supersedes / superseded by: none
