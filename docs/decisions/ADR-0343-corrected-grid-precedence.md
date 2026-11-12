# ADR-0343: Corrected Grid Precedence and Missing-Grid Output

- Status: Accepted
- Date: 2026-11-11
- Decision Origin: Troubleshooting chat; exact Approved v5

## Context

The v2 live repair retained an attached grid after equal/missing replacement
lookups. The owner subsequently required reported replacement grids to take
precedence over CTY estimates, a completed lookup before CTY use, and clearing
the old identity's location when the replacement cannot be located. Delayed
release must remain nonblocking with respect to worker/storage responses.
Unknown location also needs an output-admission measure.

## Decision

For an accepted literal call correction, both actual output consumers use:

| Replacement lookup | Final DX grid / provenance |
| --- | --- |
| Successful reported (`derived=false`) | Returned full locator, reported |
| Successful derived (`derived=true`) | Recompute with `cty.Grid4FromPrefixInfo` from the correction's captured CTY image, derived |
| Derived positive but unusable CTY | Empty, not derived |
| Unsuccessful/pending/timed-out/disabled/unavailable | Empty, not derived; no direct CTY fallback |

Equal text does not preserve the old origin flag. Invalidate/rebuild normalized
grid, prefix and fine/coarse cell metadata coherently, preserving DE facts.
Uncorrected nonempty grids and ordinary empty-grid fill keep their existing rules.
Classification and derived location share one immutable CTY image per correction,
including delayed release. CTY matching, conversion and geographic science do
not change; this decision selects authority, not station-position accuracy.

Reuse existing lookup hooks and owned worker. Delayed release uses cache-first
asynchronous lookup only, without disk/reply waits. Main uses the existing bounded
synchronous lookup plus existing asynchronous fallback. No new worker, timer,
queue, negative cache or per-call state is introduced. Successful cache hits count
as completed lookups, without a mandatory fresh disk read. Preserve existing cache
authority/freshness limits, including the known late-derived-hydration window
after cache clearing/eviction that can temporarily mask a persisted reported row.
No legacy data migration or global cache repair is authorized here.

Corrected fanout does not write DX grids. Lookup owns persistence; recomputed
output cannot overwrite a stored estimate or promote its provenance. DE writeback
remains active. Existing stored provenance is reported-versus-derived, not an
individual feed identity. Earlier path learning is not moved or repeated.

Add one fixed atomic `CorrectedOutputWithoutGrid` total. Count a locally accepted
literal correction with empty final DX grid after the final age guard and sealing,
when any ordinary lane is allowed, before family/destination processing. Count
once, regardless of destination count, later filtering, family hiding or queue
refusal. Exclude unchanged/B output, pending/suppressed/expired work and complete
ordinary thinning/SELF-only output. This is output admission, not client receipt.
Reset with statistics reset/restart, without a daily timer. Show an unsigned-safe
`Corrected output: no grid=…` row in headless stats and the three console Pipeline
Quality panes; their default height becomes 14 rows including borders.

## Alternatives considered

1. Keep the old grid on incomplete lookup: replaced by explicit owner selection;
   location belongs to the original identity and can mislead downstream filters.
2. Direct CTY fallback on a cold cache or timeout: rejected. A reported store row
   might still exist unread. CTY use requires a successful derived result first.
3. Wait synchronously on delayed release: rejected. The single release consumer
   must not stall behind a worker response; use unknown location for that output.
4. Accept stored derived coordinates: rejected. Only reported values outrank a
   recomputation from the correction's CTY image; equal-text flags still matter.
5. Add feed-specific provenance, fresh disk lookup every time, or cache/DB repair:
   outside this bounded amendment and existing storage authority.

## Consequences

### Benefits

Replacement location follows one source-selection contract on both paths. It
cannot inherit the busted call's grid, and reported precision survives. A fixed
counter measures how often missing location reaches ordinary output admission.

### Risks

A delayed cold-cache correction can leave with unknown grid even when a reported
row exists on disk. A main timeout can do the same. Late backfill cannot change
already sealed output. Missing DX location cannot match positive DX-grid filters
and supplies no DX path prediction; it does not drop all spots or prevent NEARBY
from using DE. Cache freshness and historical provenance remain limited as above.

### Operational impact

Monitor counter increases over known intervals, alongside age/retention counters.
These checks do not establish deployment, production missing-grid rate, station
position/RF accuracy, or production tail latency. No config/schema/Spot storage
changes are required; the only added retained runtime state is one atomic total.
Confidence, percentages, budgets, age/learning rules and replay remain unchanged.

## Links

- [TSR-0127](../troubleshooting/TSR-0127-stabilizer-live-output.md)
- [Live output contract](../../spot/README.md#live-stabilizer-output),
  [metric boundary](../../ui/README.md#stabilizer-matrix)
- [Approved scope and validation](../stabilizer-grid-repair-v5.md)
- Tests: `internal/cluster/corrected_grid_delivery_test.go`,
  `corrected_grid_counter_test.go`, `stabilizer_matrix_test.go`,
  `stats/corrected_grid_test.go`, `ui/stabilizer_matrix_test.go`.
- Supersedes the grid-retention/provenance portion of
  [ADR-0341](ADR-0341-stabilizer-live-output.md); its remaining decisions continue.
- Uses unchanged [ADR-0340 CTY authority](ADR-0340-cty-grid-authority.md).
- Superseded by ADR-0345 for the late-hydration limitation only.
- Follow-up: [ADR-0345](ADR-0345-grid-active-work-authority.md) replaces the known
  late-hydration window with bounded active-work authority. The v5 correction
  selection, delayed-release, output accounting and persistence rules remain.
