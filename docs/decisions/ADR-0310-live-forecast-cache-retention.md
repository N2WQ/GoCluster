# ADR-0310: Live Forecast Cache Retention

- Status: Proposed
- Date: 2026-10-16
- Decision Origin: Troubleshooting chat

## Context

The original live forecast and delay maps scan for a capacity victim before
assigning an entry. Refreshing an existing key at capacity can therefore evict
an unrelated key. Equal timestamps select a victim by map iteration order.
The owner selected replacement-only refresh and deterministic typed-key ties.

V1 and V2 attempted indexed retention and expiry. Both failed protected
performance guards despite passing correctness checks. Their source and
qualification evidence are preserved in the execution record and TSR-0100.
The indexed proposals were never accepted. Exact `Approved v3` authorizes
extracting only the two correctness fixes onto the original value-map design.
V3 functional validation passed, but timing qualification is invalid: two
refresh batches fell below the frozen minimum measured duration. Acceptance
is blocked; no performance regression or improvement is established by that
incomplete campaign. V2 results do not certify V3.

## Decision

An existing key replaces only its own entry, including when the map is full.
The complete incoming value replaces the prior value; delay replacement updates
both firstSeen and lastSeen. Ordinary delay touches still preserve firstSeen.

A new key at capacity evicts the oldest existing storage timestamp (forecast)
or last-seen timestamp (delay), then is admitted unconditionally. An incoming
older timestamp does not cause rejection. Equal timestamps evict the greatest
existing typed tuple, compared lexicographically:

- Forecast: `(userCell, dxCell, band, frequencyKHz, year, month, ssn, direction)`.
- Delay: `(userCell, dxCell, band, frequencyKHz, direction)`.

Numeric fields compare numerically, including 2 before 10; strings compare by
bytes. Timestamp comparison preserves Go time instant/monotonic semantics.
This live ordering is distinct from the persisted canonical-key ordering.

Keep the original value maps, configured caps, fallback mutex, delay updates,
exact-key deletion and request-driven full-map expiry. Forecast TTL remains
strict age greater than cache TTL; delay TTL remains cache TTL plus delay.
Equality remains valid. Preserve the original duration arithmetic, minute
throttle and backward-time behavior. No live heap, side table, deferred repair,
new diagnostic, worker, timer, config or persistence change is introduced.

## Alternatives considered

1. Keep the original code: retains unrelated eviction and nondeterministic ties.
2. Keep only fast indexed insertion: rejected because expiry and deletion must
   maintain the same indexes; passing insertion timings do not certify them.
3. Keep the complete V2 design: rejected by dense-expiry and transferred-work
   latency guards. No threshold waiver is authorized.
4. Extract the two policies into the original maps: selected by the owner;
   preserves simple bounded ownership and requires fresh qualification.

## Consequences

### Benefits

- Refresh cannot discard an unrelated forecast or delay.
- Tied capacity victims are repeatable without additional retained state.

### Risks

- New-key capacity insertion still scans the full map under its mutex.
- Typed comparisons may add scan cost, especially with equal timestamps.
  Paired timing controls, allocation checks and reader-blocking measurements
  must qualify the extracted patch; no production speedup is claimed.

### Operational impact

- Existing cache limits, TTL, delay clocks, snapshots and disk behavior remain.
- No migration, config adjustment, firewall change or restart is part of this
  local extraction. The indexed insertion speedup is deliberately abandoned.

## Links

- Related issues/PRs/commits: original baseline `260cc74`; owner Approved v3.
- Related code: `pathreliability/voacap_fallback.go`,
  `pathreliability/voacap_live_cache_order.go`.
- Related tests: `pathreliability/voacap_live_cache_policy_test.go` and
  common-source live-cache qualification fixtures.
- Related docs: [execution record](../voacap-live-cache-execution.md),
  `pathreliability/README.md`, `docs/OPERATOR_GUIDE.md`,
  `customgpt/support-cards/path-reliability.md`.
- Related TSRs: [TSR-0100](../troubleshooting/TSR-0100-live-cache-qualification.md).
- Supersedes / superseded by: none; preserves ADR-0303/0304 disk contracts.
