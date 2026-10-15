# ADR-0303: Deterministic Persisted Forecast Retention

- Status: Accepted
- Date: 2026-10-14
- Decision Origin: Design

## Context

ADR-0184 persists completed VOACAP forecast windows for restart reuse. Capacity
pruning sorted records by storage timestamp without ordering ties. Equal-time
entries therefore had no explicit, reproducible retention priority.

## Decision

Order eligible persisted records by descending storage timestamp, then ascending
canonical database key bytes. Retain the first `max_cache_entries` records. Apply
this ordering to startup hydration and worker-completion pruning. An equal-time
refresh has no recency advantage; a strictly newer timestamp does.

Preserve live-memory eviction, cache keys, disk schema/model generation, TTL,
forecast eligibility, memory-first publication and warning-only persistence
failure. No persisted format migration is required.

## Alternatives considered

1. Keep unspecified ties: rejected because it cannot provide repeatable retained
   sets across pruning and restart.
2. Prefer last write: rejected because an identical storage timestamp does not
   establish greater recency and would require additional ordering state.
3. Canonical key order: selected by the owner in the approved first-wave ledger.

## Consequences

### Benefits

- Exact retained sets can be independently asserted before restart repair.
- Write order, refresh order and restart do not change equal-time priority.

### Risks

- A lower key wins a tie; this does not mean its forecast is more accurate.

### Operational impact

- No setting, capacity, firewall rule or deployment procedure changes.
- Existing cache files remain readable. Capacity pruning may choose different
  survivors where the previous implementation left ties unspecified.

## Links

- Related code: `pathreliability/voacap_forecast_cache_store.go`
- Related tests: `pathreliability/voacap_retention_test.go`,
  `pathreliability/voacap_fallback_test.go`
- Related docs: `pathreliability/README.md`, `docs/OPERATOR_GUIDE.md`,
  `data/config/PATH_PREDICTIONS.md`,
  `customgpt/support-cards/path-reliability.md`
- Supersedes / superseded by: refines ADR-0184's persisted capacity ordering;
  preserves its persistence and live-lookup contracts.
