# ADR-0191: VOACAP Cache Overview Counters

- Status: Accepted
- Date: 2026-06-17
- Decision Origin: Scope Ledger v2

## Context

The interactive Overview page already shows H3 path-pair counts and the current
rounded VOACAP SSN generation. Operators also need a quick way to distinguish a
cold VOACAP fallback cache from delayed, inflight, or queued work without
waiting for propagation-log interval summaries or issuing a path-specific
diagnostic command.

`VOACAPClosedFallback.Snapshot()` already exposes the relevant in-memory state:
cache entries, delay entries, inflight entries, and queue depth.

## Decision

Show the existing fallback snapshot counters in the Overview Path Predictions
panel before the H3 path-pair line:

```text
VOACAP cache: <cache> (C) / <delay> (D) / <inflight> (I) / <queue> (Q)
H3 path pairs: <fine> (L2) / <coarse> (L1)
```

If the fallback snapshot provider is absent, display `VOACAP cache: n/a`.

Do not derive a new cached VOACAP path-pair count from cache keys. Do not add
new retained state, cache indexes, queue state, Pebble records, cache keys, or
prediction behavior.

## Alternatives considered

1. Show `VOACAP <nnnn>` as a cached path-pair count.
   - Rejected because cache entries can vary by band, frequency, month, SSN
     generation, and direction. Collapsing them to "path pairs" would add a new
     interpretation that operators could confuse with H3 path-pair counts.
2. Show only cache entries.
   - Rejected because delayed, inflight, and queued work explain common warm-up
     states that cache-entry count alone cannot distinguish.
3. Add a new retained side counter for distinct VOACAP path pairs.
   - Rejected because existing snapshot counters provide the operational
     visibility without introducing additional retained state or eviction
     coupling.

## Consequences

### Benefits

- Operators can see whether VOACAP fallback is cold, warming, active, or queued
  directly from the Overview page.
- The display reuses existing bounded snapshot counters.
- The H3 path-pair line remains separate and keeps its L2/L1 meaning.

### Risks

- The Path Predictions panel gains one row.
- `CacheEntries` is cache-entry cardinality, not distinct H3 path pairs or
  displayed `SHOW PROP` rows.

### Operational impact

No VOACAP prediction, fallback, queue, cache, TTL, persistence, YAML, telnet,
propagation-log, SSN, or path-glyph behavior changes. This is an interactive
console observability change only.

## Links

- Related issues/PRs/commits: none
- Related code: `internal/cluster/bootstrap.go`,
  `internal/cluster/main_runtime.go`, `pathreliability/voacap_fallback.go`
- Related tests: `internal/cluster/main_stats_test.go`,
  `ui/dashboard_v2_test.go`
- Related docs: `README.md`, `docs/OPERATOR_GUIDE.md`,
  `pathreliability/README.md`,
  `customgpt/support-cards/path-reliability.md`
- Related TSRs: none
- Supersedes / superseded by: extends ADR-0190 operator diagnostics visibility
