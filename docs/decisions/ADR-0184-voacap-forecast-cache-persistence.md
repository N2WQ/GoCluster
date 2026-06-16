# ADR-0184: VOACAP Forecast Cache Persistence

- Status: Accepted
- Date: 2026-06-16
- Decision Origin: Design

## Context

ADR-0162 intentionally made the runtime VOACAP forecast-window cache memory-only.
That kept the first implementation simple, but it meant a restart discarded
otherwise-current forecast windows. After ADR-0182 added durable runtime SSN
state, the remaining restart gap was forecast predictions: a node with a
current SSN generation could still wait through `voacap_fallback.delay_seconds`
before reusing VOACAP output that had been current before restart.

The project already uses Pebble for bounded local runtime stores. Reusing that
stack avoids a second persistence technology while keeping VOACAP predictions
separate from grid and custom-SCP data.

## Decision

Persist completed VOACAP forecast-window cache entries in a dedicated per-node
Pebble database at `voacap_fallback.forecast_cache_db_path`.

The live lookup contract remains memory-first:

- Startup opens the Pebble DB after SSN state restore and before VOACAP workers
  start.
- If `CurrentSSN(now)` is available, startup hydrates only records matching the
  current cache schema, model generation, rounded SSN generation, forecast
  month, TTL, and current UTC hour.
- Hydrated records are inserted into the existing in-memory
  `VOACAPClosedFallback` cache and behave like ordinary cache hits. A current
  restored prediction therefore bypasses `voacap_fallback.delay_seconds`.
- Worker completions update memory first, notify waiters, and then persist the
  completed forecast window to Pebble outside the fallback mutex.
- Telnet/display lookup paths do not read or write Pebble.
- Pebble records store only derived forecast-window data needed to reconstruct
  `VOACAPClosedForecast`. They do not store inflight jobs, delay windows,
  queues, counters, errors, output file paths, or elapsed runtime metadata.
- Stale, malformed, expired, overflow, or stale-generation records are pruned.
- Because predictions are derived state, Pebble open/load/write/prune failures
  are logged as warnings and the node continues with a cold memory cache.

The existing cache key semantics remain: user and DX H3 cells, band, center
frequency, forecast month/year, rounded SSN generation, and direction. A model
generation is added to the persisted value so future VOACAP deck/science changes
can invalidate disk records without changing the in-memory key.

## Alternatives considered

1. Keep the forecast cache memory-only.
   - Rejected because it preserves restart warm-up even when predictions and
     SSN state are still current.
2. Reuse the gridstore Pebble database.
   - Rejected because VOACAP predictions are disposable derived state with
     different schema, TTL, and corruption policy than grid metadata.
3. Persist delay/inflight queues as well as forecasts.
   - Rejected because those are process-lifecycle coordination state. Persisting
     them would make restart semantics less deterministic and could enqueue
     stale work.

## Consequences

### Benefits

- Current predictions survive restarts and can be used immediately after
  startup.
- The hot lookup path stays memory-only.
- The cache remains bounded by existing `cache_ttl_seconds` and
  `max_cache_entries` policy.
- Operators can remove the Pebble DB to force a cold forecast cache without
  losing SSN continuity.

### Risks

- Persisted forecasts can become scientifically stale after VOACAP deck/model
  semantics change. The persisted schema/model generation must be bumped when a
  change invalidates prior predictions.
- Pebble corruption or filesystem errors can prevent restore or persistence.
  These are warning-only because forecasts are derived and can be rebuilt.
- Startup hydration depends on a current SSN generation. If SSN state is
  unavailable, forecast restore is skipped and normal warm-up behavior applies.

### Operational impact

Production configs must include `voacap_fallback.forecast_cache_db_path` when
VOACAP fallback is enabled. The shipped config stores it under ignored
`data/voacap/`. Do not share the same forecast-cache DB between running cluster
processes.

Startup logs report how many entries were loaded and pruned. Restored current
entries count as normal VOACAP cache hits in fallback-stage counters.

## Links

- Related issues/PRs/commits: none
- Related code: `pathreliability/voacap_forecast_cache_store.go`,
  `pathreliability/voacap_fallback.go`, `internal/cluster/main_runtime.go`,
  `pathreliability/config.go`
- Related tests: `pathreliability/voacap_forecast_cache_store_test.go`,
  `pathreliability/voacap_fallback_test.go`,
  `pathreliability/config_test.go`
- Related docs: `README.md`, `docs/OPERATOR_GUIDE.md`,
  `pathreliability/README.md`, `data/config/PATH_PREDICTIONS.md`,
  `data/config/README.md`,
  `customgpt/support-cards/path-reliability.md`
- Related TSRs: none
- Supersedes / superseded by: supersedes ADR-0162's memory-only restart
  behavior; preserves ADR-0162 cache key and hourly-window semantics
