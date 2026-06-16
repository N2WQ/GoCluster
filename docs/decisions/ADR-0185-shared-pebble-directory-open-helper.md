# ADR-0185: Shared Pebble Directory Open Helper

- Status: Accepted
- Date: 2026-06-16
- Decision Origin: Design

## Context

ADR-0184 added a dedicated Pebble store for VOACAP forecast-cache persistence.
That first implementation repeated the same basic path hygiene already present
in the archive Pebble store: trim the configured path, reject empty or
non-directory paths, create the directory, and then open Pebble.

The project also has Pebble stores with stronger local ownership boundaries:
gridstore and custom SCP construct tuned Pebble options, own cache lifetimes,
and run additional load or writer-loop setup. A generic open abstraction that
absorbed those policies would hide important resource ownership and recovery
semantics.

## Decision

Add `internal/pebbleutil` as a narrow helper for shared Pebble directory
preparation and basic open forwarding.

The helper is intentionally below store policy level:

- `PrepareDir` trims the path, rejects an empty path, rejects an existing
  non-directory path, and creates the directory.
- `Open` forwards to `pebble.Open` and only defaults nil options to an empty
  `pebble.Options`.
- Callers retain ownership of Pebble option resources, including caches.
- Callers retain component-specific error wording, corruption recovery,
  checkpoint restore, deletion policy, logs, write options, and runtime
  lifecycle behavior.

Only the VOACAP forecast cache and archive DB use this helper in this slice.
Gridstore and custom SCP remain direct owners of their Pebble setup.

## Alternatives considered

1. Keep VOACAP and archive fully duplicated.
   - Rejected because two simple Pebble stores would continue to duplicate path
     hygiene and open forwarding after the project deliberately reused Pebble
     for VOACAP persistence.
2. Add the helper for VOACAP only.
   - Rejected because a single-use abstraction would add indirection without
     real reuse.
3. Refactor all Pebble stores through one generic opener.
   - Rejected because gridstore and custom SCP have tuned option and cache
     ownership that should remain explicit at their call sites.
4. Reuse `internal/pebbleresilience`.
   - Rejected because that package owns checkpoint/list/restore mechanics for
     stores that need recovery; VOACAP forecasts and archive auto-delete policy
     have different recovery contracts.

## Consequences

### Benefits

- VOACAP and archive share the same low-level directory preparation mechanics.
- Archive keeps its existing `AutoDeleteCorruptDB` behavior and operator logs.
- The helper has a clear boundary that prevents cache ownership or corruption
  policy from becoming implicit.

### Risks

- Future callers could try to push recovery policy or tuned option construction
  into `internal/pebbleutil`. That should require a separate decision because it
  changes ownership boundaries.
- Error wrapping must remain caller-owned when operator-facing wording matters.

### Operational impact

No intended runtime behavior change. Existing archive recovery behavior,
VOACAP forecast-cache cold-start fallback behavior, and Pebble corruption
policies are preserved.

## Links

- Related issues/PRs/commits: none
- Related code: `internal/pebbleutil/open.go`,
  `pathreliability/voacap_forecast_cache_store.go`, `archive/archive.go`
- Related tests: `internal/pebbleutil/open_test.go`,
  `pathreliability/voacap_forecast_cache_store_test.go`,
  `archive/archive_corruption_test.go`
- Related ADRs: ADR-0184, ADR-0151
- Related TSRs: none
- Supersedes / superseded by: none
