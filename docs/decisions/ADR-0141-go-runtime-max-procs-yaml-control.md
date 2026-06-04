# ADR-0141: Go Runtime Max Procs YAML Control

- Status: Accepted
- Date: 2026-06-04
- Decision Origin: Design

## Context
The production node runs as a direct Windows binary on a small virtual host.
Wrapper scripts can set `GOMAXPROCS`, but normal service-style deployment should
not require a profiling launcher or shell environment to bound Go scheduler
parallelism. ADR-0074 already made process-wide memory and GC runtime tuning
YAML-owned for the same deployment reason.

Recent CPU-pressure investigation also showed that scheduler parallelism needs
to be auditable in the same checked-in/operator config surface as
`GOMEMLIMIT` and `GOGC`. This control must not change p50 calculations,
retained state, queue behavior, or allocation paths.

## Decision
Add required `go_runtime.max_procs` to the merged `runtime.yaml` config.

The setting maps to Go's `GOMAXPROCS` runtime control:

- `0` leaves the Go runtime or environment-provided value unchanged.
- Positive values are applied once during startup with `runtime.GOMAXPROCS`.
- Negative values fail config validation.

The shipped public config uses `max_procs: 0` so existing deployments keep their
current Go/env/default scheduler behavior unless an operator chooses a positive
value in a private config directory.

## Alternatives considered
1. Keep using environment variables or wrapper scripts.
   - Rejected because direct-binary Windows deployment should have the same
     auditable runtime controls without a launcher dependency.
2. Hard-code `GOMAXPROCS=2`.
   - Rejected because it would silently change behavior for every deployment
     and would be wrong for hosts with different CPU allocations.
3. Clamp configured values to `runtime.NumCPU()`.
   - Rejected because clamping hides operator intent and makes config behavior
     host-dependent. Operators can deliberately oversubscribe or constrain.

## Consequences
### Benefits
- Operators can bound scheduler parallelism through the same YAML surface as
  memory and GC tuning.
- The `0` sentinel preserves compatibility with Go defaults and environment
  variables.
- Startup logs and config summaries expose the configured scheduler control.

### Risks
- A value above the host's available vCPU count can oversubscribe CPU.
- A value below available vCPU count can reduce throughput under bursts.
- The setting affects the whole process, not one subsystem.

### Operational impact
- On a 2-vCPU Windows virtual host, operators can set
  `go_runtime.max_procs: 2` in private runtime config.
- Public checked-in config remains unchanged at runtime because it uses `0`.
- Existing alternate config directories must add the required key under
  `go_runtime` because the loader enforces required YAML-owned settings.

## Links
- Related issues/PRs/commits:
- Related tests:
  - `config/go_runtime_config_test.go`
  - `internal/cluster/go_runtime_tuning_test.go`
- Related docs:
  - `data/config/runtime.yaml`
  - `data/config/README.md`
  - `docs/OPERATOR_GUIDE.md`
  - `README.md`
- Related TSRs:
  - `docs/troubleshooting/TSR-0025-p50-merge-cpu-and-heap-pressure.md`
- Supersedes / superseded by:
  - Related: ADR-0074
