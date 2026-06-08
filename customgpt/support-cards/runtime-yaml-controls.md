# Support Card: Runtime YAML Controls

## Match

Use when a node operator asks where to set memory, garbage collection, scheduler
parallelism, telnet listener settings, default filters, or other runtime-owned
operator policy.

## First Safe Check

Read the active `runtime.yaml` from the effective config directory selected by
`DXC_CONFIG_PATH`, then compare it with the checked-in example only as a schema
and comment reference.

## Must Include

- Runtime controls are YAML-owned in `runtime.yaml`.
- The Go runtime keys are `go_runtime.memory_limit_mib`,
  `go_runtime.gc_percent`, and `go_runtime.max_procs`.
- `0` leaves the Go runtime or environment-provided value unchanged for the
  relevant Go runtime control.
- The effective YAML controls the running node.

## Must Avoid

- Do not recommend hard-coded launcher defaults as the source of truth.
- Do not claim hidden runtime defaults fill missing required YAML-owned fields.

## Sources

- `customgpt/source-map.md`
- `data/config/README.md`
- `data/config/runtime.yaml`
