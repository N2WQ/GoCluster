# Codex Triggered Validation Tools

Open this recipe only after `docs/dev-runbook.md` or a triggered audit requires
the corresponding evidence. Requirements remain with their canonical owners;
this file supplies commands, not validation policy.

## Semantic and blast-radius investigation

- `gopls definition|references|implementation|call_hierarchy <target>`
- `callgraph -algo rta -test ./...`
- `go list -deps -test -json ./...`
- `rg <symbol-or-contract>` and `yq <expression> <yaml-file>`
- optional `goda`/Graphviz views when text and semantic search are insufficient

Report inspected targets and summarize large graphs. Put durable support-agent
conclusions in Markdown code maps, not generated images alone.

## Lifecycle and leak evidence

Use the narrowest applicable targeted lifecycle tests, race checks, profiles,
traces, or `scripts/run-with-profiling.ps1`. State whether evidence is static,
test/race, profile, or runtime-confirmed and preserve the required short command
excerpt.

## Triggered command families

- Fuzz parser/protocol inputs with bounded seeds and a useful oracle.
- Run `go test -race ./...` for concurrency, shared state, lifecycle,
  cancellation, timers, queues, or shutdown.
- Use `-benchmem` for hot-path claims and report ns/op, allocs/op, bytes/op,
  and before/after results.
- Use pprof for surprising CPU, allocation, contention, or retention results.
- Use `go test ./... -gcflags=all=-m` only for unclear allocation/ownership.

Missing optional tools are conditional gaps only when the triggered question
needs them.
