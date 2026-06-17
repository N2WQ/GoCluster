# docs/dev-runbook.md

This runbook defines the expected validation commands and when to use them.
For Non-trivial closeout, this file is the required checker source; do not rely
only on the abbreviated baseline in `AGENTS.md`.

## Principles
- Run the smallest useful check early, then broaden.
- Use incremental validation after each meaningful slice.
- Use the full suite for the selected validation lane before calling a
  Non-trivial task complete.
- Report commands and results honestly.

## Baseline environment
Expected Go toolchain:
- `go`
- `go test`
- `go vet`
- `staticcheck`
- `golangci-lint` pinned to the CI version in `.github/workflows/ci.yml`

Expected agentic workflow helpers:
- `rg`
- `gopls`
- `callgraph`
- `jq`
- `yq`
- `fd`
- `bat`

If a tool is missing locally, report that fact explicitly and treat it as a validation gap, not a silent success.

Optional investigation helpers:
- Graphviz `dot`, `goda`, and `go-callvis` for package/call graph visualization
- `semgrep` and `ast-grep` for structural search when text search is
  insufficient
- Sysinternals tools such as `handle.exe`, TCPView, and Process Explorer for
  Windows handle/socket/process leak evidence

Missing optional tools should be reported only as conditional evidence gaps for
the workflow that needed them. Their absence does not block ordinary Go
implementation, review, or validation.

## Tool-assisted investigation

### Code-walk examples
Use semantic tools when source walking needs more than text search:
- `gopls definition <file:line:col>`
- `gopls references -d <file:line:col>`
- `gopls implementation <file:line:col>`
- `gopls call_hierarchy <file:line:col>`
- `callgraph -algo rta -test ./...`
- `callgraph -algo vta -test ./...`

Report the files and commands inspected. Summarize callgraph output; do not
paste large graphs into the final response.

### Blast-radius examples
Use package and contract searches to decide scope and validation impact:
- `go list -deps -test -json ./...`
- `go list -deps -test -json ./... | jq <filter>`
- `rg <symbol-or-contract>`
- `yq <expression> <yaml-file>`
- `goda graph ./internal/cluster ./telnet | dot -Tsvg -o .\tmp\cluster-telnet.svg`
- `goda tree ./internal/cluster`

If optional graph or structural-search tools are unavailable, state which
question remains less certain and whether existing required tools were enough
for the approved scope.

When a dependency graph should help the custom GPT support agent later, capture
the durable conclusion in a small Markdown code map under `docs/code-maps/`.
Do not rely on SVG/PNG output as the support agent's only evidence.

### Leak-detection examples
Use the narrowest evidence that answers the leak question:
- targeted lifecycle tests for start/stop, reconnect, churn, slow clients, and
  shutdown
- `go test -race ./...` when concurrency/lifecycle rules require it
- `go test <pkg> -run <test> -memprofile mem.out -blockprofile block.out -mutexprofile mutex.out -trace trace.out`
- `go tool pprof -top mem.out`
- `go tool trace trace.out`
- `scripts/run-with-profiling.ps1` for local runtime CPU, heap, allocs, block,
  mutex, goroutine, trace, retained-state, and OS process captures

State the evidence level reached: static reasoning, local test/race evidence,
profile evidence, or runtime-confirmed long-running/load evidence.

## Default command set by task type

### Small change
Minimum expected sequence:
1. targeted test if available
2. `go test ./...`

Add `go vet ./...` and `staticcheck ./...` if the change touches exported/shared logic, parsing, or anything likely to affect multiple packages.

### Documentation-only Markdown change
Use this lane only when all changed files are Markdown documentation and the
change does not touch code, config, generated artifacts, scripts, CI, schemas,
protocol/runtime contracts, or checked-in data consumed by the runtime.

Minimum expected sequence:
1. targeted text checks for changed terms, cross-references, and required
   workflow strings
2. reviewer diff pass confirming the diff is documentation-only and internally
   consistent
3. `git diff --check`

Add only the documentation checks that apply:
- workflow-drift audit for workflow contract, runbook, template, rubric, review
  checklist, repo-managed skill, or workflow-script documentation changes
- support-agent routing review when operator-support topics or support-routing
  docs changed
- `scripts/check-troubleshooting-records.ps1` for troubleshooting record,
  `docs/troubleshooting/TSR-TEMPLATE.md`, or `docs/troubleshooting-log.md`
  changes
- `scripts/check-yaml-doc-rigor.ps1 -CommentOnlyCompare` only for comment-only
  checked-in first-party YAML changes; YAML content changes leave this lane

Do not run `go test ./...`, `go vet ./...`, `staticcheck ./...`,
`golangci-lint`, or `go test -race ./...` solely because Markdown changed.
If the diff expands beyond documentation-only Markdown, switch to the relevant
code, mixed, YAML/config, script, CI, generated-artifact, or runtime-contract
lane and run the required checks for that lane.

### Non-trivial change
Default full sequence for code, mixed, or runtime-contract changes:
1. targeted package test(s) during development
2. `go test ./...`
3. `go vet ./...`
4. `staticcheck ./...`
5. `golangci-lint run ./... --config=.golangci.yaml`

Also required as applicable:
- `scripts/check-yaml-doc-rigor.ps1` for checked-in first-party YAML additions
  or edits; add `-CommentOnlyCompare` when the intended YAML change is
  comment-only
- `scripts/check-go-crawler-entry-comments.ps1 -ChangedOnly -FailOnMissing`
  for additions or material changes to support-critical Go files
- for comment-only Go changes: `gofmt`, targeted package tests when packages
  were touched, `git diff --check`, and a reviewer diff pass confirming the
  non-comment Go diff is empty
- `go test -race ./...` for concurrency, queues, timers, cancellation, lifecycle, shutdown, or long-lived connections
- fuzzing for parser/protocol changes
- benchmarks for hot-path changes
- pprof for meaningful performance claims

## Targeted test examples
Use the narrowest useful targeted commands during implementation, then run the broader suite.

Examples:
- `go test ./internal/cluster -run TestSlowClientDropPolicy`
- `go test ./internal/parser -run TestRejectsMalformedControlBytes`
- `go test ./... -run TestGracefulShutdown`

## Fuzz guidance
Use fuzzing for parser/protocol work where malformed or adversarial input matters.

Examples:
- `go test ./internal/parser -fuzz=FuzzLineParser -fuzztime=10s`
- `go test ./... -fuzz=FuzzCommandDecoder -fuzztime=10s`

Rules:
- keep fuzz inputs bounded
- seed with real malformed cases when available
- report fuzz command and result

## Race guidance
Race checks are mandatory when touching:
- goroutines
- channels
- queue ownership
- timers/tickers
- connection lifecycle
- cancellation/shutdown
- shared mutable state

Command:
- `go test -race ./...`

If the repo has a narrower stable race target, it may be added in addition to, not instead of, the full run unless a temporary waiver is explicitly granted.

## Benchmark guidance
Benchmarks are expected for hot-path changes such as:
- fan-out/broadcast
- parser loops
- allocation-sensitive handlers
- queue operations
- lock-contention-sensitive paths

Examples:
- `go test ./internal/parser -bench . -benchmem`
- `go test ./internal/cluster -bench BenchmarkBroadcast -benchmem`

Report:
- ns/op
- allocs/op
- bytes/op
- before/after comparison when claiming improvement

## Profiling guidance
Use pprof when:
- benchmark numbers regress or surprise you
- lock contention is suspected
- memory growth or retention is in question
- CPU cost of a hot path changed materially

Examples:
- `go test ./internal/cluster -bench BenchmarkBroadcast -benchmem -cpuprofile cpu.out -memprofile mem.out`
- `go tool pprof -top cpu.out`
- `go tool pprof -top mem.out`

## Escape-analysis spot checks
Use when allocations or ownership are unclear:
- `go test ./... -gcflags=all=-m`

Do not dump noisy compiler output into the final summary. Summarize only relevant findings.

## Suggested execution sequence for Non-trivial work
Example cadence:
1. after milestone 1: targeted checks for the selected validation lane
2. after milestone 2: targeted checks plus the broader lane checks
3. before closeout: final lane-required checks
4. final if applicable: `go test -race ./...`, fuzz, benchmark, pprof

## Reporting format
In the final summary, list each command with:
- command
- why it was run
- result
- whether it was incremental or final

Example:
- `go test ./internal/cluster -run TestSlowClientDropPolicy` - targeted drop-policy regression - pass - incremental
- `go test ./...` - baseline regression suite - pass - final
- `go test -race ./...` - lifecycle/concurrency verification - pass - final
- `git diff --check` - documentation-only whitespace check - pass - final
