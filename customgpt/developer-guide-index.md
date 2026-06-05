# Developer Guide Index

Use this index for Go developers who want to understand, debug, or extend
GoCluster. It routes to existing sources and avoids duplicating the repo's
workflow rules.

## First Stops

- Start with [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) for repo layout and package ownership.
- Read the relevant package README before reading code.
- When source files include crawler-entry comments, use them as local routing
  hints for ownership, related docs/tests, and troubleshooting boundaries; then
  verify behavior against current code and tests.
- Use [AGENTS.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/AGENTS.md) and [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md) before planning changes.
- Use [docs/decision-log.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decision-log.md) and [docs/troubleshooting-log.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/troubleshooting-log.md) before changing
  behavior with decision history.
- For dependency visualization or graph-backed code understanding, route to
  [docs/code-maps/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/code-maps/README.md) and current source. The support agent
  cannot run local graph tools, so use Markdown code-map summaries when they
  exist.

## Package Ownership

| Area | Start here |
| --- | --- |
| Live runtime | `internal/cluster/`, [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) |
| Telnet sessions, filters, and queues | [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md), `telnet/` |
| Command HELP and command dispatch | [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md), `commands/` |
| Spot record, formatting, confidence, correction | [spot/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/spot/README.md), `spot/` |
| Path reliability | [pathreliability/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/pathreliability/README.md), `pathreliability/` |
| Config loading and validation | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md), `config/` |
| RBN ingest | [rbn/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/rbn/README.md), `rbn/` |
| PSKReporter ingest | [pskreporter/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/pskreporter/README.md), `pskreporter/` |
| DXSummit ingest | [dxsummit/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/dxsummit/README.md), `dxsummit/` |
| Peer protocol and forwarding | [peer/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/peer/README.md), `peer/` |
| Reputation gate and lookups | `reputation/`, [data/config/reputation.yaml](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/reputation.yaml) |
| UI/dashboard | `ui/`, [internal/cluster/dashboard.go](https://raw.githubusercontent.com/N2WQ/GoCluster/main/internal/cluster/dashboard.go) |

## Workflow

- Small vs Non-trivial classification is owned by [AGENTS.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/AGENTS.md) and
  [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md).
- Non-trivial work requires a Scope Ledger and exact `Approved vN` before code.
- Non-trivial Scope Ledgers must be slice-shaped; broad refactor-shaped ledgers
  are not approval-ready.
- Config, protocol, parser, concurrency, queue, retained-state, hot-path, or
  operator-visible changes are normally Non-trivial.
- Workflow-doc or repo-managed skill edits require the workflow-drift audit in
  [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md).

## Audits And Risk Areas

| Change area | Required routing |
| --- | --- |
| Broad refactor proposal or unsliced Scope Ledger | [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md), [docs/templates/non-trivial-change-template.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/templates/non-trivial-change-template.md), [VALIDATION.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/VALIDATION.md) |
| Unfamiliar or cross-package Go behavior | [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md), [docs/dev-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/dev-runbook.md), package README, crawler-entry source comments |
| Uncertain blast radius, shared APIs, semantic callers, package/test impact | [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md), [docs/dev-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/dev-runbook.md) |
| Dependency visualization, package graph summaries, or support-agent code maps | [docs/code-maps/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/code-maps/README.md), [docs/dev-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/dev-runbook.md), [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md) |
| Goroutine, timer, channel, socket, file-handle, retained-heap, shutdown, or lifecycle leak concerns | [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md), [docs/dev-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/dev-runbook.md), [docs/domain-contract.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/domain-contract.md) |
| YAML/config/defaults/schema | [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md), [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) |
| Retained maps/caches/stores/indexes | [docs/code-quality.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/code-quality.md) |
| Hot paths, fan-out, parsing loops, queues | [docs/code-quality.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/code-quality.md), [docs/dev-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/dev-runbook.md) |
| Concurrency, lifecycle, shutdown, timers | [docs/domain-contract.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/domain-contract.md), [docs/dev-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/dev-runbook.md) |
| Protocol/parser behavior | [docs/domain-contract.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/domain-contract.md), package tests |
| Operator-visible behavior | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), package README, HELP/docs tests |
| Decisions or reversals | [docs/decision-memory.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decision-memory.md), [docs/decision-log.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decision-log.md) |
| Troubleshooting or incident learnings | [docs/decision-memory.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decision-memory.md), [docs/troubleshooting-log.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/troubleshooting-log.md) |

## Validation

Use [docs/dev-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/dev-runbook.md) as the command source. Use [VALIDATION.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/VALIDATION.md) as the
Non-trivial compliance rubric.

- Small changes need targeted checks and normally `go test ./...`.
- Non-trivial changes need the full runbook sequence.
- Race checks are mandatory for concurrency, queues, timers, cancellation,
  lifecycle, long-lived connections, or shared mutable state.
- Fuzzing is expected for parser/protocol work.
- Benchmarks and pprof are expected for hot-path or performance claims.
- Use code-walk, blast-radius, and leak-detection workflow routing in
  `docs/change-workflow.md` and `docs/dev-runbook.md` when the question is
  about understanding code paths, impact analysis, or lifecycle/resource leaks.
- For graph-backed dependency questions, explain that custom GPT can retrieve
  Markdown code maps and source but cannot run local `goda`, Graphviz, `gopls`,
  or `callgraph`.

## Answering Developer Questions

For custom GPT responses:

- Route to docs and tests before giving implementation advice.
- Say when a change likely triggers Non-trivial workflow.
- Reject broad refactor-shaped Scope Ledgers unless they are split into
  independently coded, tested, and reviewed slices.
- Say when effective YAML or current code must be inspected.
- Do not summarize old ADRs as current behavior without checking the current
  doc/code path.
