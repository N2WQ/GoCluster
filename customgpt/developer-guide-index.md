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
- For independent-agent, subagent, or parallel delegation questions, start with the Subagent Use
  rules in [AGENTS.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/AGENTS.md) and [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md).
- Use [docs/decision-log.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decision-log.md) and [docs/troubleshooting-log.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/troubleshooting-log.md) before changing
  behavior with decision history.
- Use [docs/agent-lessons/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/agent-lessons/README.md) only for recurring model/workflow lessons; verify any implementation claim against workflow docs, source, tests, and ADR/TSR records.
- For dependency visualization or graph-backed code understanding, route to
  [docs/code-maps/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/code-maps/README.md) and current source. The support agent
  cannot run local graph tools, so use Markdown code-map summaries when they
  exist.
- For support-agent answer quality, deployment, or evaluation work, route to
  [docs/support-agent-quality-contract.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/support-agent-quality-contract.md),
  [docs/support-agent-evals.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/support-agent-evals.md), and
  [docs/support-agent-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/support-agent-runbook.md).

## Package Ownership

| Area | Start here |
| --- | --- |
| Live runtime | `internal/cluster/`, [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md) |
| Telnet sessions, filters, and queues | [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md), `telnet/` |
| Presets, saved preferences and SSID ownership | [telnet/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/telnet/README.md#named-presets), `telnet/preset_commands.go`, `filter/presets.go`, `filter/user_preferences.go` |
| Command HELP and command dispatch | [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md), `commands/` |
| Spot record, formatting, confidence, correction | [spot/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/spot/README.md), `spot/` |
| Path reliability | [pathreliability/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/pathreliability/README.md), `pathreliability/` |
| Config loading and validation | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md), `config/` |
| RBN ingest | [rbn/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/rbn/README.md), `rbn/` |
| PSKReporter ingest | [pskreporter/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/pskreporter/README.md), `pskreporter/` |
| DXSummit ingest | [dxsummit/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/dxsummit/README.md), `dxsummit/` |
| Peer protocol and forwarding | [peer/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/peer/README.md), `peer/` |
| Reputation gate and lookups | `reputation/`, [data/config/reputation.yaml](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/reputation.yaml) |
| UI/dashboard | `ui/`, [ui/dashboard_v2.go](https://raw.githubusercontent.com/N2WQ/GoCluster/main/ui/dashboard_v2.go) |
| Build, release, profiling, and helper scripts | [scripts/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/scripts/README.md), `scripts/` |
| Public example config, reference inputs, and runtime/local data | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md), [data/h3/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/h3/README.md), `data/` |

## Workflow

- Non-mutating explanation, review, audit, diagnosis, prioritization, and
  requested recommendations use the read-only route in
  [AGENTS.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/AGENTS.md)
  and [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md).
  Findings are not approved implementation scope; a later mutation enters the
  Small or Non-trivial change gate before editing.
- Small vs Non-trivial classification is owned by [AGENTS.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/AGENTS.md) and
  [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md).
- Non-trivial changes require a bounded Scope Ledger and exact `Approved vN`
  before mutation. Only explicitly agreed items are executable.
- Decompose work when real rollback, ownership, uncertainty, or validation
  boundaries exist. A bounded coherent change may remain one slice; broad
  refactor-shaped scope is not approval-ready.
- Config, protocol, parser, concurrency, queue, retained-state, hot-path, or
  operator-visible changes are normally Non-trivial.
- Specialists and independent agents are risk-triggered, not default workflow
  stages. Their unique methods remain available for unresolved ambiguity,
  scientific/model authority, genuine design forks, unclear falsifiability,
  uncertain scope or blast radius, and High-risk or substantial Go work. The
  lead owns scope, decisions, integration, validation claims, and closeout.
- Workflow-doc or repo-managed skill edits require the workflow-drift audit in
  [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md).
- Task size controls approval rigor; touched surface controls validation.
  Workflow/guidance/skill changes use the workflow/skill-doc lane, while other
  all-Markdown documentation uses the documentation-only lane.

## Audits And Risk Areas

| Change area | Required routing |
| --- | --- |
| Broad refactor proposal or scope needing real decomposition | [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md), [docs/templates/non-trivial-change-template.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/templates/non-trivial-change-template.md), [VALIDATION.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/VALIDATION.md) |
| Risk-triggered specialist, independent-agent, subagent, or fresh-verifier use | [AGENTS.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/AGENTS.md), [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md), [docs/review-checklist.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/review-checklist.md), `.agents/skills/` |
| Requirements ambiguity, scientific/model oracle, design challenge, or test-strategy review | [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md), [docs/templates/non-trivial-change-template.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/templates/non-trivial-change-template.md), `.agents/skills/` |
| Unfamiliar or cross-package Go behavior | [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md), [docs/dev-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/dev-runbook.md), package README, crawler-entry source comments |
| Uncertain blast radius, shared APIs, semantic callers, package/test impact | [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md), [docs/dev-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/dev-runbook.md) |
| Dependency visualization, package graph summaries, or support-agent code maps | [docs/code-maps/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/code-maps/README.md), [docs/dev-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/dev-runbook.md), [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md) |
| Support-agent quality, route depth, deployment, or eval changes | [docs/support-agent-quality-contract.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/support-agent-quality-contract.md), [docs/support-agent-evals.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/support-agent-evals.md), [docs/support-agent-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/support-agent-runbook.md) |
| Goroutine, timer, channel, socket, file-handle, retained-heap, shutdown, or lifecycle leak concerns | [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md), [docs/dev-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/dev-runbook.md), [docs/domain-contract.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/domain-contract.md) |
| YAML/config/defaults/schema | [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md), [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md) |
| Retained maps/caches/stores/indexes | [docs/code-quality.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/code-quality.md) |
| Hot paths, fan-out, parsing loops, queues | [docs/code-quality.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/code-quality.md), [docs/dev-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/dev-runbook.md) |
| Concurrency, lifecycle, shutdown, timers | [docs/domain-contract.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/domain-contract.md), [docs/dev-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/dev-runbook.md) |
| Protocol/parser behavior | [docs/domain-contract.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/domain-contract.md), package tests |
| Operator-visible behavior | [README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/README.md), package README, HELP/docs tests |
| Decisions or reversals | [docs/decision-memory.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decision-memory.md), [docs/decision-log.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decision-log.md) |
| Troubleshooting or incident learnings | [docs/decision-memory.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decision-memory.md), [docs/troubleshooting-log.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/troubleshooting-log.md) |
| Recurring model or workflow lessons | [docs/agent-lessons/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/agent-lessons/README.md), [docs/change-workflow.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/change-workflow.md) |

## Validation

Use [docs/dev-runbook.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/dev-runbook.md) as the command source. Use [VALIDATION.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/VALIDATION.md) as the
Non-trivial compliance rubric.

- Validation follows touched surface and risk, not the Small or Non-trivial
  label alone. Markdown-only workflow changes do not require Go tests.
- Race checks are mandatory for concurrency, queues, timers, cancellation,
  lifecycle, long-lived connections, or shared mutable state.
- Fuzzing is expected for parser/protocol work.
- Benchmarks and pprof are expected for hot-path or performance claims.
- Use code-walk, blast-radius, and leak-detection workflow routing in
  `docs/change-workflow.md` and `docs/dev-runbook.md` when the question is
  about understanding code paths, impact analysis, or lifecycle/resource leaks.
- High-risk command-backed claims need minimal checkable evidence without
  repeating full transcripts.
- For high-risk changes, expect a fresh verifier pass and evidence-backed
  progress, validation, performance, and science/model claims.
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

## Per-Mode Minimum SNR

Route command, exception, inactive-rule and machine-schema questions to
[telnet/README.md](../telnet/README.md#minimum-snr) and
[ADR-0258](../docs/decisions/ADR-0258-per-mode-minimum-snr-filter.md).
Implementation entry points are `filter/min_snr.go`, `filter/min_snr_storage.go`,
`telnet/minsnr_commands.go`, `telnet/machine_versions.go` and
`telnet/configuration_readback.go`. Check signed map presence, exact canonical
activity, predecode/precopy bounds, frozen schemas 1/2, pinned version 2 state
migration, and detached history snapshots when reviewing changes.

## Literal Comment Rules And History

For combined BAND/MODE/COMMENT history searches, also use
[commands history](../commands/README.md#archive-history) and
[ADR-0262](../docs/decisions/ADR-0262-history-band-mode-selections.md) and the
[comma/keyword boundary refinement](../docs/decisions/ADR-0263-history-list-comma-boundaries.md).
`commands/history_selection.go` owns canonical OR lists and AND query gates;
the existing history cursor retains only bounded immutable selections. Check
saved-filter narrowing, mandatory explicit selection for self-spots, malformed
request preservation and NEXT, and reader-level COMMENT punctuation after lists.
History lists require commas; PASS/REJECT lists keep their existing separators.
For configured mode aliases named BAND/MODE, inspect first-value and post-comma
precedence and repeated-clause rejection. See
[TSR-0047](../docs/troubleshooting/TSR-0047-history-mode-alias-clause-collision.md)
and `commands/history_selection_taxonomy_test.go` for the collision regression.

Route to [telnet comments](../telnet/README.md#comment-filters-and-searches) and
[ADR-0261](../docs/decisions/ADR-0261-literal-comment-filter-and-history.md).
Entry points are `filter/comment.go`, `filter/comment_storage.go`,
`telnet/comment_commands.go`, `telnet/comment_input.go`, `commands/history.go`
and `telnet/machine_versions.go`. Check literal spacing/punctuation, scoped
ASCII ingress, case-insensitive moves, independent 32/64 limits, exact duplicate
YAML lists, schema 1-3 projections/preservation, pinned MINSNR introduction at
disk 3, detached snapshots and mandatory query matching before self exceptions.
