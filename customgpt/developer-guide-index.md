---
title: Developer Guide Index
description: Source and contract entry points for GoCluster development and support.
---

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
| GO Boolean rules, native CC banks and immutable matching | [current contracts and owners](#boolean-personal-rules), [CC manual](../docs/cc-dialect.md), [normative domain contract](../docs/domain-contract.md#boolean-personal-rules) |
| Generation-seven profiles, tagged presets and SSID ownership | [current local owners](#boolean-personal-rules), [presets](../telnet/README.md#named-presets), [operator storage](../data/config/README.md#user-configuration-storage), [ADR-0330](../docs/decisions/ADR-0330-native-cc-banks-and-generation-seven.md) |
| Command HELP and command dispatch | [commands/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/commands/README.md), `commands/` |
| WWV/WCY history, login and five-report SHOW/slash commands | [bulletin history](../telnet/README.md#wwv-and-wcy-bulletin-history), [ADR-0323](../docs/decisions/ADR-0323-wwv-wcy-bulletin-history.md), `commands/bulletin_command.go` |
| Spot record, formatting, confidence, correction | [spot/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/spot/README.md), `spot/` |
| Path reliability | [pathreliability/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/pathreliability/README.md), `pathreliability/` |
| Config loading and validation | [data/config/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/README.md), `config/` |
| RBN ingest | [rbn/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/rbn/README.md), `rbn/` |
| PSKReporter ingest | [pskreporter/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/pskreporter/README.md), `pskreporter/` |
| DXSummit ingest | [dxsummit/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/dxsummit/README.md), `dxsummit/` |
| Peer protocol and forwarding | [peer/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/peer/README.md), `peer/` |
| Reputation gate, shared station allowance and lookups | `reputation/`, [data/config/reputation.yaml](https://raw.githubusercontent.com/N2WQ/GoCluster/main/data/config/reputation.yaml), [ADR-0271](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decisions/ADR-0271-shared-station-reputation.md) |
| UI/dashboard | `ui/`, [ui/dashboard_v2.go](https://raw.githubusercontent.com/N2WQ/GoCluster/main/ui/dashboard_v2.go) |
| Stabilizer transition matrix and terminal accounting | [ui matrix guide](https://raw.githubusercontent.com/N2WQ/GoCluster/main/ui/README.md#stabilizer-matrix), `stats/stabilizer_transitions.go`, `internal/cluster/stabilizer_matrix.go`, `internal/cluster/output_pipeline_delivery.go` |
| Build, release, local profiling controller and helper scripts | [scripts/README.md](https://raw.githubusercontent.com/N2WQ/GoCluster/main/scripts/README.md), `scripts/` |
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
| Test ownership, required execution, command/nested-module lanes and qualification identity | [testing architecture](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/runbooks/testing-architecture.md), [ADR-0299](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decisions/ADR-0299-test-ownership-and-current-source-qualification.md) |
| Ordinary fixture relocation, actual runner identity and Windows native process containment | [ADR-0314](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/decisions/ADR-0314-ordinary-fixture-paths-and-native-test-ownership.md), [TSR-0103](https://raw.githubusercontent.com/N2WQ/GoCluster/main/docs/troubleshooting/TSR-0103-checkout-paths-and-native-test-containment.md), [Windows test guide](https://raw.githubusercontent.com/N2WQ/GoCluster/main/scripts/README.md#windows-tests-with-stable-firewall-paths) |
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

## Boolean Personal Rules

GO owns the [Boolean rule contract](../telnet/README.md#filters) and
[normative field semantics](../docs/domain-contract.md#boolean-personal-rules).
CC owns a separate native bank; start with [Using CC](../docs/cc-dialect.md) and
[ADR-0330](../docs/decisions/ADR-0330-native-cc-banks-and-generation-seven.md).
Each bank has six independent controls. Selection activates the saved bank;
GRID, noise, dedupe, PATHSAMPLES and SOLAR remain shared.

Both banks start from effective operator mode/source defaults. Recognized
pre-seven login resets old filters, six switches and associations; old shared
presets, including v6, are cleared once. New v7 presets survive later SSID
upgrades and notice retries. Existing current-generation empty GO rules remain empty.
GO source-less PASS admits matching HUMAN and SKIMMER; explicit SOURCE guards
retain ordinary Boolean logic. Preserve exact GO expressions in readback rather
than flattening AND/OR/NOT into field summaries. CC readback shows native state.

These are current **local checkout** owners, not publication, deployment or
completed-validation claims.

| Concern | Local source owners |
| --- | --- |
| GO syntax, preparation, field compilation/evaluation and normalization | `filter/expr/parse.go`, `filter/rule_set.go`, `filter/rule_compile.go`, `filter/rule_evaluate.go`, `filter/rule_view.go` |
| Native CC canonical state, edits, frequency table and immutable matching | `filter/cc_configuration.go`, `filter/cc_prepare.go`, `filter/cc_edit.go`, `filter/cc_bands.go`, `filter/cc_match.go`, `filter/native_cc_runtime.go` |
| Both-bank defaults, capture and preparation | `filter/rule_defaults.go`, `filter/profile_v7.go`, `filter/runtime_v7.go`; activation admission in `telnet/configuration_v7_startup.go` |
| Coherent live/history program, controls and location snapshots | `filter/match_snapshot.go`, `telnet/matching_snapshot.go`, `telnet/history_filter.go`, `telnet/history_v6.go` |
| Human staging, native commands and bounded readback | `telnet/human_preference_stage.go`, `telnet/configuration_v7_human.go`, `telnet/boolean_rule_commands.go`, `telnet/native_cc_commands.go`, `telnet/native_cc_commands_parse.go`, `telnet/native_cc_readback.go`, `telnet/human_reserved_readback.go` |
| GO canonical DXCC input and one request-owned CTY snapshot | `telnet/boolean_rule_commands.go` (`newRuleDXCCResolver`), `filter/rule_compile.go`, `cty/dxcc.go`; [ADR-0321](../docs/decisions/ADR-0321-boolean-canonical-dxcc-input.md) |
| GO rule HELP and native CC HELP | `commands/boolean_rule_help.go`, `commands/boolean_rule_catalog.go`, `commands/cc_filter_help.go`, `commands/configuration_help.go`, `commands/processor.go` |
| Schema-seven headers, proposals, transactions and exports | `telnet/machine_v7_input.go`, `telnet/machine_v7_schema*.go`, `telnet/machine_v7_commands.go`, `telnet/machine_v7_readback.go`, `telnet/configuration_v7.go`, `telnet/configuration_v7_admission.go` |
| Limited schema-six compatibility over authoritative banks | `telnet/machine_v6_banked_commands.go`, `telnet/machine_v6_banked_readback.go`; frozen vocabulary/codecs in `telnet/machine_v6_*.go` and `filter/storage_v6*.go` |
| Generation-seven profiles, shared tagged presets and independent references | `filter/storage_v7.go`, `filter/storage_v7_decode.go`, `filter/preset_v7.go`, `filter/preset_v7_io.go`, `telnet/preset_commands_v7.go` |
| Reset backups, partial results, strict metadata-only notice acknowledgement | `filter/upgrade_v7.go`, `filter/upgrade_v7_decode.go`, `telnet/configuration_session_v7.go`, `telnet/reset_notice_v7.go` |
| Process reservations, queue ownership and writer cleanup | `telnet/configuration_transfer.go`, `telnet/bulletin_history.go`, `telnet/server.go` |

Review [schema ownership](../docs/cc-dialect.md#yaml-and-backups) before changing
protocols. Full schema-seven CONFIG restores both banks; ordinary edits own only
the active bank and cannot also select a dialect. Schema six is limited GO and
shared-settings compatibility, not a complete backup. One preference revision
covers both banks and shared selections; notice/login metadata does not advance
it or history.

Review [storage/recovery](../data/config/README.md#user-configuration-storage)
for strict generation-seven records, both baselines, original protection,
independent reset completion, byte/work bounds and atomicity. The one-bank
`UserRecord`/`SavedPreset` facades remain frozen at v6; current banked production
paths use explicit v7 DTOs. Do not project current CC through a Boolean carrier.

GO NEARBY uses the [recovery contract](../docs/domain-contract.md#nearby-predicate-and-recovery):
rule-only human/YAML edits waive only unchanged GRID mapping availability with
every other setting/control unchanged. Syntax and other checks remain. Missing
proximity is false; NOT includes unknown proximity, and other OR branches can
qualify. NEARBY does not suppress geography or choose dedupe. GRID/cells belong
to the client, not shared programs. CC has no NEARBY category.

## Per-Mode Minimum SNR

This retained heading routes former MINSNR questions to GO
[Boolean SNR expressions](../telnet/README.md#minimum-snr) and the
[current engine owners](#boolean-personal-rules). `HasReport` makes SNR known,
including human and zero reports; a SOURCE guard is explicit policy, not an
implicit exemption. Missing comparisons are false, including `<>`; NOT negates
that result. [ADR-0258](../docs/decisions/ADR-0258-per-mode-minimum-snr-filter.md)
records historical pre-v5 map/schema mechanics, not an active compatibility path.

## Literal Comment Rules And History

For combined BAND/MODE/COMMENT history searches, also use
[commands history](../commands/README.md#archive-history) and
[ADR-0262](../docs/decisions/ADR-0262-history-band-mode-selections.md) and the
[comma/keyword boundary refinement](../docs/decisions/ADR-0263-history-list-comma-boundaries.md).
`commands/history_selection.go` owns canonical OR lists and AND query gates;
the existing history cursor retains only bounded immutable selections. Check
saved-filter narrowing, mandatory explicit selection for self-spots, malformed
request preservation and NEXT, and reader-level COMMENT punctuation after lists.
Explicit history lists require commas; numbered-rule value lists use the
Boolean expression grammar in `HELP RULE`, not category-command separators.
For configured mode aliases named BAND/MODE, inspect first-value and post-comma
precedence and repeated-clause rejection. See
[TSR-0047](../docs/troubleshooting/TSR-0047-history-mode-alias-clause-collision.md)
and `commands/history_selection_taxonomy_test.go` for the collision regression.

Route GO saved COMMENT expressions to
[telnet comments](../telnet/README.md#comment-filters-and-searches) and the
[Boolean owners](#boolean-personal-rules), not phrase-list handlers.
Check preserved literal bytes, doubled expression quotes/literal backslashes
versus Go-quoted display chunks, unknown/NOT semantics and rule budgets.
GO expressions remain rules in both schema interfaces and the generation-seven
GO bank, not saved COMMENT maps/lists. Native CC has no saved COMMENT expression
category.
`commands/history.go` and `commands/history_selection.go` retain independent
query grammar; `telnet/history.go` owns continuation/publication,
`telnet/history_filter.go` and `telnet/history_v6.go` own captured active-bank
matching/invalidation. Explicit query clauses apply before saved self exceptions
and counting. [ADR-0261](../docs/decisions/ADR-0261-literal-comment-filter-and-history.md)
is historical for pre-v5 saved lists; its separate archive phrase grammar remains.
