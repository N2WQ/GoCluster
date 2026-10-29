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
| Boolean personal-rule engine and immutable matching | [Boolean contracts and local owners](#boolean-personal-rules), [telnet filters](../telnet/README.md#filters), [normative domain contract](../docs/domain-contract.md#boolean-personal-rules) |
| Presets, native-v6 preferences and SSID ownership | [telnet presets](../telnet/README.md#named-presets), [operator storage](../data/config/README.md#user-configuration-storage); local checkout: `filter/configuration_v6.go`, `filter/storage_v6.go`, `filter/storage_v6_decode.go`, `filter/storage_v6_io_adapter.go`, `filter/preset_v6.go`; coordination remains in `telnet/preset_commands.go`, `filter/presets.go`, `filter/user_preferences.go` |
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

Start with the [telnet rule contract](../telnet/README.md#filters),
[HELP ownership](../commands/README.md#help-source-of-truth),
[normative domain contract](../docs/domain-contract.md#boolean-personal-rules) and
[accepted test-only ADR-0317](../docs/decisions/ADR-0317-boolean-filter-replacement-mvp.md).
They describe one prepared Boolean engine, not an additional category gate.
New records receive mode/source defaults in editable PASS slot 0. A one-time
recognized old-profile login upgrade installs the same configured mode/source defaults,
preserving settings and discarding old filters/presets without translating
their logic. Reset/existing v6 empty records never reinstall defaults.
Pure v6 codecs and machine schemas remain strict; see
[ADR-0318](../docs/decisions/ADR-0318-old-profile-reset-upgrade.md) and
[ADR-0319](../docs/decisions/ADR-0319-boolean-onboarding-and-source-default.md).
A PASS without SOURCE allows HUMAN and SKIMMER. SOURCE comparisons use normal
Boolean semantics. Explicit SOURCE=HUMAN stays human-only, including saved
defaults. Shipped defaults now select both sources; saved expressions and
presets are not rewritten. Login/readbacks preserve exact expressions,
never independent field summaries that lose Boolean relationships.

These are verified **local checkout** owners, not new raw-main publication
claims. Private-cluster acceptance remains pending.

| Concern | Local source owners |
| --- | --- |
| Syntax, preparation, field compilation/evaluation and per-spot normalization | `filter/expr/parse.go`, `filter/rule_set.go`, `filter/rule_compile.go`, `filter/rule_evaluate.go`, `filter/rule_view.go` |
| New-user default construction and startup publication | `filter/rule_defaults.go`, `filter/match_snapshot.go` (`ConfigureDefaultRules`) |
| Ordinary NEARBY predicate, evaluation-local cache and single-program counts | `filter/rule_view.go`, `filter/rule_set_readback.go`, [filter package](../filter/README.md) |
| Immutable matching programs and coherent controls/cells/path captures | `filter/match_snapshot.go`, `telnet/matching_snapshot.go`; `telnet/history_filter.go` and `telnet/history_v6.go` for history snapshots/invalidation |
| Human ingress, detached admission and bounded display | `telnet/boolean_input.go`, `telnet/boolean_rule_commands.go`, `telnet/boolean_rule_readback.go`, `telnet/configuration_render.go`, `telnet/human_preference_stage.go` |
| Canonical DXCC input only, one lazy CTY snapshot per human/YAML request, numeric readback/storage | `telnet/boolean_rule_commands.go` (`newRuleDXCCResolver`), `filter/rule_set.go` (`WithRuleDXCCResolver`), `filter/rule_compile.go`, `cty/dxcc.go`; [ADR-0321](../docs/decisions/ADR-0321-boolean-canonical-dxcc-input.md) |
| Registered GO/CC expression HELP | `commands/boolean_rule_help.go` (`BooleanRuleHelpLines`), `commands/boolean_rule_catalog.go` (`installBooleanRuleHelp`), installed in `commands/processor.go`; configuration topics remain in `commands/configuration_help.go` |
| Machine headers, strict proposals/rules, transactions and readback | `telnet/machine_v6_input.go`, `telnet/machine_v6_schema.go`, `telnet/machine_v6_schema_rules.go`, `telnet/machine_v6_commands.go`, `telnet/machine_v6_readback.go`, `telnet/machine_v6_readback_capabilities.go` |
| Native configuration, record preparation/decoding, runtime adapters and presets | `filter/configuration_v6.go`, `filter/storage_v6.go`, `filter/storage_v6_decode.go`, `filter/storage_v6_io_adapter.go`, `filter/preset_v6.go` |

Review [machine schema 6](../telnet/README.md#client-yaml-configuration) for
explicit SCHEMA 6 on every GET, body schema_version 6, complete PUT/VALIDATE,
whole-list PATCH, runtime settings admission, revisions and publication fences.
Review [native storage](../data/config/README.md#user-configuration-storage)
for explicit versions on records/entries/baselines, protection, byte/work bounds
and atomicity, not frozen old-schema preservation checklists.
For NEARBY use the [recovery contract](../docs/domain-contract.md#nearby-predicate-and-recovery):
rule-only human/YAML edits need no usable location. YAML waives only mapping
availability for an unchanged GRID when a rules list is supplied and every other
setting/control is unchanged. Exact syntax and other settings checks remain.
NEARBY is false for unavailable proximity; NOT NEARBY includes unknown proximity,
and other OR branches can qualify. It does not suppress geography or choose a
dedupe lane. The client owns GRID/cells; shared programs contain no user state.
The joined fresh-child GRID fixture covers PATCH, PUT and VALIDATE against no
mappings. [ADR-0329](../docs/decisions/ADR-0329-nearby-boolean-predicate-and-generation-six.md)
replaces the affected toggle/schema/location clauses of ADR-0317 through ADR-0320.

## Per-Mode Minimum SNR

This retained heading routes former MINSNR questions to
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

Route saved COMMENT expressions to
[telnet comments](../telnet/README.md#comment-filters-and-searches) and the
[Boolean owners](#boolean-personal-rules), not phrase-list handlers.
Check preserved literal bytes, doubled expression quotes/literal backslashes
versus Go-quoted display chunks, unknown/NOT semantics and rule budgets.
Machine/native v6 carry expressions in rules, not saved COMMENT maps/lists.
`commands/history.go` and `commands/history_selection.go` retain independent
query grammar; `telnet/history.go` owns continuation/publication,
`telnet/history_filter.go` and `telnet/history_v6.go` own captured Boolean
matching/invalidation. Explicit query clauses apply before saved self exceptions
and counting. [ADR-0261](../docs/decisions/ADR-0261-literal-comment-filter-and-history.md)
is historical for pre-v5 saved lists; its separate archive phrase grammar remains.
