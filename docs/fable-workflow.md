# docs/fable-workflow.md

This document is written for Fable (Claude-based agents). When `CLAUDE.md`
sends you here, read the applicable sections before code or documentation
changes.

It defines the full workflow for Non-trivial tasks and the mechanics behind
`CLAUDE.md`. It is the Fable-native counterpart to `docs/change-workflow.md`
(Codex's detailed workflow doc) — same underlying discipline, different
proof mechanism, because Fable has harness primitives Codex does not.

## Core principle

For Non-trivial work, do not go directly from idea to code. Move through:

1. understand the current system
2. define scope inside a Plan Mode plan
3. plan the slices
4. implement one verified slice at a time
5. review the diff
6. close out with traceability and validation

Token efficiency changes reporting shape only. It does not reduce required
discovery, approval, implementation discipline, validation, review, ADR
handling, or traceability.

## Task classification

### Small

A task is Small only if it is tightly localized and does not change
contracts, concurrency/lifecycle, operational behavior, or shared
interfaces. Skip Plan Mode only when all of these hold:

- no protocol or compatibility change
- no concurrency, lifecycle, timeout, queue, or shutdown impact
- no shared-component or cross-package contract change
- no user-visible behavior change beyond a strictly local fix

State a brief classification justification before editing. If blast radius
expands mid-task, reclassify as Non-trivial immediately and enter Plan Mode
before continuing.

### Non-trivial

Anything with meaningful blast radius, uncertain impact, or operational
consequences is Non-trivial. This includes workflow-contract Markdown
changes to the files listed in `CLAUDE.md`'s Task Gates section — treat them
identically to code for gating purposes. When in doubt, choose Non-trivial.

## Current-State Discovery before the plan

Before entering Plan Mode, perform a targeted Current-State Discovery pass.
The plan must be grounded in inspected code and docs, not assumptions.

Minimum discovery:

- relevant entry points and command/API surfaces
- caller/callee flow at least one level where material
- semantic source walking for unfamiliar, cross-package, or interface-driven
  behavior — use the applicable `.claude/skills/*` audit
- persisted state, config, archive, or schema surfaces when relevant
- user-visible/operator-visible output and HELP/docs surfaces
- existing tests for the affected behavior
- the mandatory decision-memory pre-read (see Decision-Memory Handling below)

If a fact cannot be established from inspection, say `Unknown from inspected
code` and name what should be inspected next.

## Plan Mode approval mechanics

This replaces Codex's `Proposed Scope Ledger vN` / exact-token approval with
a harness-level gate:

- Use `EnterPlanMode` before any `Write`, `Edit`, or mutating `Bash` call for
  Non-trivial work — code or workflow-contract Markdown alike.
- The plan file must contain: Current-State Discovery findings, a
  slice-shaped scope (see Slice-Shaped Hard Gate below), explicit in-scope
  and out-of-scope items, and a reasoning-budget recommendation.
- Before requesting approval, run the independent adversarial pass (see
  Subagent Use) when independent agents are supported and authorized. If it
  finds material gaps, revise the plan before calling `ExitPlanMode`.
- Call `ExitPlanMode` to request approval. Only the harness's actual approval
  signal counts — chat text like "go ahead" is not approval, and there is no
  equivalent risk of a typo'd or ambiguous token because approval is a UI
  action, not a string match.
- Every scope change after approval means re-entering Plan Mode with a
  revised plan, not silently expanding scope mid-execution.
- No file writes, diffs, or full validation commands before that approval.

## Scope adversarial review before approval

Required before every Non-trivial `ExitPlanMode` request, when independent
agents are supported and authorized (see Subagent Use).

Spawn `fable-scope-adversary` (or a manual stand-in, briefed identically,
when the dedicated agent is unavailable) and ask the required question:
`What edge case would make this scope unsafe or incomplete?`

The review must check applicable edge areas, including lifecycle/shutdown,
backpressure/drops/queues, bounded memory/state, zero/nil/empty inputs,
config/YAML/schema/defaults, parser/protocol/user-visible behavior,
metrics/logging/latency, tests/benchmarks, docs/support-agent impact,
ADR/TSR obligations, and — for workflow-contract changes specifically —
forward-references to not-yet-existing files, category-list consistency
across coupled docs, and headless/unattended execution edge cases.

Classify every material issue as covered by the plan, explicitly out of
scope, or requiring a revised plan. If a material gap is found, revise the
plan and repeat the review before calling `ExitPlanMode`. Report the
independent-agent status honestly: used / unsupported / `not
authorized/not requested` / explicitly prohibited / failed / timed out.

## Subagent use

Use independent agents whenever the environment supports delegated or
parallel agent work and authorization permits spawning them (see
`CLAUDE.md`'s Subagent Use section for the default-on / headless-exception
model). Record the support/authorization/prohibition status, phase, allowed
actions, expected output, and lead-agent verification for every use.

Independent agents have their own context window. That separation is useful
adversarial evidence and also a coordination risk: every independent finding
must be dispositioned by the lead agent against the current plan and
workspace evidence. Subagent output improves evidence; it never transfers
gate ownership. The lead agent always owns plan disposition, integration,
Review Pass, validation claims, ADR/TSR handling, Scope-to-Code
Traceability, and the final response.

### Pre-approval explorers

Before `ExitPlanMode` approval, independent agents must be read-only. Do not
grant `Edit`/`Write` in the agent definition for this phase — enforce it at
the tool-grant level, not by instruction alone. Allowed purposes: code-walk
evidence, blast-radius review, config-contract review, decision-memory
review, lifecycle or leak review, retained-state review, hot-path review,
docs/support impact review, and adversarial review of the plan
(`fable-scope-adversary`).

### Post-approval workers

After `ExitPlanMode` approval, worker subagents are allowed only for
approved, disjoint implementation slices. Each worker assignment must name:
approved plan version, slice name/objective, base revision or integration
point, allowed files/packages/docs, forbidden files/packages/docs,
production-safe stopping point, targeted checks, expected output/changed
paths, and stop conditions for hidden blast radius, overlap, or scope
uncertainty. Workers must assume other agents — including Codex, working
under `AGENTS.md` in the same repository — may be active; do not revert,
overwrite, or broaden another agent's work. If write scopes overlap or a
worker discovers a required out-of-assignment change, it must stop and
report the blocker.

### Post-code Go quality explorer

For Non-trivial Go implementation work, use `fable-code-reviewer` after code
is written and before final closeout when independent agents are supported
and authorized. It reviews the Go diff against the approved plan,
`docs/code-quality.md`, validation lane, comment intent, bounded state,
lifecycle/concurrency/resource ownership, and anti-speculative
implementation. It also reports PASS/FAIL/N/A evidence for the applicable
SELF-AUDIT-equivalent rows it can inspect at its phase (see
`docs/fable-review-checklist.md`). It does not final-score rows whose
evidence does not exist yet at this phase — `fable-fresh-verifier` supplies
those later. It reports findings only; it does not edit, propose diffs, run
formatters, or run broad/full validation suites. Do not trigger it for
documentation-only Markdown changes unless the diff also changes Go code or
a runtime/code contract.

If unsupported, `not authorized/not requested`, prohibited, failed, or timed
out, report that status. For high-risk Go work, missing independent review
is a review/validation gap unless explicitly waived.

### Fresh-verifier explorer

For high-risk Non-trivial work, use `fable-fresh-verifier` after the Review
Pass and before final closeout when independent agents are supported and
authorized. It checks the approved plan against the diff, validation
evidence, ADR/TSR and support-agent impact, claim wording, and hidden
out-of-scope work. For high-risk workflow-contract-only closeout where
`fable-code-reviewer` is not applicable, use the same `fable-fresh-verifier`
role with a prompt to independently score the applicable SELF-AUDIT-
equivalent rows — this reuses the existing role rather than adding a fourth
one. It reports findings only; it does not edit. Final validation remains
lead-owned.

## Reasoning budget recommendation

Every Non-trivial plan should recommend the lowest reasoning level expected
to satisfy the workflow without skipping required artifacts:

- `low`: clearly Small, localized, low-risk work, or read-only explanation
- `medium`: ordinary Non-trivial work with known blast radius, docs-only
  workflow changes, or localized implementation with clear tests
- `high`: config/schema/protocol/parser changes, user-visible behavior,
  shared interfaces, retained state, concurrency/lifecycle, queues, hot
  paths, or production-impacting fixes
- `xhigh`: large cross-cutting architecture, ambiguous semantics,
  conflicting evidence, incident/root-cause work, or multiple high-risk
  domains at once

Format: `Reasoning budget: <level> (lowest sufficient). Rationale: <one
sentence>; escalation trigger: <one phrase or "none expected">.` Raise it if
discovery reveals hidden blast radius; do not lower it by skipping required
artifacts, dependency rigor, validation, review, or traceability.

## Slice-shaped hard gate

Every Non-trivial plan must be slice-shaped before `ExitPlanMode` approval
is requested. A slice is implementation-ready only when it is small enough
to code, test, and review independently, and states: objective, bounded
files/packages/docs, blast-radius boundary and explicit out-of-slice work,
production-safe stopping point, and targeted checks before the next slice
starts.

Do not request approval for broad entries such as "refactor the parser" or
"rewrite config handling" unless decomposed into independent slices. A
change may remain one slice only when its target set is bounded, the
transformation is genuinely uniform, and the validation path is narrow and
explicit — treat "uniform" skeptically for anything mixing prose-vocabulary
translation with mechanical porting; challenge that claim in the adversarial
review rather than accepting it at face value.

For a Document Map that references files created across multiple slices
(this doc's own build was an example), add references incrementally —
a slice's stopping point must not depend on a file that a later slice has
not yet created.

## Workflow-drift audit

Required when editing any Fable workflow contract, validation rule,
template, review checklist, or `.claude/agents|skills/*` definition,
including: `CLAUDE.md`, `docs/fable-workflow.md`,
`docs/fable-review-checklist.md`, `docs/fable-validation.md`,
`docs/templates/fable-non-trivial-change-template.md`,
`.claude/agents/*.md`, `.claude/skills/**/SKILL.md`.

Audit requirements:

- preserve exact operational strings other Fable docs rely on (agent names,
  evidence-status vocabulary, the final validation block format)
- verify moved or shortened rules remain reachable from `CLAUDE.md`'s
  Document Map
- verify validation rules, checker commands, review expectations, and
  closeout requirements do not contradict each other
- verify read-only, Small, and Non-trivial paths remain distinct
- check that `.claude/agents/*.md` frontmatter tool grants match the
  read-only/write boundaries described here
- report the audit result in the final closeout response

## Decision-memory handling

Substantive ADR/TSR rules — required fields, full-ADR vs. stub criteria,
immutability, index maintenance — are defined in `docs/decision-memory.md`
and are shared with Codex; do not fork them.

Fable-specific integration: perform the mandatory pre-read (`docs/decision-
log.md`, `docs/troubleshooting-log.md`, relevant ADRs/TSRs) during Current-
State Discovery, before the plan is written. Every Non-trivial task ends
with a full ADR, an updated ADR, or a lightweight stub — `Decision refs:
none` is not valid for Non-trivial work. Report `Decision refs: ADR-XXXX`
(and `TSR-XXXX` when applicable) in the closeout response. Check
`docs/decision-log.md` for the next free ADR number before proposing one —
another executor (Codex) may have claimed a number since you last checked;
re-verify at plan-approval time, not just during discovery, if the plan was
open for more than a few tool calls.

## Dependency rigor

Use Light rigor only when all hold: localized package, no shared component/
interface change, no protocol/parser/config/schema change, no user-visible
or operator-visible contract change, no concurrency/lifecycle impact outside
the local package. Otherwise use Full rigor and report:
`Dependency scan evidence: <commands/steps used>; reviewed files/packages:
<list>`. Full rigor is also required whenever `.claude/skills/go-blast-
radius-audit` is triggered — report its compact result before
implementation.

## Testing and checker discipline

Use `docs/dev-runbook.md` as the required checker source (shared with
Codex). Select the validation lane from the touched surface:

- **Documentation-only Markdown lane**: all changed files are Markdown and
  the diff touches no code, config, generated artifact, script, CI, schema,
  protocol/runtime contract, or runtime-consumed data.
- **Workflow-contract lane**: the diff changes `CLAUDE.md`, this doc, the
  review checklist, validation rubric, templates, or `.claude/agents|
  skills/*` — including `.claude/agents/*.md` frontmatter, which is
  structured metadata, not prose Markdown, so do not mislabel this diff as
  documentation-only. Minimum checks: targeted text checks for changed
  workflow terms, the workflow-drift audit above, reviewer diff pass, `git
  diff --check`.
- **Code/mixed/runtime-contract lane**: `go test ./...`, `go vet ./...`,
  `staticcheck ./...`, plus `go test -race ./...` for concurrency/lifecycle/
  queue/timer/shutdown/shared-mutable-state work, fuzzing for parser/
  protocol changes, and benchmarks/pprof for hot-path or performance claims.

Report each command with: exact command, why it was run, result, and
whether incremental or final.

## Documentation expectations

For every Non-trivial task, explicitly state `README impact: Required |
Not required` and `Support-agent docs impact: Required | Not required`,
each with one sentence of reasoning. Support-agent docs impact is Required
whenever operator-visible behavior, user-facing commands, config surfaces,
or troubleshooting behavior changes — see `customgpt/` for the shared
routing layer.

## Completion requirements

A Non-trivial Fable task is not complete until: the approved change is
implemented; the selected validation lane's checks are run and reported
honestly; a Review Pass and SELF-AUDIT-equivalent scoring are done (see
`docs/fable-review-checklist.md`); docs are reviewed; ADR/TSR handling is
satisfied; Scope-to-Code Traceability is complete; and the exact final
validation block from `docs/fable-validation.md` is present.
