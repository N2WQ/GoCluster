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

Keep the workflow additive, not repetitive: later sections may reference
earlier evidence instead of restating it; report each validation result once
and reference it by section/row name afterward; use one-line `N/A - reason`
entries for non-triggered areas. Full rigor means full work, not full prose.

Missing required evidence is a workflow failure, not a style issue. If a
required SELF-AUDIT row, plan field, or closeout section cannot be completed
from inspected workspace evidence, stop and report what is missing — do not
smooth it into a passing claim.

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
- when a relevant generated code map exists under `docs/code-maps/`, check
  freshness, read it during discovery, and verify conclusions against
  current source, tests, config, and ADRs before planning or editing —
  generated maps are first-pass orientation, not proof of runtime behavior

If a fact cannot be established from inspection, say `Unknown from inspected
code` and name what should be inspected next.

Do not claim runtime improvement from code shape alone. Do not claim
scientific or model correctness from plausible reasoning alone. Path,
call-correction, VOACAP, p50, propagation, and operator-diagnostic claims
need the relevant model assumption, evidence source, and remaining
uncertainty stated when they affect behavior or conclusions.

## Pre-plan independent evidence

For Non-trivial work, after Current-State Discovery and before the plan's
scope is drafted, use these bounded independent-evidence steps in order.
Each is separate from `fable-scope-adversary`, which reviews the drafted
plan afterward — these steps exist to keep semantics and architecture from
hardening into a plan in the first place.

### Bounded parallel discovery

For Full-rigor discovery with at least two separable evidence domains (for
example: source behavior vs. config/schema surface, or Go implementation
vs. YAML/data contract), spawn 2-3 `Explore` agents in a single message so
they see the same on-disk state. Give each a distinct, bounded,
locate-oriented question, an evidence contract (what to return), and a stop
condition — matching `Explore`'s own scope ("locating code... where is X
defined... which files reference Y"), not open-ended analysis or
adversarial review; set the search-breadth hint (`quick`/`medium`/`very
thorough`) per question. Synthesize the results yourself, surface any
conflicts, and disposition them before drafting the plan — `Explore` output
is evidence, not a decision.

Use one of the three roles below, not `Explore`, for anything requiring
adversarial or normative judgment rather than fact-finding.

### Scientific/model oracle

Trigger: VOACAP, propagation, p50, path reliability, call correction,
geographic/grid conversions, band/frequency mappings, units, interpolation,
thresholds, classifications, scientific diagnostics, confidence wording, or
any change whose correctness depends on a scientific model. Do not trigger
for purely mechanical work that cannot change model inputs, outputs,
semantics, or claims.

Spawn `fable-scientific-oracle` before requirements-ambiguity review,
design, or the plan's scope, when independent agents are supported and
authorized. It establishes a model contract independently of the
implementation under assessment — source hierarchy, units, boundaries,
provenance-independent golden vectors, and bounded claims — grounded in the
same "Scientific And Model Claims" standard `docs/code-quality.md` already
defines. Missing or conflicting normative evidence blocks the plan's scope
until resolved.

### Requirements ambiguity review

Trigger: filters or Boolean precedence, defaults or sentinels,
authentication or admission, reputation or correction gates, failure
behavior, thresholds, classifications, diagnostics, compatibility,
user-visible behavior, or test oracles that may admit materially different
semantic interpretations.

Spawn `fable-requirements-adversary` after Current-State Discovery and any
triggered scientific-oracle evidence, before the plan's scope is drafted,
when independent agents are supported and authorized. It produces an
ambiguity register; unresolved material semantics block the plan's scope
until the user or documented authority resolves them.

### Design challenger

Trigger: two or more viable architectures may differ in ownership, retained
state, lifecycle, queues, compatibility, migration, persistence, shared
interfaces, algorithms, or other high-risk design choices. Do not trigger
for Small or mechanical work, a single safe implementation path, or
behavior already fixed by an accepted contract.

Spawn `fable-design-challenger` after scientific-oracle and requirements-
adversary evidence is resolved, before you draft the plan, when independent
agents are supported and authorized. Assemble a neutral fact-and-constraint
packet — confirmed requirements, current-state evidence, relevant ADRs/
TSRs, resolved semantics — and withhold your own preferred solution, draft
plan, and intended diff from the spawn prompt. This is a self-reported,
instruction-level constraint, not a mechanically enforced one: if the
agent's inherited context exposes your preferred design anyway, its result
must be reported as `inconclusive - context contaminated`, not counted as
independent evidence. Disposition every material finding before drafting
the plan's scope.

For all three roles above and the parallel-discovery wave, report the
independent-agent status honestly (used / unsupported / `not authorized/not
requested` / explicitly prohibited / failed / timed out / `inconclusive -
no independent context` / `inconclusive - context contaminated`, as
applicable) in the plan's Current-State Discovery section.

## Plan Mode approval mechanics

This replaces Codex's `Proposed Scope Ledger vN` / exact-token approval with
a harness-level gate:

- Use `EnterPlanMode` before any `Write`, `Edit`, or mutating `Bash` call for
  Non-trivial work — code or workflow-contract Markdown alike.
- The plan file must contain: Current-State Discovery findings, triggered
  pre-plan independent evidence (parallel discovery, scientific/model
  oracle, requirements ambiguity, design challenge) dispositioned per the
  section above, a slice-shaped scope (see Slice-Shaped Hard Gate below),
  explicit in-scope and out-of-scope items, and a reasoning-budget
  recommendation.
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
docs/support impact review, bounded parallel discovery (`Explore`),
scientific/model oracle evidence (`fable-scientific-oracle`), requirements
ambiguity review (`fable-requirements-adversary`), design challenge
(`fable-design-challenger`), and adversarial review of the drafted plan
(`fable-scope-adversary`) — see Pre-plan independent evidence above for
phase and trigger detail.

### Post-approval workers

After `ExitPlanMode` approval, worker subagents are allowed only for
approved, disjoint implementation slices. Spawn workers via the
`general-purpose` agent type — none of the seven read-only explorer-
equivalent agents (`fable-scope-adversary`, `fable-code-reviewer`,
`fable-fresh-verifier`, `fable-scientific-oracle`,
`fable-requirements-adversary`, `fable-design-challenger`,
`fable-test-strategy-adversary`) can write, so a worker is never one of
those seven. Each worker assignment must name:
approved plan version, slice name/objective, base revision or integration
point, allowed files/packages/docs, forbidden files/packages/docs,
production-safe stopping point, targeted checks, expected output/changed
paths, and stop conditions for hidden blast radius, overlap, or scope
uncertainty. Workers must assume other agents — including Codex, working
under `AGENTS.md` in the same repository — may be active; do not revert,
overwrite, or broaden another agent's work. If write scopes overlap or a
worker discovers a required out-of-assignment change, it must stop and
report the blocker.

### Pre-implementation test-strategy explorer

Trigger: parser or protocol behavior, configuration/default/schema
semantics, concurrency or lifecycle, retained state, compatibility,
operator-visible classifications, performance claims, scientific/model
behavior, implementation-mirroring fixtures, or workflow/checker changes
with false-green risk. Do not trigger for localized documentation-only work
or mechanical changes whose accepted checks directly prove the unchanged
contract.

After `ExitPlanMode` approval and a detailed `DESIGN`, but before the first
`IMPLEMENTATION` slice begins, spawn `fable-test-strategy-adversary` when
independent agents are supported and authorized. Provide the approved plan,
detailed design, normative contracts, relevant ADR/TSR and oracle/ambiguity
results, current tests, and the proposed checker plan — not a completed
implementation or intended test answers. It returns a contract-to-test
matrix classifying every finding as `covered`, `checker-only refinement`,
`material scope or behavior gap`, or `normative evidence conflict`. A
`material scope or behavior gap` requires a revised plan, a repeated
`fable-scope-adversary` pass, and exact `ExitPlanMode` reapproval before
implementation continues; block the first slice until findings are
dispositioned and the matrix is adequate.

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

High-risk work includes config/schema/protocol/parser changes, user-visible
or operator-visible behavior, shared interfaces, retained state,
concurrency/lifecycle, queues, hot paths, production-impacting fixes, and
scientific/model behavior such as call-correction, path reliability, p50,
propagation, or VOACAP semantics.

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

- `low`: narrow Non-trivial work with known, localized blast radius and a
  direct validation path
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

Use `docs/templates/adr-template.md` for new ADRs and `docs/troubleshooting/
TSR-TEMPLATE.md` for new TSRs — these are the templates actually matched by
current practice and by `scripts/check-troubleshooting-records.ps1`'s
required `## RCA Summary` check. Two other files share similar names
(`docs/decisions/ADR-TEMPLATE.md`, `docs/templates/tsr-template.md`) but are
stale and unused; do not use them even though other shared docs may still
cite them by mistake.

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

## Config contract audit

Required when a task touches YAML files, config structs, config loaders,
normalizers, runtime defaults, reference tables, operator settings, or
optional tool/secret config. Config/schema changes require Full dependency
rigor unless strictly local test fixture changes. Use
`.claude/skills/go-config-contract-audit` for the full procedure; it must
distinguish YAML-owned operator settings from validation constants,
algorithm constants, compatibility boundaries, and test fixtures.

## Go comment intent rigor

Required when a task adds or edits support-critical Go, including runtime
pipelines, telnet/user-facing behavior, retained state, caches, queues,
timers, goroutine lifecycle, hot paths, logging/metrics/diagnostics,
replay/profiling tools, exported/shared APIs, or code the support agent is
likely to inspect. Use `docs/code-quality.md` (shared with Codex) as the
source of truth for Go comment intent — its Go Comment Intent section
already defines the required coverage. Run `scripts/check-go-crawler-entry-
comments.ps1 -ChangedOnly -FailOnMissing` (shared with Codex) when adding or
materially changing support-critical Go files. For comment-only Go changes,
include a reviewer diff pass confirming the non-comment Go diff is empty.
Score SA6 (`Go crawler-entry audit`) `N/A` only when no
support-critical Go file was added or materially changed.

## YAML documentation rigor

Required when adding or editing checked-in first-party YAML, especially
`data/config/*.yaml`. Use `data/config/README.md` (shared with Codex) as the
source of truth for YAML file headers and key-comment standards. Verify:

- checked-in `data/config/*.yaml` files keep the exact five-line header
  (`Purpose`/`Ownership`/`Runtime behavior`/`Safe edits`/`Source`)
- new or changed keys explain purpose when units, sentinel values,
  ownership, side effects, runtime consequences, or safe-edit boundaries are
  non-obvious
- obvious boolean toggles are not comment-noised unless side effects are
  non-obvious
- repeated list/table schemas document the first occurrence rather than
  duplicating comments on every row

Run `scripts/check-yaml-doc-rigor.ps1` (shared with Codex) for mechanical
checks; use `-CommentOnlyCompare` when the intended change is comment-only.
Score SA4 (`YAML comment/header audit`) `N/A` only when no
checked-in first-party YAML changed. `.claude/agents/**/*.md` and
`.claude/skills/**/SKILL.md` frontmatter is not first-party runtime config
and is not subject to this standard — see the workflow-contract lane's
metadata/body sync check instead.

## Testing and checker discipline

Use `docs/dev-runbook.md` as the required checker source (shared with
Codex). Select the validation lane from the touched surface:

- **Documentation-only Markdown lane**: all changed files are Markdown and
  the diff touches no code, runtime config, generated artifact, script, CI,
  schema, protocol/runtime contract, or runtime-consumed data.
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
each with one sentence of reasoning.

Support-agent docs impact is `Required` when a change adds, removes,
renames, or materially changes any operator-support topic, including:
operator-visible behavior or output; user-facing commands, HELP, filters,
modes, EVENT families, glyphs, or diagnostics; YAML/config surfaces,
defaults, sentinel values, or startup validation; logging, observability,
troubleshooting, startup, service, or deployment behavior; source, ingest,
peer, or connection behavior that support may be asked to explain.

`customgpt/` is the shared routing layer with Codex. Updating it for
Fable-specific routing is currently out of scope (see `ADR-0206`) — report
`Support-agent docs impact: Required, deferred per ADR-0206` rather than
silently marking it Not required when a trigger applies.

## Completion requirements

A Non-trivial Fable task is not complete until: the approved change is
implemented; the selected validation lane's checks are run and reported
honestly; a Review Pass and SELF-AUDIT-equivalent scoring are done (see
`docs/fable-review-checklist.md`); docs are reviewed; ADR/TSR handling is
satisfied; Scope-to-Code Traceability is complete; and the exact final
validation block from `docs/fable-validation.md` is present.
