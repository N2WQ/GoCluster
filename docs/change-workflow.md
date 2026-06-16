# docs/change-workflow.md

This document is written for Codex. When `AGENTS.md` sends you here, read the
applicable sections before code.

It defines the full workflow for Non-trivial tasks and the deeper rules behind `AGENTS.md`.

## Core principle
For Non-trivial work, do not go directly from idea to code. Move through:
1. understand the current system
2. define scope
3. plan the slices
4. implement one verified slice at a time
5. review the diff
6. close out with traceability and validation

Token efficiency changes reporting shape only. It does not reduce required
discovery, approval, implementation discipline, validation, review, ADR
handling, or traceability.

Validation is proportional to the touched surface. Documentation-only Markdown
changes have their own validation lane: use documentation review, targeted text
checks, and whitespace/diff checks instead of Go code validation, unless the
change also touches code, config, generated artifacts, scripts, CI, schemas,
protocol/runtime contracts, or checked-in data consumed by the runtime.

Keep the workflow additive, not repetitive:
- later sections may reference earlier evidence instead of restating it
- only restate facts when the later section adds a new conclusion, delta, or
  final disposition
- keep required evidence markers, but compress duplicate narration
- use one-line `N/A - reason` entries for non-triggered areas
- report each validation result in one place, then reference it by marker name
- Full rigor means full work, not full prose

Missing required evidence is a workflow failure, not a style issue. If Codex
cannot complete a required evidence marker from inspected workspace evidence,
stop and report what is missing.

## IDE context discipline
When using the Codex VS Code extension:
- Prefer the user's open files as the primary context.
- Ask the user to open the most relevant caller/callee files when context is thin.
- Use selected code as a focus anchor when a single function, method, or protocol path is under discussion.
- If Auto Context is available and on, still name the critical files explicitly in your analysis so the user can verify you are looking in the right place.

## Task classification
### Small
A task is Small only if it is tightly localized and does not change contracts, concurrency/lifecycle, operational behavior, or shared interfaces.

When a Small task changes code, state a brief classification justification before
editing. The justification should name why the change is localized, why blast
radius is low, and why no Non-trivial triggers apply.

### Non-trivial
Anything with meaningful blast radius, uncertain impact, or operational consequences is Non-trivial.

When in doubt, choose Non-trivial.

## Approval and pre-code gates
Required before every change:
- confirm the current Scope Ledger version and the status of each item
- classify the task as Small or Non-trivial
- record `Skill check: selected <skill>` or `Skill check: none applicable`

For Non-trivial changes:
- do not edit files, propose diffs, run formatters, or run full checker suites until the user has replied with the exact approval token: `Approved vN`
- record `Ledger status: Approved vN found: yes/no`
- do not treat discussion, "please implement", "go ahead", or any non-exact wording as approval
- every scope change after approval requires a new ledger version
- broad refactor-shaped Scope Ledger items are not approval-ready; split them
  into independently coded, tested, and reviewed slices before presenting the
  approval token
- before showing the approval token, perform `SCOPE ADVERSARIAL REVIEW`; if it
  finds a material gap, revise the Scope Ledger version and repeat the review

### Current-State Discovery before Scope Ledger
Before proposing or confirming a Non-trivial Scope Ledger, perform a targeted
Current-State Discovery pass. The first ledger must be grounded in inspected
code and docs, not assumptions.

Minimum discovery:
- relevant entry points and command/API surfaces
- caller/callee flow at least one level where material
- `Code-walk evidence` when behavior is unfamiliar, cross-package, or depends
  on semantic callers/interfaces; use `go-code-walk` when available
- persisted state, config, archive, or schema surfaces when relevant
- user-visible/operator-visible output and HELP/docs surfaces
- existing tests for the affected behavior
- applicable repo-managed, runtime/system, or explicitly available plugin skills

Ask product or semantic questions only after discoverable code facts have been
checked. If a fact cannot be established from inspection, say
`Unknown from inspected code` and name what should be inspected next.

### Scope adversarial review before approval
Required before presenting the exact approval token for every Non-trivial Scope
Ledger.

After `Proposed Scope Ledger vN`, Codex must ask:
`What edge case would make this scope unsafe or incomplete?`

The review must check applicable edge areas, including lifecycle/shutdown,
backpressure/drops/queues, bounded memory/state, zero/nil/empty inputs,
config/YAML/schema/defaults, parser/protocol/user-visible behavior,
metrics/logging/latency, tests/benchmarks, docs/support-agent impact, and
ADR/TSR obligations.

Each material edge case must be classified as covered by the ledger, explicitly
out of scope, or requiring a revised ledger. If any material gap is found, do
not present the approval token for the current ledger version. Create the next
`Proposed Scope Ledger vN`, repeat `SCOPE ADVERSARIAL REVIEW`, and continue
until the disposition is exactly `nothing material found`.

### Reasoning budget recommendation
Every Proposed Scope Ledger must recommend the lowest reasoning level expected
to satisfy the workflow without skipping required artifacts:
- `low`: clearly Small, localized, low-risk work, or read-only explanation
- `medium`: ordinary Non-trivial work with known blast radius, docs-only
  workflow changes, or localized implementation with clear tests
- `high`: Full-rigor work, config/schema/protocol/parser changes,
  user-visible behavior, shared interfaces, retained state,
  concurrency/lifecycle, queues, hot paths, or production-impacting fixes
- `xhigh`: large cross-cutting architecture, ambiguous semantics, conflicting
  evidence, incident/root-cause work, or multiple high-risk domains at once

Format:
- `Reasoning budget: <low|medium|high|xhigh> (lowest sufficient). Rationale: <one sentence>; escalation trigger: <one phrase or "none expected">.`

The recommendation is advisory. Raise it if discovery reveals hidden blast
radius; do not lower it by skipping required workflow artifacts, skill audits,
dependency rigor, validation, review, or traceability.

Before implementation, explicitly identify:
- impacted contracts, or `No contract changes`
- user-visible behavior changes, or `No user-visible behavior changes`
- README impact: `Required` or `Not required`
- Support-agent docs impact: `Required` or `Not required`
- validation lane, checker set, and validation command order

In the compact template, these are reported under the `DESIGN` marker.

### Slice-shaped Scope Ledger hard gate
Every Non-trivial Scope Ledger must be slice-shaped before it can be approved.
A ledger item is implementation-ready only when it is small enough to code,
test, and review independently.

Each approved implementation slice must state:
- objective
- bounded files, packages, or docs expected to change
- blast-radius boundary and explicit out-of-slice work
- production-safe stopping point
- targeted checks to run before the next slice starts

Do not approve broad entries such as "refactor the parser", "clean up
telnet", or "rewrite config handling" unless they are decomposed into
independent slices. A mechanical migration may remain one slice only when its
target set is bounded, the transformation is uniform, and the validation path is
narrow and explicit.

## Workflow-drift audit
Required when editing any workflow contract, validation rule, runbook, review
checklist, Codex guidance, or repo-managed skill, including:
- `AGENTS.md`
- `VALIDATION.md`
- `docs/change-workflow.md`
- `docs/review-checklist.md`
- `docs/dev-runbook.md`
- `docs/code-quality.md`
- `docs/WORKING_WITH_CODEX.md`
- `codex-skills/**/SKILL.md`

Use `workflow-contract-audit` when available.

Audit requirements:
- preserve exact strings that other workflow docs or users rely on
- check that moved or shortened rules remain reachable from `AGENTS.md`
- verify that skill triggers, validation rules, runbook commands, and review
  expectations do not contradict each other
- run targeted text checks for the key workflow phrases touched by the change
- report the audit result in the final summary

## Git preflight
Required for every Non-trivial change:
- record branch name
- confirm working tree state
- identify rollback point
- note any unrelated dirty files that must not be touched

Output format:
- `Git preflight: branch=<name>; worktree=<clean|dirty acknowledged>; rollback=<hash/tag/branch>`

## Current-State Understanding Note
This is mandatory before implementation planning. It extends the pre-ledger
Current-State Discovery with the detail needed for implementation.

If the pre-ledger discovery already captured part of this cleanly, reuse it and
add only the implementation-relevant delta.

Quality rules:
- ground statements in inspected code
- mention concrete file/package identifiers
- say `Unknown from inspected code` rather than guessing
- keep it concise but specific

## Requirements & Edge Cases Note
Required for Non-trivial work.

This is where hidden implementation expectations should be surfaced before
code after approval. It does not replace the pre-approval `SCOPE ADVERSARIAL
REVIEW`, which decides whether the Scope Ledger itself is complete enough to
approve.

## Dependency rigor decision tree
Choose `Light` or `Full`.

### Light rigor
Use Light only when all are true:
- localized package
- no shared component/interface change
- no protocol/parser/config/schema change
- no user-visible or operator-visible contract change
- no concurrency/lifecycle impact outside the local package

Expected coverage is defined by the template. Keep it concise.

### Full rigor
Use Full when any are true:
- shared package or interface
- parser/protocol/config/schema changes
- concurrency/lifecycle/timeout/backpressure/shutdown changes
- metrics/logging/observability contract changes
- user-visible or operator-visible behavior changes
- uncertain blast radius
- fan-out, queueing, caching, or hot-path changes

Required output:
- exact one-line evidence block:
  `Dependency scan evidence: <repo search commands/steps used>; reviewed files/packages: <list>`

When Full rigor is triggered by uncertain blast radius, shared interfaces,
semantic callers, package dependency impact, or docs/support routing impact,
also use `go-blast-radius-audit` when available and report a compact
`Blast-radius audit` result before implementation.

## Tool-assisted analysis
Use tool-assisted analysis to improve evidence, not to replace source review.

Generated code maps under `docs/code-maps/` may be used as first-pass package,
file, test, and ADR orientation when relevant and fresh. They do not prove
runtime behavior, interface dispatch, goroutine lifecycle, config-specific
paths, or concrete data flow; verify conclusions against current source, tests,
config, and ADRs before planning or editing.

Required baseline tools for this workflow are the repository's normal Go and
validation tools plus semantic/navigation helpers already called out by the
triggered skill. Missing required tools are validation or discovery gaps.

Optional tools such as Graphviz `dot`, `goda`, `go-callvis`, `semgrep`,
`ast-grep`, and Sysinternals improve specific investigations. Report missing
optional tools as conditional evidence gaps only for the workflow that needed
them; their absence does not block ordinary Go implementation, review, or
validation.

### Code-walk evidence
Use `go-code-walk` when current-state discovery needs semantic source walking,
especially for unfamiliar behavior, cross-package behavior, interface dispatch,
or caller/callee chains. Record inspected files, commands, tests, ADRs/TSRs,
and unknowns. Do not treat callgraph output as proof of concrete runtime
behavior.

### Blast-radius audit
Use `go-blast-radius-audit` before approval or implementation when a change may
affect shared APIs, interfaces, cross-package behavior, config/docs/support
routing, or uncertain dependency/test surfaces. Classify each discovered impact
as in scope, out of scope, requiring revised ledger, validation follow-up, or
documentation/support follow-up.

When graph output is useful beyond the current turn, summarize the durable
package edges and limits in a Markdown code map under `docs/code-maps/`. The
custom GPT support agent can retrieve Markdown and source files; it cannot run
local graph tools or treat rendered images as authoritative evidence.

### Leak-detection audit
Use `go-leak-detection` when work touches or investigates goroutines, timers,
tickers, channels, sockets, file handles, queues, shutdown, long-lived
lifecycle, retained heap, or pprof/trace leak evidence. Distinguish static
reasoning, local test/race evidence, profile evidence, and long-running runtime
confirmation.

### Connection lifecycle audit
Use `go-connection-lifecycle-audit` when work touches or investigates
long-lived inbound or outbound connection behavior, reconnect, retry/backoff,
keepalive, deadlines, EOF/read-loop recovery, silent-stall or zero-data modes,
connection shutdown, source liveness, or operator-visible connection
diagnostics.

The audit must distinguish connection health from data-stream health, and must
not treat keepalive on an existing socket as proof of recovery after a lost or
never-established connection. When implementation touches goroutines, sockets,
timers, channels, cancellation, shutdown, or queues, also use
`go-leak-detection` and run `go test -race ./...` unless explicitly waived.

## Config Contract Audit
Required when a task touches YAML files, config structs, config loaders,
normalizers, runtime defaults, reference tables, operator settings, or optional
tool/secret config.

Config/schema changes require Full dependency rigor unless they are strictly
local test fixture changes.

Use the template's triggered-audit subsection and include only the config
details that apply.

The audit must distinguish YAML-owned operator settings from validation
constants, algorithm constants, compatibility boundaries, and test fixtures.

### YAML Documentation Rigor
Required when a task adds or edits checked-in first-party YAML, especially
`data/config/*.yaml`.

Use `data/config/README.md` as the source of truth for YAML file headers and
key-comment standards. Verify:
- checked-in `data/config/*.yaml` files keep the exact five-line header
- new or changed YAML keys explain purpose when units, sentinel values,
  ownership, side effects, runtime consequences, or safe-edit boundaries are
  non-obvious
- obvious true/false toggles are not comment-noised unless side effects are
  non-obvious
- repeated list/table schemas document the first occurrence or use a field
  guide instead of duplicating comments on every row
- YAML comments remain local context only, not schema, defaults, or runtime
  proof when code or docs disagree

Run `scripts/check-yaml-doc-rigor.ps1` for mechanical checks. Use
`-CommentOnlyCompare` when the intended YAML change is comment-only. Treat
script warnings as review prompts; human review remains responsible for
subjective comment quality and drift against loaders/docs/code.

### Go Comment Intent Rigor
Required when a task adds or edits support-critical Go, including runtime
pipelines, telnet/user-facing behavior, retained state, caches, queues, timers,
goroutine lifecycle, hot paths, logging/metrics/diagnostics, replay/profiling
tools, exported/shared APIs, or code the support agent is likely to inspect.

Use `docs/code-quality.md` as the source of truth for Go comment intent. Verify:
- new or materially changed support-critical package entry files, subsystem
  integration files, replay/tool entry points, and support-critical leaf files
  have a concise crawler-entry comment when package/file ownership is not
  obvious from an existing package comment or README
- comments explain intent/why, ownership, invariants, resource bounds, and
  troubleshooting meaning where those are not obvious from local code
- drop, delay, overflow, fail-open/fail-closed, cleanup, and lifecycle paths are
  discoverable from nearby comments when they affect operators or support
- retained-state comments identify the cap, expiry, cleanup coupling, or
  bounded-lifetime proof required by the retained-state standard
- comments do not mechanically restate assignments, simple booleans, or every
  repeated branch once the pattern has been explained
- comments do not drift from code, tests, config, docs, ADRs, or support-agent
  routing docs

Run `scripts/check-go-crawler-entry-comments.ps1 -ChangedOnly -FailOnMissing`
when adding or materially changing support-critical Go files. For comment-only
Go changes, include a reviewer diff pass confirming the non-comment Go diff is
empty. These are mechanical scope checks only; human review remains responsible
for subjective intent quality.

## Implementation Plan
Distinct from the Scope Ledger. The ledger says what is approved. The plan says how to do it.

Use the template's `DESIGN` marker. Keep the plan production-safe and minimal.

Rules:
- milestone 1 must be the smallest production-safe slice
- do not combine multiple uncertain changes into one slice
- keep the first slice easy to verify

## Architecture Note
Mandatory for every Non-trivial change before code.

Use the template's `DESIGN` marker and cover only the fields material to the
change.

## User Impact and Determinism Note
Required for every Non-trivial change.

Use the template's `DESIGN` and `CLOSEOUT` markers. If there is no
user-visible change, say so explicitly.

## Implementation slicing rules
- Implement only the current milestone.
- Run the milestone's checks before continuing.
- If results reveal hidden blast radius, stop and update the Scope Ledger.
- Keep diffs narrow.
- Do not sneak in opportunistic cleanup unless it is required for correctness or clarity and is called out explicitly.

## Testing and checker discipline
Use `docs/dev-runbook.md` as the required checker source for Non-trivial
closeout. Select the validation lane from the touched surface before choosing
commands.

Documentation-only Markdown lane:
- eligible only when the diff changes Markdown documentation and no code,
  config, generated artifact, script, CI, schema, protocol/runtime contract, or
  runtime-consumed data
- minimum checks are targeted text checks for the changed workflow/domain terms,
  reviewer diff pass, and `git diff --check`
- add repository-specific documentation checks only when they apply, such as
  support-agent routing review or workflow-drift audit
- do not run Go validation solely because a Markdown file changed

Code, mixed, or runtime-contract lane minimum:
- `go test ./...`
- `go vet ./...`
- `staticcheck ./...`

Also required when applicable:
- `go test -race ./...` for concurrency/lifecycle
- fuzzing for parser/protocol work
- benchmarks and profiling for hot paths

Rules:
- run checks incrementally
- report commands and results honestly
- add regression tests for changed behavior when feasible
- explain why any test was not added
- if a documentation-only change later expands into code, config, generated
  artifact, script, CI, schema, protocol/runtime contract, or runtime-data
  changes, reclassify the validation lane and run the required code or mixed
  checks before closeout

## Performance evidence
Required when behavior touches hot paths, fan-out, queueing, parsing, allocation pressure, timers, or lock contention.

Evidence should include as applicable:
- before/after benchmark numbers
- allocs/op
- pprof CPU or heap evidence
- lock/contention evidence
- explanation of why the change is safe under nominal and overload conditions

Do not make optimization claims without measurements.

## Documentation expectations
Review and update when applicable:
- README
- operator docs
- support-agent routing docs under `customgpt/`
- protocol docs
- comments on invariants/ownership/concurrency/drop policy
- ADR/TSR records
- test names and descriptions for operator-facing behavior

For every Non-trivial task, explicitly say:
- `README impact: Required`
- or `README impact: Not required`
with one sentence of reasoning

Also explicitly say:
- `Support-agent docs impact: Required`
- or `Support-agent docs impact: Not required`
with one sentence of reasoning

### Support-agent documentation sync
Support-agent docs impact is `Required` when a change adds, removes, renames,
or materially changes any operator-support topic, including:
- operator-visible behavior or output
- user-facing commands, HELP, filters, modes, EVENT families, glyphs, or diagnostics
- YAML/config surfaces, defaults, sentinel values, or startup validation
- logging, observability, troubleshooting, startup, service, or deployment behavior
- source, ingest, peer, or connection behavior that support may be asked to explain

When required, inspect and update the relevant routing/support files:
- `customgpt/source-map.md`
- `customgpt/common-questions.md`
- `customgpt/operator-guide-index.md`
- `customgpt/troubleshooting-index.md`
- `customgpt/gpt-instructions.md`

Keep `customgpt/` as a routing layer. Do not duplicate full operator docs
there; point support answers to the authoritative repo docs and note effective
YAML/current-code caveats when needed.

## Reporting shape
Use `docs/templates/non-trivial-change-template.md` for the strict compact
reporting shape:
- Phase A approval packet before `Approved vN`
- Phase B execution ledger after approval

Required rigor does not imply a long narrative. Reuse earlier evidence by
reference where possible.

## Decision-memory requirement
Every Non-trivial task requires ADR handling:
- full ADR when a durable decision changed
- lightweight ADR stub when no durable decision changed

Use `decision-memory-audit` when available.

Use `docs/decision-memory.md` for the detailed rules.

## Completion requirements
A Non-trivial task is not complete until:
- the approved code or documentation change is implemented
- checks are run
- Review Pass is done
- docs are reviewed
- ADR handling is satisfied and TSR obligations are satisfied when applicable
- Scope-to-Code Traceability is complete
- the exact 3-line validation block is present
