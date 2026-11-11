# Codex Change Workflow

This document owns detailed Codex execution semantics. `AGENTS.md` owns change
authority, `docs/dev-runbook.md` owns validation commands, and specialist skills
own their unique engineering methods. Read only the sections triggered by the
task.

## Read-only Work

Explanation, review, audit, diagnosis, prioritization, and requested
recommendations inspect current evidence without mutation or change-closeout
ceremony. Findings are evidence, not latent approval. If the user later asks for
changes, enter the Small or Non-trivial route before editing.

Use the repo-managed `explain-code` skill for code understanding, from a quick
orientation to a thorough investigation; use any more specific triggered
specialist for its engineering method.

## Change Classification

Small work is localized, low blast radius, and does not change protocol,
compatibility, concurrency, lifecycle, queues, shutdown, shared interfaces, or
material user-visible behavior. It also cannot change runtime config, schema,
default, or sentinel semantics; parser behavior; authentication or admission;
persisted state; scientific/model semantics; hot-path behavior; shared
contracts; material operator-visible behavior; or durable decisions. Anything
else, including uncertain impact, is Non-trivial.

Task classification controls approval. Touched surface and engineering risk
control validation. A documentation or workflow change does not require Go
validation merely because it is Non-trivial.

## Non-trivial Planning

### Discovery

Inspect enough current evidence to establish:

- current behavior and the material execution or ownership path;
- affected callers, consumers, contracts, tests, docs, and decision records;
- compatibility, operator, resource, and validation consequences;
- facts, assumptions, and unresolved unknowns.

Before proposing scope, use the smallest safe read-only proof to resolve an
unknown that could invalidate the approach or materially change scope. If it
cannot be resolved safely, carry it as an explicit assumption or unknown; do
not present an assumed mechanism as current fact.

Use fresh code maps and generated graphs only as orientation. Verify material
conclusions against current source and tests. Use parallel discovery only for
genuinely disjoint, material questions when its coordination cost is justified.

Reconcile requirements across consumers before scope: a file-message limit
cannot silently replace a different dashboard display limit. When credible
alternatives exist, compare reuse, adaptation, and replacement against the
actual objective, including compatibility, failure handling, resource bounds,
performance, and evidence cost. The smallest diff is not necessarily the
smallest complete solution. Justify new machinery and optional improvements by
a concrete requirement; a larger design is appropriate when the evidence
requires it. Do not manufacture alternatives when only one safe design exists.

Unresolved approach-invalidating feasibility blocks dependent implementation.
Use the smallest sufficient read-only evidence first. If mutation is needed to
resolve feasibility, propose a bounded investigation before committing to the
dependent design. Stop before dependent work if that investigation fails; do
not use queue counters, representative samples, or passing tests as proof of a
different promised bound. A peripheral unknown may remain explicit when the
approved boundaries permit safe work that does not depend on its resolution.

Use the owner-communication rules in `AGENTS.md`. Separate unresolved product
semantics from engineering choices, batch related questions already known to
be necessary, and do not reopen answered questions without material new
evidence. Explain that evidence and its consequences if a decision reopens.

When a durable decision may change or the task originates in troubleshooting,
follow [Codex discovery and record handling](decision-memory.md#codex-discovery-and-record-handling)
before selecting scope. ADR/TSR edits use that same method; Non-trivial work
alone does not require a new record.

### Scope

A Proposed Scope Ledger identifies its version, objective, agreed items,
boundaries, material risks or unknowns, and validation plan. Only explicitly
agreed items may be implemented.

The summary and detailed scope authorize the same work. Supporting detail must
not add executable scope absent from the approval summary. Preserve a complete
reviewable contract, without requiring the owner to judge internal mechanisms.

Challenge the initial proposed scope before approval. After a material scope
revision, challenge the revised scope again before requesting approval. Focus
the repeated challenge on affected areas unless broader assumptions changed.
Repeat until every material finding is resolved or explicitly accepted as a
residual risk; unresolved product decisions and unaccepted material risks block
an approval-ready ledger. A lead recommendation does not accept risk for the
owner. New evidence requires a revised
ledger and exact reapproval when it materially changes what is authorized,
scope boundaries, accepted risk, or required validation. Wording-only changes
do not require a new version. Independent scope review is appropriate only for
High-risk, uncertain, disputed, difficult-to-reverse, or materially
consequential scope.

Briefly state who challenged the scope, whether review was lead-owned or used
a separate independent context, and material findings with their disposition.
A review with no material findings is valid; do not invent findings. This is
evidence disclosure, not a fixed agent-result envelope or a universal subagent
requirement.

Decompose work when real rollback, ownership, uncertainty, or validation
boundaries exist. A bounded coherent change may remain one slice. Broad
refactor-shaped scope is not approval-ready, but no fixed per-slice field schema
is required.

Only exact `Approved vN` for the current ledger authorizes Non-trivial mutation.
Stop and obtain revised approval when required work exceeds the approved scope.

### Scope Challenge Method

Every Non-trivial scope receives the initial and material-revision challenges
required above. Use this deeper method when scope is High-risk, uncertain,
disputed, difficult to reverse, or leaves material residual uncertainty; do not
make every detailed prompt mandatory for every ledger.

Run before presenting the approval token. Inspect the proposed ledger and
relevant current approval, discovery, code, tests, validation, documentation
and decision evidence. Verify material conclusions from generated maps, prior
summaries and other agents instead of treating them as proof.

Ask: `What edge case would make this scope unsafe or incomplete?`

Check the applicable failure surfaces:

- lifecycle, shutdown, cancellation, goroutines, timers and sockets;
- backpressure, queues, drops, disconnects and overload;
- retained state, caches, maps, indexes, bounds and cleanup;
- zero, nil, empty, malformed and boundary inputs;
- YAML, config, schema, defaults and sentinel values;
- parsers, protocols, compatibility and user-visible behavior;
- metrics, logs, latency, p99 and performance claims;
- tests, race checks, fuzzing, benchmarks and profiling;
- documentation, support routing and ADR/TSR obligations; and
- shared interfaces, semantic callers and dependency impact.

Classify each material issue as covered, explicitly out of scope and safe to
defer, requiring a revised ledger before approval, or a validation,
documentation, support or decision follow-up. The lead must disposition every
material finding. Apply the Scope section's material-revision challenge and
unresolved-decision gates after revisions, focusing on affected areas unless
wider assumptions changed.

Use a separate non-steered read-only reviewer when credible challenge evidence
depends on reasoning outside the lead's accumulated assumptions; otherwise the
method may remain lead-owned. A separate reviewer supplies findings only and
never transfers approval or scope authority. State inspected evidence, unknowns,
material findings and disposition without a fixed result envelope. Disclose
unavailable or failed required independent review; a fresh lead pass is not
independent evidence.

## Risk Routing

Standard and High-risk are internal routes. Mention the route only when it helps
the user understand a material decision. Uncertainty that could affect safety,
scope, validation, or compatibility is High-risk until resolved.

### Standard

Use lead-owned discovery, scope challenge, implementation, review, and
touched-surface validation. Do not invoke specialists, parallel discovery, or
independent agents merely because work is Non-trivial.

### High-risk And Specialist Triggers

Load a specialist only for its concrete positive trigger:

| Risk | Trigger | Do not trigger merely because |
| --- | --- | --- |
| Requirements ambiguity | Current evidence still permits materially different product, operator, compatibility, failure, or classification semantics | Requirements are detailed or the task is Non-trivial |
| Scientific/model oracle | Correctness or claims depend on scientific/model definitions, units, boundaries, interpolation, classifications, or normative evidence | Domain terminology appears without changing semantics or claims |
| Design challenge | Two or more viable architectures materially differ in ownership, lifecycle, state, compatibility, or operability | One safe design follows from settled constraints |
| Test-strategy adversary | The planned evidence may not falsify a broken design or checker | The accepted checks directly prove a mechanical change |
| Scope adversary | Use the [Scope Challenge Method](#scope-challenge-method) for High-risk, uncertain, disputed, difficult-to-reverse scope or material residual uncertainty | Every Non-trivial ledger exists |
| Go code-quality review | Use [Substantial Go Review Method](review-checklist.md#substantial-go-review-method) when Go work is High-risk or substantial; the repo skill is its discovery entry point | Every Non-trivial Go edit exists |
| Code walk | Current behavior is unfamiliar or crosses packages and cannot be established locally | A known localized path is already understood |
| Blast-radius audit | Shared or semantic impact remains uncertain | Text search already establishes a local bounded impact |
| Config, lifecycle, leaks, retained state, hot path | The touched surface matches the skill's engineering risk | The task is generally important |

Substantial Go applies when any of these is true: High-risk classification; a
shared or exported interface changes; an algorithm or state machine changes
materially; a production file is substantially rewritten; or meaningful
uncertainty remains after implementation. Changes to multiple production
packages also trigger the Go code-quality review method when shared behavior,
ownership, interfaces, contracts, or meaningful cross-package uncertainty are
affected.
Line count alone does not determine substantiality.

Retained specialist skills preserve their unique engineering methods and
failure checks. Trigger narrowing, reporting simplification, or execution
context selection must not remove normative model derivation, independent
golden vectors, ambiguity analysis, design comparison, falsifiability analysis,
code-path walking, blast-radius analysis, or domain engineering checks.

## Subagents When Used

Subagents are risk-triggered tools, not default workflow stages. The repository
owner provides standing authorization when active platform policy permits, so a
task prompt need not repeat that authorization. Standing authorization does not
require use, expand scope, bypass approval, authorize pre-approval edits, or
transfer lead authority.

Use the lead for localized, already-understood specialist work where delegation
adds little value. When a triggered specialist investigation materially
benefits from context partitioning, use a bounded subagent when supported so
broad evidence or specialist reasoning does not unnecessarily consume or
contaminate the lead context. Require a separate non-steered context only when
same-context reasoning would compromise the credibility of the evidence.

When preferred delegation is unavailable, disclose that limitation; the lead
may perform a genuinely fresh pass and must not describe it as independent.
When required independent context is unavailable, pause or proceed only with
explicit user approval and clearly limit the affected claim. The lead verifies
material evidence, dispositions findings, resolves conflicts, and retains all
workflow authority without duplicating the complete delegated investigation.
No execution-mode field or routine narration is required.

Give each agent a bounded material question and a completion condition. Reuse
valid findings, wait for meaningful results rather than repeatedly polling,
and avoid overlapping discovery or repeated reviews without changed evidence.
Do not impose arbitrary reasoning, testing, agent, time, or token caps.

- Before approval, an agent may inspect and report findings but may not edit,
  propose diffs, format, generate artifacts, or run closeout suites.
- After approval, a worker needs an approved disjoint objective, allowed paths,
  targeted checks, and stop conditions for overlap, hidden blast radius, or
  failed assumptions. Add base or forbidden paths when they matter.
- Findings never replace lead judgment. Report only material use, failure, or
  residual gaps; no fixed result envelope is required.

## Implementation

Before editing, inspect the branch, worktree, approved baseline, and unrelated
changes. Preserve user work.

Implement the smallest correct approved unit. Do not add unapproved
abstractions, refactors, compatibility paths, fallbacks, flags, future-proofing,
or cleanup. Stop when an assumption fails or the blast radius expands.

For code changes, read the universal kernel in `docs/code-quality.md` and only
the risk sections that apply. For first-party YAML or support-critical Go, use
the canonical config/comment rules when those surfaces are actually touched.

For each meaningful unit, review actual source completeness before dependent
work. Check the approved normal and failure paths, compatibility, ownership,
resource bounds, documentation, and relevant performance requirements. Map
obligations to implementation and evidence capable of exposing a mistake;
passing tests, a traceability table, static analysis, or absence of placeholders
is not sufficient alone. Use targeted tests and static checks appropriate to
the unit before advancing. Internal checkpoints do not require new approval
unless authority, scope, accepted risk, or required validation changes.

Reuse suitable permanent tests and retain distinct failure detection under
`docs/code-quality.md`. Do not repeat semantic matrices or build a new runner
without a concrete evidence gap. Define relevant performance acceptance and
workloads before claiming a performance result; preserve triggered benchmarks,
profiles, race checks, and scientific or model evidence.

## Reassessment

Reassess before expanding an approach when feasibility fails, discoveries
materially change scope or risk, or repeated redesign, rediscovery, or
coordination produces no useful progress. Identify the failed assumption,
still-valid evidence, and smallest sound path forward. Explain consequences
in plain language; do not continue broad implementation on a failed premise.
Continue diagnosis and safe work already covered by approval. Resolve reopened
policy and obtain revised approval only for a material change to authority,
scope, accepted risk, or required validation. Preserve unrelated work and leave
an accurate, coherent stopped state if no authorized sound path remains.

Do not trade required reasoning or evidence for a smaller apparent cost. First
completed delivery requires the approved obligations and evidence; it is not a
promise that development has no failed checks, fixes, or reruns.

## Review And Validation

Use `docs/review-checklist.md` for the final-diff review and
`docs/dev-runbook.md` for the touched-surface lane.

Run targeted checks during meaningful implementation units. Run the complete
selected lane once on the final relevant state. If review causes changes,
rerun affected targeted checks. Rerun the full lane only when the fix can
invalidate broader results, including shared behavior, build configuration,
interfaces, concurrency, or cross-package contracts.

High-risk work receives a fresh final verification pass against the approved
scope, final diff, selected validation, claims, and decision disposition. That
pass may be lead-owned. A fresh lead pass is not independent review; use a
separate non-steered context when the credibility of required evidence depends
on independence.

Report material commands and results once. Include rationale or excerpts only
for failures, skips, waivers, surprising behavior, high-risk command-backed
claims, benchmarks, profiles, or runtime evidence. Never promote static
reasoning into test or runtime confirmation.

Classify failures using evidence: introduced defect, relevant inherited defect,
unrelated failure, or invocation/environment error. If causation is unresolved,
say so. A successful retry alone does not establish that a failure is unrelated
or harmless. Correct setup before repeating expensive checks, use the selected
native runner and platform prerequisites, preserve failed evidence, and rerun
the evidence affected by a fix. Explain the practical consequence, corrective
action, and remaining limitation instead of asking the owner to diagnose logs.
Do not silently expand product scope to fix an unrelated failure or waive a
required check. Retain raw evidence; summarize ordinary output and inspect
relevant failures without flooding context with entire logs.

Existing push-to-main GitHub CI, its path filters and manual triggers, and the
nightly race schedule remain in place. Local validation follows the touched
surface; post-push CI does not replace it. Workflow-only edits do not remove or
skip configured GitHub checks. See `docs/dev-runbook.md` for commands.

## Documentation, Decisions, And Closeout

Update authoritative, user, operator, or support documentation only when the
change affects it.

For every Non-trivial change, explicitly decide `Support-agent docs impact:
Required | Not required`. It is required when operator-support topics change,
including commands, HELP, filters, modes, EVENT families, glyphs, diagnostics,
YAML or config surfaces, defaults, sentinel values, logging, observability,
troubleshooting, startup, service, deployment, source, ingest, peer, or
connection behavior. When required, inspect and update the relevant `customgpt/`
routing and support files without duplicating authoritative documentation.

Every Non-trivial closeout considers decision memory. For Codex, create or
update an ADR only when a durable architecture, operations, scientific, or
workflow decision changes. Create or update a TSR when durable troubleshooting
learning exists. Otherwise state the decision disposition in the closeout; do
not create a no-change ADR.

Use [Codex discovery and record handling](decision-memory.md#codex-discovery-and-record-handling)
for the record choice, preserved history, indexes and closeout disposition.

Closeout contains only what is material:

- outcome and material findings, gaps, waivers, or residual risks;
- validation commands/results that establish the claim;
- compact mapping from approved items to changed locations and validation;
- affected documentation and durable decision references when applicable;
- the support-agent documentation impact decision and relevant updates.

No numeric score, fixed headings, marker order, or visible list of irrelevant
checks is required.
