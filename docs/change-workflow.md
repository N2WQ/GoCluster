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

Use code maps and generated graphs only as orientation. Verify material
conclusions against current source and tests. Use parallel discovery only for
genuinely disjoint, material questions when its coordination cost is justified.

### Scope

A Proposed Scope Ledger identifies its version, objective, agreed items,
boundaries, material risks or unknowns, and validation plan. Only explicitly
agreed items may be implemented.

Challenge the proposed scope before approval. New evidence requires a revised
ledger and exact reapproval when it materially changes what is authorized,
scope boundaries, accepted risk, or required validation. Wording-only changes
do not require a new version. Independent scope review is appropriate only for
High-risk, uncertain, disputed, difficult-to-reverse, or materially
consequential scope.

Decompose work when real rollback, ownership, uncertainty, or validation
boundaries exist. A bounded coherent change may remain one slice. Broad
refactor-shaped scope is not approval-ready, but no fixed per-slice field schema
is required.

Only exact `Approved vN` for the current ledger authorizes Non-trivial mutation.
Stop and obtain revised approval when required work exceeds the approved scope.

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
| Scope adversary | Scope is High-risk, uncertain, disputed, difficult to reverse, or leaves material residual uncertainty | Every Non-trivial ledger exists |
| Go code-quality review | Go work is High-risk or substantial | Every Non-trivial Go edit exists |
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

## Documentation, Decisions, And Closeout

Update authoritative, user, operator, or support documentation only when the
change affects it. Do not emit mandatory impact fields for unaffected surfaces.

Every Non-trivial closeout considers decision memory. For Codex, create or
update an ADR only when a durable architecture, operations, scientific, or
workflow decision changes. Create or update a TSR when durable troubleshooting
learning exists. Otherwise state the decision disposition in the closeout; do
not create a no-change ADR.

Closeout contains only what is material:

- outcome and material findings, gaps, waivers, or residual risks;
- validation commands/results that establish the claim;
- compact mapping from approved items to changed locations and validation;
- affected documentation and durable decision references when applicable.

No numeric score, fixed headings, marker order, or visible list of irrelevant
checks is required.
