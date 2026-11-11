---
name: unattended-execution
description: "Plan, execute, or resume an approved multi-slice gocluster work queue while the owner is unavailable, including overnight work. Use for unattended planning and execution requests; a long test, ordinary edit, or read-only review alone does not trigger this skill."
---

# Unattended Execution

Prepare enough bounded, reviewable work that execution can proceed while the
owner is away. Use the mode actually requested: plan, execute, or resume.
Planning alone does not authorize implementation.

## Governing Sources

[AGENTS.md](../../../AGENTS.md) owns approval and scope authority.
[Change workflow](../../../docs/change-workflow.md) owns discovery, scope
challenge, meaningful-unit completeness, failure diagnosis and reassessment.
[Dev runbook](../../../docs/dev-runbook.md) owns validation selection, and
[review checklist](../../../docs/review-checklist.md) owns final review.
Apply specialists only for their positive risk triggers; preserve their methods.
This skill adds unattended orchestration under those contracts.

## Plan Before The Owner Leaves

1. Inspect the actual checkout, branch, baseline and unrelated work. Reconcile
   proposed work with current implementation, contracts and prior owner
   decisions. Treat supplied reviews and past run records as evidence rather
   than authority. Keep discovery and the proposed ledger in the response
   until artifact creation and mutation are authorized.
2. Resolve material behavior decisions before proposing executable scope.
   Separate repair obligations from conditional experiments. Choose slices at
   real dependency, rollback, ownership or validation boundaries. Identify
   independent work that can continue if another slice is safely deferred.
3. Define observable acceptance evidence before implementation. Include
   failure detection and relevant performance budgets/workloads rather than
   accepting any green test or faster average. Select each slice's checks and
   the final integrated lane from the actual touched surface and risk.
4. Establish execution prerequisites: usable tools, validation dependencies,
   permissions, required external authentication and known host availability.
   Check what is observable without starting pre-approval closeout suites.
   Explain unresolved prerequisites and their effects before claiming readiness.
   A skill supplies neither a persistent executor nor additional permissions;
   do not install schedulers or change authentication, firewall or host policy
   as an implicit prerequisite fix.
5. Record the requested completion condition and, when supplied, execution
   window and timezone. Reserve enough time for final review, required checks,
   likely reruns and a coherent handoff. Size the reserve to the selected lane;
   eight hours of work and two hours of reserve are historical examples, not
   defaults. Do not invent a deadline when none was requested.
6. Prepare and challenge a versioned Scope Ledger under the governing sources.
   Use [ledger template](references/ledger-template.md) as an adaptable starting
   point, not a required field schema. Include authority for recording progress,
   isolation/checkpoints, conditional experiments and safe deferral. State
   external-action boundaries explicitly. Obtain the matching exact approval
   before Non-trivial mutation; a previous run's approval does not carry over.

## Execute The Approved Queue

After verifying approval and the current baseline, establish the agreed
workspace and durable run record. Use an isolated worktree when it usefully
protects other work. Record the exact path, branch, starting revision,
unrelated changes, approved ledger/version, evidence location and window.
Checkpoints may use commits or another agreed recoverable record; checkpoint
authority is separate from merge, push, release or deployment authority.

For each next eligible slice:

1. Verify prerequisites, dependency results, current ownership and remaining
   closeout time. Start no new slice when doing so threatens required final
   validation. Keep one implementation slice writing at a time; bounded
   read-only investigation or review may overlap when useful and permitted.
2. Derive distinguishing cases from the accepted contract. Establish a failing
   regression where practical, implement the smallest complete approved unit,
   and run its targeted checks. For conditional optimization, compare against
   the pre-agreed acceptance criteria using comparable evidence.
3. Review actual source completeness, including relevant failure, overload and
   retirement paths, before dependent work. Handle triggered documentation and
   decision records under their existing owners. A passing suite or traceability
   table alone does not establish completeness.
4. Checkpoint only a coherent state. Record source revision or owned diff,
   actual commands/results, skips, review findings, costs, remaining limits and
   the next eligible action. Targeted passes remain provisional until the final
   relevant state passes its selected integrated lane.

Diagnose failures within approved scope and preserve failed evidence. If an
optimization misses acceptance, reject it rather than changing the budget.
If new policy, material scope/risk or validation changes require approval,
record the needed decision and defer that slice and its dependents. Continue
safe independent approved work without waiting for the absent owner. Stop the
run if the failed premise affects the whole queue or no authorized path remains.

Restore a failed candidate only when its edits and effects are attributable and
safely separable. Never reset a whole worktree, discard unrelated changes, or
assume a commit boundary proves ownership. When restoration is uncertain,
preserve the state, record the conflict, and avoid dependent or overlapping
mutation. Do not waive checks or weaken tests to advance the queue.

Finish early when the feasible approved queue is complete. Do not add work to
fill the window. If execution is blocked by the host or tools, record the last
observed state and missing evidence; a work window is not a guarantee that an
OS operation or callback will return before its end.

## Resume From Evidence

Read the run record, then inspect the live checkout and tool state. Reconcile
the approved ledger, actual branch/tip, owned/unattributed edits, in-flight
operations and recorded evidence before writing or reverting anything. A
saved "next action" is a hypothesis until these match. Reuse checks only when
their relevant source, inputs and environment remain applicable.

Recover a clean checkpoint or a safe next action without overwriting other
work. If approval, ownership or prerequisite state cannot be established,
record what is missing and defer affected work. Material changes return to
the governing reapproval route; safe independent approved work may continue.

## Close Out

Stop starting slices at the reserve boundary. Review the retained result and
run the complete selected lane on the final relevant state. Diagnose failures,
make authorized fixes and rerun affected evidence under the existing workflow.
When the required lane cannot finish, leave a coherent state and explicitly
report incomplete validation; do not promote targeted passes to final success.

Deliver the validated subset with deferred/rejected items, exact checkout and
revision or remaining diff, evidence locations, material failures/skips,
performance costs and residual uncertainty. Identify any pending owner decisions
and the next safe action. Distinguish local changes from externally published
results and execute only external actions actually authorized for this run.

## Historical Example

The [2026-11-10 overnight ledger](../../../docs/codebase-correctness-overnight-2026-11-10.md)
shows dependency-aware deferral, a rejected performance experiment and repaired
full-lane failure. Its scope, approval, commands, timings and counts describe
that run. They do not authorize new work or establish present validation.
