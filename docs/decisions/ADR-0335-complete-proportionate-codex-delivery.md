# ADR-0335: Complete Proportionate Codex Delivery

- Status: Accepted
- Date: 2026-11-10
- Decision Origin: Workflow review; Approved v2

## Context

Review of logging commit `3838ad631948f338ca209edc094aadd7cbeed870` and its
original session found seven scope versions, repeated owner questions,
substantial agent coordination, late redesign and an unresolved resource proof.
The [logging evidence](../shared-logging-v3-validation.md) distinguishes passed
checks from the unproved bound. This episode does not establish that all of its
changes were unnecessary, that tests caused the failures, or that a different
model would have prevented them.

The owner's primary goal is complete, correct, high-performance code at the
first completed delivery. Rigor and reasoning remain necessary, while decisions
must be understandable without requiring the owner to supervise engineering.
The owner explicitly selected risk-based independent review and retained direct
pushes to main with the existing GitHub CI checks.

## Decision

Refine ADR-0221's communication and execution policy and ADR-0224's treatment
of unresolved feasibility. Preserve exact approval, material reapproval,
specialist methods, scientific independence, risk-based review, production
validation lanes and Fable ownership.

1. Put a concise plain-language explanation ahead of engineering detail. The
   summary and full scope authorize the same work. Reuse owner decisions and
   evidence before asking behavior/risk questions; external recommendations
   are not owner selections. The agent owns engineering and diagnosis.
2. Compare genuinely viable reuse, adaptation and replacement. Justify new
   machinery against requirements and preserve established behavior unless the
   objective or a demonstrated defect requires change. A smaller diff must not
   displace a necessary larger design.
3. Resolve approach-invalidating feasibility before dependent implementation.
   Use read-only evidence first; approve a bounded investigation when mutation
   is necessary. Stop dependent work on failure. Peripheral unknowns may remain
   explicit if safe approved work does not depend on their resolution. This
   selectively refines ADR-0224 Decision 2 without requiring all unknowns to be
   eliminated or permitting pre-approval edits.
4. Review actual source completeness and relevant failure, compatibility,
   resource and performance contracts for meaningful units before dependent
   work. Preserve targeted evidence and the complete final lane. Tests, maps
   and static checks alone are not completeness proof.
5. Diagnose failures using evidence, preserve failed results, correct setup and
   rerun affected evidence. A successful retry alone does not prove a failure
   unrelated or harmless. Do not silently expand scope or waive required checks.
6. Reassess failed feasibility or repeated unproductive expansion. Reuse valid
   evidence and bounded agent assignments without arbitrary resource caps.
   Continue safe authorized work; reapprove only material authority, scope,
   accepted-risk or validation changes.
7. Disclose review identity, independence and material outcomes compactly.
   Challenge material scope revisions as required by ADR-0331; do not introduce
   universal additional reviewers or fixed response envelopes.

`AGENTS.md` owns concise authority and communication; `docs/change-workflow.md`
owns execution detail; the existing template illustrates presentation; the
review checklist and validation contract own their respective outcomes. The
three affected skills retain their specialist methods. Existing shared code
quality, permanent-testing policy, local command lanes, push-to-main CI/path
filters/manual triggers and nightly race schedule are unchanged.

## Alternatives Considered

1. Add only a shorter approval card: leaves design, feasibility and diagnosis
   failures untreated. Retain the summary, but align it with the full contract.
2. Add universal independent agents or hard token/time limits: no demonstrated
   benefit from this incident; can multiply coordination or weaken necessary
   reasoning. Retain risk-based review and evidence requirements.
3. Build a new coordinator, hook system or evaluation runner now: introduces
   additional configuration, ownership and validation before the core contract
   is coherent. Keep tooling proposals separate below.
4. Force reuse and minimize changed files: can preserve an unsafe design.
   Compare actual requirements and evidence rather than file-count targets.

## Consequences

### Benefits And Costs

The intended benefit is better first-delivery completeness and less discarded
work. Neither quality improvement nor token savings has been measured.
Development still includes failures, fixes and reruns; zero defects cannot be
guaranteed. Instruction compliance remains necessary because this change adds
no permission enforcement.

One-time work is a coherent 16-file workflow/skill/checker change, this single
decision record, six policy walkthroughs and verification. Recurring work makes
existing feasibility, completeness, diagnosis and review responsibilities more
explicit. Hard tasks can require more investigation. No extra approval is
required per checkpoint; no universal additional agent, scorecard or mandatory
scenario execution is introduced. Detailed evidence is retained once rather
than repeated in owner-facing reports.

### Risks And Operational Impact

The rules still require engineering judgment. Over-triggering investigation
can recreate overhead; skipping critical proofs can repeat the logging failure.
Use the smallest sufficient evidence without weakening required reasoning.
Static fixtures establish representable contract consistency, not real agent
compliance or efficiency. The six walkthroughs review policy, not agent behavior.

No production code, runtime config, GitHub workflow, user settings or Fable-owned
files change. Direct-to-main operation and existing hosted checks remain in
place. Support-agent docs impact: Not required; runtime/operator behavior and
the inspected developer routing destinations remain unchanged.

## Deferred Proposals

These are recorded proposals, not authorized work, a scheduled backlog, or
promises of savings. Each needs separately bounded scope if pursued.

| Proposal | Reason for deferral |
| --- | --- |
| Hooks and read-only permission enforcement | Test the actual tool boundaries and client behavior separately. |
| Mandatory Plan mode | A planning aid cannot establish design correctness or approval compliance. |
| Live behavioral smoke tests or evaluation framework | Bound the cases, permissions, evidence and model usage before execution. |
| PR-triggered CI and branch protection | Changes the owner's direct-to-main method; current CI remains. |
| Repository map and command pointers in AGENTS.md | Assess value without duplicating authoritative commands. |
| Model and reasoning-effort table | Requires comparisons on representative tasks rather than a universal prescription. |
| Existing test reductions and mutation-testing trials | Identify redundancy and retained unique protection in bounded scopes. |
| Automated token measurement | Verify usage accounting and start with a small pilot. |
| Copilot/Kiro cleanup and duplicate-skill synchronization | Review ownership, applicability and compatibility separately. |
| File/function-size or complexity lint warnings | Calibrate useful warnings; existing soft size-review triggers remain. |
| Named reviewer definition, model/effort and VS Code visibility checks | Verify configuration and real execution separately; metadata is not independence proof. |
| Compact validation-lane wrapper | Assess existing runner summaries first; preserve raw logs, failure status and relevant diagnostics. |
| Cheaper-model explorer evaluation | Compare narrow tasks before adopting a model policy. |
| GitHub-specific review guidance | Confirm applicability to direct-to-main operation separately. |

## Implementation And Validation Record

Baseline: `471bb112d28b3f84c72efd2f7e2f8d8522f7ec06`. Exact `Approved v2`
preceded mutation. A separate read-only pre-approval reviewer identified the
need for an explicit pre-edit falsifiability gate; it was added and the revised
scope was challenged again without remaining material findings.

Before the first edit, the lead reviewed this contract-to-test matrix against
the existing checker and fixture methods. No material scope gap remained.
This was lead-owned analysis, not another independent review.

| Approved obligation | Plausible failure / stimulus | Observable evidence and checker | Owner |
| --- | --- | --- | --- |
| Plain decisions, no hidden scope | Keep summary wording while permitting extra executable detail or handoff-selected policy | Source review plus missing/contradictory/permitted policy fixtures; answered-question walkthrough | AGENTS, template, ambiguity skill; existing PowerShell fixtures |
| Design and feasibility | Treat counters as a complete bound; permit dependent work on unresolved feasibility; forbid safe peripheral unknowns | Feasibility removal/inversion/boundary fixtures; limit, proof and machinery walkthroughs | Workflow discovery, design/test skills; checker and fixtures |
| Source completeness and performance | Declare acceptance from tools alone or omit failure/resource/performance paths | Review actual source-review obligations and preserved production lanes; missing/contradictory/permitted fixtures | Workflow, review checklist, VALIDATION, test skill; checker and fixtures |
| Failure diagnosis | Successful retry labels failure unrelated; waive evidence or silently fix unrelated code | Retry removal/inversion/boundary fixtures and failure walkthrough | Workflow validation; checker and fixtures |
| Controlled expansion | Continue broad work on failed premise, or force new approval for routine checks | Reassessment rule fixtures and legitimate continuation review | Workflow reassessment; checker and fixtures |
| Visible risk-based review | Rename lead review independent or skip material rechallenge | Existing independence/challenge negative controls; disclosure source review and revision walkthrough | Workflow, template, existing fixtures |
| Coherent delivery and preserved CI | Contradict a rule while retaining keywords; weaken hosted checks; claim measured behavior from text | Specific failure labels, permitted expressions, existing CI/ownership guards, skill verification, diff/link review | Existing checker/fixtures and final lead verification |

Selected lane: native PowerShell parsing of edited scripts; workflow checker
against the baseline; existing positive/negative fixtures; repo-skill verifier;
six policy walkthroughs; cross-reference/ownership review, direct final diff,
`git diff --check`, and fresh final verification. No Go lane or code maps are
selected for this workflow/script-only implementation. Hosted CI remains
configured and is not reported passed without observed hosted evidence.

Local implementation validation passed: both edited scripts parsed; the
baseline-aware checker, complete existing fixture suite (including 19 new
missing-rule, contradictory-permission and permitted-wording cases), and
repo-skill verifier passed. The lead reviewed all six policy walkthroughs
against the final rules and their counterexamples. All 31 local Markdown links
in changed documents resolved, the exact 16-path boundary held, and
`git diff --check` passed. A separate read-only final reviewer inspected the
16-file implementation and reported no material findings; that reviewer did
not independently execute checks or verify conversational approval.

These are local contract/source-review results, not live agent evaluations,
production-Go validation, hosted CI results or measured efficiency gains.

## Links

- Refines [ADR-0221](ADR-0221-codex-authority-and-evidence-workflow.md) and
  [ADR-0224](ADR-0224-evidence-before-scope-and-material-reapproval.md).
- Preserves [ADR-0223](ADR-0223-bounded-specialist-context-and-independent-evidence.md),
  [ADR-0316](ADR-0316-proportionate-permanent-testing.md) and
  [ADR-0331](ADR-0331-repeat-material-scope-challenges.md).
- Related checks: `scripts/check-workflow-contract.ps1`,
  `scripts/test-workflow-contract.ps1`, `scripts/verify-codex-skills.ps1`.
- Policy walkthroughs: [workflow cases](../workflow-eval-cases.md).
- Supersedes / superseded by: selective refinements above; not superseded.
