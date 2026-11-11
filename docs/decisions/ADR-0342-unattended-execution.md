# ADR-0342: Repo-Owned Unattended Execution Skill

- Status: Accepted
- Date: 2026-11-11
- Decision Origin: Design; owner-approved Scope Ledger v1 for this skill

## Context

The owner requested a reusable skill for the planning and execution method used
in the 2026-11-10 overnight correctness run. That run used an approved bounded
queue, explicit dependencies, targeted validation and checkpoints, a reserve for
final integrated validation, and an evidence-bearing handoff. It rejected an
optional optimization when measurements did not establish its acceptance and
repaired an introduced startup regression before rerunning the final lane.

The completed run is evidence of that execution, not proof of general
unattended reliability. Reusing its literal approval, time limits, test counts
or production scope would be unsafe. The repository already owns approval,
specialist methods, source review and validation through established contracts.

## Decision

Add `.agents/skills/unattended-execution/` as a repo-owned Codex skill with a
short planning/execution/resumption method, an adaptable ledger template and UI
metadata. Register it in the existing skill catalog. Normal implicit discovery
remains available for requests that match its unattended multi-slice trigger.

The skill coordinates work under existing authority rather than creating a new
approval route. `AGENTS.md` continues to own approval and scope;
`docs/change-workflow.md` owns engineering execution; the dev runbook owns
validation selection; specialists retain their positively triggered methods.
Planning stays read-only until artifact creation and mutation are authorized.

The reusable additions are explicit queue dependencies and safe independent
continuation, one writing slice at a time, progress and recovery evidence,
predefined experiment acceptance, and a final-validation reserve sized to the
actual run. A failed slice can be deferred only when the retained subset is
coherent and safe. Restoration requires attributable, separable edits. New
owner policy is recorded for later resolution rather than invented during the
owner's absence. No new work is added merely to consume the available window.

The skill records execution prerequisites and uncertainty. It does not create
a persistent executor, scheduler, authentication, host/firewall changes or
publication authority. The historical eight-hour window and two-hour reserve
remain examples. A selected final lane that cannot finish remains an unmet
obligation, even when targeted checks passed.

## Alternatives Considered

1. Keep only the historical overnight ledger. This preserves one run but makes
   later agents rediscover which parts are reusable and which are historical.
2. Add the orchestration detail to always-loaded `AGENTS.md` or duplicate the
   canonical specialist methods. This increases unrelated context and creates
   competing owners for existing rules.
3. Add a scheduler or executable queue controller. This is unnecessary for
   capturing the method and would introduce separate operational authority,
   failure handling and maintenance obligations.

## Consequences

### Benefits

- The workflow follows the repository and has an identifiable discovery trigger.
- Approved queues can preserve dependency, acceptance and recovery information
  across unattended slices and interruptions.
- Rejected work and incomplete validation remain explicit in the handoff.

### Risks

- The skill and template add a small maintenance surface alongside their
  canonical contract references.
- Metadata checks and read-only scenario trials cannot establish future
  long-duration reliability, host availability or live discovery by a client.
- Performance and engineering-quality improvements remain unmeasured hypotheses
  until supported by comparable usage evidence.

### Operational Impact

This is a Codex workflow/skill documentation change. Runtime code, production
services/data, CI configuration, Fable-owned rules, scheduling, credentials and
publication behavior are unchanged. Support-agent docs impact: Not required;
operator-support contracts are unchanged.

## Links

- Related tests: skill quick validator; `scripts/verify-codex-skills.ps1`;
  `scripts/check-workflow-contract.ps1`; `scripts/test-workflow-contract.ps1`;
  targeted metadata/reference checks and read-only fresh-context scenario trials.
- Related docs: [skill](../../.agents/skills/unattended-execution/SKILL.md),
  [ledger template](../../.agents/skills/unattended-execution/references/ledger-template.md),
  [historical overnight ledger](../codebase-correctness-overnight-2026-11-10.md),
  [change workflow](../change-workflow.md), [dev runbook](../dev-runbook.md).
- Related ADRs: [ADR-0155](ADR-0155-repo-authoritative-codex-skills.md),
  [ADR-0221](ADR-0221-codex-authority-and-evidence-workflow.md),
  [ADR-0336](ADR-0336-codex-method-ownership-and-retained-bounds.md).
- Related TSRs: none; this records a reusable workflow decision.
- Supersedes / superseded by: none; existing authority and methods are retained.
