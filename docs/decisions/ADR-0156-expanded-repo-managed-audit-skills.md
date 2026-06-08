# ADR-0156: Expanded Repo-Managed Audit Skills

- Status: Accepted
- Date: 2026-06-08
- Decision Origin: Design

## Context
ADR-0155 made `codex-skills/` the authoritative project skill source for
gocluster work. A follow-up read-only skill coverage audit found that the
current bundle was strong for Go code walking, blast radius, config contracts,
leak detection, retained state, hot paths, pprof, and security work, but three
recurring workflow and operational audit surfaces were still under-covered:

- long-lived connection lifecycle behavior such as reconnect, retry/backoff,
  keepalive, silent-stall detection, shutdown, and operator diagnostics
- mandatory ADR/TSR handling for Non-trivial and troubleshooting work
- drift between `AGENTS.md`, detailed workflow docs, templates, validation
  rules, runbooks, repo-managed skills, and workflow scripts

The connection-lifecycle gap was reinforced by a live GitHub bug report about
`human_telnet` not recovering after initial connection failure or mid-stream
drop. This ADR records the skill and workflow-routing decision only; it does
not fix that runtime bug.

## Decision
Add three repo-managed audit skills:

- `go-connection-lifecycle-audit`
- `decision-memory-audit`
- `workflow-contract-audit`

Route them from `AGENTS.md` and `docs/change-workflow.md`, and add evidence
fields to the Non-trivial change template where the audits can be reported.

Keep the skills concise, SKILL.md-centered, and directly checked into
`codex-skills/`. Do not reintroduce any user-level skill copy/install workflow.

## Alternatives considered
1. Fold connection lifecycle into `go-leak-detection`.
   - Rejected because leak detection does not fully cover data-stream liveness,
     reconnect parity, silent-zero-data modes, keepalive-vs-recovery semantics,
     or operator diagnostics.
2. Leave ADR/TSR handling only in `docs/decision-memory.md`.
   - Rejected because the policy is mandatory and recurring enough to deserve
     a triggerable skill with closeout expectations.
3. Leave workflow drift only as prose in `docs/change-workflow.md`.
   - Rejected because edits to workflow contracts and repo-managed skills are
     frequent enough that a dedicated trigger improves consistency.
4. Add release and PowerShell audit skills at the same time.
   - Rejected as lower-priority scope until release or script churn proves they
     need separate triggerable workflows.

## Consequences
### Benefits
- Connection lifecycle work has a named audit path before runtime fixes.
- Non-trivial and troubleshooting closeouts have a triggerable ADR/TSR audit.
- Workflow edits and repo-managed skill edits get a reusable drift check.
- A fresh checkout carries the expanded workflow with the repository.

### Risks
- Skill triggers can overlap. Agents must compose the smallest applicable set
  instead of loading every adjacent workflow.
- Workflow-skill additions can create process weight if used mechanically
  without inspecting current source and docs.
- The connection lifecycle skill may reveal runtime bugs, but this ADR does not
  approve fixing them.

### Operational impact
- No runtime, telnet, ingest, peer, parser, config, archive, queue, shutdown,
  or user-visible behavior changes.
- Future connection lifecycle work may require additional targeted lifecycle
  tests, race checks, and operator-diagnostic review when the new skill
  triggers.
- Future workflow and skill edits include an explicit workflow-drift audit.

## Links
- Related issues/PRs/commits: GitHub issue #4,
  `https://github.com/N2WQ/GoCluster/issues/4`
- Related tests:
  - `scripts/verify-codex-skills.ps1`
  - skill-creator `quick_validate.py` for the new skills
  - targeted workflow text checks
  - standard Non-trivial validation commands from `docs/dev-runbook.md`
- Related docs: `AGENTS.md`, `docs/change-workflow.md`,
  `docs/templates/non-trivial-change-template.md`, `codex-skills/README.md`,
  `codex-skills/go-connection-lifecycle-audit/SKILL.md`,
  `codex-skills/decision-memory-audit/SKILL.md`,
  `codex-skills/workflow-contract-audit/SKILL.md`
- Related TSRs:
- Supersedes / superseded by:
