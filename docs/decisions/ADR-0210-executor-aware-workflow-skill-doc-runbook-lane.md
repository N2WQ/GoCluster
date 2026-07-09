# ADR-0210: Executor-Aware Workflow Skill Doc Runbook Lane

- Status: Accepted
- Date: 2026-07-09
- Decision Origin: Design

## Context

`docs/dev-runbook.md` is the shared checker source for Non-trivial closeout.
Codex reaches it through `AGENTS.md`; Fable reaches it through `CLAUDE.md`.
After the Fable-native workflow contract landed, the runbook's workflow/skill-
doc lane still named Codex guidance, repo-managed skills, and
`codex-skills/**/agents/openai.yaml` but did not name Fable's workflow
surfaces.

That mismatch created checker-source drift: Fable was required to use the
shared runbook, while the runbook's specialized workflow lane did not explicitly
cover `CLAUDE.md`, `docs/fable-workflow.md`, Fable review/validation/template
files, `.claude/agents/*.md`, or `.claude/skills/**/SKILL.md`.

## Decision

Make the workflow/skill-doc lane in `docs/dev-runbook.md` executor-aware.

The lane now names Codex and Fable workflow surfaces separately, preserves
Codex-specific skill checks, and adds Fable-specific frontmatter, tool-grant,
and skill frontmatter/body reviews for `.claude/agents|skills/*`.

This decision changes checker-source wording only. It does not change approval
gates, validation scoring, templates, subagent authorization, ADR handling, or
runtime behavior.

## Alternatives considered

1. Leave the runbook Codex-oriented and rely on `docs/fable-workflow.md`.
   - Rejected because `CLAUDE.md` requires `docs/dev-runbook.md` as the shared
     checker source; the shared source should not omit Fable workflow surfaces.
2. Add a separate Fable runbook.
   - Rejected because Fable and Codex intentionally share codebase-level
     checker standards where the mechanics are executor-neutral.
3. Bundle this with Codex's stale `AGENTS.md` TSR template pointer fix.
   - Rejected because that is a separate Codex-only routing correction already
     called out as separate follow-up risk by ADR-0208.

## Consequences

### Benefits

- The shared runbook now covers both executor workflow families.
- Codex skill metadata checks remain explicit.
- Fable agent frontmatter and tool-grant checks are routed through the same
  workflow/skill-doc validation lane Fable already depends on.

### Risks

- The lane is broader, so future agents must still select only the checks that
  apply to the touched executor surfaces.
- The separate `AGENTS.md` stale TSR template pointer remains unresolved until a
  later Codex-only slice.

### Operational impact

- No runtime, config, protocol, parser, telnet, peer, archive, queue,
  persistence, connection, CI, generated-artifact, or operator command behavior
  changes.
- Documentation-only workflow validation remains sufficient for this change.

## Links

- Related issues/PRs/commits:
- Related tests: targeted workflow text checks, workflow-drift audit, reviewer
  diff pass, `git diff --check`
- Related docs: `docs/dev-runbook.md`, `AGENTS.md`, `CLAUDE.md`,
  `docs/change-workflow.md`, `docs/fable-workflow.md`,
  `docs/review-checklist.md`, `docs/fable-review-checklist.md`,
  `VALIDATION.md`, `docs/fable-validation.md`,
  `docs/templates/non-trivial-change-template.md`,
  `docs/templates/fable-non-trivial-change-template.md`, `codex-skills/**`,
  `.claude/agents/*.md`, `.claude/skills/**/SKILL.md`
- Related ADRs: ADR-0206, ADR-0208, ADR-0209
- Related TSRs:
- Supersedes / superseded by: none
