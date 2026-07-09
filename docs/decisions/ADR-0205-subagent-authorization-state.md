# ADR-0205: Subagent Authorization State

- Status: Accepted
- Date: 2026-07-09
- Decision Origin: Design

## Context

ADR-0199 introduced subagent delegation boundaries using both environment and
user authorization as prerequisites. ADR-0202 later made independent review
agents default-on when supported and not explicitly prohibited.

Some Codex surfaces expose subagent tooling but only permit spawning when the
user explicitly asks for subagents, delegation, or parallel agent work. The
repository's default-on wording did not model that platform authorization
state. That could cause either a tool-policy violation or a false validation
failure when a tool exists but the active session has not authorized spawning.

## Decision

Independent agents remain default-on workflow controls, but only when all
required gates are true:

1. The active environment supports delegated or parallel agent work.
2. Active tool policy and user/session authorization permit spawning subagents.
3. The user has not explicitly prohibited independent-agent use.

If active tool policy requires an explicit user request, the absence of that
request is reported as `not authorized/not requested`. It is not `unsupported`
and it is not `explicitly prohibited`.

Repository policy, `AGENTS.md`, or prior ADR language does not self-authorize
subagent spawning when the active platform requires an explicit user/session
authorization. Exact `Approved vN` approves the Scope Ledger; it is not by
itself an explicit subagent request when the active platform requires one.

When authorization exists, keep the role requirements from ADR-0202:

1. Use `scope-ledger-adversarial-review` before presenting an approval token
   for Non-trivial Scope Ledgers.
2. Use `go-code-quality-review` after Non-trivial Go implementation work is
   written and before final closeout.
3. Use read-only fresh-verifier explorers for high-risk closeout.
4. Treat independent-agent findings as evidence only; final workflow ownership
   remains with the lead Codex agent.

## Alternatives considered

1. Keep ADR-0202 wording unchanged.
   - Rejected because it does not distinguish supported tooling from authorized
     tooling.
2. Treat missing platform authorization as unsupported.
   - Rejected because the tool can exist and still be unusable until the user
     authorizes it.
3. Treat missing platform authorization as explicit user prohibition.
   - Rejected because silence is not a prohibition; it is a separate
     authorization state.
4. Make repo policy itself an authorization source.
   - Rejected because repository workflow docs cannot override active platform
     tool policy.

## Consequences

### Benefits

- Preserves default-on independent review where the active environment permits
  it.
- Prevents accidental subagent spawning when the platform requires an explicit
  user request.
- Prevents validation auto-fails when subagent tooling is present but not
  authorized for the current session or phase.
- Gives templates and closeout markers a precise evidence status:
  `not authorized/not requested`.

### Risks

- Agents must evaluate subagent authorization per phase/use rather than once
  globally.
- If agents overuse `not authorized/not requested`, they could weaken the
  default-on quality bar. Validation must still fail omitted independent review
  when subagents were supported, authorized, and not explicitly prohibited.

### Operational impact

- No runtime, config, protocol, parser, telnet, peer, archive, queue,
  persistence, connection, CI, generated-artifact, or operator command behavior
  changes.
- Future workflow responses should report independent-agent support,
  authorization, prohibition, failure/timeout, waiver, and lead-owned
  disposition separately.

## Links

- Related issues/PRs/commits:
- Related tests:
  - `scripts/verify-codex-skills.ps1`
  - targeted workflow text checks
  - targeted support-agent routing text checks
  - `git diff --check`
- Related docs: `AGENTS.md`, `docs/change-workflow.md`,
  `docs/review-checklist.md`, `docs/dev-runbook.md`, `VALIDATION.md`,
  `docs/templates/non-trivial-change-template.md`,
  `docs/WORKING_WITH_CODEX.md`,
  `codex-skills/scope-ledger-adversarial-review/SKILL.md`,
  `codex-skills/go-code-quality-review/SKILL.md`,
  `customgpt/source-map.md`, `customgpt/developer-guide-index.md`,
  `customgpt/common-questions.md`
- Related ADRs: ADR-0199, ADR-0202, ADR-0203, ADR-0204
- Related TSRs: none
- Supersedes / superseded by: clarifies ADR-0202 default-on independent-agent
  behavior with ADR-0199's authorization prerequisite
