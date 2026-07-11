# ADR-0211: Standing Subagent Request

- Status: Superseded
- Date: 2026-07-09
- Decision Origin: Design

## Context

ADR-0199 defined safe subagent delegation boundaries. ADR-0202 made
independent review agents default-on when supported and not explicitly
prohibited. ADR-0205 later separated unsupported tooling, missing
authorization, and explicit prohibition so Codex would not spawn subagents when
active platform policy required a user request that had not been made.

The repository owner clarified the intended GoCluster workflow: Codex should
not wait for every future task prompt to repeat a subagent request. The desired
default is to use subagents in this repo when the active environment supports
delegated or parallel agent work and active tool/session policy permits
spawning, unless the user explicitly prohibits independent-agent use.

## Decision

Record a standing repository-owner request for Codex to use subagents by
default in this repo.

When active tool/session policy permits that standing request, Codex must not
report `not authorized/not requested` merely because the current task prompt
does not repeat a subagent, delegation, or parallel-agent request.

This standing request is authorization only. It does not approve scope, does
not replace `Approved vN`, does not allow pre-approval subagents to edit files,
does not allow worker subagents before an approved disjoint slice, and does not
override active environment or tool/session policy. If the active environment
does not support subagents, active tool/session policy blocks spawning, the
subagent attempt fails or times out, or the user explicitly prohibits
subagents, Codex must report that evidence status directly.

ADR-0199's phase boundaries, ADR-0202's default-on independent-review roles,
and ADR-0209's explorer/worker type selection remain in force.

## Alternatives considered

1. Keep ADR-0205 unchanged.
   - Rejected because future Codex agents could continue reporting
     `not authorized/not requested` even though the owner has requested default
     subagent use for this repo.
2. Make `AGENTS.md` alone self-authorize all subagent spawning.
   - Rejected because repository text cannot override active environment or
     tool/session policy.
3. Require the user to repeat "use subagents" in every task.
   - Rejected because it defeats the default-on independent-review workflow.
4. Remove the `not authorized/not requested` evidence status.
   - Rejected because some active sessions or platforms may still block
     spawning until authorization exists.

## Consequences

### Benefits

- Future Codex agents no longer wait for a repeated subagent request when
  tooling and active policy permit spawning.
- Explicit user prohibition, unsupported tooling, blocked tool/session policy,
  failures, and timeouts remain separate evidence states.
- Existing approval and phase gates stay intact.

### Risks

- Agents could overread the standing request as scope approval unless the
  workflow continues to emphasize `Approved vN` and worker slice gates.
- Active platform policy may change; Codex must still verify tool availability
  and report blocked spawning honestly.

### Operational impact

- No runtime, config, protocol, parser, telnet, peer, archive, queue,
  persistence, connection, CI, generated-artifact, or operator command behavior
  changes.
- Future workflow responses should treat omitted repeated subagent wording in a
  task prompt as authorized by the standing request when active tool/session
  policy permits spawning and the user has not explicitly prohibited subagents.

## Links

- Related issues/PRs/commits:
- Related tests:
  - targeted workflow text checks
  - workflow-drift audit
  - `scripts/verify-codex-skills.ps1`
  - `git diff --check`
- Related docs: `AGENTS.md`, `docs/change-workflow.md`,
  `docs/WORKING_WITH_CODEX.md`, `VALIDATION.md`,
  `codex-skills/scope-ledger-adversarial-review/SKILL.md`,
  `codex-skills/go-code-quality-review/SKILL.md`,
  `customgpt/source-map.md`, `customgpt/developer-guide-index.md`
- Related ADRs: ADR-0199, ADR-0202, ADR-0205, ADR-0209
- Related TSRs: none
- Supersedes / superseded by: superseded by ADR-0221
