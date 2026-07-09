# ADR-0209: Codex Subagent Type Selection

- Status: Accepted
- Date: 2026-07-09
- Decision Origin: Design

## Context

ADR-0202 made independent review agents default-on workflow controls when
supported and authorized. ADR-0205 clarified that repository policy does not
self-authorize spawning when the active platform requires an explicit user or
session request.

The Codex platform can expose typed subagents. The workflow already calls
`scope-ledger-adversarial-review`, `go-code-quality-review`, and
fresh-verifier roles read-only explorers, but it did not explicitly map those
read-only review roles to the Codex `explorer` agent type. That left room for a
lead agent to choose a generic or worker role for evidence-only review.

## Decision

When the active Codex platform exposes typed subagents, spawn read-only
independent review roles as `explorer` agents:

1. Use an `explorer` for `scope-ledger-adversarial-review` before presenting a
   Non-trivial approval token when supported, authorized, and not explicitly
   prohibited.
2. Use an `explorer` for `go-code-quality-review` after Non-trivial Go
   implementation work is written and before final closeout when supported,
   authorized, and not explicitly prohibited.
3. Use an `explorer` for high-risk fresh-verifier closeout review when
   supported, authorized, and not explicitly prohibited.
4. Reserve `worker` agents for approved post-approval implementation slices
   with explicit write scope, allowed paths, forbidden paths, stopping
   conditions, targeted checks, and lead-owned integration.

This decision does not change ADR-0205 authorization semantics. If the active
platform requires an explicit subagent request and that request is absent, the
status remains `not authorized/not requested`.

## Alternatives considered

1. Leave the workflow with only generic "explorer" wording.
   - Rejected because typed Codex platforms make agent-type selection an
     operational choice, and the read-only roles should not be spawned as
     workers.
2. Require custom specialist subagent types for each review role.
   - Rejected because Codex specialist behavior is supplied by the prompt and
     repo-managed skill, while the platform type should describe broad
     capability and write permissions.
3. Forbid `worker` subagents entirely.
   - Rejected because approved post-approval implementation slices may still
     benefit from workers with disjoint write ownership.

## Consequences

### Benefits

- Keeps independent review roles read-only by default on typed Codex platforms.
- Preserves the distinction between evidence-gathering explorers and
  implementation workers.
- Reduces ambiguity in prompts for `scope-ledger-adversarial-review`,
  `go-code-quality-review`, and fresh-verifier roles.

### Risks

- Typed subagent names are platform-specific. The workflow must keep the rule
  conditional on the active Codex platform exposing typed subagents.
- Agents still must prompt explorers with explicit allowed actions, forbidden
  actions, expected output, and lead-disposition requirements.

### Operational impact

- No runtime, config, protocol, parser, telnet, peer, archive, queue,
  persistence, connection, CI, generated-artifact, or operator command behavior
  changes.
- Future Codex workflow responses should treat read-only independent review
  roles as `explorer` spawns when typed subagents are supported and authorized.

## Links

- Related issues/PRs/commits:
- Related tests:
  - targeted workflow text checks
  - workflow-drift audit
  - `git diff --check`
- Related docs: `AGENTS.md`, `docs/change-workflow.md`,
  `docs/WORKING_WITH_CODEX.md`, `docs/review-checklist.md`,
  `docs/dev-runbook.md`, `VALIDATION.md`,
  `codex-skills/scope-ledger-adversarial-review/SKILL.md`,
  `codex-skills/go-code-quality-review/SKILL.md`
- Related ADRs: ADR-0202, ADR-0205
- Related TSRs:
- Supersedes / superseded by: clarifies ADR-0202 and ADR-0205 without
  superseding either decision
