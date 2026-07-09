# ADR-0202: Default-On Independent Review Agents

- Status: Accepted
- Date: 2026-07-09
- Decision Origin: Design

## Context

ADR-0199 defined safe subagent delegation boundaries for GoCluster. It allowed
pre-approval explorers, adversarial-review explorers, post-approval workers,
and fresh-verifier explorers, but it rejected making subagent use mandatory
because tool availability and user authorization can vary.

The workflow has since gained an environment where independent agents are
available during Codex execution, and the user clarified the desired default:
independent agents should be used when the environment supports them unless the
user explicitly prohibits independent-agent use. The user also clarified that
independence means a separate agent from the lead Codex agent with its own
context window, not merely the lead agent re-reading its own work.

## Decision

Make independent review agents default-on workflow controls when supported and
not explicitly prohibited:

1. Use `scope-ledger-adversarial-review` as a read-only independent explorer
   before presenting an approval token for Non-trivial Scope Ledgers.
2. Use `go-code-quality-review` as a read-only independent explorer after
   Non-trivial Go implementation work is written and before final closeout.
3. Continue to use read-only fresh-verifier explorers for high-risk closeout
   when independent agents are supported and not explicitly prohibited.
4. Treat independent-agent findings as evidence only. The lead Codex agent
   still owns Scope Ledger disposition, `SCOPE ADVERSARIAL REVIEW`,
   integration, final Review Pass, validation claims, ADR/TSR handling,
   Scope-to-Code Traceability, and the final response.
5. If an independent review is unsupported, explicitly prohibited, fails, or
   times out, Codex must report that status. For high-risk work, missing
   independent review is a review or validation gap unless explicitly waived.

## Alternatives considered

1. Keep ADR-0199's optional independent-agent wording.
   - Rejected because the desired quality bar is default independent review
     whenever the environment supports it.
2. Make independent agents mandatory with no fallback.
   - Rejected because environments can lack independent-agent support and
     independent explorers can fail or time out. The workflow must report that
     evidence status rather than deadlocking.
3. Let independent agents replace lead-agent review.
   - Rejected because it would weaken the approval gate, validation claims,
     traceability, and final accountability.
4. Use one generic review skill for both phases.
   - Rejected because pre-approval scope attack and post-code Go quality review
     have different phase boundaries, allowed actions, and failure modes.

## Consequences

### Benefits

- Scope approval gets an independent challenge from a separate context window.
- Go implementation closeout gets a separate code-quality review pass before
  the lead agent finalizes validation and traceability claims.
- Missing or failed independent review becomes visible evidence instead of a
  silent fallback.
- The new roles are discoverable through repo-managed skills and support-agent
  developer routing.

### Risks

- The workflow has more coordination overhead for Non-trivial work.
- Independent agents can time out or inspect stale context; the lead agent must
  verify and disposition findings against current workspace evidence.
- Overuse on Small or documentation-only work would add noise, so the default
  requirement targets Non-trivial Scope Ledgers and Non-trivial Go
  implementation work.

### Operational impact

- No runtime, config, protocol, parser, telnet, peer, archive, queue,
  persistence, connection, CI, generated-artifact, or operator command behavior
  changes.
- Future Non-trivial workflow responses should report independent-agent
  support/prohibition status, use or failure, findings, and lead-owned
  disposition.

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
  `docs/WORKING_WITH_CODEX.md`, `codex-skills/README.md`,
  `codex-skills/scope-ledger-adversarial-review/SKILL.md`,
  `codex-skills/go-code-quality-review/SKILL.md`,
  `customgpt/source-map.md`, `customgpt/developer-guide-index.md`,
  `customgpt/common-questions.md`
- Related ADRs: ADR-0119, ADR-0144, ADR-0156, ADR-0194, ADR-0199,
  ADR-0203, ADR-0205
- Related TSRs: none
- Supersedes / superseded by: extends ADR-0199 default behavior
