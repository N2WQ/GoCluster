# ADR-0154: Support-Agent Quality Contract

- Status: Accepted
- Date: 2026-06-07
- Decision Origin: Troubleshooting chat

## Context

The support-agent retrieval action was reachable, but a reported browser
transcript showed poor answer quality. After a Windows startup clarification,
the agent still gave broad prompts and generic startup checks instead of a
specific, evidence-grounded diagnostic sequence. Because the support agent is
the primary support mechanism, "action works" is not sufficient; support
answers need a durable quality, routing-depth, evaluation, and deployment-check
contract.

Existing support-agent ADRs already establish action contract alignment,
repo-derived routes, bearer authentication, deployment-bundle isolation, and
PowerShell script retrieval. This decision extends those contracts without
changing cluster runtime behavior or weakening action authentication.

## Decision

Adopt a repository-owned support-agent quality contract:

1. Keep `customgpt/support-agent/` as a deployment bundle and keep the Worker
   deny rule for retrieving that bundle through the action.
2. Put support answer quality, routing depth, and failure gates in
   `docs/support-agent-quality-contract.md`.
3. Put representative prompt checks and pass/fail criteria in
   `docs/support-agent-evals.md`.
4. Put GPT Builder setup, Worker setup, smoke checks, and release checks in
   `docs/support-agent-runbook.md`.
5. Require route specificity in support-agent instructions and routing docs:
   platform-specific symptom routes beat generic routes, and troubleshooting
   answers must retrieve both the symptom route and underlying authoritative
   docs when practical.
6. Add `scripts/check-support-agent.ps1` as the versioned smoke check for the
   support-agent bundle and Worker behavior.
7. Support `OPTIONS` preflight in the Worker for browser/manual diagnostics
   while preserving bearer-auth requirements for JSON retrieval endpoints.

## Alternatives considered

1. Fix only the Windows startup route.
   - Rejected because the user explicitly needed an end-to-end support-agent
     review, and the same shallow-answer pattern can appear in other symptoms.
2. Copy full troubleshooting answers into custom GPT instructions.
   - Rejected because it would create stale duplicated content and exceed the
     purpose of the deployment instructions.
3. Add a hosted support app immediately.
   - Deferred because the current Custom GPT plus read-only Worker can be made
     materially better with a quality contract, evals, and smoke checks first.

## Consequences

### Benefits

- Support-agent quality becomes a checked-in contract instead of a prompt-only
  convention.
- Operators get more concrete troubleshooting procedures, with the source docs
  and evidence gaps stated explicitly.
- Maintainers can smoke test the action bundle and deployed Worker before
  relying on Preview behavior.
- Existing bearer-auth and deployment-bundle isolation decisions remain intact.

### Risks

- The GPT can still produce weak answers if the deployed instructions or action
  schema are not updated from the checked-in bundle.
- Manual Preview evaluation remains necessary because model synthesis quality is
  not fully testable by repository smoke checks.
- Route docs require ongoing maintenance as new support surfaces are added.

### Operational impact

- No GoCluster runtime, config, telnet, parser, protocol, queue, archive, peer,
  replay, or long-lived connection behavior changes.
- Support-agent deployment now has a clearer release gate: update the GPT
  instructions/schema, update the Worker, run the smoke script, and run the
  representative eval prompts.

## Links

- Related issues/PRs/commits: none
- Related tests: `scripts/check-support-agent.ps1`
- Related docs: `docs/support-agent-quality-contract.md`,
  `docs/support-agent-evals.md`, `docs/support-agent-runbook.md`,
  `customgpt/support-agent/agent-instructions.txt`,
  `customgpt/support-agent/actions-schema.yaml`,
  `customgpt/support-agent/cloudflare-worker.js`,
  `customgpt/source-map.md`, `customgpt/troubleshooting-index.md`
- Related TSRs: `docs/troubleshooting/TSR-0027-support-agent-shallow-answers.md`
- Supersedes / superseded by: extends ADR-0107, ADR-0108, ADR-0109,
  ADR-0112, and ADR-0113
