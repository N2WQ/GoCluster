# TSR-0027: Support-Agent Shallow Answers

- Status: Resolved
- Date opened: 2026-06-07
- Status date: 2026-06-07

## Trigger

A support-agent transcript showed the repository action working in the browser,
but answers to Windows startup troubleshooting stayed generic. The agent asked
for logs repeatedly, drifted toward broad startup checks, and did not provide a
specific, pragmatic Windows diagnostic sequence even after the user clarified
the platform.

## Symptoms and impact

The support agent is the primary support mechanism for GoCluster operators.
Shallow answers can waste operator time, miss platform-specific evidence, and
make a working repository action look unreliable. In the reported transcript,
the agent cited routing docs but did not dig into the underlying Windows run
and config documentation deeply enough to produce an actionable answer.

## Hypotheses tested

1. The GPT action could not reach the Worker.
   - Disproved for the reported browser path; the Worker also returned expected
     local smoke responses during repository inspection.
2. The repository lacked Windows startup or config troubleshooting content.
   - Disproved by `docs/OPERATOR_GUIDE.md`, `README.md`, and
     `data/config/README.md`.
3. The quality gap came from missing route specificity, missing answer-quality
   gates, and missing versioned support-agent smoke/eval checks.
   - Supported by the generic startup route appearing before the Windows route
     and by the lack of a checked-in support-agent evaluation/runbook contract.

## Evidence

- User-provided transcript where "cluster fails on startup" followed by
  "cluster is running on windows" produced generic prompts for more logs and
  broad startup checks.
- `customgpt/troubleshooting-index.md` had both a generic startup route and a
  Windows route, but the rules did not force platform-specific routes ahead of
  generic routes.
- `customgpt/support-agent/agent-instructions.txt` required action retrieval,
  but did not define enough troubleshooting depth, route specificity, or answer
  shape requirements.
- No checked-in `scripts/check-support-agent.ps1` existed to validate the
  support-agent bundle and Worker behavior.

## Root cause or best current explanation

The repository action was not the only issue. The support path had a working
retrieval mechanism, but not a durable answer-quality contract. Route
specificity, troubleshooting answer shape, eval prompts, release smoke checks,
and deployment runbook steps were underspecified, so the GPT could satisfy
"retrieved something" while still giving a thin answer.

## Fix or mitigation

- Added `docs/support-agent-quality-contract.md` for answer shape, depth, and
  failure gates.
- Added `docs/support-agent-evals.md` with representative support prompts and
  pass criteria.
- Added `docs/support-agent-runbook.md` for GPT Builder, Worker, local smoke,
  and deployed smoke procedures.
- Updated support-agent instructions, source-map routing, common questions,
  developer routing, and troubleshooting routing to prefer the narrowest
  authoritative route.
- Added `scripts/check-support-agent.ps1` for local and optional deployed
  support-agent smoke checks.
- Added Worker `OPTIONS` handling for CORS preflight diagnostics without
  weakening bearer authentication.

## Why an ADR was or was not required

- ADR required because the support agent is a durable operator-facing support
  mechanism, and the fix establishes a lasting support quality, evaluation, and
  deployment-check contract.

## Links

- Related ADRs: `docs/decisions/ADR-0154-support-agent-quality-contract.md`
- Related issues/PRs/commits: none
- Related tests: `scripts/check-support-agent.ps1`
- Related docs: `docs/support-agent-quality-contract.md`,
  `docs/support-agent-evals.md`, `docs/support-agent-runbook.md`,
  `customgpt/troubleshooting-index.md`,
  `customgpt/support-agent/agent-instructions.txt`
