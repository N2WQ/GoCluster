# TSR-0027: Support-Agent Shallow Answers

- Status: Resolved
- Date opened: 2026-06-07
- Status date: 2026-06-07

## RCA Summary

- What happened: The support-agent action was reachable, but answers to Windows
  startup troubleshooting stayed generic and repeatedly asked for logs instead
  of giving a concrete Windows diagnostic sequence.
- Why: Retrieval worked, but the repository lacked a durable answer-quality
  contract: route specificity, support cards, `must_include`/`must_avoid`
  obligations, eval prompts, release smoke checks, and deployment runbook steps
  were underspecified.
- What fixed it: ADR-0154 added the support-agent quality contract, route and
  coverage docs, support cards, `/support-route`, bounded `/search`, smoke and
  eval scripts, Worker CORS diagnostics, and agent-instruction/schema updates
  that make route contracts the first retrieval step.
- How we know: The original transcript reproduced shallow answers; later
  deterministic support-agent retrieval evals passed 18/18, and live
  `gpt-5-nano` answer evals passed 18/18 after the route/card contract was in
  place.
- Operator/support answer: If support answers are shallow, do not stop at
  "retrieval works"; verify the selected route card, required sources,
  `must_include` obligations, and live-answer eval output.

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
4. The remaining gap was not just the number of prompt categories; it was the
   lack of a route contract tied to telnet-user, node-operator, and
   future-developer personas.
   - Supported by eval cases that could retrieve source files yet still
     produce shallow or incomplete live answers without a support card and
     explicit `must_include` / `must_avoid` route contract.

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
- The v7 deterministic support-agent eval suite passed 18/18 retrieval cases
  after adding coverage ledger, support cards, `/support-route`, and `/search`.
- The v7 live `gpt-5-nano` eval suite passed 18/18 answer cases in
  `.tmp/support-agent-evals/20260607-205708-275-02026333`.

## Root cause or best current explanation

The repository action was not the only issue. The support path had a working
retrieval mechanism, but not a durable answer-quality contract. Route
specificity, troubleshooting answer shape, eval prompts, release smoke checks,
and deployment runbook steps were underspecified, so the GPT could satisfy
"retrieved something" while still giving a thin answer.

The later evals showed a second root cause: plain retrieval leaves too much
answer-shape judgment to the model. High-risk support paths need an explicit
route contract and source-backed support card so the model knows what it must
include, what shortcuts to avoid, and which persona/domain it is serving.

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
- Added `docs/support-agent-eval-cases.json` and
  `scripts/evaluate-support-agent.ps1` so support-agent prompt regressions can
  be exercised locally against the checked-in Worker and current workspace
  files. The harness validates retrieval/source coverage and can score pasted or
  live-generated answers.
- Added `docs/support-agent-coverage-ledger.md` to inventory support domains
  across telnet users, node operators, future developers, and cross-cutting
  concerns.
- Added source-backed support cards under `customgpt/support-cards/`.
- Added Worker `/support-route?query=` for persona/domain routing, support card
  retrieval, required sources, and `must_include` / `must_avoid` answer
  contracts.
- Added Worker `/search?query=` over a curated safe corpus for exact
  diagnostics and symbol/source lookups.
- Updated the action schema and agent instructions so `getSupportRoute` is the
  first support retrieval step, with search used to deepen exact-source lookup
  rather than replace route evidence.

## Why an ADR was or was not required

- ADR required because the support agent is a durable operator-facing support
  mechanism, and the fix establishes a lasting support quality, evaluation, and
  deployment-check contract.

## Links

- Related ADRs: `docs/decisions/ADR-0154-support-agent-quality-contract.md`
- Related issues/PRs/commits: none
- Related tests: `scripts/check-support-agent.ps1`,
  `scripts/evaluate-support-agent.ps1`
- Related docs: `docs/support-agent-quality-contract.md`,
  `docs/support-agent-coverage-ledger.md`,
  `docs/support-agent-eval-cases.json`, `docs/support-agent-evals.md`,
  `docs/support-agent-runbook.md`,
  `customgpt/support-cards/`,
  `customgpt/troubleshooting-index.md`,
  `customgpt/support-agent/agent-instructions.txt`
