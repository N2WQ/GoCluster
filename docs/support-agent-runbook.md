# Support-Agent Runbook

This runbook covers deployment, smoke checks, and routine maintenance for the
GoCluster support agent.

## Deployment Payloads

The deployable bundle is intentionally limited to:

- `customgpt/support-agent/agent-instructions.txt`
- `customgpt/support-agent/actions-schema.yaml`
- `customgpt/support-agent/cloudflare-worker.js`

Do not add a README or documentation index under `customgpt/support-agent/`.
That directory is deployment input, not an action-retrievable support source.

## GPT Builder Setup

In the GPT editor:

1. Paste `agent-instructions.txt` into the GPT instructions.
2. Create one action from `actions-schema.yaml`.
3. Set action authentication to **None**. The Worker is public and requires no token.
4. Replace the existing action schema with the current deployment file.
5. Set the privacy policy URL to:
   `https://gocluster-docs-action.n2wq-api.workers.dev/privacy`
6. In Preview, test `getVersion`, `getSupportRoute`, `searchSupportCorpus`,
   `getSourceMap`, `getTroubleshootingIndex`, and `getDoc`.

Official OpenAI guidance supports unauthenticated GPT Actions. Set authentication
to None, supply the OpenAPI schema, and test actions in Preview.
Managed workspaces can also restrict action domains.

## Cloudflare Worker Setup

Deploy `cloudflare-worker.js`. No secret binding or Authorization header is
required. All allowed GET retrieval endpoints are public and report
`auth: "none"`; `/privacy` remains public. OPTIONS remains available, and
unsupported methods are rejected. Safe-path restrictions and deployment-bundle
isolation remain enforced.

Public access removes the caller credential gate. Anyone can consume Worker
requests and cause upstream GitHub reads, including up to 46 fetches per
unscoped search. Existing per-request bounds remain; no rate limiter is added.
Cloudflare client blocking is separate from Worker authentication.

An existing `GOCLUSTER_DOCS_ACTION_TOKEN` Cloudflare secret is unused by this
Worker version and can be removed after deployment. Clear the GPT Action's
old API-key configuration by selecting None. Do not change credentials used
by GoCluster login, peering, or the optional OpenAI live-model evaluator.

## Local Smoke Check

Run:

```powershell
scripts/check-support-agent.ps1
```

This validates the checked-in instructions, schema, Worker syntax,
support-route contracts, search selection/coverage fixtures from
`scripts/test-support-search.mjs`, route extraction, public-access
behavior, safe-path denial, line windows, and local in-process Worker behavior
without credentials. It does not print or require production secrets.

## Local Eval Harness

Run:

```powershell
scripts/evaluate-support-agent.ps1
```

This imports the checked-in Worker without credentials, serves GitHub
raw/API fetches from the current workspace, executes the machine-readable cases
in `docs/support-agent-eval-cases.json`, and writes reports under
`.tmp/support-agent-evals/`. It does not call the deployed Cloudflare Worker.

To score real GPT Preview/browser/app answers, save answers in a JSON file or a
directory of `SA-001.md`/`SA-002.txt` files and run:

```powershell
scripts/evaluate-support-agent.ps1 -AnswersPath .tmp/support-agent-answers.json -RequireAnswers
```

To generate local evidence-synthesis answers when the OpenAI API is available:

```powershell
$env:OPENAI_API_KEY = "<redacted>"
scripts/evaluate-support-agent.ps1 -LiveModel -RequireAnswers
```

Live model mode still uses the local Worker simulation. It is useful for
regression screening, but GPT Preview remains the final check for deployed
Custom GPT behavior.

## Deployed Smoke Check

Run without a token:

```powershell
scripts/check-support-agent.ps1 -Deployed
```

This checks the public privacy page, OPTIONS, `/version` returning HTTP 200
with `auth: "none"`, and source-map retrieval. A 401 indicates that the old
Worker authentication gate is still deployed. A Cloudflare 403 must be
diagnosed separately from Worker access.

## Release Checklist

Before treating support-agent changes as complete:

1. Run `scripts/check-support-agent.ps1`.
2. Run `scripts/evaluate-support-agent.ps1`.
3. Score at least the changed or failed prompt category with either real GPT
   Preview/browser/app answers or `-LiveModel` when an API key is available.
4. Run `scripts/check-support-agent.ps1 -Deployed` when network access is
   available.
5. Confirm the deployed version reports `auth: "none"` and the GPT Action
   authentication is None.
6. In GPT Preview, run the prompts in `docs/support-agent-evals.md`.
7. Confirm `agent-instructions.txt` remains under the GPT instruction size
   budget.
8. Confirm `customgpt/` routes still point to authoritative docs rather than
   duplicating runtime behavior.
9. Confirm support cards trace back to authoritative docs/source and do not
   become independent runtime truth.

## Browser And App Notes

If the GPT works in the browser but not in the app, treat the Worker as probably
reachable and check ChatGPT client, model/mode, action approval, workspace
policy, and action availability. Public Worker access does not resolve
client-specific or Cloudflare blocking.

## Local Search Measurements

Compare the checked-in Worker with its preceding version using identical
workspace-backed fetch responses. Record serialized response bytes, elapsed
local time, eligible/successful fetch counts and coverage for broad and exact
queries. Repeat with partial failures and capped sources. These measurements
exclude production network latency and do not establish deployed answer quality.
The full search may fetch all 46 files; use a corpus path scope to narrow work.
The search change requires deploying the Worker and updating the GPT action
schema/instructions together, followed by Preview checks. Repository validation
alone does not deploy those payloads.

## Search response budget

Search responses must fit 99,000 serialized characters, below the GPT Actions
100,000-character limit. The budget includes escaped JSON, formatting, both
snippet copies, and metadata. `scripts/test-support-search.mjs` measures actual
response bodies for dense matches, long lines, escaping, Unicode, boundary
sizes, and workspace-backed `on`/`a` queries.

When `response_budget_truncated` or `snippet_truncated` is true, treat the
returned slice as partial evidence. Use its source URL and line range for
follow-up retrieval; column bounds identify partial lines. `matched_line`
anchors an original match even when the full phrase or all-word span cannot
fit. Source coverage remains independent of the response budget. Update the
Worker, action schema and GPT instructions together, then check dense-search
behavior in Preview before release.

See [ADR-0256](decisions/ADR-0256-support-search-response-budget.md) and
[TSR-0043](troubleshooting/TSR-0043-support-search-action-limit.md).

## Public-access update

After updating the Worker, replace the Action schema (version 4.10.0), set
Authentication to None, and save the GPT changes. Test `getVersion` before
continuing retrieval checks; it must return HTTP 200 with `auth: "none"`.
Then test `searchSupportCorpus` for `on` and `a`, confirm the response budget
is 99,000 characters and partial flags are present, and check authoritative
source follow-up in Preview.

See [ADR-0257](decisions/ADR-0257-public-support-agent-retrieval.md) and
[TSR-0044](troubleshooting/TSR-0044-support-agent-token-mismatch.md).
