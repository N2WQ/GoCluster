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
3. Configure authentication as API key, Bearer.
4. Use the same secret value stored in the Cloudflare Worker secret binding
   `GOCLUSTER_DOCS_ACTION_TOKEN`.
5. Set the privacy policy URL to:
   `https://gocluster-docs-action.n2wq-api.workers.dev/privacy`
6. In Preview, test `getVersion`, `getSourceMap`, `getTroubleshootingIndex`,
   and `getDoc`.

Official OpenAI guidance requires GPT Actions to have both authentication
configuration and an OpenAPI schema, and recommends testing actions in Preview.
Managed workspaces can also restrict action domains.

## Cloudflare Worker Setup

The Worker must be deployed from `cloudflare-worker.js` and must have the
`GOCLUSTER_DOCS_ACTION_TOKEN` secret configured. A missing secret fails closed
with `401` on JSON retrieval endpoints. `/privacy` remains public.

Protected endpoints must require `Authorization: Bearer <token>`:

- `/version`
- `/source-map`
- `/troubleshooting-index`
- `/external-authorities`
- `/list-dir`
- `/find-files`
- `/doc`
- `/file`
- `/bundle`

## Local Smoke Check

Run:

```powershell
scripts/check-support-agent.ps1
```

This validates the checked-in instructions, schema, Worker syntax, route
extraction, auth behavior, safe-path denial, line windows, and local in-process
Worker behavior using a dummy token. It does not print or require production
secrets.

## Deployed Smoke Check

Without a token:

```powershell
scripts/check-support-agent.ps1 -Deployed
```

This confirms the public privacy page is reachable and protected routes fail
closed with `401`.

With a token available in an environment variable:

```powershell
$env:GOCLUSTER_DOCS_ACTION_TOKEN = "<redacted>"
scripts/check-support-agent.ps1 -Deployed -TokenEnv GOCLUSTER_DOCS_ACTION_TOKEN
```

The script uses the token only for requests and does not print it.

## Release Checklist

Before treating support-agent changes as complete:

1. Run `scripts/check-support-agent.ps1`.
2. Run `scripts/check-support-agent.ps1 -Deployed` when network access is
   available.
3. If a production token is available locally, run the deployed authenticated
   check.
4. In GPT Preview, run the prompts in `docs/support-agent-evals.md`.
5. Confirm `agent-instructions.txt` remains under the GPT instruction size
   budget.
6. Confirm `customgpt/` routes still point to authoritative docs rather than
   duplicating runtime behavior.

## Browser And App Notes

If the GPT works in the browser but not in the app, treat the Worker as probably
reachable and check ChatGPT client, model/mode, action approval, workspace
policy, and action availability. Do not weaken Worker authentication to work
around a client-specific issue.
