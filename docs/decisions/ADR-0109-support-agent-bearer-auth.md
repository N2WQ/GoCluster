# ADR-0109: Support-Agent Bearer Authentication

- Status: Superseded
- Date: 2026-05-04
- Decision Origin: Design

## Context

The custom GPT support-agent action schema already declared bearer
authentication, but the Cloudflare Worker still accepted unauthenticated
repository retrieval requests. That mismatch could make the GPT Builder action
configuration appear secure while leaving the deployed retrieval endpoints
public.

## Decision

Require `Authorization: Bearer <token>` for all JSON repository retrieval
endpoints in `customgpt/support-agent/cloudflare-worker.js`.

The expected token is supplied through the Cloudflare Worker secret binding
`GOCLUSTER_DOCS_ACTION_TOKEN`. The `/privacy` endpoint remains public so the
custom GPT privacy policy can be reached before action use. The Worker reports
`auth: "bearer"` in successful JSON responses so smoke tests and GPT-side
diagnostics can detect the active contract.

## Alternatives considered

1. Keep endpoints public until deployment is complete.
   - Rejected because the schema already advertises authentication and the
     contract should be tested before deployment.
2. Add a query-string token.
   - Rejected because tokens in URLs are easier to leak through logs,
     browser history, and copy/paste.
3. Add per-endpoint tokens or role scopes.
   - Rejected for now because the action is read-only and the immediate risk is
     the schema/Worker contract mismatch.

## Consequences

### Benefits

- The Worker behavior now matches the OpenAPI action security contract.
- Missing or invalid tokens fail closed with 401 before repository retrieval.
- Token setup is operationally explicit and does not require committing
  secrets.

### Risks

- The GPT Builder bearer/API-key value and the Cloudflare Worker secret must
  match after deployment or every repository retrieval action will fail with
  401.
- A missing Worker secret also fails closed, which is safer but can look like a
  bad GPT key during setup.

### Operational impact

- No cluster runtime, config, parser, telnet, archive, peer, replay, queue,
  timing, or long-lived connection behavior changes.
- The support-agent action deployment now requires a Worker secret before JSON
  retrieval endpoints are usable.

## Links

- Related issues/PRs/commits: none
- Related tests: Worker syntax check, OpenAPI YAML parse, local Worker auth
  smoke test, instruction character-count check
- Related docs: `customgpt/support-agent/actions-schema.yaml`,
  `customgpt/support-agent/cloudflare-worker.js`
- Related TSRs: none
- Supersedes / superseded by: extends ADR-0107 and ADR-0108


## Supersession

[ADR-0257](ADR-0257-public-support-agent-retrieval.md) supersedes this Bearer
authentication contract with explicitly selected public retrieval access.
