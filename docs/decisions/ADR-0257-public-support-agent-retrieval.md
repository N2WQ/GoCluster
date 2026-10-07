# ADR-0257: Public Support-Agent Retrieval

- Status: Accepted
- Date: 2026-10-07
- Decision Origin: Troubleshooting chat

## Context

The deployed support-agent returned unauthorized during the GPT setup.
TSR-0044 identifies the admission failure but does not establish the precise
credential mismatch. The user explicitly selected removing all support-agent
authentication and authorized Scope Ledger v2. All retrievable content is from
the public repository, and the Worker provides read-only GET access.

## Decision

1. Make all allowed support-agent retrieval endpoints public. Remove Worker
   Bearer admission, the secret dependency, comparison helpers and 401 errors.
   Report `auth: "none"` and align the privacy page with public access.
2. Declare `security: []` in the Action schema, remove Bearer schemes and 401
   responses, and configure the GPT Action authentication as None.
3. Exercise retrieval with no credentials and an empty Worker environment in
   smoke, search and evaluation fixtures. Deployed smoke checks require no
   token and verify public version metadata and source retrieval.
4. Preserve read-only methods, safe-path restrictions, deployment-bundle
   isolation, source caps, corpus membership and the 99,000-character serialized
   search budget. No authentication toggle, retained state, rate limiter or
   other admission mechanism is added.
5. Scope applies only to the support-agent API. GoCluster login, peer
   authentication and OpenAI API credentials remain unchanged.

## Alternatives considered

1. Repair the Bearer configuration: would preserve the existing caller gate;
   the user selected removal instead.
2. Change AUTH_MODE only: rejected because it changes metadata without removing
   the authentication gate.
3. Set GPT Action authentication to None without deploying the Worker: rejected
   because the old Worker would continue returning 401.
4. Add a temporary bypass flag: not requested and introduces another deployment
   configuration surface rather than implementing public access directly.

## Consequences

### Benefits

- Worker and GPT Action agree on a credential-free retrieval contract.
- Token setup no longer prevents public repository retrieval.

### Risks

- Any caller can consume Worker quota and initiate upstream GitHub reads,
  including up to 46 reads per unscoped search. Existing per-request bounds do
  not bound total public traffic; deployment usage must be monitored.
- Cloudflare client filtering can still deny requests before Worker execution.
- Repository tests establish local behavior, not deployed GPT answer quality.

### Operational impact

Deploy the Worker, replace the Action schema (4.10.0), select authentication
None, and save the GPT. Existing Worker token secrets are unused and can be
removed after deployment. Verify getVersion before running search and source
retrieval checks in Preview.

## Links

- Related tests: `scripts/check-support-agent.ps1`, `scripts/test-support-search.mjs`, `scripts/evaluate-support-agent.ps1`
- Related docs: [Runbook](../support-agent-runbook.md), [Quality contract](../support-agent-quality-contract.md)
- Related TSRs: [TSR-0044](../troubleshooting/TSR-0044-support-agent-token-mismatch.md)
- Supersedes / superseded by: Supersedes [ADR-0109](ADR-0109-support-agent-bearer-auth.md); refines admission clauses in [ADR-0154](ADR-0154-support-agent-quality-contract.md). Other quality and isolation decisions remain active.
