# TSR-0044 - Support Agent Token Mismatch

Status: Monitoring
Date Opened: 2026-10-07
Date Resolved: n/a
Owner: Repository maintainers
Technical Area: support-agent deployment and access
Trigger Source: Chat request
Led To ADR(s): ADR-0257

## RCA Summary
- What happened: GPT getVersion failed during the support-agent update, with the Worker returning unauthorized.
- Root cause: The deployed Worker rejected a missing or invalid Bearer token. Whether the secret was absent, the Action omitted its header, or values differed was not established.
- What fixed it: The user selected public retrieval. The implementation removes both Worker admission and Action schema authentication; redeployment and selecting None in the GPT editor are required.
- How we know: The supplied response was the Worker's unauthorized JSON. Local endpoint checks exercise the new public contract with an empty environment and no Authorization header. Deployment remains pending.
- Operator/support answer: Deploy the updated Worker, replace the schema, select Authentication None, and test getVersion for HTTP 200 with auth none.

## Triggering Request
- Request date: 2026-10-07
- Request summary: Remove all support-agent authentication first.
- Request reference: This chat; exact Approved v2 authorized the public-access change.

## Symptoms and Impact
Every protected endpoint could return 401 despite reachable infrastructure.
Changing AUTH_MODE alone changed metadata, not admission behavior.

## Timeline
1. 2026-10-07: Worker deployment was reported; getVersion failed in the GPT.
2. 2026-10-07: Direct probes returned 200 for privacy and 401 for unauthenticated version with accepted client signatures; Python's default signature separately received Cloudflare 403/1010.
3. 2026-10-07: The user supplied unauthorized JSON, selected authentication removal, and approved Scope Ledger v2.

## Hypotheses and Tests
1. Worker was unreachable.
   - Evidence: Direct probes reached privacy and the version admission gate.
   - Outcome: Rejected for those probes.
2. Worker Bearer admission caused the supplied JSON error.
   - Evidence: The deployed error matches unauthorizedResponse; the current gate required a secret and matching header.
   - Outcome: Supported; the precise credential-configuration mismatch remains unknown.
3. Cloudflare filtering may independently reject clients.
   - Evidence: Python's default signature received 403/1010; alternative client signatures reached the Worker.
   - Outcome: Supported for local probes, not established as the cause of the GPT failure.

## Findings
Public access removes token-related admission failures but does not fix Cloudflare filtering. Unauthenticated callers can consume Worker quota and upstream requests; per-request limits and allowed-path restrictions remain.

## Decision Linkage
- ADR created/updated: ADR-0257 supersedes ADR-0109 and refines ADR-0154 admission clauses.
- Decision delta summary: All allowed support retrieval endpoints become public.
- Contract/behavior changes: No token or secret dependency; schema authentication is None and responses report auth none.

## Verification and Monitoring
- Validation steps: Native syntax/schema checks, local public endpoint fixtures, support-agent checks, search fixtures, and retrieval evaluation. Final closeout reports observed outcomes.
- Signals to monitor: Deployed version status, GPT Preview retrieval, Cloudflare denials and quota usage.
- Rollback triggers: Unacceptable public-request usage. Reinstating authentication requires coordinated Worker and GPT settings, not just an AUTH_MODE edit.

## References
- Related ADRs: [ADR-0257](../decisions/ADR-0257-public-support-agent-retrieval.md), [ADR-0109](../decisions/ADR-0109-support-agent-bearer-auth.md).
- Related docs: [Runbook](../support-agent-runbook.md), [Quality contract](../support-agent-quality-contract.md).
