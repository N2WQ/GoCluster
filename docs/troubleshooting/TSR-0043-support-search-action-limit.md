# TSR-0043 - Support Search Action Limit

Status: Monitoring
Date Opened: 2026-10-07
Date Resolved: n/a
Owner: Repository maintainers
Technical Area: customgpt support search
Trigger Source: Chat request
Led To ADR(s): ADR-0256
Tags: search, actions, response-budget, partial-evidence

## RCA Summary
- What happened: Broad searches returned HTTP 200 with payloads above the GPT Actions limit.
- Why: Region-count and source-prefix bounds did not bound serialized JSON; overlapping snippets merged transitively, and evidence appeared in both matches and files.
- What fixed it: A 99,000-character serialized-response budget shortens snippets and explicitly reports partial evidence.
- How we know: Workspace-backed upstream mocks reproduced 101,744 characters for `on` and 346,893 for `a` before the fix. After the fix and documentation updates, both queries retain 25 results within 99,000 characters. Endpoint fixtures cover source slices, dense matches, long lines, escaping, Unicode and the budget boundary.
- Operator/support answer: Inspect response_budget_truncated and snippet_truncated, then retrieve authoritative source windows before interpreting missing context.

## Triggering Request
- Request date: 2026-10-07
- Request summary: Fix the remaining support-search response bound before clearing the branch.
- Request reference: This troubleshooting chat, baseline 3c3101a.

## Symptoms and Impact
Search could succeed at the Worker but exceed the Action payload limit, preventing useful retrieval. The Canadian licence fix was separately cleared by the supplied review.

## Timeline
1. 2026-10-07: The user reported oversized `on` and `a` responses; local read-only reproduction matched both sizes exactly.
2. 2026-10-07: The user selected shortened oversized regions and approved Scope Ledger v1.
3. 2026-10-07: Implemented budgeted serialization and partial-slice contracts; deployment remains pending.

## Hypotheses and Tests
1. The 25-region limit bounds the Action payload.
   - Evidence: Workspace-backed public-endpoint reproduction at 3c3101a.
   - Outcome: Rejected; 25 regions produced both oversized responses.
2. Measuring snippet text alone is sufficient.
   - Evidence: JSON duplicates snippets, escapes text, and includes matched-line arrays and other metadata.
   - Outcome: Rejected; the complete serialized body must be measured.
3. Shortening around original match locations can preserve useful evidence within the bound.
   - Evidence: Public-endpoint fixtures compare returned slices with independently extracted source text, including late matches and Unicode.
   - Outcome: Supported locally; deployed Preview remains unverified.

## Findings
- Root cause: The previous finite bounds were resource bounds, not an Action wire-size guarantee. This changes a durable response and partial-evidence contract, requiring ADR-0256.

## Decision Linkage
- ADR created/updated: ADR-0256, refining ADR-0255.
- Decision delta summary: Budget full serialization; retain literal shortened evidence and separate response truncation from source coverage.
- Contract/behavior changes: Search responses cap at 99,000 characters; snippet columns and truncation fields identify partial source slices.

## Verification and Monitoring
- Validation steps: Node public-endpoint fixtures, syntax checks, local support checks and retrieval evaluation. See final change closeout for observed command outcomes.
- Signals to monitor: Deployed response length, partial-snippet flags, correct source follow-up in GPT Preview.
- Rollback triggers: Incorrect source locations or lost truncation signals. Rolling back also restores the known oversized-response defect.

## References
- Commit: Reproduction baseline 3c3101a.
- Related ADRs: [ADR-0256](../decisions/ADR-0256-support-search-response-budget.md), [ADR-0255](../decisions/ADR-0255-support-search-evidence-and-coverage.md).
- Related docs: [Quality contract](../support-agent-quality-contract.md), [Runbook](../support-agent-runbook.md).
- External limit: [OpenAI Actions production notes](https://developers.openai.com/api/docs/actions/production).
