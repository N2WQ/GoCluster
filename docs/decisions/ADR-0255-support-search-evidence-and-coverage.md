# ADR-0255: Support Search Evidence and Coverage

- Status: Accepted
- Date: 2026-10-07
- Decision Origin: Troubleshooting chat

## Context

The support search returned the first 12 matching lines in a fixed 35-file
corpus. Early routing summaries could exhaust the result budget before current
feature guidance. HTTP failures were skipped, character-capped sources lost
their partial-coverage metadata, and exactly 12 matches reported truncation
without proving another match existed. Recent configuration, history,
release and FCC/ISED documentation was available through getDoc but outside
search. The user approved Scope Ledger v2 after explicitly selecting exact-first
file diversity, curated path scopes and partial-result failure semantics.

## Decision

1. Expand the fixed public corpus to 46 files, adding the configuration card,
   data.yaml example, scripts README, history TSR, decision/troubleshooting
   indexes, environment/runbook/code-map entry points and FCC/Canadian validation.
   Document verified FCC/ISED diagnostic strings in data/config/README.md.
2. Scan each eligible corpus file before selecting at most 25 merged regions.
   Preserve case-insensitive substring/same-line all-word matching. Merge
   overlapping two-line context windows transitively within each file; rank
   the region by its strongest constituent match. Exact phrases precede
   all-word-only matches. Within a tier, file rounds precede additional regions;
   code-point path order and line order break ties deterministically.
3. Optional path scopes restrict corpus membership to one file or a directory
   prefix. Omitted scope selects the corpus; unsafe, empty or non-corpus scopes
   fail with 400 before any fetch. This does not expose repository-wide search.
4. Preserve the query limit of 96 characters and searchable source prefix of
   140,000 characters. The finite eligible corpus bounds fetch count; all
   candidates and sorting state are request-owned. No index, retained cache,
   background process or retry loop is added.
5. Report eligible/successful counts, failed paths, capped sources, coverage
   completeness and result overflow separately. Legacy truncated remains a
   conservative partial-evidence flag. Exactly 25 regions alone is not overflow.
   Preserve available evidence with HTTP 200 when any eligible source was read,
   including zero-match partial searches. All-source failure is HTTP 502.
6. Require the action schema/instructions to distinguish missing matches from
   incomplete coverage and to retrieve authoritative sources before claims.

## Alternatives considered

1. Raise only the result cap: leaves fixed-order bias and hidden source gaps.
2. Search every safe repository file: increases per-request work and broadens
   discovery beyond the approved curated contract.
3. Add a persistent hosted search index: adds deployment, refresh, stale-index
   and ownership responsibilities beyond this bounded change.
4. Fail all searches on any source failure: discards usable evidence; the user
   instead selected explicit partial coverage.

## Consequences

### Benefits

- Current support documents and exact FCC/ISED diagnostics become searchable.
- Later exact candidates survive early loose-match noise.
- Repeated overlapping lines no longer consume separate result slots.
- Empty partial searches cannot be mistaken for completed evidence discovery.

### Risks

- Full scans may require all 46 upstream reads and increase network latency.
- Merged regions can exceed five lines; source prefix and region-count bounds
  remain finite but are not a production latency guarantee.
- A curated corpus still excludes most code. Ranking tiers are deterministic
  retrieval heuristics, not a guarantee of semantic relevance or answer quality.
- Source-prefix caps are reported rather than silently treated as complete.

### Operational impact

Update the Worker, GPT action schema and instructions together, then exercise
Preview. Repository validation alone does not update those deployments.
Cluster runtime, licensing, filtering, persisted state and configuration
contents are unchanged.

## Links

- Related issues/PRs/commits: none
- Related tests: `scripts/test-support-search.mjs`, `scripts/check-support-agent.ps1`, `scripts/evaluate-support-agent.ps1`, SA-028
- Related docs: `docs/support-agent-quality-contract.md`, `docs/support-agent-runbook.md`, `docs/support-agent-coverage-ledger.md`, `data/config/README.md`
- Related TSRs: `docs/troubleshooting/TSR-0027-support-agent-shallow-answers.md`
- Supersedes / superseded by: refines ADR-0154 search evidence and coverage; other quality-contract decisions remain active


## Subsequent refinement

[ADR-0256](ADR-0256-support-search-response-budget.md) adds a serialized-response
budget and explicit shortened-snippet metadata, and extends result truncation
to include shortened evidence. The original finite source/region bounds did
not guarantee that Action payloads stayed below 100,000 characters.
