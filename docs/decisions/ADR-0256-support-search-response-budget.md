# ADR-0256: Support Search Response Budget

- Status: Accepted
- Date: 2026-10-07
- Decision Origin: Troubleshooting chat

## Context

ADR-0255 bounded sources and result count, but transitive region merging and
duplicated snippet content could exceed the GPT Actions payload limit of less
than 100,000 characters. TSR-0043 reproduced this at 3c3101a. The user explicitly
selected shortening oversized regions and authorized Scope Ledger v1.

## Decision

1. Limit the complete serialized `/search` JSON to 99,000 characters, including
   pretty-printing, escaping, match metadata and both snippet copies. Other
   endpoints retain their current contracts.
2. Preserve the existing ranked selection of up to 25 regions. If serialization
   exceeds the budget, search for a feasible shared snippet ceiling, retaining
   the last response verified to fit. No cache, background work or new fetches
   are introduced; all sizing state is request-local within the finite corpus.
3. Return literal contiguous slices around the strongest source matching phrase
   or all-word span where possible. A match span wider than the available
   snippet remains explicitly partial. Preserve Unicode surrogate pairs and
   normalize line endings to LF as search already did.
4. Describe returned slices with line ranges and one-based UTF-16 columns,
   inclusive start and exclusive end. `matched_line` and `match_type` retain
   the original strongest source anchor. `matched_lines` includes only source
   matches fully visible in the returned slice; it can be empty.
5. Add `response_budget_truncated` and per-match/file `snippet_truncated`.
   `results_truncated` now includes shortened or omitted result evidence;
   legacy `truncated` also includes incomplete source coverage. Coverage flags
   and HTTP 200/502 source-failure behavior remain independent and unchanged.
6. Keep Worker, schema, GPT instructions, support card and authoritative quality
   documentation aligned. Validate wire bodies and source slices through the
   public endpoint, including dense regions, long lines and budget boundaries.

## Alternatives considered

1. Return only whole regions: the user instead selected shortened evidence;
   dropping one large region could discard the only useful match.
2. Clamp shared JSON serialization for every endpoint: broadens this fix and
   cannot preserve search-specific location and truncation semantics.
3. Cap only snippet lengths or remove the duplicate files array: a snippet-only
   cap misses metadata and escaping; removing files breaks the existing consumer
   shape and still does not establish a serialized-response bound.
4. Give all remaining space to the first ranked region: may eliminate later
   files; a shared ceiling preserves ranked file diversity when feasible.

## Consequences

### Benefits

- Broad searches fit the Action limit while retaining useful ranked evidence.
- Partial lines and snippets are explicitly identifiable and source-addressable.
- Source coverage remains distinct from result truncation.

### Risks

- Sizing requires repeated local serialization; no production latency claim is
  made. The search remains bounded by its finite corpus and source prefixes.
- A shortened all-word span may not display every token. Source anchors remain
  metadata, not proof that all matching text is visible.
- Feasible sizing need not maximize every spare character; correctness depends
  on the actual serialization bound rather than maximal utilization.

### Operational impact

Deploy the Worker and update the Action schema/instructions together, then
exercise dense-search recovery in GPT Preview. Local repository validation
does not prove deployed answer quality or change the deployed bundle.

## Links

- Related tests: `scripts/test-support-search.mjs`, `scripts/check-support-agent.ps1`, `scripts/evaluate-support-agent.ps1`
- Related docs: [Quality contract](../support-agent-quality-contract.md), [Runbook](../support-agent-runbook.md)
- Related TSRs: [TSR-0043](../troubleshooting/TSR-0043-support-search-action-limit.md)
- Supersedes / superseded by: Refines ADR-0255 response-bound and result-truncation clauses; other decisions remain active.
- External authority: [OpenAI Actions production notes](https://developers.openai.com/api/docs/actions/production)
