# ADR-0193: TSR RCA Summary And Checker Contract

- Status: Accepted
- Date: 2026-06-17
- Decision Origin: Design

## Context

Troubleshooting records are read by maintainers, Codex, and the support agent.
The existing TSR corpus usually contained enough root-cause and remediation
evidence, but readers had to scan the detailed timeline, hypotheses, and links
before reaching the practical answer.

That slowed human review and made support-agent retrieval more likely to quote
historical details without first stating the short causal chain. The repository
needed a lightweight summary layer that preserves detailed evidence instead of
rewriting or flattening incident history.

## Decision

Every real TSR must include an `RCA Summary` near the top of the file with:

- What happened
- Why
- What fixed it
- How we know
- Operator/support answer

The summary is the first-pass explanation for humans and support agents. The
detailed TSR body remains authoritative for evidence, hypotheses, timelines,
validation, ADR linkage, and caveats.

Add `scripts/check-troubleshooting-records.ps1` as the repository-owned
mechanical check for troubleshooting records. It verifies:

- every indexed TSR link points to an existing file
- every real TSR file is indexed
- file and index statuses match
- every real TSR has `RCA Summary`
- root-cause and remediation evidence exists
- indexed `Led To ADR` values are mentioned in the TSR body and resolve to ADR
  files

Run the checker after editing any TSR, `docs/troubleshooting/TSR-TEMPLATE.md`,
or `docs/troubleshooting-log.md`.

## Alternatives considered

1. Rewrite the full TSR corpus into a new prose style.
   - Rejected because the detailed evidence is useful and accepted history
     should not be flattened.
2. Add summaries only to the six densest TSRs.
   - Rejected because the checker and support-agent behavior need a consistent
     corpus-wide contract.
3. Improve only support-agent routing docs.
   - Rejected because the source records themselves should be readable before a
     model or route layer summarizes them.

## Consequences

### Benefits

- Each TSR now answers "what happened, why, what fixed it, how we know" in the
  first 30 seconds.
- Support-agent answers can use the RCA Summary as the first-pass historical
  explanation while still verifying current behavior against live docs/source.
- The checker catches stale links, status drift, missing summaries, and broken
  ADR references before those defects reach operators or future agents.

### Risks

- Summaries can drift from detailed evidence if future edits update one layer
  but not the other.
- A mechanical checker cannot judge whether the summary is concise or accurate;
  reviewer judgment remains required.
- Historical compact and full-template TSR formats still coexist, so the
  checker accepts both `Fix or mitigation` sections and full-template
  `Decision Linkage` evidence.

### Operational impact

- No GoCluster runtime, config, parser, protocol, telnet, peer, archive,
  queue, persistence, or connection behavior changes.
- Documentation and support-agent retrieval become easier to scan.
- Future TSR/template/index edits have a new read-only workflow checker.

## Links

- Related issues/PRs/commits:
- Related tests: `scripts/check-troubleshooting-records.ps1`
- Related docs: `docs/decision-memory.md`, `docs/troubleshooting-log.md`,
  `docs/troubleshooting/TSR-TEMPLATE.md`, `docs/dev-runbook.md`,
  `scripts/README.md`, `customgpt/troubleshooting-index.md`
- Related TSRs: TSR-0002 through TSR-0031
- Supersedes / superseded by:
