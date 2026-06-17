# ADR-0192: ADR Reader Guide And Current Contract Map

Status: Accepted
Date: 2026-06-17
Decision Origin: Design

## Context

The ADR corpus is large enough that readers can confuse historical records,
proposals, superseded decisions, and lightweight no-durable-decision stubs with
current behavior. The path-reliability and VOACAP areas are especially dense
because many ADRs intentionally refine earlier runtime and diagnostics choices.

The decision log also carried a template-like ADR-0001 placeholder, an unused
ADR-0150 number, and an ADR-0022 Proposed record that can look current when
read without later context.

## Decision

No durable runtime decision change.

Add ADR reader guidance and a path/VOACAP current-contract map. Clarify the
ADR index notes for ADR-0001, ADR-0150, and ADR-0022 without rewriting decision
history or changing any runtime, config, parser, protocol, command, diagnostic,
or operator behavior.

## Alternatives considered

1. Rewrite older ADRs in place.
   - Rejected because ADRs are decision history and should not be reshaped into
     current-state documentation.
2. Change ADR-0022 from Proposed to another status.
   - Rejected because the file records a proposal, and the safer cleanup is to
     explain how to read it rather than rewrite its historical status.
3. Leave navigation to the raw ADR index.
   - Rejected because support agents and new human readers need a narrow
     current-reading path through dense decision chains.

## Consequences

### Benefits

- Human readers and AI agents get a clearer first path through ADR history.
- Path-reliability and VOACAP decisions are easier to navigate without treating
  old or superseded records as live contracts.
- Support-agent routing can point to the reader guide and current-contract map.

### Risks

- The current-contract map can drift if future path or VOACAP ADRs do not
  update it.
- The map intentionally summarizes navigation, so readers still need current
  docs, source, tests, and generated code maps for implementation claims.

### Operational impact

- No runtime behavior, config schema, parser behavior, command behavior,
  protocol behavior, diagnostics, queueing, lifecycle, CI, scripts, generated
  artifacts, or operator setting changed.

## Links

- Related issues/PRs/commits:
- Related tests: documentation-only validation, targeted text checks,
  `git diff --check`
- Related docs: `docs/decision-log.md`, `docs/decisions/README.md`,
  `docs/decisions/current-path-voacap-contract-map.md`,
  `customgpt/source-map.md`
- Related TSRs:
- Supersedes / superseded by:
