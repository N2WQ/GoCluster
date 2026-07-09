# ADR-0212: AGENTS TSR Template Pointer

- Status: Accepted
- Date: 2026-07-09
- Decision Origin: Design

## Context

`AGENTS.md` is the always-loaded Codex workflow contract for this repository.
Its Document Map still routed TSR work to `docs/templates/tsr-template.md`,
while `docs/decision-memory.md` and `docs/troubleshooting-log.md` identify
`docs/troubleshooting/TSR-TEMPLATE.md` as the active troubleshooting record
template.

ADR-0208 corrected the shared decision-memory pointer and explicitly left the
matching `AGENTS.md` pointer as a separate Codex-contract risk. ADR-0210 again
kept that correction separate from the shared runbook lane change.

## Decision

Update only the `AGENTS.md` Document Map TSR template pointer to
`docs/troubleshooting/TSR-TEMPLATE.md`.

No durable design, runtime, operator, validation, or troubleshooting-record
decision changes. This ADR is a lightweight traceability stub for a
Non-trivial workflow-contract documentation correction.

## Alternatives considered

1. Leave `AGENTS.md` unchanged.
   - Rejected because Codex sessions using only the always-loaded Document Map
     could still be routed to the unused legacy TSR template.
2. Delete `docs/templates/tsr-template.md`.
   - Rejected as higher-risk cleanup that should remain separately scoped.
3. Bundle Fable template or lane-name drift fixes.
   - Rejected because this slice is Codex-only and follows the separate risk
     boundary recorded in ADR-0208 and ADR-0210.

## Consequences

### Benefits

- Codex's always-loaded Document Map now points to the active TSR template.
- The correction is traceable without changing broader workflow behavior.

### Risks

- The unused legacy TSR template file still exists and can mislead by directory
  browsing; deletion remains separately scoped.

### Operational impact

- No runtime, config, protocol, parser, telnet, peer, archive, queue,
  persistence, connection, CI, generated-artifact, or operator command behavior
  changes.

## Links

- Related issues/PRs/commits:
- Related tests:
  - targeted TSR path text checks
  - workflow-drift audit
  - reviewer diff pass
  - `git diff --check`
- Related docs: `AGENTS.md`, `docs/decision-memory.md`,
  `docs/troubleshooting-log.md`, `docs/troubleshooting/TSR-TEMPLATE.md`
- Related ADRs: ADR-0208, ADR-0210
- Related TSRs: none
- Supersedes / superseded by: none
