# ADR-0208: Fable Workflow Gap Corrections

- Status: Accepted
- Date: 2026-07-09
- Decision Origin: Design

## Context

Following `ADR-0206`, the user required an exhaustive, verified re-read of
every Codex-side and Fable-side workflow-related file before further changes
to the Fable contract, after two earlier rounds of self-audit still missed
real content. That exhaustive pass, followed by an independent adversarial
review, found two small Fable-contract completions and, incidentally, two
stale template cross-references in shared decision-memory infrastructure
that both executors rely on.

## Decision

No durable architecture or workflow-authority decision changed. This ADR
records completion and correction work only:

1. Ported `codex-skills/pprof-impact-review/SKILL.md` to `.claude/skills/
   pprof-impact-review/SKILL.md`, marked optional/non-gated to match its
   actual (non-required) status in `AGENTS.md`'s own Skill Check list.
2. Added the generated-code-map freshness check to `docs/fable-workflow.md`'s
   Current-State Discovery section, matching `AGENTS.md`'s equivalent
   requirement.
3. Corrected `docs/fable-workflow.md`'s Decision-Memory Handling section to
   name the templates verified as actually in use
   (`docs/templates/adr-template.md`, `docs/troubleshooting/TSR-TEMPLATE.md`)
   rather than deferring silently to shared docs that themselves contained
   stale pointers.
4. Fixed `docs/decision-log.md`'s stale `ADR-TEMPLATE.md` pointer to name
   `docs/templates/adr-template.md`, the template current ADR practice
   (`ADR-0206`, `ADR-0092`, and others) actually follows.
5. Fixed `docs/decision-memory.md`'s stale `docs/templates/tsr-template.md`
   pointer to name `docs/troubleshooting/TSR-TEMPLATE.md` — confirmed by the
   exact match between that file's `RCA Summary` sub-bullets and the
   required-fields list already documented in `docs/decision-memory.md`, and
   by `scripts/check-troubleshooting-records.ps1`'s mechanical requirement
   for a literal `## RCA Summary` heading, which only `TSR-TEMPLATE.md` has.

## Alternatives considered

1. Leave the stale pointers in place and only add a caveat note in
   `docs/fable-workflow.md`.
   - Rejected because it protects Fable specifically but leaves the actual
     shared bug in place for Codex and any future reader of
     `docs/decision-log.md`/`docs/decision-memory.md`.
2. Also fix `AGENTS.md`'s matching stale TSR pointer.
   - Rejected for this ADR because `AGENTS.md` is Codex's primary contract
     file and stays outside this repository's established Fable-work
     boundary; flagged for separate handling.
3. Delete the two unused legacy template files
   (`docs/decisions/ADR-TEMPLATE.md`, `docs/templates/tsr-template.md`).
   - Rejected as higher-risk cleanup than a pointer fix and deferred to a
     separately-scoped ledger.

## Consequences

### Benefits

- Fable's contract no longer has a SELF-AUDIT row (`YAML comment/header
  audit`'s sibling gaps around Go crawler-entry and now pprof review) or
  discovery step without backing content.
- Both executors now get correctly routed to the ADR/TSR templates that
  match real practice and the mechanical checker, closing a bug that
  predated this work and affected Codex equally.

### Risks

- `AGENTS.md`'s own stale TSR pointer remains uncorrected; a Codex session
  reading only `AGENTS.md`'s Document Map (not `docs/decision-memory.md`)
  could still be misdirected until that file is fixed separately.
- The two unused legacy template files still exist and could still mislead
  a reader who finds them by directory browsing rather than by following
  the corrected pointers.

### Operational impact

- No runtime, protocol, config, or code behavior changes.
- Pointer-only corrections to shared, unforked decision-memory index files.

## Links

- Related issues/PRs/commits:
- Related tests: `git diff --check`, reviewer diff pass
- Related docs: `docs/fable-workflow.md`, `docs/decision-log.md`,
  `docs/decision-memory.md`, `docs/templates/adr-template.md`,
  `docs/troubleshooting/TSR-TEMPLATE.md`, `.claude/skills/
  pprof-impact-review/SKILL.md`
- Related ADRs: ADR-0206
- Related TSRs: none
- Supersedes / superseded by: none — extends ADR-0206
