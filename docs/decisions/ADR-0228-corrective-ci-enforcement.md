# ADR-0228: Corrective CI Enforcement

- Status: Accepted
- Date: 2026-07-12
- Decision Origin: Design

## Context

The first hosted runs of ADR-0227's workflows exposed two portability defects.
The baseline CI failed because `cmd/codemap` merged valid `go list -json`
stdout with cold-cache download diagnostics from stderr. The Windows contract
workflow failed because mutation fixtures assumed LF line endings while the
checkout used CRLF.

Review also found that ADR-0227 included an unapproved always-run context-
measurement fixture, covered only some checker invariants with negative
fixtures, enumerated only the three current workflow files, and relied on
string checks without continuous GitHub Actions syntax validation or complete
write-permission rejection.

## Decision

1. This decision selectively supersedes ADR-0227 Decision 3's always-run
   context-measurement validation. Both measurement scripts remain native path
   triggers, but their fixture runs only when the exact pushed range changes
   either script.
2. The Codex workflow owns `.github/workflows/**` so future workflow files
   trigger contract validation without enumerated-file maintenance.
3. Baseline CI installs Actionlint `v1.7.12` and validates all checked-in `.yml`
   workflow files before other repository checks.
4. Workflow permissions remain `contents: read`. The contract checker rejects
   `write-all`, workflow-level write grants, additive write grants, and job-
   level write grants unless a later accepted decision explicitly permits one.
5. `cmd/codemap` captures Go command stdout and stderr separately. Successful
   module and package discovery consume stdout only; failed commands preserve
   stderr diagnostics in their errors.
6. Workflow fixture mutation is line-ending independent and explicitly tested
   with CRLF input.
7. Named negative fixtures cover each material CI invariant category, including
   triggers, commands, versions, runner, shell, permissions, workflow ownership,
   conditional measurement routing, nightly behavior, documentation, and
   decision-index integrity.

All unrelated ADR-0227 decisions remain accepted, including full-history
checkout, strict pushed-range handling, post-push limitations, the nightly race
backstop, and touched-surface validation ownership.

## Alternatives considered

1. Pre-download modules before running codemap.
   - Rejected because warnings or diagnostics could still corrupt machine-
     readable output; separating stdout and stderr fixes the actual boundary.
2. Remove context-measurement scripts from the workflow paths.
   - Rejected because changes to those scripts would no longer trigger their
     required fixture.
3. Run the context-measurement fixture after every workflow change.
   - Rejected because the canonical runbook requires it only when informational
     context measurement changes.
4. Continue enumerating current workflow files.
   - Rejected because a future workflow could bypass contract validation.
5. Accept `contents: read` as sufficient permission evidence.
   - Rejected because another workflow-level or job-level write grant can exist
     alongside it.

## Consequences

### Benefits

- Cold-cache code-map validation remains machine-readable across platforms.
- Windows and LF-based fixture runs exercise the same mutation semantics.
- Conditional validation stays reachable without becoming universal overhead.
- Future workflow files receive syntax and contract validation automatically.
- Least-privilege checks fail on additive and job-level write grants.

### Risks

- Static permission checks understand the workflow permission shapes enforced
  here, not arbitrary YAML semantics.
- The conditional step depends on the pushed baseline remaining resolvable;
  ADR-0227 already requires failure when that evidence is unavailable.
- Actionlint in push CI cannot rescue a syntactically invalid `ci.yml` that
  prevents the workflow from starting, so local Actionlint remains required.

### Operational impact

- No production runtime, configuration, parser, protocol, queue, lifecycle,
  Fable workflow, operator command, or support-agent behavior changes.
- CI installs one additional pinned Go tool and conditionally executes the
  existing context-measurement fixture.
- Codemap subprocess handling changes only repository workflow tooling.

## Links

- Related issues/PRs/commits: hosted runs `29178767571` and `29178767558`
- Related tests: `go test ./cmd/codemap`,
  `scripts/test-workflow-contract.ps1`, `actionlint`, isolated-cache code-map
  validation
- Related docs: `.github/workflows/ci.yml`,
  `.github/workflows/codex-workflow-contract.yml`,
  `docs/runbooks/codex-workflow-checks.md`, `scripts/README.md`
- Related decisions: ADR-0147, ADR-0179, ADR-0210, ADR-0221, ADR-0222,
  ADR-0227
- Related TSRs: none
- Supersedes / superseded by: selectively supersedes ADR-0227 Decision 3 and
  extends its enforcement details; not superseded
