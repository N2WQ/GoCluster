# ADR-0220: Codex Natural-Owner Workflow Deduplication

- Status: Superseded
- Date: 2026-07-10
- Decision Origin: Design

## Context

The Codex workflow repeats approval, routing, evidence, validation, marker, and
independent-review instructions across files that are loaded on the same task
paths. A failed uncommitted factoring attempt reduced some source files but
increased important loaded paths by requiring every specialist to read a new
shared contract.

The repository owner wants lower instruction-context cost without weakening
discovery, approval, independent review, validation, code-quality obligations,
decision memory, or traceability.

## Decision

Adopt one natural Markdown owner for each repeated Codex concern and leave only
the smallest usable route or report field in consumers. Reuse `AGENTS.md` for
the common specialist contract because it is already loaded in every specialist
context. Keep `docs/dev-runbook.md` as the sole validation-lane authority,
route Codex-specific workflow checks to a Codex component, and load optional
tool recipes only when their trigger applies.

Measure eleven declared instruction-context manifests at immutable baseline and
candidate revisions. Adoption requires no word or UTF-8-byte regression in any
manifest, shrinkage in every specialist manifest, and at least five percent
mean reduction across the eleven manifests for both metrics.

These measurements are deterministic context-footprint proxies. They do not
measure or claim billed, cached, reasoning, output, end-to-end, or model token
usage, model quality, or specialist effectiveness.

Fable-owned files remain unchanged. The shared runbook may move only explicitly
Codex-specific material; unchanged Fable checks and independent compatibility
review guard the shared semantics.

## Alternatives considered

1. Add one shared specialist contract file.
   - Rejected because each fresh specialist context would load it again.
2. Centralize all workflow semantics in `AGENTS.md`.
   - Rejected because it would increase every Codex context with phase-specific
     detail and command recipes.
3. Keep duplicated prose and rely on synchronization checks.
   - Rejected because it preserves prompt cost and competing semantic owners.
4. Run live model evaluations in this change.
   - Rejected from this scope. The accepted claim is deliberately limited to
     deterministic declared instruction-context manifests.

## Consequences

### Benefits

- Repeated Codex instructions have one reviewable semantic owner.
- Specialist contexts avoid a second common-contract read.
- Optional command recipes leave ordinary workflow paths.
- Mechanical checks can reject missing routes and competing owners.

### Risks

- Over-compression can make a mandatory rule unreachable.
- Static context proxies can be overstated as model-token evidence.
- Shared-runbook edits can accidentally affect Fable.

Exact route fixtures, bounded claim wording, per-path non-regression, unchanged
Fable checks, cross-executor review, and fresh verification mitigate these
risks.

### Operational impact

- No Go runtime, config, protocol, parser, queue, persistence, lifecycle, CI,
  deployment, or operator-command behavior changes.
- Only Codex workflow Markdown, repo skills, and PowerShell workflow tooling
  change. Fable-owned files remain unchanged.
- No live model evaluation is performed.

## Links

- Related tests: `scripts/test-measure-codex-workflow-context.ps1`,
  `scripts/test-workflow-contract.ps1`, `scripts/verify-codex-skills.ps1`
- Related docs: `AGENTS.md`, `VALIDATION.md`, `docs/change-workflow.md`,
  `docs/dev-runbook.md`, `docs/review-checklist.md`,
  `docs/templates/non-trivial-change-template.md`, `codex-skills/`
- Related ADRs: ADR-0092, ADR-0194, ADR-0210, ADR-0213, ADR-0214, ADR-0219
- Related TSRs: none
- Supersedes / superseded by: superseded by ADR-0221
