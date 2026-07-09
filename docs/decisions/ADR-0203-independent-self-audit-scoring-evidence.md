# ADR-0203: Independent SELF-AUDIT Scoring Evidence

- Status: Accepted
- Date: 2026-07-09
- Decision Origin: Design

## Context

ADR-0202 made independent review agents default-on when supported and not
explicitly prohibited. That decision established three canonical review roles:
`scope-ledger-adversarial-review`, `go-code-quality-review`, and
fresh-verifier explorers. It also kept final workflow ownership with the lead
Codex agent.

The remaining gap was SELF-AUDIT scoring. The lead agent could still assign
`PASS` to high-risk rows after implementation even when the relevant
independent reviewer failed, timed out, was unavailable, or had not inspected
that category. That weakens the purpose of the independent review evidence.

## Decision

SELF-AUDIT keeps one canonical 15-row category set shared by
`docs/review-checklist.md` and
`docs/templates/non-trivial-change-template.md`:

1. Scope and dependency coverage
2. Code-walk and blast-radius evidence
3. Contract, config, and protocol correctness
4. YAML comment/header audit
5. Go comment intent audit
6. Go crawler-entry audit
7. Concurrency, backpressure, and resource bounds
8. Leak-detection evidence
9. Fresh verification and claim evidence
10. Independent-agent/subagent use and lead ownership
11. Anti-speculative implementation guard
12. Verification and checker discipline
13. Documentation, decision memory, and traceability
14. Workflow-drift audit
15. Validation block completeness

Independent reviewers supply evidence for the riskiest applicable rows:

- `go-code-quality-review` reports PASS/FAIL/N/A evidence for rows it can
  inspect after Non-trivial Go implementation code is written.
- `go-code-quality-review` does not final-score late evidence that does not
  exist yet. If a later fresh-verifier pass is required, Fresh verification and
  claim evidence remains partial or `N/A - not yet run` until that pass.
- Fresh-verifier explorers provide the later independent evidence for high-risk
  closeout, including workflow or repo-managed skill changes where
  `go-code-quality-review` is not applicable.
- Unsupported, prohibited, failed, timed-out, missing, or stale independent
  evidence cannot be silently converted to `PASS`. It must be reported as
  `FAIL`, a gap/waiver, or `N/A` only when genuinely inapplicable.
- The lead Codex agent remains responsible for final PASS/FAIL/N/A
  disposition, fixes, validation claims, ADR/TSR handling, traceability, and
  the final response.

Repo skill metadata YAML is not subject to the exact
`data/config/README.md` runtime-config five-line header standard unless a
stricter local skill-metadata standard is later adopted. When repo skill YAML
changes, Codex must explicitly disposition the YAML comment/header SELF-AUDIT
row and replace the runtime-config header check with metadata/body sync,
frontmatter/manifest consistency, and `scripts/verify-codex-skills.ps1`.

## Alternatives considered

1. Keep SELF-AUDIT scoring entirely lead-owned.
   - Rejected because it allows the lead agent to self-grade the highest-risk
     rows even when independent evidence is absent or stale.
2. Add a separate workflow-specific review role.
   - Rejected because ADR-0202 already defines the canonical independent
     review roles. High-risk workflow or skill closeout fits the existing
     fresh-verifier role with a specialized prompt.
3. Let independent reviewers own final SELF-AUDIT scores.
   - Rejected because ADR-0202 intentionally keeps final gate ownership,
     validation claims, traceability, and closeout with the lead agent.

## Consequences

### Benefits

- High-risk SELF-AUDIT rows are grounded in independent evidence when the
  environment supports it.
- Missing or stale independent evidence becomes visible instead of being
  smoothed into a lead-filled `PASS`.
- The change reuses ADR-0202's role model and does not add new subagent
  machinery.

### Risks

- The workflow has more closeout coordination and more explicit evidence
  disposition.
- Independent reviewers can still miss context; the lead agent must verify and
  disposition findings against the approved scope and current workspace.
- Category scoring is phase-specific, so closeout must distinguish post-code Go
  review evidence from later fresh-verifier evidence.

### Operational impact

- No runtime, config, protocol, parser, telnet, peer, archive, queue,
  persistence, connection, CI, generated-artifact, or operator command behavior
  changes.
- Non-trivial workflow closeout now reports which SELF-AUDIT rows were
  independently evidenced, which were lead-owned, and which were gaps or
  waivers.

## Links

- Related issues/PRs/commits:
- Related tests:
  - `scripts/verify-codex-skills.ps1`
  - targeted workflow text checks
  - targeted support-agent routing text checks
  - `git diff --check`
- Related docs: `AGENTS.md`, `docs/change-workflow.md`,
  `docs/review-checklist.md`, `docs/dev-runbook.md`, `VALIDATION.md`,
  `docs/templates/non-trivial-change-template.md`,
  `docs/WORKING_WITH_CODEX.md`, `codex-skills/README.md`,
  `codex-skills/go-code-quality-review/SKILL.md`,
  `customgpt/source-map.md`, `customgpt/developer-guide-index.md`,
  `customgpt/common-questions.md`
- Related ADRs: ADR-0144, ADR-0179, ADR-0194, ADR-0199, ADR-0202
- Related TSRs: none
- Supersedes / superseded by: extends ADR-0202 review-agent evidence model
