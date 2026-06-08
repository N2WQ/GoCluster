# ADR-0155: Repo-Authoritative Codex Skills

- Status: Accepted
- Date: 2026-06-08
- Decision Origin: Design

## Context
gocluster already vendors several Codex workflow skills under `codex-skills/`,
but the workflow still documented copying selected skills into a user-level
Codex skills directory. The installer and verifier made a fresh checkout depend
on machine-local skill state, which conflicted with the portability goal: pull
the repo on another computer and continue gocluster work without maintaining
separate local skill copies.

## Decision
Make `codex-skills/` the authoritative project skill source for gocluster work.

- Repo-managed skills are authoritative when their triggers match.
- User-level skills do not override repo-managed skills and are outside the
  gocluster execution contract unless a task is explicitly user-specific.
- The repo no longer provides a script that copies skills into user-level skill
  storage.
- Skill verification checks the checked-in bundle itself, including metadata,
  duplicate names, referenced skill assets, and stale user-skill path wording.
- Runtime/system Codex skills, connector/plugin capabilities, credentials, and
  auth state remain external machine/runtime responsibilities.

## Alternatives considered
1. Keep the installer and require running it after each checkout.
   - Rejected because it preserves local drift and a second source of truth.
2. Vendor every locally available skill.
   - Rejected because runtime/system, current-docs, PDF, screenshot, plugin,
     and credential-dependent capabilities are not gocluster project contracts.
3. Rely only on plugin-cache skills.
   - Rejected because plugin cache and connector auth are machine-local and do
     not travel with the repository.

## Consequences
### Benefits
- A fresh checkout carries the gocluster skill workflow with the source tree.
- Skill changes are reviewable and traceable like other workflow changes.
- The verifier checks the durable repo artifact instead of a copied local
  shadow.

### Risks
- A machine can still have same-named user-level skills. `AGENTS.md` resolves
  this by making repo-managed skills authoritative for gocluster work.
- Plugin-backed GitHub connector behavior still depends on plugin availability
  and auth. Repo skills may document `gh` CLI workflows, but connectors remain
  external.
- Some generic utility skills are intentionally not vendored, so tasks such as
  PDF or screenshot work still depend on runtime/tool availability.

### Operational impact
- Onboarding no longer includes copying repo skills into user skill storage.
- Operators and agents should validate the repo bundle with
  `scripts/verify-codex-skills.ps1`.
- Secrets, tokens, and auth files remain machine-local and must not be committed.

## Links
- Related issues/PRs/commits:
- Related tests:
  - `scripts/verify-codex-skills.ps1`
  - targeted workflow text checks
- Related docs: `AGENTS.md`, `docs/change-workflow.md`, `docs/ENVIRONMENT.md`,
  `codex-skills/README.md`, `CLAUDE.md`
- Related TSRs:
- Supersedes / superseded by:
