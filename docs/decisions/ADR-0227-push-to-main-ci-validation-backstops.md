# ADR-0227: Push-to-Main CI Validation Backstops

- Status: Accepted
- Date: 2026-07-11
- Decision Origin: Design
- Selective supersession: ADR-0228 replaces Decision 3's always-run
  context-measurement fixture with path-triggered conditional execution and
  strengthens workflow ownership, syntax validation, permission enforcement,
  and hosted portability. All other ADR-0227 decisions remain accepted.

## Context

GoCluster is maintained by one developer who commits and pushes directly to
`main` without pull requests. The existing GitHub Actions workflow ran for both
pull requests and pushes, used a shallow checkout, and covered code-map
freshness, unit tests, and golangci-lint. It did not run the repository's full
production-Go validation baseline, check the complete pushed range for
whitespace errors, exercise the repo-owned Codex workflow checkers, or provide
a scheduled broad race backstop.

The repository already selects additional validation from the touched
engineering surface through the Codex Scope Ledger. CI must reinforce that
contract without pretending filenames can identify semantic risk or that
post-push checks prevent an invalid direct push from temporarily landing.

## Decision

1. The baseline CI workflow runs after every push to `main` and supports manual
   dispatch. It uses full Git history and runs code-map freshness, `go test`,
   `go vet`, Staticcheck 2026.1 (`v0.7.0`), the configured golangci-lint, and
   `git diff --check` over the exact pushed range.
2. Push-range checks require the event's non-zero pre-push revision. A missing
   local revision is fetched explicitly, and an unavailable revision fails the
   job instead of silently narrowing validation. Manual baseline runs require
   and check `HEAD^..HEAD`.
3. A separate Windows PowerShell workflow runs the repository-owned Codex
   contract checker, its fixture suite, the context-measurement fixture, the
   repo-skill verifier, and pushed-range whitespace validation when native
   GitHub path filters select current Codex workflow, validation, skill,
   checker, template, CI, or governing decision files.
4. A nightly workflow runs `go test -race -count=1 ./...` at 07:17 UTC and
   supports manual dispatch.
5. All workflows use least-privilege `contents: read` permissions and fail when
   a required command fails. No pull-request, branch-protection, review, or
   third-party changed-path policy is introduced.
6. The workflow checker and named fixtures protect mechanically representable
   CI invariants. Static checks do not prove hosted execution, semantic-risk
   selection, validation sufficiency, or engineering quality.
7. Push CI is post-push verification, and the nightly race job is a broad
   regression backstop. Targeted tests, change-triggered race checks, fuzzing,
   benchmarks, pprof, configuration checks, and provenance-independent
   scientific or model vectors and evaluations remain selected through the
   applicable Codex Scope Ledger.

## Alternatives considered

1. Require pull requests or branch protection.
   - Rejected because they do not match the repository owner's direct-to-main
     development method.
2. Run race, fuzzing, benchmarks, profiling, and model evaluations on every
   push.
   - Rejected because these checks have different cost and semantic triggers;
     a universal push lane would be disproportionate and still could not infer
     the correct oracle from filenames.
3. Use a third-party changed-path action for the Codex workflow.
   - Rejected because native `paths` filtering is sufficient and avoids an
     unnecessary dependency.
4. Treat CI as a replacement for local touched-surface validation.
   - Rejected because post-push baseline checks cannot establish parser,
     protocol, lifecycle, performance, configuration, or scientific/model
     correctness by themselves.

## Consequences

### Benefits

- Every direct push receives the complete baseline validation used for
  production-Go closeout.
- Codex workflow drift and repo-skill drift receive a dedicated bounded lane.
- Broad race regressions get regular coverage without burdening every push.
- Pushed-range checks fail closed when their baseline evidence is unavailable.

### Risks

- An invalid commit can remain on `main` until post-push CI reports failure.
- GitHub-hosted execution can fail for runner, service, or network reasons that
  local static validation cannot reproduce.
- Native path filters require maintenance as authoritative workflow ownership
  changes.
- A nightly race pass does not establish that a change received the targeted
  race, fuzz, performance, configuration, or model validation it required.

### Operational impact

- GitHub Actions gains one expanded push/manual baseline, one path-filtered
  Codex workflow-contract job, and one nightly/manual race job.
- The nightly schedule is 07:17 UTC with a 60-minute timeout.
- No production Go, runtime configuration, parser, protocol, queue, lifecycle,
  operator command, deployment, Fable workflow, or support-agent behavior
  changes.

## Links

- Related issues/PRs/commits:
- Related tests: `scripts/test-workflow-contract.ps1`,
  `scripts/test-measure-codex-workflow-context.ps1`,
  `scripts/verify-codex-skills.ps1`, `actionlint`, `git diff --check`
- Related docs: `.github/workflows/ci.yml`,
  `.github/workflows/codex-workflow-contract.yml`,
  `.github/workflows/nightly-race.yml`, `docs/dev-runbook.md`,
  `docs/runbooks/codex-workflow-checks.md`, `scripts/README.md`
- Related decisions: ADR-0147, ADR-0155, ADR-0156, ADR-0179, ADR-0194,
  ADR-0210, ADR-0221, ADR-0222, ADR-0223, ADR-0224, ADR-0225
- Related TSRs: none
- Supersedes / superseded by: none
