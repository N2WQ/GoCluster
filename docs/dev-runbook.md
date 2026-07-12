# Development Validation Runbook

This shared runbook owns validation lane selection and commands. Codex and
Fable keep separate approval, planning, review, and reporting contracts.
Changing a workflow's reporting requirements does not change the other
executor's contract.

## Principles

- Task classification controls approval; touched surface and engineering risk
  control validation.
- Run the smallest useful targeted checks while working, then one complete
  selected lane on the final relevant state.
- A missing required tool is a reported gap. Missing optional investigation
  tooling matters only when that investigation is required.
- Report observed results honestly. Do not infer runtime, performance, or
  scientific confirmation from code shape.

Triggered race, fuzz, benchmark, profile, semantic-navigation, dependency, and
leak-investigation recipes live in
`docs/runbooks/codex-triggered-validation-tools.md` for Codex. Fable continues
to use the routes defined by `CLAUDE.md` and `docs/fable-workflow.md`.

## CI Backstops

Push-to-`main` CI is post-push verification. It can detect an invalid commit
after it lands, but it cannot prevent that commit from temporarily reaching
`main`. The nightly full race suite is a broad regression backstop; it does not
replace race validation triggered locally by concurrency, shared-state,
lifecycle, cancellation, queue, timer, or shutdown changes.

CI path filters and filenames cannot determine semantic engineering risk. They
do not replace targeted tests during implementation or the Scope Ledger's
selection of fuzzing, benchmarks, pprof, runtime-config checks,
provenance-independent scientific or model vectors, evaluations, and other
validation required by the touched behavior. These backstops do not change
Fable's separate approval, validation, or workflow contract.

## Documentation-only Markdown

Use this lane only when all changed files are ordinary Markdown documentation
and the change does not touch workflow contracts, code, config, scripts, CI,
schemas, generated artifacts, runtime data, or runtime contracts.

1. Check changed terms, links, and cross-references.
2. Review the diff for documentation-only scope and internal consistency.
3. Run `git diff --check`.

Do not run Go tests, vet, static analysis, lint, race, fuzz, benchmarks, or
profiles solely because Markdown changed.

## Workflow And Skill Documentation

Use this lane for executor contracts, workflow docs, validation rules,
runbooks, review checklists, templates, repo-managed skills, and their
structured metadata.

1. Run targeted text, ownership, trigger, and cross-reference checks.
2. Run the applicable executor workflow checker and its positive/negative
   fixtures.
3. Validate changed skill metadata and referenced assets.
4. Review shared-document changes for cross-executor semantic drift.
5. Review the final diff and run `git diff --check`.

Codex uses `docs/runbooks/codex-workflow-checks.md`. Fable's existing contract
remains owned by `CLAUDE.md`, `docs/fable-workflow.md`,
`docs/fable-review-checklist.md`, `docs/fable-validation.md`,
`docs/templates/fable-non-trivial-change-template.md`, `.claude/agents/`, and
`.claude/skills/`. Fable frontmatter, tool grants, metadata, and validation
requirements remain unchanged unless a separate Fable scope approves them.

Preserved Fable-specific checks:

- Fable agent frontmatter/tool-grant review when `.claude/agents/*.md` changes,
  confirming names, descriptions, models, and tool grants match the read-only or
  worker boundaries in `CLAUDE.md` and `docs/fable-workflow.md`
- Fable skill frontmatter/body review when `.claude/skills/**/SKILL.md` changes,
  confirming trigger descriptions, skill names, and referenced workflow docs
  stay coherent
- explicit YAML comment/header audit disposition when Fable workflow
  frontmatter changes: the `data/config/README.md` five-line runtime-config
  header standard is `N/A` unless a stricter local metadata standard applies;
  replace it with frontmatter, tool-grant, and Fable workflow consistency review

Do not run Go validation solely because workflow Markdown, skill Markdown, or
skill metadata changed. If the diff changes scripts, add the script lane. If it
changes runtime surfaces, use the applicable code/config/mixed lane.

## Script-only

Use this lane when scripts change without changing Go code, runtime config, CI,
schemas, generated artifacts, or runtime-consumed data.

1. Parse or syntax-check each changed script with its native engine.
2. Run the narrow positive and negative fixtures owned by the script.
3. Review failure behavior, command output, and script documentation.
4. Run `git diff --check`.

Do not infer Go validation from a script extension or from a script that checks
workflow text. Run Go commands only if the script changes Go build, test,
generation, or runtime behavior.

## Small Go Change

1. Run the targeted package test or checker when available.
2. Run `go test ./...`.

Add `go vet ./...` and `staticcheck ./...` when exported, shared, parsing, or
cross-package behavior can be affected.

## Non-trivial Code, Config, Or Mixed Change

For Codex, every Non-trivial change that touches production Go runs targeted
package tests during implementation and this complete baseline once on the
final relevant state. Fable's code/mixed/runtime-contract lane remains defined
by `docs/fable-workflow.md` and uses the same complete baseline:

1. `go test ./...`;
2. `go vet ./...`;
3. `staticcheck ./...`;
4. `golangci-lint run ./... --config=.golangci.yaml`.

Add as applicable:

- config/YAML rigor checks for first-party runtime configuration;
- comment-intent checks for support-critical Go;
- `go test -race ./...` for concurrency, lifecycle, or shared state;
- fuzzing for parser or protocol changes;
- benchmarks and pprof for performance claims.

For Codex Non-trivial config or mixed changes with no production Go diff,
select the commands required by the actual non-Go surface rather than inferring
Go tests. Fable continues to follow its own lane definition.

Run only commands relevant to the changed behavior. Report unavailable required
tools and failed or skipped checks rather than substituting a pass.

## Review Fixes

After a review fix, rerun affected targeted checks. Rerun the complete selected
lane only if the fix can invalidate broader evidence, such as shared behavior,
build configuration, interfaces, concurrency, or cross-package contracts.
