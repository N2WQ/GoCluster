# docs/dev-runbook.md

This runbook defines the expected validation commands and when to use them.
For Non-trivial closeout, this file is the required checker source; do not rely
only on the abbreviated baseline in `AGENTS.md`.

## Principles
- Run the smallest useful check early, then broaden.
- Use incremental validation after each meaningful slice.
- Use the full suite for the selected validation lane before calling a
  Non-trivial change complete.
- Report commands and results honestly.
- Ground validation, performance, and science/model claims in the command
  output, benchmark/profile data, runtime captures, source inspection, or
  decision records actually inspected in the current session.
- Report commands through the canonical `Verification command reporting`
  contract in `docs/review-checklist.md`.

## Baseline environment
Expected Go toolchain:
- `go`
- `go test`
- `go vet`
- `staticcheck`
- `golangci-lint` pinned to the CI version in `.github/workflows/ci.yml`

Expected agentic workflow helpers:
- `rg`
- `gopls`
- `callgraph`
- `jq`
- `yq`
- `fd`
- `bat`

If a tool is missing locally, report that fact explicitly and treat it as a validation gap, not a silent success.

Optional investigation helpers:
- Graphviz `dot`, `goda`, and `go-callvis` for package/call graph visualization
- `semgrep` and `ast-grep` for structural search when text search is
  insufficient
- Sysinternals tools such as `handle.exe`, TCPView, and Process Explorer for
  Windows handle/socket/process leak evidence

Missing optional tools should be reported only as conditional evidence gaps for
the workflow that needed them. Their absence does not block ordinary Go
implementation, review, or validation.

## Triggered investigation recipes

When semantic navigation, blast-radius analysis, leak evidence, fuzzing, race
checks, benchmarks, profiling, or escape analysis is triggered, use
`docs/runbooks/codex-triggered-validation-tools.md` for command recipes.

## Default command set by task type

Task classification controls approval rigor; touched surface controls
validation commands. Select the applicable lane before using the command sets
below. Workflow contracts, executor guidance, and repo-managed skills use the
workflow/skill-doc lane even when Markdown-only. Other all-Markdown
documentation uses the documentation-only lane. Code, config, scripts, CI,
schemas, generated artifacts, runtime data, and runtime contracts use their
applicable code/config/script/mixed lane. Structured workflow metadata adds
checks within the workflow lane; it does not select task size.

### Small code change
Minimum expected sequence:
1. targeted test if available
2. `go test ./...`

Add `go vet ./...` and `staticcheck ./...` if the change touches exported/shared logic, parsing, or anything likely to affect multiple packages.

### Documentation-only Markdown change
Use this lane only when all changed files are Markdown documentation and the
change does not touch code, config, generated artifacts, scripts, CI, schemas,
protocol/runtime contracts, or checked-in data consumed by the runtime.
Workflow contracts, executor guidance, runbooks, rubrics, templates, and
repo-managed skills are excluded; they use the workflow/skill-doc lane.

Minimum expected sequence:
1. targeted text checks for changed terms, cross-references, and required
   workflow strings
2. reviewer diff pass confirming the diff is documentation-only and internally
   consistent
3. `git diff --check`

Add only the documentation checks that apply:
- workflow-drift audit for workflow contract, runbook, template, rubric, review
  checklist, repo-managed skill, or workflow-script documentation changes
- fresh verifier and claim-evidence text checks when workflow docs change those
  requirements
- subagent-use text checks when workflow docs change delegated or parallel
  agent rules, including `Approved vN`, `SCOPE ADVERSARIAL REVIEW`,
  `scope-ledger-adversarial-review`, `go-code-quality-review`,
  `fresh-verifier explorer`, `parallel-discovery`,
  `scientific-model-oracle`, `requirements-ambiguity-review`,
  `design-challenger`, `test-strategy-adversary`, independent-agent
  support/authorization/prohibition status, `not authorized/not requested`,
  separate context windows, allowed actions, lead ownership, and stop
  conditions
- support-agent routing review when operator-support topics or support-routing
  docs changed
- `scripts/check-troubleshooting-records.ps1` for troubleshooting record,
  `docs/troubleshooting/TSR-TEMPLATE.md`, or `docs/troubleshooting-log.md`
  changes
- `scripts/check-yaml-doc-rigor.ps1 -CommentOnlyCompare` only for comment-only
  checked-in first-party YAML changes; YAML content changes leave this lane

Do not run `go test ./...`, `go vet ./...`, `staticcheck ./...`,
`golangci-lint`, or `go test -race ./...` solely because Markdown changed.
If the diff expands beyond documentation-only Markdown, switch to the relevant
code, mixed, YAML/config, script, CI, generated-artifact, or runtime-contract
lane and run the required checks for that lane.

### Workflow/skill-doc change
Use this lane when the diff changes workflow contracts, validation rules,
runbooks, review checklists, templates, Codex guidance, Fable guidance,
repo-managed Codex skills, or Fable `.claude/agents|skills/*` definitions.
This lane also applies when those surfaces are Markdown-only. Structured
workflow metadata such as `codex-skills/**/agents/openai.yaml` or
`.claude/agents/*.md` frontmatter adds metadata-specific checks below.

Minimum expected sequence:
1. targeted text checks for changed workflow terms, cross-references, required
   evidence markers, and exact workflow strings
2. workflow-drift audit against the applicable executor surfaces. Codex uses
   `docs/runbooks/codex-workflow-checks.md`; Fable uses `CLAUDE.md`, `docs/fable-workflow.md`,
   `docs/fable-review-checklist.md`, `docs/fable-validation.md`,
   `docs/templates/fable-non-trivial-change-template.md`, `.claude/agents/*.md`,
   and `.claude/skills/**/SKILL.md` for Fable
3. reviewer diff pass confirming the diff is internally consistent and stays
   within the approved workflow/documentation/skill scope
4. `git diff --check`

Add the applicable Codex checks from `docs/runbooks/codex-workflow-checks.md`,
or the Fable checks below:
- Fable agent frontmatter/tool-grant review when `.claude/agents/*.md` changes,
  confirming names, descriptions, models, and tool grants match the read-only or
  worker boundaries in `CLAUDE.md` and `docs/fable-workflow.md`
- Fable skill frontmatter/body review when `.claude/skills/**/SKILL.md` changes,
  confirming trigger descriptions, skill names, and referenced workflow docs
  stay coherent
- explicit YAML comment/header audit disposition when repo skill metadata YAML
  or Fable workflow frontmatter changes: the `data/config/README.md` five-line
  runtime-config header standard is `N/A` unless a stricter local metadata
  standard applies; replace it with metadata/body sync, frontmatter/manifest or
  tool-grant consistency, and the relevant repo skill or Fable workflow review
- fresh-verifier explorer for high-risk workflow or skill changes when
  independent agents are supported, authorized, and not explicitly prohibited;
  otherwise report the canonical independent-agent status, a separate detail,
  and any separate waiver disposition
- support-agent routing review when developer/support routing docs changed

Do not run `go test ./...`, `go vet ./...`, `staticcheck ./...`,
`golangci-lint`, or `go test -race ./...` solely because workflow docs or repo
skill metadata changed. If the diff expands into Go code, runtime config,
scripts, CI, generated artifacts, schemas, checked-in runtime data, or
protocol/runtime contracts, switch to the relevant lane.

### Script-only change
Use this lane when the diff changes scripts but does not change Go code,
runtime config, CI, generated artifacts, schemas, checked-in runtime data, or
protocol/runtime contracts. Task size still controls whether approval is Small
or Non-trivial.

Minimum expected sequence:
1. parse or syntax-check each changed script with its native engine
2. run the narrow positive and negative fixture tests owned by the script
3. review script documentation, failure behavior, and changed command output
4. run `git diff --check`

Add only checks invoked or governed by the changed script. Do not infer Go
validation from the script extension or from a script that merely checks
workflow text. Run Go commands only when the script change modifies Go build,
test, generation, or runtime behavior, and then run the commands whose behavior
or outputs can change.

### Non-trivial code, mixed, or runtime-contract change
Default full sequence for code, mixed, or runtime-contract changes:
1. targeted package test(s) during development
2. `go test ./...`
3. `go vet ./...`
4. `staticcheck ./...`
5. `golangci-lint run ./... --config=.golangci.yaml`

Also required as applicable:
- `scripts/check-yaml-doc-rigor.ps1` for checked-in first-party YAML additions
  or edits; add `-CommentOnlyCompare` when the intended YAML change is
  comment-only
- `scripts/check-go-crawler-entry-comments.ps1 -ChangedOnly -FailOnMissing`
  for additions or material changes to support-critical Go files
- for comment-only Go changes: `gofmt`, targeted package tests when packages
  were touched, `git diff --check`, and a reviewer diff pass confirming the
  non-comment Go diff is empty
- `go test -race ./...` for concurrency, queues, timers, cancellation, lifecycle, shutdown, or long-lived connections
- fuzzing for parser/protocol changes
- benchmarks for hot-path changes
- pprof for meaningful performance claims

## Targeted test examples
Use the narrowest useful targeted commands during implementation, then run the broader suite.

Examples:
- `go test ./internal/cluster -run TestSlowClientDropPolicy`
- `go test ./internal/parser -run TestRejectsMalformedControlBytes`
- `go test ./... -run TestGracefulShutdown`

## Suggested execution sequence for Non-trivial work
Example cadence:
1. after milestone 1: targeted checks for the selected validation lane
2. after milestone 2: targeted checks plus the broader lane checks
3. before closeout: final lane-required checks
4. final if applicable: `go test -race ./...`, fuzz, benchmark, pprof

When full-suite Go validation is required, keep final validation lead-owned and
sequential unless isolated worktrees and caches are intentionally used. If
parallel validation causes Go cache or export-data errors, run
`go clean -cache -testcache` and rerun the affected suite sequentially.

## Reporting format
Use `docs/review-checklist.md` `Verification command reporting`; later markers
reference that evidence rather than repeating it. The final marker uses the
exact block owned by `VALIDATION.md`.
