# ADR-0140: Agentic Code-Walk, Blast-Radius, and Leak Workflow

- Status: Accepted
- Date: 2026-06-04
- Decision Origin: Design

## Context
The repository already had strict Codex workflow gates, retained-state and
hot-path audit skills, pprof review guidance, and validation rules. It did not
have first-class workflow routing for three recurring agentic development needs:

- understanding unfamiliar or cross-package code paths before planning
- identifying semantic, package, test, docs, and support blast radius
- investigating goroutine, lifecycle, handle/socket, and retained-heap leaks

The local workstation now has strong baseline tools for this work, including
`gopls`, `callgraph`, `jq`, `yq`, `fd`, `bat`, `govulncheck`, `dlv`,
`goimports`, `gotestsum`, and `benchstat`. Some useful tools remain optional,
including `goda`, `go-callvis`, `semgrep`, `ast-grep`, and Sysinternals.

## Decision
Add three repo-managed Codex skills:

- `go-code-walk`
- `go-blast-radius-audit`
- `go-leak-detection`

Keep `AGENTS.md` as a compact router that names the triggered skills without
embedding command recipes. Put detailed procedures in the skills and
`docs/dev-runbook.md`, and put workflow trigger rules in
`docs/change-workflow.md`.

Define a required/optional tool boundary:

- required tools are the repo's baseline Go workflow and semantic/navigation
  helpers needed by the triggered workflow
- optional tools improve specific investigations
- missing optional tools are conditional evidence gaps only when that workflow
  specifically needs them, not blockers for ordinary Go implementation, review,
  or validation

Add `scripts/verify-agentic-tools.ps1` to report required, recommended, and
optional local tools separately.

## Alternatives considered
1. Put all command guidance in `AGENTS.md`.
   - Rejected because `AGENTS.md` is always-loaded and should remain compact.
2. Add one large generic "agentic tools" skill.
   - Rejected because code walking, blast-radius analysis, and leak detection
     trigger for different reasons and produce different evidence.
3. Make optional tools mandatory.
   - Rejected because missing `goda`, `semgrep`, or Sysinternals should not
     block normal Go work when required baseline tools can answer the question.
4. Leave guidance only in conversation history.
   - Rejected because future runs need discoverable repo workflow rules.

## Consequences
### Benefits
- Codex can discover current code paths more systematically before planning.
- Blast-radius analysis has a named evidence path before scope approval and
  implementation.
- Leak investigations distinguish static reasoning, local tests, profile
  evidence, and runtime confirmation.
- Optional missing tools are visible without becoming broad blockers.
- `AGENTS.md` stays compact while detailed workflows remain reachable.

### Risks
- New skill triggers can overlap with retained-state, hot-path, and pprof
  skills; future agents must compose the minimal relevant skill set.
- Tool output can create false confidence if call graphs or profile snapshots
  are treated as concrete runtime proof.
- The verifier can drift if local workflow tools change.

### Operational impact
- No runtime, telnet, config, parser, protocol, queue, archive, peer, replay, or
  user-visible behavior changes.
- Future Non-trivial workflow closeouts can include `Code-walk evidence`,
  `Blast-radius audit`, and `Leak-detection audit` when triggered.
- Support-agent routing gains developer-workflow pointers only; it remains a
  routing layer.

## Links
- Related issues/PRs/commits:
- Related tests:
  - skill validation for `go-code-walk`, `go-blast-radius-audit`, and
    `go-leak-detection`
  - `scripts/verify-agentic-tools.ps1`
  - workflow text checks
- Related docs: `AGENTS.md`, `docs/change-workflow.md`,
  `docs/dev-runbook.md`, `docs/review-checklist.md`,
  `docs/templates/non-trivial-change-template.md`, `VALIDATION.md`,
  `docs/WORKING_WITH_CODEX.md`, `customgpt/source-map.md`,
  `customgpt/developer-guide-index.md`, `customgpt/common-questions.md`,
  `codex-skills/`
- Related TSRs:
- Supersedes / superseded by:
