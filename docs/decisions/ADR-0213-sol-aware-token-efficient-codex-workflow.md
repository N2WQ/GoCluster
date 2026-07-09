# ADR-0213: Sol-Aware Token-Efficient Codex Workflow

- Status: Accepted
- Date: 2026-07-09
- Decision Origin: Design

## Context

ADR-0092 established a compact marker-driven Codex workflow without reducing
discovery, approval, validation, review, decision memory, or traceability.
ADR-0194 later added model-neutral fresh verification, current-session claim
evidence, and anti-speculative implementation.

The Codex contract has since accumulated repeated skill, subagent, evidence,
validation, and closeout instructions across the always-loaded contract and its
detailed documents. The repository owner also intends to use GPT-5.6 Sol for
future work. Current official guidance says GPT-5.6 benefits from shorter
prompts and deliberate reasoning-effort selection, while important constraints,
approval boundaries, and success criteria should remain explicit.

## Decision

Evaluate and, only where evidence preserves rigor, adopt a thinner always-loaded
Codex contract with canonical detailed owners, mechanical contract linting,
compact SELF-AUDIT references, and less repeated evidence.

Keep the workflow capability-aware rather than configuring the repository for a
specific model. Sol-specific `max` or `ultra` guidance may be adopted only after
representative read-only evaluation. Reasoning settings never grant approval,
delegation authority, write scope, or reduced validation.

Implementation is governed by Approved Scope Ledger v4. This ADR remains
the durable record for the completed slices and explicitly deferred work.

## Evaluation evidence

Baseline runs used Codex CLI `0.144.0-alpha.4`, `gpt-5.6-sol`, low reasoning,
ephemeral sessions, the read-only sandbox, revision `f6b1a4f`, and the six cases
in `docs/workflow-eval-cases.md`. Codex emitted exact per-turn token usage:

| Case | Input | Cached input | Output | Reasoning output | Final words |
| --- | ---: | ---: | ---: | ---: | ---: |
| E1 | 550,609 | 486,400 | 2,451 | 315 | 280 |
| E2 | 139,105 | 115,456 | 1,873 | 599 | 118 |
| E3 | 738,207 | 660,480 | 4,449 | 1,404 | 636 |
| E4 | 414,025 | 366,080 | 4,013 | 903 | 911 |
| E5 | 593,349 | 523,264 | 4,843 | 1,260 | 1,039 |
| E6 | 581,543 | 516,864 | 5,072 | 1,014 | 1,083 |
| Total | 3,016,838 | 2,668,544 | 22,701 | 5,495 | 4,067 |

The exact token counts include system, tool, repository, and cached context; they
are not attributable to `AGENTS.md` alone. Final-answer words are a local proxy,
not billed usage.

Candidate runs used the same prompts, model, effort, and permissions after the
approved workflow edits:

| Case | Input | Cached input | Output | Reasoning output | Final words |
| --- | ---: | ---: | ---: | ---: | ---: |
| E1 | 372,977 | 322,048 | 2,549 | 633 | 224 |
| E2 | 330,900 | 292,096 | 2,228 | 847 | 129 |
| E3 | 466,374 | 402,688 | 3,667 | 1,448 | 586 |
| E4 | 1,590,956 | 1,495,552 | 6,936 | 2,052 | 968 |
| E5 | 623,127 | 551,424 | 4,145 | 979 | 772 |
| E6 | 679,081 | 608,768 | 5,614 | 1,639 | 971 |
| Total | 4,063,415 | 3,672,576 | 25,139 | 7,598 | 3,650 |

The comparison is inconclusive for model-specific adoption. The initial harness
retained only the last assistant message, while Codex emits the required skill
marker in an earlier commentary message on some runs. A diagnostic full-stream
E2 run confirmed exactly one marker even though its last message contained
none. The runs also varied substantially in source exploration, so aggregate
input and reasoning tokens cannot be attributed to the smaller contract. Final
answer words fell 10.3%, but output tokens rose 10.7% and input tokens rose
34.7%. Under the fail-closed rubric, these results block Sol-specific `max` or
`ultra` workflow guidance; no quality or token-saving model claim is accepted.

## Implementation outcome

- `AGENTS.md` was reduced from 2,072 to 1,382 words (33.3%) while retaining the
  exact approval, skill, ledger-status, marker, traceability, and validation
  strings.
- `scripts/check-workflow-contract.ps1` now checks mechanical contract
  coherence and explicitly does not claim to prove conversational approval.
  Positive and required negative fixtures are executable through
  `scripts/test-workflow-contract.ps1`.
- SELF-AUDIT reporting uses one canonical SA1-SA15 mapping and a fail-closed
  applicability manifest instead of repeated `N/A` rows.
- YAML and Go comment rigor remains owned by `data/config/README.md` and
  `docs/code-quality.md`; the detailed workflow retains triggers, commands, and
  evidence routing.
- Sol-specific `max`/`ultra` guidance was not adopted because the representative
  comparison was inconclusive.
- Skill-body migration, Fable/custom-GPT changes, decision-memory redesign,
  runbook restructuring, model configuration, and API-only features remain
  deferred.

Validation completed with the workflow-contract positive/negative fixtures,
PowerShell parser checks, targeted exact-string checks, `git diff --check`,
`go test ./...`, `go vet ./...`, `staticcheck ./...`, and
`golangci-lint run ./... --config=.golangci.yaml`. An independent fresh verifier
reported no blocking findings and identified label-based specialist-skill
evidence as an explicit deferred migration.

## Alternatives considered

1. Keep the verbose contract unchanged.
   - Rejected because repeated instructions consume context and create drift
     without adding independent rigor.
2. Remove approval, review, or validation gates because Sol is more capable.
   - Rejected because model capability does not replace explicit authority or
     repository evidence.
3. Hard-code GPT-5.6 Sol in project configuration.
   - Rejected because model selection is an operator/platform choice and the
     workflow should remain portable.
4. Adopt API-only GPT-5.6 features in the Codex contract.
   - Rejected because API availability does not prove exposure in the active
     Codex surface.

## Consequences

### Benefits
- Lower always-loaded and repeated reporting cost when evaluations pass.
- Clearer ownership between the router, workflow, template, checklist, rubric,
  and scripts.
- Mechanical detection of exact-string and marker drift.
- Reasoning effort can use Sol capabilities without weakening phase gates.

### Risks
- Over-compression could make a mandatory rule unreachable.
- Proxy measurements could be mistaken for billed token usage.
- Automatic delegation modes could be misread as approval or write authority.
- Model behavior can change, so representative evaluation remains necessary.

### Operational impact
- No Go runtime, protocol, parser, telnet, queue, config, persistence, or
  operator-command behavior changes.
- Workflow-script changes receive script-specific tests and the selected
  Non-trivial validation lane.
- Fable, custom-GPT routing, repo model configuration, and API-only features are
  outside this decision.

## Links
- Related issues/PRs/commits:
- Related tests: `docs/workflow-eval-cases.md`, workflow-contract checker tests,
  targeted exact-string checks, reviewer diff, `git diff --check`
- Related docs: `AGENTS.md`, `docs/change-workflow.md`,
  `docs/templates/non-trivial-change-template.md`,
  `docs/review-checklist.md`, `VALIDATION.md`,
  `docs/WORKING_WITH_CODEX.md`, `docs/agent-lessons/README.md`
- Related external references:
  `https://developers.openai.com/api/docs/guides/latest-model`,
  `https://developers.openai.com/api/docs/models/gpt-5.6-sol`
- Related ADRs: ADR-0092, ADR-0156, ADR-0194, ADR-0202, ADR-0203, ADR-0204,
  ADR-0211, ADR-0212
- Related TSRs: none
- Supersedes / superseded by: extends ADR-0092 and ADR-0194
