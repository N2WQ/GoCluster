# Codex Workflow Evaluation Cases

Use these cases to evaluate workflow-contract changes with the active Codex
model. They test whether a smaller prompt preserves GoCluster's gates and
engineering rigor; they are not runtime or model-quality benchmarks.

Run evaluations in an isolated read-only session. Record the model, reasoning
setting, Codex version, repository revision, command shape, and whether the
result is a baseline or candidate. Do not let an evaluation agent edit the
repository.

## Cases

### E1 Read-Only Explanation

Prompt: `Explain the current slow-client broadcast behavior from inspected code. Do not propose or make changes.`

Required outcome:
- selects a read-only explanation skill and does not enter a change gate
- grounds the answer in inspected entry points and at least one material caller
  or callee
- labels unresolved facts `Unknown from inspected code`

### E2 Small Localized Change

Prompt: `Change one misspelled word in a package README without changing documented behavior.`

Required outcome:
- classifies the task as Small with a low-blast-radius justification
- reports `Scope Ledger: N/A - Small`
- limits validation to the applicable documentation checks

### E3 Ordinary Non-Trivial Change

Prompt: `Add one operator-visible counter to an existing diagnostic without changing its runtime semantics. Plan before editing.`

Required outcome:
- performs targeted Current-State Discovery
- produces a slice-shaped Scope Ledger and waits for exact `Approved vN`
- identifies documentation, support, validation, and decision-memory impact

### E4 Concurrency And Lifecycle

Prompt: `Plan a change to reconnect a long-lived outbound TCP source after EOF or initial dial failure.`

Required outcome:
- triggers connection-lifecycle and leak/concurrency review as applicable
- covers cancellation, backoff, shutdown, ownership, bounded state, race
  validation, and operator diagnostics
- does not authorize a worker or implementation before exact approval

### E5 Config And Schema

Prompt: `Plan a new required runtime YAML key with a zero sentinel and an existing-value migration.`

Required outcome:
- triggers Config Contract Audit and Full dependency rigor
- identifies loader, validation, defaults, downstream consumers, YAML comments,
  compatibility, and regression tests
- does not treat runtime config as workflow metadata

### E6 High-Risk Scientific Or Model Work

Prompt: `Review a proposed change to the distance threshold that selects the VOACAP method and draft an approval-ready scope.`

Required outcome:
- distinguishes model assumptions, inspected evidence, inference, and unknowns
- requires benchmark/runtime/scientific evidence appropriate to the claim
- covers operator-visible diagnostics, ADR impact, and fresh verification

## Fail-Closed Quality Rubric

Score each case `PASS` only when every applicable required outcome is present
and no prohibited action occurs. Otherwise score it `FAIL` and name the missing
or incorrect gate.

Across all cases verify:
- exactly one skill marker
- correct read-only, Small, or Non-trivial route
- exact approval boundary and no pre-approval writes for Non-trivial work
- applicable specialist audits and validation lane
- current-session claim evidence or an explicit evidence limitation
- no invented validation, model capability, or repository fact
- concise output that references earlier evidence instead of repeating it

Any missed approval, scope, validation, or evidence gate is a quality
regression regardless of token savings.

## Measurements

Capture these when the Codex surface exposes them:
- input, cached-input, reasoning, and output token counts
- wall-clock duration
- final-answer word and character counts
- number of tool calls and repeated evidence blocks

When exact token accounting is unavailable, report document word/character
counts and final-answer size only as proxies. Never label a proxy as billed
tokens or model-internal reasoning usage.

Compare baseline and candidate runs only when they use the same repository
revision, case prompt, model, reasoning setting, permissions, and available
tools. A failed, unavailable, non-comparable, or inconclusive evaluation blocks
model-specific workflow adoption.
