# docs/review-checklist.md

This document defines the mandatory review posture for Non-trivial Codex tasks.
The compact output shape is owned by
`docs/templates/non-trivial-change-template.md`.

## Review Pass
The Review Pass happens after implementation and before final closeout.

Purpose:
- switch from implementer mode to reviewer mode
- find hidden regressions, edge cases, and missing tests
- verify that the diff matches the approved scope
- verify that material progress, validation, performance, and science/model
  claims match current evidence

Required output:
- findings first, ordered by severity
- then confirmed fixes
- then rerun of affected validations for the selected validation lane
- then fresh verifier outcome when the task is high-risk

## Fresh Verifier Pass
For high-risk Non-trivial work, perform a fresh verifier pass after the Review
Pass and before final closeout.

Use a read-only fresh-verifier explorer only when the active environment and
user authorization support delegated or parallel agent work. Otherwise, perform
a fresh self-verification pass by resetting reviewer context and re-checking
the approved scope, current diff, evidence, validation lane, ADR/TSR impact,
and claim wording.

The verifier pass must fail the closeout if implementation, validation,
performance, scientific/model, or operator-facing claims are not supported by
current-session source inspection, command output, tests, benchmark/profile
data, runtime evidence, or decision records.

Fresh-verifier explorer findings are evidence only. The lead agent owns the
final Review Pass, integration of any fixes, validation claims, ADR/TSR
handling, Scope-to-Code Traceability, and closeout wording.

Review focus:
- correctness
- code-walk evidence for unfamiliar or cross-package behavior
- blast-radius coverage for shared, semantic, package, docs, and support impact
- protocol/format compatibility
- hidden behavior drift
- YAML schema, required-key, null, and sentinel-value behavior
- hidden runtime defaults or downstream config re-defaulting
- edge cases
- concurrency and lifecycle safety
- leak-detection evidence for goroutine, timer, channel, socket, file-handle,
  retained-heap, shutdown, or lifecycle concerns
- cancellation and shutdown
- backpressure, queue, drop, and disconnect semantics
- memory/allocation risks
- performance regressions
- unsupported performance, latency, p99, memory, scientific, model, path,
  VOACAP, p50, propagation, or call-correction claims
- speculative abstractions, compatibility shims, fallback paths, feature flags,
  broad cleanup, or future-proof hooks outside the approved scope
- maintainability and readability
- missing tests
- documentation gaps
- subagent assignments, if used, stayed within approved phase, write scope,
  allowed actions, and lead-owned disposition
- support-agent routing drift when operator docs or operator-visible behavior changed
- new or materially changed support-critical Go entry/integration files have
  crawler-entry comments where package/file ownership, related docs/tests, or
  troubleshooting routes would otherwise be hard to discover
- Go comments on support-critical code explain intent/why, ownership,
  invariants, resource bounds, lifecycle, and troubleshooting meaning
- Go comments avoid mechanical restatement of obvious code, simple booleans, or
  every repeated branch after the pattern is explained
- Go comment drift against code, tests, config, docs, ADRs, or support-agent
  routing docs
- YAML header consistency on new or changed first-party config files
- YAML key-comment coverage for non-obvious units, sentinels, ownership,
  side effects, runtime consequences, and safe-edit boundaries
- repeated YAML list/table schemas documented once by first occurrence or field
  guide, without duplicated row noise
- YAML comment drift against loaders, config docs, current code, or ADRs

If there are no material findings, say:
- `Review Pass findings: none material`

## Self-Audit
After the Review Pass, produce a Self-Audit with pass/fail for each category below.

### Required categories
- Scope and dependency coverage
- Code-walk and blast-radius evidence
- Contract, config, and protocol correctness
- Go comment intent audit
- Concurrency, backpressure, and resource bounds
- Leak-detection evidence
- Fresh verification and claim evidence
- Subagent use and lead ownership
- Anti-speculative implementation guard
- Verification and checker discipline
- Documentation, decision memory, and traceability
- Validation block completeness

### Self-Audit rules
- Use `PASS`, `FAIL`, or `N/A` only.
- `N/A` is allowed only when the category truly does not apply.
- Every `FAIL` must include a short explanation and next action.
- Do not hide uncertainty. If evidence is incomplete, fail the category.
- Use one short note per grouped category. Reference earlier review evidence when
  that already establishes the point.

## Closeout evidence
Every Non-trivial task must end with the template's `CLOSEOUT`,
`TRACEABILITY`, and `VALIDATION` markers. Keep the closeout concise and refer
to earlier markers instead of repeating evidence.

## Scope-to-Code Traceability
Map every Scope Ledger item with status `Agreed` or `Pending` as of the start of the implementation cycle to:
- code locations
- tests
- docs/comments updated
- support-agent docs updated or explicitly not impacted
- decision refs if applicable

No omissions allowed.

## Verification command reporting
For each major command, report:
- exact command
- why it was run
- result
- whether it was incremental or final

Example shape:
- `go test ./...` - baseline regression check - pass
- `go test -race ./...` - concurrency/lifecycle verification - pass
- `go test ./internal/cluster -run TestSlowClientDropPolicy` - targeted regression - pass
- `git diff --check` - documentation-only whitespace check - pass

## Final validation block
The `VALIDATION` marker must end with these exact three lines:

Validation Score: X/6
Failed items: none | <comma-separated failed item numbers/names>
Auto-fail conditions triggered: no | yes (<conditions>)
