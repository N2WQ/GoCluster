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

Use a read-only fresh-verifier explorer when the active environment supports
independent agents and the user has not explicitly prohibited independent-agent
use. Otherwise, perform a fresh self-verification pass by resetting reviewer
context and re-checking the approved scope, current diff, evidence, validation
lane, ADR/TSR impact, and claim wording. Report unsupported, prohibited,
failed, or timed-out independent review as evidence status.

The verifier pass must fail the closeout if implementation, validation,
performance, scientific/model, or operator-facing claims are not supported by
current-session source inspection, command output, tests, benchmark/profile
data, runtime evidence, or decision records.

Fresh-verifier explorer findings are evidence only. The lead agent owns the
final Review Pass, integration of any fixes, validation claims, ADR/TSR
handling, Scope-to-Code Traceability, and closeout wording.

For high-risk workflow, runbook, rubric, template, or repo-managed skill
changes where `go-code-quality-review` is not applicable, use the existing
fresh-verifier explorer role with a prompt to independently score applicable
SELF-AUDIT rows. This specializes the fresh-verifier role; it does not create a
new review role.

## Go Code Quality Review

For Non-trivial Go implementation work, use an independent
`go-code-quality-review` explorer after code is written and before final
closeout when independent agents are supported and not explicitly prohibited.
The explorer has its own context window and reviews the Go diff against the
approved scope, code-quality rules, validation lane, comment intent, bounded
state, lifecycle/resource ownership, anti-speculative implementation, and
claim evidence.

The Go quality explorer reports findings only. It must not edit, propose diffs,
run formatters, create generated artifacts, or run broad/full validation
suites. If the explorer is unsupported, prohibited, failed, or timed out, report
that status in the Review Pass and Self-Audit; for high-risk Go work, treat it
as a review/validation gap unless explicitly waived.

The Go quality explorer must score only the SELF-AUDIT rows it can inspect at
its post-code phase. It must not final-score Fresh verification and claim
evidence when a later fresh-verifier pass has not yet happened.

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
- independent pre-code and post-code explorers were used when supported and
  not explicitly prohibited, or their unavailable/prohibited/failed/timed-out
  status was reported
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
- YAML comment/header audit
- Go comment intent audit
- Go crawler-entry audit
- Concurrency, backpressure, and resource bounds
- Leak-detection evidence
- Fresh verification and claim evidence
- Independent-agent/subagent use and lead ownership
- Anti-speculative implementation guard
- Verification and checker discipline
- Documentation, decision memory, and traceability
- Workflow-drift audit
- Validation block completeness

### Self-Audit rules
- Use `PASS`, `FAIL`, or `N/A` only.
- `N/A` is allowed only when the category truly does not apply.
- Every `FAIL` must include a short explanation and next action.
- Do not hide uncertainty. If evidence is incomplete, fail the category.
- Use one short note per grouped category. Reference earlier review evidence when
  that already establishes the point.
- Independently reviewed high-risk rows must cite the independent evidence
  source or reported gap/waiver. Do not silently lead-fill `PASS` after an
  independent review is unsupported, prohibited, failed, timed out, missing, or
  stale.
- Command-backed `Concurrency, backpressure, and resource bounds` or
  `Leak-detection evidence` rows must reference the captured excerpt in
  `Verification command reporting`. Do not paste the same excerpt again in
  Self-Audit; if the required excerpt is missing, failed, timed out, stale,
  cached without usable output, or waived, report that status instead of
  scoring the row as `PASS`.
- The lead agent owns the final PASS/FAIL/N/A disposition for every row. The
  Independent-agent/subagent use and lead ownership row is final-scored by the
  lead from independent evidence/status plus the lead's own gate checks.

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

This section is the single canonical location for captured validation command
excerpts. Later `SELF-AUDIT` and `CLOSEOUT` entries should reference this
section by name instead of repeating excerpts.

For command-backed high-risk concurrency, lifecycle, queue, timer, shutdown,
shared-state, or leak-detection claims, include a short captured transcript
excerpt here. The excerpt must show enough current-session output to support
the claim:
- command or evidence source
- target scope
- pass, fail, timeout, skip, cached, partial, or waived status
- the key line(s) that prove the result, profile/trace finding, or failure mode
- whether the result was incremental or final

Do not paste full logs, environment dumps, secrets, tokens, credentials,
private hostnames, unnecessary user data, or large runtime traces. Redact
sensitive content and say what was redacted. Static source reasoning remains
allowed, but label it as static reasoning and name the inspected files instead
of presenting it as command-backed validation.

Example shape:
- `go test ./...` - baseline regression check - pass
- `go test -race ./...` - concurrency/lifecycle verification - pass - final;
  excerpt: `ok  github.com/N2WQ/GoCluster/internal/cluster  ...`
- `go test ./internal/cluster -run TestSlowClientDropPolicy` - targeted regression - pass
- `git diff --check` - documentation-only whitespace check - pass

## Final validation block
The `VALIDATION` marker must end with these exact three lines:

Validation Score: X/6
Failed items: none | <comma-separated failed item numbers/names>
Auto-fail conditions triggered: no | yes (<conditions>)
