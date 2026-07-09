# docs/fable-review-checklist.md

This document defines the mandatory review posture for Non-trivial Fable
tasks. The compact output shape is owned by
`docs/templates/fable-non-trivial-change-template.md`. It is the Fable-native
counterpart to `docs/review-checklist.md` (Codex) — same 15-category risk
taxonomy, so the two contracts stay legible side by side, scored through
different mechanisms.

## Review Pass

The Review Pass happens after implementation and before final closeout.

Purpose:

- switch from implementer mode to reviewer mode
- find hidden regressions, edge cases, and missing tests
- verify the diff matches the approved plan
- verify material progress, validation, performance, and science/model
  claims match current evidence

Required output: findings first, ordered by severity; then confirmed fixes;
then rerun of affected validations for the selected lane; then
`fable-fresh-verifier` outcome when the task is high-risk. If no material
findings: `Review Pass findings: none material`.

## Fresh Verifier Pass

For high-risk Non-trivial work, use `fable-fresh-verifier` after the Review
Pass and before final closeout when independent agents are supported and
authorized (see `CLAUDE.md`). Otherwise perform a fresh self-verification
pass by resetting reviewer context and re-checking the approved plan,
current diff, evidence, validation lane, ADR/TSR impact, and claim wording.
Report unsupported, `not authorized/not requested`, prohibited, failed, or
timed-out independent review as evidence status.

The verifier pass must fail closeout if implementation, validation,
performance, scientific/model, or operator-facing claims are not supported
by current-session source inspection, command output, tests, benchmark/
profile data, runtime evidence, or decision records. Findings are evidence
only — the lead agent owns the final Review Pass, fixes, validation claims,
ADR/TSR handling, traceability, and closeout wording.

## Go Code Quality Review

For Non-trivial Go implementation work, use `fable-code-reviewer` after code
is written and before final closeout when independent agents are supported
and authorized. It has its own context window and reviews the Go diff
against the approved plan, `docs/code-quality.md`, validation lane, comment
intent, bounded state, lifecycle/resource ownership, anti-speculative
implementation, and claim evidence available at its phase. It also reports
PASS/FAIL/N/A evidence for the SELF-AUDIT rows it can inspect at that phase;
it must not final-score rows whose evidence does not exist yet (`fable-
fresh-verifier` supplies those later).

It reports findings only — it must not edit, propose diffs, run formatters,
create generated artifacts, or run broad/full validation suites.

Review focus:

- correctness
- code-walk evidence for unfamiliar or cross-package behavior
- blast-radius coverage for shared, semantic, package, docs, and support
  impact
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
- speculative abstractions, compatibility shims, fallback paths, feature
  flags, broad cleanup, or future-proof hooks outside the approved plan
- maintainability and readability
- missing tests
- documentation gaps
- subagent assignments, if used, stayed within approved phase, write scope,
  allowed actions, and lead-owned disposition
- independent pre-code and post-code explorers were used when supported and
  authorized, or their unsupported/`not authorized/not requested`/prohibited/
  failed/timed-out status was reported
- support-agent routing drift when operator docs or operator-visible
  behavior changed

If no material findings: `Go code quality review findings: none material`.

## Self-Audit

After the Review Pass, produce a Self-Audit with PASS/FAIL/N/A for each
category below.

### Required categories

- SA1 Scope and dependency coverage
- SA2 Code-walk and blast-radius evidence
- SA3 Contract, config, and protocol correctness
- SA4 YAML comment/header audit
- SA5 Go comment intent audit
- SA6 Go crawler-entry audit
- SA7 Concurrency, backpressure, and resource bounds
- SA8 Leak-detection evidence
- SA9 Fresh verification and claim evidence
- SA10 Independent-agent/subagent use and lead ownership
- SA11 Anti-speculative implementation guard
- SA12 Verification and checker discipline
- SA13 Documentation, decision memory, and traceability
- SA14 Workflow-drift audit
- SA15 Validation block completeness

These IDs and labels are canonical for Fable closeout reporting and match
Codex's `docs/review-checklist.md` numbering exactly, so the two contracts
stay legible side by side.

### Self-Audit rules

- Start with an applicability manifest that classifies every SA1-SA15 ID
  exactly once as applicable or not applicable.
- Applicable IDs require a `PASS` or `FAIL` result. Not-applicable IDs
  replace individual `N/A` rows and require an unambiguous reason; IDs with
  the same reason may be grouped.
- Missing, unknown, or duplicate IDs fail the Self-Audit.
- Every `FAIL` must include a short explanation and next action.
- Do not hide uncertainty — if evidence is incomplete, fail the category.
- Use one short note per grouped category; reference earlier review evidence
  when that already establishes the point.
- Independently reviewed high-risk IDs (SA7, SA8, SA9 in particular) must
  cite the independent evidence source or a reported gap/waiver. Do not
  silently lead-fill `PASS` after independent review is unsupported, `not
  authorized/not requested`, prohibited, failed, timed out, missing, or
  stale.
- Command-backed SA7 and SA8 results must reference the captured excerpt in
  Verification Command Reporting below rather than repeating it.
- The lead agent owns every final PASS/FAIL/applicability disposition. SA10
  (Independent-agent/subagent use and lead ownership) is always lead-scored,
  synthesized from independent-agent status plus the lead's own gate checks —
  it is not an ID an independent reviewer scores about itself.

## Verification Command Reporting

For each major command, report: exact command, why it was run, result,
whether incremental or final.

For command-backed `Concurrency, backpressure, and resource bounds` or
`Leak-detection evidence` claims specifically, also include a short pasted
excerpt from the actual command output or evidence source — the result
status line, and any failure/race/leak indication if one occurred. Do not
paste full logs, secrets, tokens, credentials, or large runtime traces;
redact and say so if needed. If output is unavailable, stale, skipped,
failed, timed out, or waived, report a gap/waiver instead of `PASS`. Static
reasoning remains allowed for these rows but must be labeled as static
evidence with the files inspected, not presented as command evidence.

Do not paste the same excerpt again in `SELF-AUDIT` or `CLOSEOUT` — those
markers reference this section by command name instead.

Example shape:

- `go test -race ./...` - concurrency/lifecycle verification - pass -
  final - `ok  internal/cluster  2.34s`

## Closeout Evidence

Every Non-trivial task must end with the template's `CLOSEOUT`,
`TRACEABILITY`, and `VALIDATION` sections. Keep closeout concise and refer
to earlier sections instead of repeating evidence.

## Scope-to-Code Traceability

Map every plan item that was in-scope at the start of implementation to:
code locations, tests/checks, docs/comments updated, support-agent docs
updated or explicitly not impacted, and decision refs if applicable. No
omissions allowed.

## Final Validation Block

The closing response must end with the exact three-line block defined in
`docs/fable-validation.md`.
