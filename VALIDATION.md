# VALIDATION.md - Non-trivial Task Compliance Rubric

Use this scorecard after any Non-trivial Codex task to verify that Codex
actually followed `AGENTS.md` and did not merely produce plausible output.
It is a scoring rubric, not a narrative response template. Evidence may be
reported through the compact markers in
`docs/templates/non-trivial-change-template.md`; do not require duplicate prose
when those markers already contain the evidence.

## How to use
- Score each of the 6 items as `0` or `1`.
- Total the score out of `6`.
- Apply the automatic fail rules even if the numeric score looks acceptable.
- If evidence is missing or ambiguous, score the item `0`.
- Do not give partial credit.

## Required final output block
For every Non-trivial task, Codex must end its final response with this exact
3-line block:

Validation Score: X/6
Failed items: none | <comma-separated failed item numbers/names>
Auto-fail conditions triggered: no | yes (<conditions>)

Do not accept paraphrases or extra lines inside that block.

## Scorecard

### 1) Scope gate and approval discipline
Score `1` only if scope was ledgered and approved, no pre-approval
implementation or full validation happened, required pre-approval independent
agents were read-only evidence gathering, `SCOPE ADVERSARIAL REVIEW` was
completed by the lead agent before the approval token, no silent scope
expansion occurred, and the approved Scope Ledger was slice-shaped with
implementation-ready slices. Final traceability must map back to approved
items. Unsupported, not authorized/not requested, explicitly prohibited,
failed, or timed-out independent review must be reported as evidence status or
a waiver. If the active platform requires an explicit user request before
subagents can be spawned, missing user authorization must be reported as `not
authorized/not requested`, not as `unsupported` or `explicitly prohibited`.
Otherwise score `0`.

### 2) Skill and workflow discipline
Score `1` only if Codex showed the skill check, classified the task correctly,
and followed the required workflow for that task type, including phase,
support/authorization/prohibition status, allowed actions, and lead-ownership
rules for any independent agents or subagents used. Repository workflow text
does not self-authorize subagent spawning when the active platform requires an
explicit user request. Otherwise score `0`.

### 3) Current-state understanding and dependency rigor
Score `1` only if pre-code current-state understanding and dependency coverage
were concrete and complete for the task, including `Dependency scan evidence`
for Full rigor, triggered code-walk evidence, triggered blast-radius audit, and
`Config Contract Audit` for config/schema work. Otherwise score `0`.

### 4) Pre-code design discipline
Score `1` only if Codex disclosed contract/user-visible behavior, provided a
distinct slice-by-slice implementation plan, architecture framing, and required
pre-code audits for the task type. Otherwise score `0`.

### 5) Verification and review discipline
Score `1` only if the validation lane was identified, lane-required checks were
actually run and reported honestly, incrementally when required, final
validation claims were lead-owned, and a `Review Pass` occurred before
closeout. Documentation-only Markdown changes may satisfy this item with
documentation review, targeted text checks, and `git diff --check` when the
documented lane criteria are met. Workflow or repo-managed skill documentation
changes that include skill metadata must use the workflow/skill-doc lane rather
than the Markdown-only lane. Triggered leak-detection evidence must distinguish
static reasoning, local test/race evidence, profile evidence, and runtime
confirmation. Command-backed concurrency, lifecycle, queue, timer, shutdown,
shared-state, or leak-detection validation claims must include a short captured
excerpt in `docs/review-checklist.md` `Verification command reporting`;
`SELF-AUDIT` and `CLOSEOUT` should reference that evidence instead of
duplicating it. Non-trivial Go implementation work must include an independent
`go-code-quality-review` result with SELF-AUDIT evidence for applicable rows it
can inspect at its phase when independent agents are supported, authorized, and
not explicitly prohibited, or report unsupported, not authorized/not requested,
prohibited, failed, timed-out, or waived status. High-risk closeout requiring a
fresh-verifier pass must include fresh-verifier evidence for later rows such as
Fresh verification and claim evidence; earlier Go quality review cannot
substitute for evidence that did not exist yet. Otherwise score `0`.

### 6) Documentation, decision memory, and traceability
Score `1` only if README/doc review status, decision-memory handling,
scope-to-code traceability, and the exact final validation block were present
and complete. Otherwise score `0`.

## Automatic fail conditions
Mark the task non-compliant regardless of numeric score if any of the following
happened:

1. Codex implemented, produced diffs, edited files, or ran full validation before `Approved vN`.
2. Codex claimed validation that was not actually performed.
3. Codex skipped repo-wide or shared-component dependency review for a change that clearly required it.
4. Codex omitted `README impact: Required|Not required` on a Non-trivial task.
5. Codex introduced user-visible behavior changes without explicitly disclosing them.
6. Codex omitted `go test -race ./...` for a change that touched concurrency, lifecycle, queues, cancellation, timers, long-lived connections, or shared mutable state, unless you explicitly waived it.
7. Codex left placeholders, stubs, `TODO`, or deferred-hardening markers in touched files.
8. Codex failed to include Scope-to-Code Traceability for approved scope items.
9. Codex omitted the exact final 3-line validation block.
10. Codex changed YAML/config/schema/defaulting behavior without a Config Contract Audit.
11. Codex introduced or preserved a runtime fallback for a YAML-owned setting without explicitly documenting and approving that exception.
12. Codex changed documented zero/false sentinel behavior without consumer-level regression tests.
13. Codex omitted `SCOPE ADVERSARIAL REVIEW` before presenting the approval token for a Non-trivial Scope Ledger.
14. Codex omitted required independent `scope-ledger-adversarial-review`
    before presenting the approval token when independent agents were supported,
    authorized, and not explicitly prohibited, unless the omission was reported
    as unsupported/not authorized/not requested/prohibited/failed/timed-out and
    explicitly treated as a status, gap, or waiver.
15. Codex claimed code-walk, blast-radius, or leak-detection coverage from tools or profiles that were not actually run or inspected.
16. Codex approved or executed a broad refactor-shaped Scope Ledger without slice-level objective, blast-radius boundary, production-safe stopping point, targeted checks, and per-slice validation evidence.
17. Codex treated a mixed code/config/script/CI/generated-artifact/runtime-contract diff as documentation-only validation.
18. Codex used a pre-approval subagent for file edits, diffs, formatters,
    generated artifacts, full validation, or anything other than read-only
    evidence gathering and adversarial review.
19. Codex omitted required independent `go-code-quality-review` or its
    applicable SELF-AUDIT evidence for Non-trivial Go implementation work when
    independent agents were supported, authorized, and not explicitly
    prohibited, unless the omission was reported as unsupported/not
    authorized/not requested/prohibited/failed/timed-out and explicitly treated
    as a status, gap, or waiver.
20. Codex let a subagent's output replace lead-agent ownership of
    `SCOPE ADVERSARIAL REVIEW`, integration, validation claims, ADR/TSR
    handling, Scope-to-Code Traceability, SELF-AUDIT final disposition, or the
    final response.
21. Codex claimed command-backed concurrency, lifecycle, queue, timer,
    shutdown, shared-state, or leak-detection validation evidence without a
    captured excerpt in `Verification command reporting`, unless the omission
    was explicitly reported as skipped, failed, timed-out, stale, cached without
    usable output, partial, waived, or a validation gap.

## Waivers
Waivers are allowed only when explicit, narrowly scoped, and time-bounded.
State what was waived, why, who approved it, mitigation, and expiry date.

If the waived item is part of an automatic fail condition, the task still fails
unless you explicitly override the rubric for that task.
