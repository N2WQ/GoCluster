# docs/fable-validation.md - Non-Trivial Task Compliance Rubric (Fable)

Use this scorecard after any Non-trivial Fable task to verify that Fable
actually followed `CLAUDE.md` and did not merely produce plausible output.
It is a scoring rubric, not a narrative response template. This is the
Fable-native counterpart to `VALIDATION.md` (Codex) — a new file, not a
section inside `VALIDATION.md`, since that file is part of Codex's contract
and this workflow does not modify Codex-primary docs.

## How to use

- Score each of the 6 items as `0` or `1`.
- Total the score out of `6`.
- Apply the automatic fail rules even if the numeric score looks acceptable.
- If evidence is missing or ambiguous, score the item `0`.
- Do not give partial credit.

## Required final output block

For every Non-trivial task, Fable must end its final response with this
exact 3-line block:

Validation Score: X/6
Failed items: none | <comma-separated failed item numbers/names>
Auto-fail conditions triggered: no | yes (<conditions>)

Do not accept paraphrases or extra lines inside that block.

## Scorecard

### 1) Plan gate and approval discipline

Score `1` only if the plan was written and approved via `ExitPlanMode`
before any file write, no pre-approval implementation or full validation
happened, required pre-approval independent agents were read-only evidence
gathering, the scope adversarial review was completed by the lead agent
before requesting approval, no silent scope expansion occurred, and the
approved plan was slice-shaped with implementation-ready slices. Triggered
`fable-scientific-oracle`, `fable-requirements-adversary`, and
`fable-design-challenger` evidence must be completed or reported as a
gap/waiver before the plan's scope was drafted. Final traceability must map
back to approved items. Unsupported, `not authorized/not requested`,
explicitly prohibited, failed, or timed-out independent review must be
reported as evidence status or a waiver — never collapsed into a different
status or silently skipped. Otherwise score `0`.

### 2) Skill and workflow discipline

Score `1` only if Fable classified the task correctly and followed the
required workflow for that task type, including phase, support/
authorization/prohibition status, allowed actions, and lead-ownership rules
for any independent agents or subagents used. `CLAUDE.md` text alone does
not self-authorize subagent spawning in a context that requires explicit
user/session authorization. Otherwise score `0`.

### 3) Current-state understanding and dependency rigor

Score `1` only if pre-code current-state understanding and dependency
coverage were concrete and complete, including `Dependency scan evidence`
for Full rigor and triggered blast-radius/code-walk/config-contract skill
results. Triggered bounded parallel discovery (`Explore` wave) must show
disjoint evidence domains/questions, a shared on-disk state, conflicts
surfaced, and lead synthesis. Otherwise score `0`.

### 4) Pre-code design discipline

Score `1` only if Fable's plan disclosed contract/user-visible behavior,
provided a distinct slice-by-slice implementation plan, architecture
framing, and required pre-code audits for the task type. Triggered
`fable-scientific-oracle`, `fable-requirements-adversary`,
`fable-design-challenger`, and `fable-test-strategy-adversary` evidence
must be present, accurately statused, and lead-dispositioned. Otherwise
score `0`.

### 5) Verification and review discipline

Score `1` only if the validation lane was identified, lane-required checks
were actually run and reported honestly, incrementally when required, final
validation claims were lead-owned, and a Review Pass occurred before
closeout. Documentation-only Markdown changes may satisfy this item with
documentation review, targeted text checks, and `git diff --check` when
lane criteria are met — a diff that also touches `.claude/agents|skills/*`
frontmatter must use the workflow-contract lane, not the documentation-only
lane. Command-backed `Concurrency, backpressure, and resource bounds` or
`Leak-detection evidence` claims must include the captured excerpt in
`docs/fable-review-checklist.md`'s Verification Command Reporting;
`SELF-AUDIT` and `CLOSEOUT` must reference that evidence instead of
duplicating it. Non-trivial Go implementation work must include an
independent `fable-code-reviewer` result with SELF-AUDIT evidence for
applicable rows it can inspect at its phase when independent agents are
supported, authorized, and not explicitly prohibited, or report unsupported,
`not authorized/not requested`, prohibited, failed, timed-out, or waived
status. High-risk closeout requiring a `fable-fresh-verifier` pass must
include its evidence for later rows such as Fresh verification and claim
evidence; earlier Go quality review cannot substitute for evidence that did
not exist yet. Otherwise score `0`.

### 6) Documentation, decision memory, and traceability

Score `1` only if README/doc review status, decision-memory handling,
Scope-to-Code Traceability, and the exact final validation block were
present and complete. Otherwise score `0`.

## Automatic fail conditions

Mark the task non-compliant regardless of numeric score if any of the
following happened:

1. Fable implemented, produced diffs, edited files, or ran full validation
   before `ExitPlanMode` approval.
2. Fable claimed validation that was not actually performed.
3. Fable skipped repo-wide or shared-component dependency review for a
   change that clearly required it.
4. Fable omitted `README impact: Required|Not required` on a Non-trivial
   task.
5. Fable introduced user-visible behavior changes without explicitly
   disclosing them.
6. Fable omitted `go test -race ./...` for a change that touched
   concurrency, lifecycle, queues, cancellation, timers, long-lived
   connections, or shared mutable state, unless explicitly waived.
7. Fable left placeholders, stubs, `TODO`, or deferred-hardening markers in
   touched files.
8. Fable failed to include Scope-to-Code Traceability for approved plan
   items.
9. Fable omitted the exact final 3-line validation block.
10. Fable changed YAML/config/schema/defaulting behavior without a Config
    Contract Audit.
11. Fable introduced or preserved a runtime fallback for a YAML-owned
    setting without explicitly documenting and approving that exception.
12. Fable changed documented zero/false sentinel behavior without
    consumer-level regression tests.
13. Fable omitted the scope adversarial review before requesting
    `ExitPlanMode` approval for a Non-trivial plan.
14. Fable omitted required independent `fable-scope-adversary` review
    before requesting approval when independent agents were supported,
    authorized, and not explicitly prohibited, unless the omission was
    reported as unsupported/`not authorized/not requested`/prohibited/
    failed/timed-out and explicitly treated as a status, gap, or waiver.
15. Fable claimed code-walk, blast-radius, or leak-detection coverage from
    tools or skills that were not actually run or inspected.
16. Fable requested approval for or executed a broad, non-slice-shaped plan
    without slice-level objective, blast-radius boundary, production-safe
    stopping point, targeted checks, and per-slice validation evidence.
17. Fable treated a mixed code/runtime-config/script/CI/generated-artifact/
    runtime-contract diff as documentation-only or workflow-contract-lane
    validation.
18. Fable used a pre-approval subagent for file edits, diffs, formatters,
    generated artifacts, full validation, or anything other than read-only
    evidence gathering and adversarial review.
19. Fable omitted required independent `fable-code-reviewer` or its
    applicable SELF-AUDIT evidence for Non-trivial Go implementation work
    when independent agents were supported, authorized, and not explicitly
    prohibited, unless the omission was reported as unsupported/`not
    authorized/not requested`/prohibited/failed/timed-out and explicitly
    treated as a status, gap, or waiver.
20. Fable let a subagent's output replace lead-agent ownership of the scope
    adversarial review, integration, validation claims, ADR/TSR handling,
    Scope-to-Code Traceability, SELF-AUDIT final disposition, or the final
    response.
21. Fable claimed a command-backed `Concurrency, backpressure, and resource
    bounds` or `Leak-detection evidence` row as `PASS` without a captured
    excerpt per `docs/fable-review-checklist.md`'s Verification Command
    Reporting.
22. Fable collapsed `not authorized/not requested` into `unsupported` or
    `explicitly prohibited`, or treated `CLAUDE.md`/repository policy text
    alone as sufficient authorization to spawn a subagent in a context
    requiring explicit user/session authorization.
23. Fable omitted a triggered `fable-scientific-oracle`,
    `fable-requirements-adversary`, or `fable-design-challenger` independent
    pass when independent agents were supported, authorized, and not
    explicitly prohibited, unless the omission was reported as a gap or
    explicit waiver.
24. Fable began the first implementation slice without a triggered
    `fable-test-strategy-adversary` matrix and lead disposition, or
    continued after that review found a material scope gap without revised
    `ExitPlanMode` approval.
25. Fable labeled evidence independent when it did not come from a
    genuinely separate context window, or treated a `fable-design-
    challenger` result exposed to the lead's preferred solution as
    independent alternative-design evidence instead of `inconclusive -
    context contaminated`.

## Waivers

Waivers are allowed only when explicit, narrowly scoped, and time-bounded.
State what was waived, why, who approved it, mitigation, and expiry date.
If the waived item is part of an automatic fail condition, the task still
fails unless the user explicitly overrides the rubric for that task.
