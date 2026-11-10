# Historical Codex Workflow Smoke Cases

These cases are optional, non-authoritative prompts for manual smoke review.
They are not an adoption gate, model-quality benchmark, token-savings proof, or
required validation lane. Do not run them unless separately requested.

## Current Contract Expectations

When a case is used, review outcomes rather than response formatting:

- read-only work inspects current evidence without entering a change gate;
- Small work remains localized and uses touched-surface validation;
- Non-trivial mutation waits for exact `Approved vN` and stays within agreed
  scope;
- broad refactor-shaped scope is decomposed when real rollback, ownership,
  uncertainty, or validation boundaries exist;
- specialists trigger only for concrete risks and preserve their unique
  methods;
- validation follows touched surface and risk;
- material claims match observed evidence; and
- final review, documentation, durable decisions, and compact traceability are
  handled when applicable.

No skill marker, fixed report headings, numeric score, audit taxonomy,
independent-agent envelope, or visible irrelevant category is expected.

## Example Prompts

1. Explain slow-client broadcast behavior from inspected code without changes.
2. Correct one README typo without changing documented behavior.
3. Audit a workflow defect, then request implementation in a later turn.
4. Plan an operator-visible diagnostic counter.
5. Plan reconnect behavior for a long-lived TCP source.
6. Plan a required YAML key with missing, zero, and existing-value semantics.
7. Review a scientific threshold change and its supportable claims.

Any future comparison must use comparable repository revision, prompt, model,
reasoning setting, permissions, and tools. Document word or byte counts are
context proxies only; they are not billed-token or engineering-quality proof.

## Logging-derived Policy Walkthroughs

These six cases support the Approved v2 policy review in
[ADR-0335](decisions/ADR-0335-complete-proportionate-codex-delivery.md). Review
the written rules and their distinguishing counterexamples; this is not a live
agent run, behavioral evaluation, or measured quality or token result. Future
execution remains optional and separately requested, not an automatic lane.

| Case | Expected policy result | Counterexample the review must reject | Governing evidence |
| --- | --- | --- | --- |
| Previously answered question | Use the recorded owner decision. Reopen only for material new evidence and explain what changed. An external recommendation is not a new owner selection. | Ask the same question without changed evidence, or replace the owner's risk-based review choice from a handoff. | `AGENTS.md` communication; ambiguity skill output |
| Different file and dashboard limits | Reconcile a 2 KiB file limit with a 4 KiB display limit before scope; ask only if the desired behavior remains unresolved. | Treat shared 2 KiB admission as preservation of the established 4 KiB dashboard behavior. | Workflow discovery; design challenger |
| Unproved feasibility | Establish an approach-invalidating resource claim using sufficient evidence or bound an approved investigation before dependent implementation. Peripheral unknowns may remain explicit when safe work does not depend on them. | Treat queue reservations or sampled message maxima as proof of all retained backing, or require experiments for every peripheral unknown. | Workflow discovery; test-strategy adversary |
| New machinery beyond the goal | Compare genuinely viable reuse, adaptation and replacement; justify necessary new machinery, including a larger design when correctness or performance needs it. | Build archive-recovery machinery without a requirement, or force a small wrapper that cannot satisfy the contract. | Workflow discovery; design challenger |
| Setup or unrelated test failure | Preserve failure evidence, diagnose causation, correct the platform invocation, and rerun affected evidence. Explain impact without asking the owner to diagnose logs. | Call a failure unrelated solely because a retry passed, silently widen product scope, or waive required evidence. | Workflow review/validation; review checklist |
| Material scope revision | Rechallenge affected scope, broaden review if assumptions changed, and obtain exact reapproval when authority, scope, accepted risk or validation changes. Preserve risk-based independence. | Skip challenge because no new risk category appeared, force a subagent for every revision, or require reapproval for wording-only edits. | Workflow scope and risk routing; existing challenge fixtures |
