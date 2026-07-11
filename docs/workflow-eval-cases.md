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
