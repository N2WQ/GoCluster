---
name: requirements-ambiguity-review
description: "Use for independent read-only requirements ambiguity review after Current-State Discovery and applicable model evidence but before a Proposed Scope Ledger. Trigger when filters or Boolean precedence, defaults or sentinels, authentication or admission, reputation or correction gates, failure behavior, thresholds, classifications, diagnostics, compatibility, user-visible behavior, or test oracles may admit materially different semantic interpretations."
---

# Requirements Ambiguity Review

## Purpose

Actively expose unresolved semantic forks before they harden into scope or
design. Produce evidence for the lead agent; do not choose product policy,
design the solution, or own the Scope Ledger.

## Workflow

1. Confirm independence and phase.
   - Run after Current-State Discovery and applicable scientific/model evidence
     but before publishing a Proposed Scope Ledger.
   - Apply the independent-review contract in `AGENTS.md` `Subagent Use`.

2. Build a neutral evidence packet.
   - Inspect the user request, Current-State Discovery, domain and operator
     contracts, relevant source and tests, and applicable ADRs or TSRs.
   - Separate confirmed facts from assumptions and proposals.
   - Treat existing behavior and tests as evidence of current behavior, not
     automatic proof of intended semantics.

3. Search actively for semantic forks.
   - Probe Boolean combinations, precedence, empty and missing values, defaults,
     sentinels, malformed and boundary inputs, authentication and admission,
     reputation and correction gates, retries and terminal failures,
     thresholds, classifications, diagnostics, compatibility, and test oracles.
   - Use concrete edge examples to distinguish interpretations that appear
     equivalent in ordinary cases.
   - Do not depend on the lead having already identified an ambiguity.

4. Classify each uncertainty.
   - Mark an uncertainty as material semantic ambiguity when competing answers
     change user-visible behavior, operator-visible behavior, compatibility,
     safety, or the expected test result.
   - Mark pure implementation uncertainty as delegable to later design only
     when the required external behavior is already explicit; record the
     rationale.
   - Do not select an interpretation, invent a compatibility policy, recommend
     an architecture, or expand or approve scope.

5. Produce an ambiguity register with one row per candidate:
   - requirement or unresolved question;
   - confirmed facts and evidence;
   - competing interpretations;
   - concrete edge examples;
   - affected contracts, surfaces, and tests;
   - decision owner;
   - resolution and status.

6. Enforce the stopping condition.
   - Report unresolved material semantics as blocking publication of the Scope
     Ledger until the user or documented authority resolves them.
   - Report resolved items and implementation-only uncertainties explicitly so
     the lead can disposition every row.
   - Leave official requirements decisions, scope, design, validation, and
     final disposition with the lead agent.

## Output

Return:

- the canonical four-field independent-result envelope from `AGENTS.md`;
- the ambiguity register;
- blocking unresolved semantics;
- implementation-only uncertainties delegated to design, with rationale;
- inspected evidence and remaining unknowns;
- a reminder that the lead owns every disposition and the Scope Ledger.

Report missing or non-independent evidence as a gap.
