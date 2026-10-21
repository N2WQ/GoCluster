---
title: "ADR-0316: Proportionate Permanent Testing"
description: "Reuse owner-level tests and limit permanent harness growth without weakening required evidence."
---

- Status: Accepted
- Date: 2026-10-20
- Decision Origin: Design, testing-policy reset, Approved v2

## Context

October's development added substantial regression and qualification code.
Some additions address new protocol and resource obligations; others introduce
dedicated testing infrastructure around narrowly scoped fixes. The shared
code-quality standard was unchanged from the last pre-October commit, so the
growth does not establish that a new October workflow rule caused it.

July already had unit, network, fuzz and benchmark tests. Its size does not
establish completeness, and more tests do not correct an erroneous expectation.
We need proportionate permanent protection, not a file-count target or a blanket
return to historical tests.

## Decision

Use the shared code-quality standard as the owner of permanent testing rules:
reuse existing tests and fixtures, keep semantics primarily with their component,
and add integration coverage for distinct boundary risks. New permanent runners,
cross-process handoffs, receipt formats and miniature protocol services need a
concrete evidence gap, an explanation of why existing tools are insufficient,
and a maintenance justification within the existing plan or review.

Separate temporary investigation from permanent regression protection. Review
all added testing code, including helpers, scripts, fixtures, repeated scenarios
and CI, against the smallest suite that detects the same material failures.
Neither file count nor test-to-production line ratio is an acceptance target.
These are review criteria, not a new per-test approval process or checker.

Keep existing risk-driven race, performance, scientific and lifecycle evidence,
execution lanes and qualification obligations. This policy-only change retires
no tests, runners or CI. Subsequent reductions require separately approved,
bounded scope identifying redundant evidence and retained unique protection.

## Alternatives considered

1. Restore July literally: historical size does not prove coverage and omits
   obligations introduced by later features.
2. Cap test files or lines: encourages cosmetic merging or deletion of useful
   checks without reducing infrastructure complexity.
3. Retain every investigative fixture permanently: adds ongoing maintenance
   without requiring distinct regression value.

## Consequences

### Benefits

Testing additions must justify distinct failure detection and maintenance cost.
Existing evidence remains valid within its original limits. Reduced future
growth is an intended outcome, not a measured result of this policy change.

### Risks

Proportionality can be misread as permission to weaken verification. Required
evidence remains controlling, and a smaller suite must still detect the same
material failures.

### Operational impact

No production behavior, test execution command, CI lane, qualification threshold
or executor approval mechanism changes. The existing suite is not reduced by
this decision.

## Links

- Related tests: Existing suites remain unchanged.
- Related docs: [Code quality](../code-quality.md#proportionate-permanent-testing),
  [testing architecture](../runbooks/testing-architecture.md).
- Supersedes / superseded by: None. Extends
  [ADR-0299](ADR-0299-test-ownership-and-current-source-qualification.md)
  without replacing its execution or qualification requirements.
