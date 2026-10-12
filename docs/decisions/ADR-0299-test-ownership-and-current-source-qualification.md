# ADR-0299: Test Ownership and Current-Source Qualification

- Status: Accepted
- Date: 2026-10-12
- Decision Origin: Testing architecture review, Approved v4

## Context

Repeated integration matrices and copied fixture machinery increase maintenance.
Package success does not establish execution of opt-in command, nested-module or
full qualification lanes. Discarded collector prefixes, parent PASS records and
local socket closure can conceal incomplete evidence.

## Decision

Keep exhaustive semantics with the owning package and representative integration
checks for each distinct wiring, transport, persistence and lifecycle mechanism.
Removed variants retain owner coverage but relinquish their individual
integration runs. Preserve literal expectations, atomicity, compatibility,
resource limits and exercised historical oracles. Reuse existing helpers rather
than introduce a general testing framework.

Charge all new helpers, runners, fixtures and CI code against source reductions.
Test declarations and executed scenarios are separate metrics. Added automated
invocations do not reduce execution cost. The v4 baseline is commit 9ca33d6,
including prior pruning.

Extend ADR-0265's stable Windows runner to four explicit module roots and tags.
Required case identity is package plus complete test/subtest name, proven by
marker-mode same-binary RUN followed by PASS. Parent PASS, SKIP and discovery
cannot qualify a required leaf. Preserve stable paths, the shared lock,
uncached execution, no stale binaries and caller-state restoration.

Extend ADR-0227/0228's push/manual backstop with Windows product/command and
selected maintained-module jobs. Preserve Linux baseline, nightly race,
conditional workflow fixtures and read-only permissions. No PR gating or Fable
authority changes. Broker FVT and multi-hour qualification remain separate.

Use Go 1.27.1 for fresh PC92 qualification. This selectively replaces the former
Go 1.26.4 acceptance identity, not any workload, duration, latency threshold,
allocation partition, correction review, platform proof or external reference.
Historical evidence retains its original identity and status.

Finish code, docs, decision records and maps before full capture. Capture,
review and finalization artifacts belong outside the checkout. Source identity
uses raw bytes, including cross-platform line endings. Changed source or inputs
invalidate affected evidence. Failed or unavailable gates remain open and do not
authorize production repairs under this test-only scope.

## Alternatives

- Retain every integration variant: more variant-specific evidence, with the
  existing fixture cost; the owner selected representative mechanisms.
- Add a universal runner: another owner and source burden; existing bounded
  runners suffice.
- Update only the compiler string: synthetic acceptance cannot establish fresh
  performance or resources; the owner requires complete qualification.

## Consequences

Routine source and repeated scenarios decrease while required execution and
isolation become explicit. New CI lanes increase automated invocation cost.
Full qualification may fail; its acceptance is separate from implementation.

## References

- [Testing architecture runbook](../runbooks/testing-architecture.md)
- [Development validation](../dev-runbook.md)
- [Stable Windows executables](ADR-0265-stable-windows-test-executables.md)
- [CI backstops](ADR-0227-push-to-main-ci-validation-backstops.md)
- [Corrective CI enforcement](ADR-0228-corrective-ci-enforcement.md)
- [PC92 qualification](../pc92-qualification.md)
