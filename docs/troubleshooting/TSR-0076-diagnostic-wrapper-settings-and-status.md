---
title: "TSR-0076: Diagnostic Wrapper Settings and Status"
description: Preserve diagnostic settings, child exit-status, and subsequent native test-ownership evidence.
---

Status: Monitoring
Date Opened: 2026-10-10
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: diagnostic startup wrapper, GC precedence and native child status
Trigger Source: Chat request
Led To ADR(s): none
Tags: tools, PowerShell, diagnostics, GC, exit-status

## RCA Summary
- What happened: the diagnostic wrapper set an environment key the server did
  not read, described initial GOGC as if it were final and omitted explicit
  native child status propagation.
- Why: the wrapper disagreed with the actual diagnostic and YAML consumers.
- What fixed it: use DXC_PPROF_ADDR, describe positive-YAML/zero precedence and
  propagate executable status or launch failure.
- How we know: checkpoint31 in the execution record contains qualification of
  native PowerShell fixtures and their authenticated child retirement controls.
- Operator/support answer: the wrapper requests local diagnostic startup;
  positive runtime.yaml gc_percent overrides initial GOGC50, while zero retains
  it. Check command status and startup logs for the actual outcome.

## Triggering Request
- Request date: 2026-10-10.
- Request summary: implement approved v3 diagnostic wrapper repair.
- Request reference: `docs/command-fix-scope-ledger-v3.md`, checkpoint31.

## Symptoms and Impact
- HEAP_DIAG_ADDR had no effect on maybeStartDiagServer.
- A positive YAML GC setting replaces the wrapper's initial value.
- Automation could miss a failed executable when the script did not propagate
  its status explicitly.

## Hypotheses and Tests
1. A banner proves the environment setting reaches its consumer.
   - Evidence: the fixture poisons inherited values and observes the actual
     child environment, runtime GC value, config.Load and GC consumer.
   - Outcome: require environment and consumer witnesses together.
2. PowerShell process exit proves the native child retired.
   - Evidence: cancelling PowerShell can leave a native child alive.
   - Outcome: authenticate the live child image using a retained kernel handle,
     then join and close that handle before returning or reusing fixture paths.
     A private supervisor joins a fresh job before PowerShell starts; job
     accounting also covers failed startup receipts before authentication.
3. Any nonzero wrapper status proves child-status propagation.
   - Evidence: launch failure is also nonzero.
   - Outcome: pair a child that exits23 with an otherwise identical wrapper
     copy lacking only the explicit exit; distinguish both native statuses.

## Findings
- Root cause: wrapper settings and feedback diverged from server consumers.
- No runtime configuration, diagnostic server lifecycle or GC default changed.
- Windows fixtures start an owned test executable, not the cluster or listener.

## Decision Linkage
- ADR created/updated: none; established YAML ownership is preserved.
- Contract changes: truthful settings banner and unsuccessful child outcomes.

## Verification and Monitoring
- Validation steps: checkpoint31 in `docs/command-fix-execution.md`.
- Signals: inherited and effective GC values, diagnostic address, exact child
  and wrapper statuses, kernel-confirmed child retirement.
- Rollback triggers: wrong environment key, false precedence or lost status.
- Limits: no port binding, endpoint availability, firewall acceptance, live
  cluster startup or production memory result is established by these fixtures.

## References

The later [TSR-0103](TSR-0103-checkout-paths-and-native-test-containment.md)
records checkout-path and native containment failures on a different host and
the bounded test-harness repair. It does not rewrite checkpoint31's original
observations or change the production wrapper's settings/status contract.

- Issue(s): approved v3 diagnostic wrapper checkpoint.
- PR(s): none.
- Commit(s): checkpoint31 on `fix_commands`; see execution history.
- Related ADR(s): [ADR-0074](../decisions/ADR-0074-go-runtime-memory-tuning.md).
- Related docs: `scripts/README.md`, `cmd/run-with-diag.ps1`,
  `internal/cluster/go_runtime_tuning.go`, `internal/cluster/bootstrap.go`.
- Native fixture ownership: Microsoft [job objects](https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects)
  and [basic accounting](https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-jobobject_basic_accounting_information).
