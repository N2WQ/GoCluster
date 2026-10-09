# ADR-0265: Stable Windows Test Executables

- Status: Accepted
- Date: 2026-10-09
- Decision Origin: Design

## Context

Native Windows tests include network listeners and child processes that relaunch
the current test executable. The user reported repeated firewall approval
prompts and selected stable executable paths through Scope Ledger v1. Firewall
program rules identify full paths. The installed Go test implementation runs
ordinary tests from temporary build paths, including when `-o` saves a copy.

## Decision

Use `scripts/test-windows.ps1` for native Windows package test execution. Compile
each repository package with `go test -c -o`, then execute the saved binary in
its package source directory. Map the full import path to an ignored directory
under `.tmp/windows-tests/` with the filename `package.test.exe`. Ordinary and
race builds reuse that path. Disable test-result caching with `-test.count=1`
and retain Go's `-test.paniconexit0` safeguard against silently exiting tests.
Default to a ten-minute timeout per package. Build-check packages without
test files. Refuse overlapping runner invocations through one filesystem lock
held across discovery, compilation, and execution. Stop on build or test failure.

Preparation-only mode builds and prints paths without executing tests, including
the Windows peer diagnostic companion beside its test binary. Firewall rules
remain user-managed. No production Go code, runtime contracts, CI configuration,
or temporary VOACAP helper ownership changes.

## Alternatives considered

1. Ordinary `go test` or `go test -o`: execution still uses temporary paths in
   the inspected toolchain, so saved output alone does not solve the problem.
2. One shared executable for all packages: packages have separate Go test
   binaries, and filename-only identity does not provide firewall path identity.
3. Broad firewall exceptions or automatic policy changes: unnecessary for this
   workflow and outside the approved scope.

## Consequences

### Benefits

Repeated builds, race builds, and relaunched test children use predictable paths.
Per-package directories avoid basename collisions and preserve the companion's
relative placement. Manual firewall setup can precede test execution.

### Risks

Stable paths do not prove firewall permissions or resolve Application Control
denials. Moving the checkout changes rule paths. Temporary non-listening VOACAP
fixtures still use private paths. The lock covers runner invocations in this
checkout, not manual executable launches or external output-tree writers.

### Operational impact

Use PowerShell 7 and native Windows Go. Selected packages execute serially with
no test-result caching; a failing package stops later packages. Additional test
flags such as fuzzing, benchmarks and coverage require a separate command.
No stale binary runs after a build failure. Output junctions and symlinks are
refused. The caller's working directory and PATH are restored. CI/Linux and
other validation-lane commands retain their existing requirements.

## Links

- Related decision: [ADR-0259](ADR-0259-native-windows-codex-environment.md)
- Implementation: [test-windows.ps1](../../scripts/test-windows.ps1)
- Regression fixtures: [test-test-windows.ps1](../../scripts/test-test-windows.ps1)
- Usage: [script guide](../../scripts/README.md#windows-tests-with-stable-firewall-paths)
- Validation: [development runbook](../dev-runbook.md#native-windows-test-execution)
- Firewall path identity: [Microsoft documentation](https://learn.microsoft.com/en-us/windows/security/operating-system-security/network-security/windows-firewall/rules)
