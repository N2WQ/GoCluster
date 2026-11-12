# ADR-0346: Stable Real-Main Test Image

- Status: Accepted
- Date: 2026-11-12
- Decision Origin: Troubleshooting chat; exact Approved v3

## Context

The native runner already saves package test images at fixed checkout-relative
paths. The real-main peer-logging test independently built `gocluster.exe` in
`t.TempDir()` and started application listeners. Read-only inspection found
firewall application rules for distinct temporary copies of that child.
Approving the package test image did not cover this separate executable.

## Decision

Extend ADR-0265 with one runner-prepared plain main image at
`.tmp/windows-tests/dxcluster/internal/cluster/main/gocluster.exe`. Build it
whenever first-party `internal/cluster` is selected, under the existing exclusive
runner lock. `-PrepareOnly` builds and prints it without executing tests or the
child. Preserve the existing plain build mode: a race parent does not silently
turn this child into a race or tagged application build.

Preserve the two-minute child-build cancellation threshold, including redirected
output draining. Join the owned process and managed readers before returning.
Publish private `GOCLUSTER_TEST_MAIN_EXECUTABLE` metadata only after successful
current preparation. Never run a stale image after failure. Override inherited
metadata and restore its exact prior presence/value on every exit.

The Windows test validates the saved current parent, exact child location,
regular files and each Windows output ancestor before creating fixture listeners.
An unavailable or invalid handoff fails with the native-runner instruction;
there is no temporary-image fallback. Query the actual live Windows child image.
Keep work/config/data temporary, PATH restricted, and both working and image
directories free of `peerdiag` and `peerdiag.exe`. Keep termination and observed
process-wait completion; retirement failure cannot qualify the test. Retain the
existing direct non-Windows build and functional logging witness.

The output tree is private to the supported runner. This is current-build
ownership, not cryptographic authentication of arbitrary manual handoffs.
Manual image launches, hostile concurrent ancestor replacement and a malformed
toolchain that abandons descendants remain outside that contract. The build
deadline cancels owned waiting/readers; OS refusal to retire a process is not
a successful invocation or a hard elapsed-time guarantee.

## Alternatives Considered

1. Build the stable child inside the test and separately prepare it in the
   runner. Two owners/recipes can diverge; runner ownership reuses the existing
   lock and avoids starting test processes for preparation.
2. Keep the temporary image or add a missing-handoff fallback. Either leaves
   the recurring path-identity problem reachable.
3. Change firewall policy or use the production executable. Both bypass test
   ownership and exceed this repair.

## Consequences

### Benefits

- Real-main execution through this Windows route has one documented path for
  a given checkout, including ordinary and race-parent runs.
- Preparation lists the separate listener image before unattended validation.
- Peer-log coverage still uses the real entrypoint and isolated inputs.

### Risks

- Selecting cluster builds main even when the filter excludes this case;
  this adds build work and can expose a main-build error earlier.
- A new checkout/worktree changes the absolute path. Initial approval can
  still be required; path stability does not grant it.
- Native process observations and fault fixtures establish local tested paths,
  not universal OS or firewall behavior.
- Cleanup covers returning fixtures; a hard package/process abort can bypass
  Go defers. Independent supervision for those aborts is outside this repair.

### Operational Impact

No production code/config, firewall rule, service, CI or companion behavior
changes. Use the native runner and its printed paths. Support-agent documentation
impact is Required for Windows testing/firewall troubleshooting.

## Links

- [TSR-0130](../troubleshooting/TSR-0130-main-test-firewall-path.md)
- [Runner](../../scripts/test-windows.ps1),
  [runner fixtures](../../scripts/test-test-windows.ps1),
  [real-main test](../../internal/cluster/unified_v3_main_binary_test.go).
- [Windows test guide](../../scripts/README.md#windows-tests-with-stable-firewall-paths),
  [validation runbook](../dev-runbook.md#native-windows-test-execution).
- Related ADRs: [ADR-0265](ADR-0265-stable-windows-test-executables.md),
  [ADR-0314](ADR-0314-ordinary-fixture-paths-and-native-test-ownership.md).
- Supersedes / superseded by: refines ADR-0265 only for the separately built
  real-main child; remaining path/locking contracts are preserved.
