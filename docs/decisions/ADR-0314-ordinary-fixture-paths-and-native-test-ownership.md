---
title: "ADR-0314: Ordinary Fixture Paths and Native Test Ownership"
description: "Keep ordinary fixtures checkout-relative and establish Windows test containment through actual native process ownership."
---

## Status

* Status: Accepted
* Date: 2026-10-19
* Decision Origin: Troubleshooting chat, Approved v2

## Context

The full Windows lane exposed ordinary fixtures pinned to a former checkout
and diagnostic tests whose intended job contained only one of three expected
processes. Live queries confirmed that wrapper PowerShell and the test child
were outside that job. Lowering the accounting assertion would conceal the
missing ownership proof.

The selected runner is native ARM64 PowerShell on the observed Windows host,
while Go reports amd64 host/target. Those identities are independent. API
feasibility probes succeeded in the actual native PowerShell context, but
atomic job-list creation from the Go caller to the packaged PowerShell image
was refused. Plain Go launch of that image, and Go job-list launch of ordinary
executables, succeeded. The exact internal Windows denial is not established.

## Decision

* Use exact current-checkout executable paths for the three ordinary PSKR
  witnesses. Discover the checkout through the existing working-directory
  helper, not compiler-recorded source paths.
* Keep ordinary profile-lifecycle artifacts under the checkout's existing
  temporary subtree, rejecting escapes and existing redirected ancestors before
  writing. Preserve its Linux default and failure/retirement semantics.
* Do not alter opt-in controlled campaign, profile, preflight or soak roots,
  workloads, numeric oracles, provenance requirements or historical results.
* Forward the actual native PowerShell 7 runner image as one private
  `GOCLUSTER_TEST_POWERSHELL` identity and restore its exact prior state in
  `finally`. No PATH or alternate-installation fallback qualifies.
* Retain the parent, supervisor, wrapper PowerShell and Go test-child ownership
  responsibilities. Implement the private supervisor in that native PowerShell
  image so the native process-creation API runs in its qualified context.
  No new coordinator or service is introduced.
* The parent retains a fresh named kill-on-close job. The supervisor joins and
  verifies that job before creating the wrapper PowerShell with creation-time
  job admission, a standard-handle allowlist and a process-local desktop-app
  no-breakaway policy. This changes no machine policy or production script.
* Transfer a duplicate of the creation-returned native handle while PowerShell
  remains suspended. Keep the source handle alive through adoption. Verify
  actual images, distinct process identities and live intended-job membership
  before release. The Go child verifies inherited membership rather than
  repairing missing membership after startup.
* Preserve the existing normal timeout and child-watchdog experiment.
  Emergency job termination is unsuccessful invocation cleanup, never normal
  qualification. Release all process/job references before final zero-active
  accounting; unconfirmed retirement fences subsequent fixture writes.

## Alternatives considered

1. Move the checkout or replace one hard-coded developer path with another.
   Rejected because ordinary regression identity belongs to the current saved
   executable, not one developer's directory.
2. Create suspended PowerShell and assign it afterward. Rejected because
   launcher loss between creation and assignment can strand an unowned process.
3. Keep the Go-native creation adapter and weaken checks on this host.
   Rejected after isolated denial controls. The same supervisor responsibility
   can use the selected native PowerShell context without an extra process role.
4. Trust a receipt, an image name or aggregate accounting alone. Rejected
   because none proves three distinct live intended-job members.

## Consequences

### Benefits

Ordinary fixture portability is separated from controlled qualification.
Native ownership remains falsifiable through real handle, membership,
owner-loss and retirement controls rather than successful-looking replies.

### Risks

Private-tree path checks do not cover hostile concurrent ancestor replacement.
The native supervisor adds Windows-only test code and depends on supported
PowerShell/Windows APIs. Unsupported native creation remains explicit failure.
Local passes do not establish universal packaged-app compatibility.

### Operational impact

Use the stable Windows runner and its printed executable paths. Firewall
approval remains user-managed; moving the checkout can still require rule
updates. No production service, runtime setting, client state, dependency,
cluster executable or diagnostic startup script changes.

## Links

* Related decisions: [ADR-0265](ADR-0265-stable-windows-test-executables.md),
  [ADR-0299](ADR-0299-test-ownership-and-current-source-qualification.md)
* Related TSR: [TSR-0103](../troubleshooting/TSR-0103-checkout-paths-and-native-test-containment.md)
* Related tests: [ordinary path boundaries](../../internal/cluster/ordinary_harness_paths_boundaries_test.go),
  [native wrapper contracts](../../internal/cluster/diagnostic_wrapper_windows_test.go),
  [runner fixtures](../../scripts/test-test-windows.ps1)
* Related docs: [testing architecture](../runbooks/testing-architecture.md),
  [script guide](../../scripts/README.md#windows-tests-with-stable-firewall-paths)
* Supersedes only ordinary developer-path assumptions and implicit native test
  containment; historical qualification decisions and observations remain.
