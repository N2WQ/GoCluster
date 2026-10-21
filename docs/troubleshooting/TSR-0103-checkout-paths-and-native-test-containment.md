---
title: "TSR-0103: Checkout Paths and Native Test Containment"
description: "Separate ordinary path restrictions from Windows native process-membership failures and retain the failed diagnostic evidence."
---

Status: Monitoring
Date Opened: 2026-10-19
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: ordinary fixture paths, Windows test process ownership
Trigger Source: Chat request
Led To ADR(s): ADR-0314

## RCA Summary

* The full repository lane failed in ordinary PSKR/profile lifecycle fixtures
  and Windows diagnostic-wrapper process accounting after the dialect fix.
* Ordinary guards assumed a former checkout. The diagnostic job did not contain
  all intended processes; aggregate zero-active accounting therefore could not
  prove complete retirement.
* The repair separates ordinary paths from controlled qualification, and keeps
  the private native supervisor responsible for creation-time job admission and
  explicit live process membership before workload release.
* Final uncached ordinary and race lanes each passed all 70 repository packages:
  54 executed packages, 16 build-only packages, 11,479 passed cases and 99
  reported existing skips. The final focused race run enforced all 104 exact
  required repair identities: 125 cases passed, none skipped.
* Runner fixtures passed 49 groups, including 35 same-process restoration
  combinations. Final receipt fuzzing, analyzers, independent review and
  fresh verification supplied additional bounded evidence described below.

## Triggering Request

The owner requested a bounded test-only repair and authorized Scope Ledger v2
with `Approved v2`. The owner explicitly selected the actual native PowerShell
7 runner image as the qualification target. Production cluster logic, dialects,
client settings, the running service and the startup wrapper remain outside scope.

## Root cause or best current explanation

Three ordinary PSKR witnesses reused a controlled executable guard pinned to
`C:\src\gocluster`. Both ordinary profile-lifecycle children reused a controlled
artifact guard pinned to that checkout. They failed before their intended
behavior ran from `C:\gocluster`.

The diagnostic supervisor's implicit descendant-job inheritance was insufficient
on the observed packaged PowerShell host. Independent live kernel queries
showed wrapper PowerShell and the actual Go test child outside the intended job.
Neither a low process count nor an eventual zero-active count alone explains
or repairs that gap.

## Hypotheses and Tests

1. The default executable path alone explains all failures.
   Rejected. Ordinary path controls passed after their bounded repair, while
   native process admission remained a separate failure mechanism.
2. Pointing PATH at the actual PowerShell image repairs containment.
   Rejected by an unchanged-test-binary comparison; the original job counts
   remained one. No alternate PowerShell installation was selected.
3. Create-then-assign is equivalent to creation-time job membership.
   Rejected by independent scope review. A killed launcher cannot run deferred
   cleanup for a still-unassigned suspended process.
4. The demonstrated native API combination works identically from the Go
   caller and from native packaged PowerShell.
   Rejected by implementation probes. Go creation of packaged PowerShell with
   job-list admission returned `0xC0070005`; plain selected-image Go execution
   and Go job-list creation of ordinary executables succeeded. Full job rights,
   no-console/Unicode environment variations, explicit native machine selection
   and a disposable probe-job package ACE did not remove the refusal.
   None of those experimental permission variations is a selected fallback.
5. The same private supervisor role can use the actual native PowerShell
   context without adding a coordinator.
   A bounded Go-parent probe confirmed that native PowerShell could join the
   parent-created job and retire with zero active membership. Native-context
   atomic-creation, actual Go-child inheritance and owner-loss probes supplied
   feasibility evidence, not final implementation qualification.

## Fix or mitigation

Ordinary executable/artifact guards use the current checkout and reject
redirection before writes. Controlled qualification helpers remain unchanged.

The parent owns the supervisor, output worker and job. The private native
supervisor owns atomically admitted wrapper PowerShell and its handle transfer.
The actual Go test child independently verifies inherited membership before
publishing startup. Live image, distinct PID and intended-job checks gate
release; no missing membership is repaired after startup.

The underlying internal Windows reason for the caller-context-specific
creation denial remains unknown. The selected adapter uses the demonstrated
native context within the same ownership roles, not a machine policy change,
installation switch or weaker fallback.

## Decision Linkage

[ADR-0314](../decisions/ADR-0314-ordinary-fixture-paths-and-native-test-ownership.md)
records ordinary path ownership and explicit native test containment. It
preserves earlier qualified workloads, failed observations and operator
startup behavior. [TSR-0076](TSR-0076-diagnostic-wrapper-settings-and-status.md)
retains the original wrapper repair history.

## Verification and Monitoring

Final observed commands and results on 2026-10-20:

| Command or checker | Observed result |
| --- | --- |
| `scripts/test-windows.ps1` | Passed all 70 packages; 54 executed, 16 build-only; 11,479 passed cases, 99 reported skips. |
| `scripts/test-windows.ps1 -Race` | Passed the same 70 packages and 11,479 cases; 99 reported skips. |
| Focused race selection using `-RequireCases` | All 104 literal required identities ran and passed in the same retained binary; 125 total cases, zero skips. Includes original PSKR/lifecycle/native cases and every new path, ownership, cancellation and receipt control. |
| `scripts/test-test-windows.ps1` | Exit 0; `PASS: 49 Windows test runner fixture groups.` Includes 35 same-caller-process restoration cases across absent and upper/lower/mixed-case empty/nonempty values, each through success, preparation and early/build/test failure. |
| PowerShell `Parser.ParseFile` on both changed scripts | Both passed, exit 0. |
| `GOFLAGS=-trimpath` ordinary path-boundary selection | Passed 36 cases, zero skips. Proves the new ordinary guards, not all existing data-fixture discovery. |
| `go test ./internal/cluster -run '^$' -fuzz '^FuzzDiagnosticWrapperHandleReceipt$' -fuzztime=15s -parallel=2` | Passed 29,149 final fuzz executions. Arbitrary fuzz input never invokes native handle APIs. |
| `go vet ./...` | Passed, exit 0. |
| `staticcheck ./...` | Passed, exit 0. |
| `golangci-lint run ./... --config=.golangci.yaml` | Passed, exit 0; `0 issues.` |

The 99 full-lane skips include external DXSpider evidence, opt-in qualification
and subprocess-helper cases. They are not interoperability or full performance
qualification. No required repair case was skipped. No running WSL instance
was available, so no Linux execution is claimed.

The independent code review found two coupled defects before closeout:
available receipts could bypass an already-canceled context, and mixed-case
Windows environment names could lose prior values. Both were fixed. Actual
cancellation now traverses common admission checks; a retained available-receipt
control proves refusal. The environment matrix first reproduced the old
lowercase-empty failure, then passed all 35 corrected cases. A separate
read-only fresh verifier found no material remaining issue in the inspected
scope; the lead retained command results, freeze comparison and final authority.

Frozen test/source-script hashes were retained for the 15 touched code/script
files and checked for drift during validation. Earlier path/native/race counts
were intermediate and are not substituted for the final lanes. The runner
fixture and AST results were observed by the delegated worker; no separate raw
fixture log was saved. Final root-suite, required-identity and fuzz logs were
retained in the session work record.

The five new test files contain 1,124 lines, including embedded native fixture
source; existing test/runner files add 896 and remove 141 lines. This is a net
1,879-line test-infrastructure increase, not a runtime optimization. No original
test root is retired. The native adapter, parent owner and contract-oracle
files exceed the soft 500-line review trigger; they retain one cohesive
native/ownership boundary each rather than adding a general framework or
scattering handle lifetimes across more files.

Generated-map freshness, troubleshooting-index consistency, workflow-doc
conformance and final whitespace checks complete the documented closeout lane.
No parent PASS, skip, forced termination or missing receipt alone qualifies a
required mechanism.

Monitor explicit API/admission failures and the retirement fence. Stop on
unconfirmed cleanup; do not reuse fixture paths or weaken membership assertions.
No local test or probe claims production leak freedom or runtime compatibility.

## References

* Original failure record: TSR-0102, preserved in parked checkpoint `43b5144`.
  That VE7CC-specific record is intentionally not imported with this testing repair.
* [Windows runner](../../scripts/test-windows.ps1)
* [Native ownership tests](../../internal/cluster/diagnostic_wrapper_windows_test.go)
* [Microsoft startup attributes](https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-updateprocthreadattribute)
* [Microsoft nested jobs](https://learn.microsoft.com/en-us/windows/win32/procthread/nested-jobs)
