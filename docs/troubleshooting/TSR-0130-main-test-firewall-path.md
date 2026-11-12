# TSR-0130 - Repeated Firewall Prompts for the Real-Main Test

Status: Monitoring
Date Opened: 2026-11-11
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: Windows test runner and real-main integration fixture
Trigger Source: Operator report
Led To ADR(s): ADR-0346
Tags: Windows, firewall, tests, executable path, process ownership

## RCA Summary
- What happened: Windows repeatedly requested access for compiled `gocluster.exe`
  during testing despite the stable package-test runner.
- Why: the real-main test separately compiled its listener child beneath a fresh
  `t.TempDir()`. Windows application rules identify the full image path.
- What fixed it: runner-owned preparation of one stable child image, validated
  Windows handoff, existing lock ownership and isolated temporary runtime data.
- How we know: the new network-free preparation regression failed on the old
  runner because the separate main image was absent. Separate-child path,
  stale-build, lock, redirection, environment and retirement checks distinguish
  the repair from a stable outer test executable alone.
- Operator/support answer: use `scripts/test-windows.ps1 -PrepareOnly` and inspect
  the printed main-child path as well as package test paths. Initial permission
  remains machine-local; a new worktree has a different full path.

## Triggering Request
- Request date: 2026-11-11; implementation authorized 2026-11-12.
- Request summary: repair recurring firewall prompts for the compiled main test.
- Request reference: exact Approved v3 for the stable real-main executable repair.

## Symptoms and Impact

The outer `package.test.exe` had a stable path, while its independently built
child did not. Read-only application-filter inspection found rules for multiple
`TestV6MainExecutablePeerLoggingWithoutCompanion.../001/gocluster.exe` paths.
These observations explain the mismatch; they do not establish permission for
a new image or authorize rule changes.

## Timeline
1. 2026-11-11 - Source and local firewall application-filter evidence identified
   the separate temporary image; neutral ownership comparison preceded scope.
2. 2026-11-12 - Owner approved v3. Repair isolated on branch
   `firewall-stable-main-test` from `bb44ecf1`; bulletin history preserved.
3. 2026-11-12 - Preparation regression reproduced the missing-child defect.
   Initial path tests exposed an accepted ancestor junction; direct Windows
   reparse-attribute checks replaced reliance on final-path resolution here.

## Hypotheses and Tests
1. The stable package runner covers every listening child.
   - Evidence: old test-internal `go build` selected its own temporary image;
     preparation did not produce that child.
   - Outcome: Rejected; separate child preparation needs explicit ownership.
2. Keeping the filename is sufficient.
   - Evidence: application rules referred to that filename in different paths.
   - Outcome: Rejected; full paths must be stable for reusable approval.
3. Checking only the working directory preserves no-companion coverage.
   - Evidence: image relocation separates that directory from the runtime cwd.
   - Outcome: Rejected; both directories and companion spellings are checked.
4. A bounded root-process wait also bounds redirected output.
   - Evidence: a controlled compiler root exits while a descendant retains output
     handles. One cancellation token now covers root waiting and both readers;
     the fault fixture owns and retires its intentionally abandoned descendant.
   - Outcome: Rejected; output cancellation is included in the deadline.

## Findings

- Root cause: the real-main listener image escaped the preparation contract
  through a test-internal `t.TempDir()` build; the outer saved test image did
  not establish its child's full executable path.

Runtime configuration/data remain private temporary fixtures. Current build
success, path checks before listeners, environment restoration and actual
retirement are required alongside stable naming. No general runner or new
firewall policy is needed.

## Decision Linkage

[ADR-0346](../decisions/ADR-0346-stable-real-main-test-image.md) refines ADR-0265
for the separately compiled child. Parent race runs preserve its plain build
mode. Non-Windows retains the original isolated build.

## Verification and Monitoring

Raw receipts remain under `.tmp/firewall-main-v3/` in the repair worktree.
The old preparation failed as expected (`red-preparation.log`). The first
ordinary selection exposed an ancestor junction (`target-ordinary.log`); the
repaired ordinary/race selections each passed 13 cases with zero skips
(`target-ordinary-fixed.log`, `target-race.log`). Runner fixtures exercise an
actual separate main executable and managed-build success/failure/cancellation.
The final runner fixture script passed all 50 groups (`runner-fixtures-final.log`).
The final Linux selection passed the common companion control and real-main
logging case (`linux-target-final.log`); its image remains temporary by design.

Native validation used PowerShell 7.6.10 and Go 1.27.1 windows/amd64. Linux used
Go 1.27.1 linux/amd64 in Ubuntu 24.04 under WSL2. The full root-module ordinary
lane (`scripts/test-windows.ps1`) passed: 59 executed packages, 15 build-only,
12,674 passing cases and 93 skips (`full-ordinary-final.log`). The skips comprise
12 subprocess-only helpers, 56 external DXSpider cases, 24 opt-in controlled,
live, data or performance cases, and one platform-unavailable H3 mapper case.
They do not establish the omitted qualification, live or interoperability claims.
The full root-module race lane (`scripts/test-windows.ps1 -Race`) passed with
the same package/case totals (`full-race-final.log`), and its exact skip
inventory matched ordinary. All four copied reference inputs still matched
their recorded hashes after both lanes. No owned fixed-path main child remained
live at the final process check.

`go vet ./internal/cluster`, `staticcheck ./internal/cluster` and the affected
package's golangci-lint check passed on final Go source; lint reported zero
issues. Both PowerShell scripts parsed successfully. Troubleshooting-record,
Codex workflow and offline support-search checks passed. A separate read-only
quality review and the lead's final verification found no unresolved material
defect within this repair. This is local validation, without a hosted CI run
or production firewall-permission qualification.

The fresh worktree's first full ordinary run stopped in the unchanged
`TestGeographyStartup/prediction_off` fixture because ignored local reliability
tables were absent. Its next targeted attempt exposed the also-absent configured
confusion model. Three reliability tables and `confusion_model.json` were copied
from the original checkout into ignored worktree paths; source-before,
destination and source-after SHA-256 hashes matched for each file
(`reference-input-identities.json`). The unchanged geography selection then
passed all eight cases (`geography-prerequisite-final.log`). These were fixture
prerequisites, not a change to application defaults or test assertions.

Reference identities below are relative to `data/rbn/priors/` in each checkout:

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| `spotter_reliability.txt` | 6073 | `9FFEB672C70F09C40795610EF89496B46C2F3EF57B2AB30F0E0569AABF8BCCE3` |
| `spotter_reliability_cw.txt` | 5958 | `3B18AF2B10E949D0D936E25C38F02C5EAA5A078A58B68E094E0716FC43FEFAF1` |
| `spotter_reliability_rtty.txt` | 1173 | `D7627EB8881584C84C6433CCD1E881D91FB044765267CB213774E84AAA57497B` |
| `confusion_model.json` | 290525 | `6523DB2642C5F524B7184055D6C1772FF3502FC59E8607567E15155C949385B7` |

| Approved obligation | Implementation and evidence |
| --- | --- |
| Stable, current, plain child; preparation without execution; bounded build and lock ownership | `scripts/test-windows.ps1`; runner controls cover repeated separate children, race-parent/plain-child identity, stale build refusal, held lock, sleeping compiler and inherited output handles |
| Private handoff and caller restoration; refuse invalid paths before listeners | Windows `main_executable_*_test.go` helpers and runner; exact parent/child checks, absent/invalid handoffs, ancestor/leaf junctions, external sentinels and absent/empty/nonempty environment controls |
| Temporary runtime inputs, both companion-free directories, actual child image and retirement | Common real-main/companion fixtures plus Windows live-image and controlled-failure retirement checks; targeted ordinary/race and Linux selections |
| Durable instructions and support routing | ADR-0346, this TSR, both indexes, Windows test/validation guides and `customgpt/` developer/troubleshooting routes; record, workflow, support-search and final map checks |

Observe actual prompt behavior separately from path evidence. Tests do not grant
firewall permission. Stop reuse on build/path/retirement failure and inspect the
error; do not approve a succession of random temporary images as a repair.
The retirement witnesses cover returning fixtures and controlled failures;
hard package/process aborts can bypass Go defers and are not independent child
supervision. That existing harness boundary is unchanged.

## References
- [Windows test guide](../../scripts/README.md#windows-tests-with-stable-firewall-paths)
- [Native Windows validation](../dev-runbook.md#native-windows-test-execution)
- [Microsoft application filters](https://learn.microsoft.com/en-us/powershell/module/netsecurity/get-netfirewallapplicationfilter)
- [Real-main fixture](../../internal/cluster/unified_v3_main_binary_test.go)
- [Runner fixtures](../../scripts/test-test-windows.ps1)
