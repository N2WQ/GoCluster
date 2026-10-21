---
title: Testing Architecture
description: Test ownership, execution lanes, fixture isolation, and qualification boundaries.
---

Owner tests retain exhaustive semantics. Composed tests witness distinct wiring,
transport, persistence and lifecycle mechanisms. Retired integration variants
retain owner coverage, but lose their individual integration runs. Package
counts and test names alone do not establish end-to-end qualification.

## Permanent testing scope

Apply the shared [proportionate permanent testing rules](../code-quality.md#proportionate-permanent-testing)
when planning and reviewing test additions. Reuse existing fixtures and lanes;
keep detailed semantics with the owner and use composed checks for distinct
boundary risks. Full qualification machinery is not the default for each fix.
Explain exceptional infrastructure within the existing plan or review, without
creating another approval step or checker.

[ADR-0316](../decisions/ADR-0316-proportionate-permanent-testing.md) extends the
ownership policy without retiring any lane below. July's suite is a reference
for simplicity, not a completeness guarantee or a rollback target. Existing
test reduction needs separately approved, bounded scope distinguishing redundant
evidence from unique regression protection. Renaming or merging files alone is
not a reduction in maintenance burden.

## Execution lanes

| Lane | Entry point | Evidence boundary |
| --- | --- | --- |
| Windows packages | `scripts/test-windows.ps1` | Actual saved binaries; execution, build-only and optional skips reported separately |
| Linux packages | `go test -count=1 ./...`, ordinary/race as selected | Root module, not nested modules or full optional qualification |
| Connected CustomSCP | `scripts/test-customscp-regression.ps1` and explicit CI stage | One fresh database/run identity across producer and consumer processes |
| Command oracle controls | Both Python unittest suites | Collector/accounting faults, not server behavior |
| Checkout command integration | `scripts/test-command-fixes.py` | Source/binary/config identity, both transports, persistence/restart and cleanup |
| Maintained dependencies | Explicit module selections | Selected local-fork regressions omitted by root `./...` |
| Full PC92 qualification | Original wrappers and finalizer | Complete workload/resource/platform obligations on frozen source |

Push/manual CI is post-landing verification. Windows/dependency jobs do not
replace nightly Linux race or risk-triggered local validation. Full qualification
remains outside ordinary CI.

## Required Windows execution

`-ModuleRoot` accepts only `.`, `third_party/go-sqlite3`,
`third_party/go-sqlite3/qualification` and `third_party/paho.mqtt.golang`.
SQLite's engine is dependency/build coverage. `-Tags` applies to discovery and
compilation. Actual module/package/output paths must stay in allowed roots
without reparse-point escapes.

Required identities are `package::Test/subtest`, with actual RUN then PASS from
the same retained binary. Parent PASS and SKIP cannot qualify a leaf.
Slash-qualified filters require explicit identities. Unfiltered package lanes
may retain documented optional skips.

```powershell
./scripts/test-windows.ps1 -Packages ./telnet `
  -Run '^TestConfigurationClosedOwnerFinalSave$' `
  -RequireCases 'dxcluster/telnet::TestConfigurationClosedOwnerFinalSave'
./scripts/test-windows.ps1 -ModuleRoot third_party/go-sqlite3
```

Use `-PrepareOnly` for stable executable-path preparation and `-Race` when
selected. Firewall policy remains user-managed. The summary separates executed
packages, build-only packages, passed cases and skipped cases. A filtered lane
with no passing cases fails; exact required identities protect the intended set.

Ordinary executable guards and lifecycle artifacts are anchored to the current
checkout. They do not relax the opt-in controlled qualification paths, workloads
or historical evidence. New ordinary guards discover the checkout from the
package working directory and `go.mod`, independently of recorded compiler
source filenames. The output tree remains private to the runner; pre-write
path checks do not promise protection against hostile concurrent replacement.

Windows diagnostic fixtures qualify the actual native PowerShell 7 runner
image, not an arbitrary PATH resolution. A private native supervisor preserves
the existing parent/job, native-wrapper and test-child owners. Atomic job-list
creation, restricted stdio inheritance and a creation-local desktop-app policy
fund the native process tree's containment. Actual live image, distinct PID and
intended-job membership precede release; aggregate process totals alone are not
admission. Failed invocations require confirmed retirement before fixture reuse.
See [ADR-0314](../decisions/ADR-0314-ordinary-fixture-paths-and-native-test-ownership.md).

## Command collection and fixture isolation

Use PyYAML 6.0.3 in the pinned environment, without optimized Python:

```powershell
& .tmp/telnet-test-venv/Scripts/python.exe -m unittest discover -s scripts -p 'test*_test.py'
& .tmp/telnet-test-venv/Scripts/python.exe scripts/test-command-fixes.py
```

The driver builds current source and runs native/ziutek command, reputation and
dedupe profiles. Run offline fixtures, the driver and native runner sequentially:
offline controls genuinely exercise their shared exclusive lock.

Qualification requires independently inventoried leaves, strict envelope types
and raw framing, complete correlated replies, remote logout, immediate disk and
restart evidence, and successful cleanup. Malformed output remains failure even
when parsing can continue. Partial receipts are bounded. Family status reflects
failed, blocked or missing children. Admission refusal cannot become an empty
history pass; bounded archive polling requires exact admitted labels/identities.

Close owned sockets and await registry retirement before restoring working
directories. Unregistration saves before registry removal, so the client-empty
fence covers final profile writes. Runtime witnesses submit work and FIFO
sentinels through the same upstream owner; never run a synchronous resolver
driver beside the live resolver.

## Dependency and qualification boundaries

Select SQLite/VFS and its separate qualification module explicitly on Windows
and Linux. Keep native/fallback, crash/reopen, lock/coherence, mapping budgets and
lifecycle evidence distinct from sustained profiles. Required Windows file
symlink evidence sets `GOCLUSTER_SQLITE_SYMLINK_REQUIRED=1`; unavailable privilege
is failure for that lane.

Bounded dependency CI selects the four portable WAL roots explicitly. It does
not claim the separately required full-qualification Windows symlink proof.

Selected broker-free Paho regressions cover worker parallelism, three inbound
queue outcomes, custom connection and backoff, plus packets. Ordinary/race passes
do not establish broker FVT; full FVT requires separately provisioned endpoints.

Count all changed testing source, including new helpers, scripts, fixtures and
CI. Report declarations, retained semantic vectors and executed rows separately.
Do not fund infrastructure by deleting unreviewed tests.

Fresh PC92 evidence uses Go 1.27.1, all original 11 profiles, 16 check records,
20 correction reviews and ten partitions totaling at most 480 MiB. Rates,
durations, thresholds and pinned DXSpider inputs remain unchanged. Short or
synthetic runs cannot qualify full profiles.

Finish source/docs/maps, freeze raw bytes, and store captures, reviews and
finalization outside the checkout. Cross-platform line endings must match.
Changed source/assets invalidate affected evidence. Failed or unavailable gates
keep acceptance open; production repairs need separate authority.

See [ADR-0299](../decisions/ADR-0299-test-ownership-and-current-source-qualification.md)
and the [qualification contract](../pc92-qualification.md).
