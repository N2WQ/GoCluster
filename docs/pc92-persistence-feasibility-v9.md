# PC92 persistence feasibility: v9 evidence

**Verdict: rejected under the approved boundaries.** The isolated
`github.com/ncruces/go-sqlite3 v0.35.6` candidate cannot satisfy the selected
ownership and compatibility contract with only the permitted repairs.
Production continues to use `modernc.org/sqlite v1.36.1`; this experiment made
no production source, dependency, configuration, schema or database changes.

This completes the negative-result exit in the
[approved v9 record](pc18-pc92-scope-ledger-v9.md). It does not complete the
original [Q1-Q6 qualification](pc92-qualification.md) or prove the
[480 MiB aggregate bound](pc92-allocation-proof.md).

## Provenance and reproduction

- Date: 2026-10-01; final manifest captured at 20:16:50.6067426 UTC.
- Production baseline: `0d8a728354cc423f3d7929208aac2b72b05f9d5f`, branch
  `p92`, observed clean before the experiment.
- Toolchain: Go 1.26.4, windows/amd64; OS version 10.0.26200.0.
- Execution: GOMAXPROCS 2, GOGC 50, GOMEMLIMIT 1536 MiB.
- Candidate: `github.com/ncruces/go-sqlite3 v0.35.6`,
  origin `091633ebb62b0c78988d3e079149333b36337599`,
  module sum `h1:0JGlMne89YzKNP2CJBuiH21EEzSQNuB7pfvCbKBn0Jg=`.
- Generated engine: `github.com/ncruces/go-sqlite3-wasm/v6 v6.3.35304`,
  origin `d923b955a175c983578bd2196ce9ac0511f6159d`,
  module sum `h1:dBSZlcEFdtBMvNRg34y50mConBPO/petSddSwGQVlSI=`.
- Comparison: current-driver `modernc.org/sqlite v1.36.1`.

The experimental workspace is
`C:/Users/Developer/AppData/Local/Temp/gocluster-v9-20261001-eb5768e8`.
It contains the copied candidate module, isolated probe module, runner and
evidence directory. The runner uses paths from this machine; update its
repository/cache paths before reproduction on another machine.

A preserved copy, including source, the test executable, manifests, logs and
these two v9 documents, is packaged as
`C:/Users/Developer/Downloads/GoCluster_PC92_v9_persistence_evidence_20261001.zip`.
The archive keeps the experimental directory layout; extract it before running
the reproduction script. Downloaded Go dependencies are resolved by the pinned
module files and are not all vendored into the archive.

From that workspace, run:

```powershell
.\run-probes.ps1
```

The runner saves/restores environment variables and working directory, checks
the upstream-source modification whitelist, records module/source hashes,
builds the test executable, runs vet and captures the failing probes. Its
compiled test invocation uses literal PowerShell arguments:

```powershell
& .\evidence\v9-probe.test.exe '-test.v' '-test.run=^TestV9' '-test.count=1' '-test.timeout=60s'
```

Canonical evidence is `evidence/final-probes.log`, `manifest.json`,
`source-manifest.json`, `modules.txt`, `build.log` and `vet.log`.
Earlier setup/diagnostic logs are not the final verdict.

| Artifact | SHA256 |
| --- | --- |
| Test executable | CE2DA8DC736624FF831113A5A992D9B1123F450321C54C6C2ED87D11B7C0AF17 |
| Runner | A82221FC58C76E25697E8B867C459487DEE6382CF1F686FF14236EF240E90715 |
| Source manifest | BA51D8F5D37C0BDA6C813330F28672F01F4D5ED22DADD60654B87ABDF7B52407 |

The manifest covers 290 source files. Only two upstream files were modified:
`wrap.go` reports the created wrapper's scalar extent, and
`internal/sqlite3_wrap/sys_windows.go` permits a test to select the existing
unavailable-API branch. Two new helper files provide those observation and
availability seams. The observer retains no wrapper reference.
Allocator, VFS, generated engine and Close behavior were not repaired.

## Observed results

Build and vet exited 0. The adversarial test binary exited 1 in 0.1274093
seconds of runner wall time. The runner exited 0 only because its rejection
conditions were all observed; it does not turn failing candidate tests into a
passing implementation.

| Probe | Observation | Interpretation |
| --- | --- | --- |
| VirtualQuery extent accounting | A 65,536-byte reservation with 4,096 committed bytes was measured across split regions; release reduced occupied bytes to zero. | Observer control passed. Reserved/committed regions are counted without overlap. |
| Native open ownership | Successful open/close released its entire extent. Eight failed opens to a missing parent each retained an 8,388,608-byte extent with 327,680 committed bytes. Requery after the eighth found 67,108,864 reserved bytes including 2,621,440 committed bytes across pairwise-disjoint extents. | Failed-open cleanup is incomplete in this candidate. These are 64 MiB of virtual reservations and 2.5 MiB committed, not 64 MiB RSS. |
| Stock OOM outcome | An 8 MiB engine processing a 16 MiB zeroblob insert raised the exact typed errutil.OOMErr panic rather than returning an ordinary operation error. Explicit test cleanup then closed it and released the native extent. | The selected ordinary-error/retirement contract is not supplied by the stock path. Successful explicit cleanup proves only this observed path. |
| Existing fallback growth | Starting at the wrapper's five-page initialization and legally growing one page at a time to the 8,388,608-byte logical limit produced 9,969,664 bytes of backing capacity. A changed backing address established an old/new overlap lower bound of 17,940,480 bytes. | The 8 MiB logical setting does not bound backing at 8 MiB. Growth overlap alone exceeds the complete provisional 10 MiB allowance. |
| Ordinary WAL compatibility | The modernc control and candidate native control completed WAL setup, transaction and content check. The candidate's unchanged unavailable-API fallback failed the ordinary WAL path with IOERR_SHMMAP. | The existing fallback does not preserve this required file behavior. The probe does not identify the exact failing statement or cover the complete compatibility matrix. |

The fallback growth figures are a backend growth-sequence lower bound, not an
observed SQL projection peak. They exclude any additional temporary allocation.
The fallback test selected an existing branch through an availability fault;
it did not run an older Windows installation. The
[tagged candidate source](https://github.com/ncruces/go-sqlite3/blob/v0.35.6/internal/sqlite3_wrap/sys_windows.go)
contains that branch. The
[Go Windows support baseline](https://go.dev/wiki/MinimumRequirements) and
[VirtualAlloc2 availability](https://learn.microsoft.com/en-us/windows/win32/api/memoryapi/nf-memoryapi-virtualalloc2)
make silently narrowing the platform contract an inappropriate substitute for
qualifying it.

No forced GC was used to excuse failed-open retention. Native extent metadata
contains only scalar addresses/counts. The process-wide handle count changed
from 104 to 105 during the failed-open probe; its ownership was not identified,
so no handle-leak or handle-cleanliness conclusion is drawn.

## Stop condition and limits

The observed failed-open and OOM behavior fit the kinds of small lifecycle
repair v9 allowed investigating. However, the existing fallback allocation and
WAL failures require an allocator/VFS change or platform restriction outside
the approved boundary. No repair was attempted after this hard gate.

The aggregate host proof, full atomicity/DSN/reopen matrix, real SQL/lock
cancellation matrix, 1,000 lifecycle cycles, 30-minute sustained profile and
Linux execution are **NOT RUN**, not passed or waived. Production integration
was not authorized. A passing modernc ordinary-WAL control establishes that
control only; it does not establish the current driver's complete memory bound.

A fresh reviewer checked the final source seams, controls, byte interpretation,
module inventory and source/binary/runner hashes. This review used inherited
context and was not independent non-steered evidence. The lead reviewed the
verdict and retained the explicit limitations above.

The retained lesson is that logical engine size, actual backing, overlapping
growth, failed initialization and supported fallback behavior require separate
evidence. These candidate failures must not be attributed to GoCluster's
current modernc driver. See
[TSR-0035](troubleshooting/TSR-0035-pc92-qualification-accounting.md).
