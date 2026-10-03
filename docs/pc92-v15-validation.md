# PC18/PC92 v15 consolidated validation record

Authority: [approved v15](pc18-pc92-scope-ledger-v15.md), baseline
`2413beb47551d428d96d06a3f9178e2577d8ec9d`. All records below start unqualified.
Historical v14 passes and the retained Q1 failure remain in
[v14 evidence](pc92-v14-validation.md). No v15 execution result is implied by
this test plan.

## Completion status

Updated 2026-10-03 11:30 UTC. **Overall acceptance is incomplete.** The
component and diagnostic results below do not certify the current working tree
or replace the original acceptance workloads.

| Requirement | Current evidence | Remaining work |
| --- | --- | --- |
| Spot delivery and dedupe correctness | Exact-key and atomic-cleanup regressions pass; completed corrected warm runs delivered every required token. | Final frozen-source original workloads. |
| V16 history maintenance | Both algorithms pass targeted normal/race/static and mutation checks; matched benchmarks and profiles show lower maintenance costs. | The combined warm run still fails the unchanged 5 ms enqueue criterion. |
| Permitted V15 writer optimization | Byte/lifecycle controls and matched benchmarks pass. Full 900+660-second diagnostic delivered all 17,355,750 required reads; every first-byte cohort passed. | Enqueue p99 remains above 5 ms in 1,186 client cohorts; final-source acceptance remains open. |
| Dedicated diagnostic helper | Current Windows and Linux normal/race/vet checks and native ownership derivation pass; actual Linux companion built with the minimal dependency graph. A refreshed Windows package passed actual sibling, missing-sibling and retirement checks. | Aggregate qualification and outstanding lint disposition. |
| SQLite persistence | Windows cleanup-failure, native metadata, saved-target and malformed filename checks pass. The latest affected Windows lane and Linux native06 short normal/race/native/WAL/lifecycle/vet lane pass. Earlier sustained results retain their exact sources. | Current-source sustained checks remain. Process-global pragma compatibility is demonstrably different; required real Windows file-symlink coverage is unavailable. |
| 480 MiB ownership ceiling | Fixed context-parent and helper ownership work is implemented; reservations alone are not claimed as proof. | Standard-library descendant retirement and the remaining SQLite ownership proof are open. No additional memory exclusion or ceiling increase is selected. |
| Final closeout | Full normal/race suites, vet and Staticcheck pass; checker fixtures reject incomplete, mismatched and failed evidence. | Broader lint reports 42 issues. Original qualification profiles and a final engineering proof for the same frozen source remain required. |

No production restart, deployment, commit or push has been performed for this
work. The detailed records below retain failed attempts and the exact source
generation for each component result.

The current process-global pragma finding is not resolved by copying one
directory setting. A design-aware source review verified that modernc executes
each sorted `_pragma` as potentially multiple SQL statements; a global effect
can survive a later error. Existing candidate authorizer arguments also conflate
an absent value with an explicit empty reset. ULS independently changes modernc's
temporary directory. A parsed, ordered shared-state bridge would need additional
architecture, compatibility and allocation proof; none is implemented or
authorized by this finding. No narrowed DSN-compatibility policy has been selected.

The latest affected Windows SQLite lane ran 03:59:23-04:00:33 UTC on October 3:
normal/race fork checks, callback pointer checks, prior-driver comparisons,
vet and affected peer race checks all passed. Before/after relevant-source
manifests match SHA-256
`3550d230ebf1b2c7e1a3ca917adb47295f2431f218d93b4b2ba475e35d0b0fe5`.
Its command record is `sqlite/callback-backing/psow-final/final-commands.json`
under the external evidence root. The later native06 Linux result below adds
short correctness evidence; neither result waives the documented compatibility
or allocation gaps.

## Controlled operator trial assessment

The user prioritized correctness for a small external peer trial over the
5 ms spot-enqueue target. The lead recommends testing the current Windows
build with a few cooperating DXSpider operators, using ordinary topology
database filenames. This recommendation is not final production acceptance,
does not change the selected DSN compatibility contract, and does not waive
the one-second membership or five-second recovery obligations. Configurations
that depend on process-global SQLite directory pragmas remain outside this
readiness recommendation because their incompatibility is unresolved.

On October 3 the full normal and race suites each passed 45 packages and
2,818 test/subtest events. Tests using the pinned actual DXSpider receiver ran
and passed, including PC18 identity, both handshake directions, complete C
snapshots, typed membership, C/A metadata recovery and process restart.
Seven conditional/helper/qualification tests were skipped; their exact names
are retained in `trial-readiness/normal-summary.json` and `race-summary.json`.
These ordinary suites do not substitute for the original qualification runs.

The source/file-set witnesses were stable during each suite. After the normal
suite, the SQLite qualification fixture was corrected to join a killed writer
before attempting recovery. After the race suite, Staticcheck found an unused
ownership-test helper; removing it changed no production code, and both
remaining ownership tests passed again normally and with the race detector.
Final root `go vet ./...` and `staticcheck ./...` passed. The broader
`golangci-lint` run failed with 42 reported items, mainly test-context calls
plus cancellation ownership, diagnostic filesystem and style findings; the
failure remains recorded rather than converted into a pass.

A design-aware read-only triage found no demonstrated peer-trial blocker in
the runtime lint findings. Stored context cancels reach operation teardown and
manager shutdown; the flagged Windows comparisons inspect raw native errors
before wrapping; Boolean suggestions preserve the existing guards. Ignored
diagnostic-file removals can leave old logs but acquire no native handles and
do not block protocol service. These findings still require lint disposition
for closeout and do not settle the separate descendant-memory proof. The lead
verified the cited ownership and filesystem paths; this was not independent
certification.

Linux native05's recovery-write `database is locked` failure is retained.
Its fixture called Kill without waiting for process exit. The corrected
fixture includes a live-writer control proving that reads and integrity checks
can pass while a write lock is still held. Both Windows normal/race/checkptr
and Linux native06 normal/race checks then passed with explicit process-exit
witnesses, unchanged deadlines and immediate recovery writes. Native06 also
passed capacity, native/VFS ownership, peer lifecycle and vet gates. No database
implementation change was needed for this correction. Its source differs from
native05 only in the qualification fixture; all 12 retained binaries and 15
gate verdicts were verified. The VM was verified idle and paused. The
30-minute persistence run was deliberately not executed in this short lane.

A fresh local Windows PackageOnly build completed at 11:29 UTC. Both packaged
binary hashes matched the manifest; all required notices were present; the
cluster version command and actual companion present/missing/retirement
checks passed outside the repository. Version:
`v26.03.10-2413beb47551+dirty`. Evidence and the unpublished ZIP are under
`trial-readiness/package`. Production source bytes match the successful full
suites; `source-fixture-only-delta.json` records the later unused-test-helper
removal. No running cluster was restarted and no package was published.

The recommended field checks are handshake in each configured direction,
user/node additions and withdrawals as seen by the remote operator, complete
state after reconnect/restart, and expected spot forwarding without loops or
unexplained loss. This is a lead assessment supported by executed checks;
no independent certification or live external-operator result is claimed.

## Detailed postapproval review

Three design-aware read-only specialist passes reviewed dedup/performance,
context/diagnostics and SQLite. The lead inspected the actual qualification
owners, wrappers, status, packaging and inherited acceptance contract. All
material findings were dispositioned before implementation: covered or
checker-only refinement within the approved design. No independent review is
claimed. Literal key/byte/event vectors, independent database readers, actual
OS process boundaries and the pinned receiver reduce correlated-oracle risk.

The slice records own their complete eight-column matrices and actual results:

- `pc92-v15-dedup-validation.md`: V15-02/03 and consumer regressions.
- `pc92-v15-ownership-validation.md`: V15-06/07/08 and context/helper proof.
- `pc92-v15-sqlite-validation.md`: V15-09/10/11, persistence and native proof.
- `pc92-v15-writer-validation.md`: V15-05 measured writer work, byte/lifecycle
  oracles and guarded before/after evidence.

The lead retains integration, scope, validation claims and final acceptance
authority. The following matrix covers the remaining shared evidence surfaces.
New named tests/checkers are planned until their result is explicitly recorded.

| Contract/invariant | Failure/boundary | Stimulus/fault | Observable result | Evidence level | False-green risk | Exact checker | Owner |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Warm diagnosis retains full Q1 inputs and cannot qualify acceptance | Short run or diagnostic promoted to acceptance | Fixed 15+11 profile; duration/profile/diagnostic mutations | Full 1,560 seconds required; accepted/overall flags stay false | Unit/workflow/runtime | All previous diagnostics have zero minimum duration | TestPC92WarmDiagnosticProfileContract; wrapper fixtures and warm-diagnostic run | cluster harness/scripts |
| Three real CPU windows and complete one-second observations | Early-only sampling, missed window, catch-up pretending missed samples existed | 0-20,60-180,660-780 windows; delayed/missing/duplicate sequences | Actual start/end/sequence retained and validated; missed data invalidates diagnosis | Unit/profile/runtime | File names alone appear correct | TestPC92WarmProfileSchedule; TestPC92WarmProfileRejectsIncompleteWindows | cluster profile owner |
| Profiling does not alter load offering | Blocking control or forced collection occurs during load | Profile worker lifecycle and cancellation; actual warm load | Generator schedule/rates unchanged; no mid-load runtime.GC; service-owned worker joined | Source/race/runtime | Cold allocation helper forces two GCs | Profile source audit and warm artifact schedule | lead |
| Extra diagnostic backing is bounded | Sample arrays, JSON copies or report IPC omitted | Maximum population/duration and maximal scalar values | Fixed capacities and full encoding fit declared fixture budget/RPC | Unit/allocation | Counting len without capacity/overlap | TestPC92WarmProfileBackingAndPacketBound | cluster harness |
| Performance change is supported by comparable warm evidence | Only cold or mismatched settings improve | Retained unchanged baseline then each permitted patch with same workload/runtime/config/CTY | Named cost and predicted reduction compared; missing or inconclusive evidence stays open | Benchmark/profile/runtime | Comparing instrumented and uninstrumented versions | warm-diagnostic wrapper plus named-owner before/after benches | lead/touched owner |
| Complete-token denominators survive checker changes | Missing/duplicate/unknown recipient treated as success | Existing token/oracle fault corpus plus changed report fields | Every required delivery and per-minute latency stays binding | Unit/integration | Aggregate counts hide wrong token identity | Existing runtime oracle, missing-evidence and counter tests | cluster harness |
| Both binaries and exact source inputs are attested | Missing/stale/swapped helper; source/CTY/reference edits | Wrapper fault fixtures for each identity and phase | Failed provenance prevents accepted verdict; retained binaries match execution | Workflow | Test executable attested but helper loaded from elsewhere | test-pc92-qualification-contract.ps1; manifest tests | scripts |
| Final aggregate acceptance requires all original profiles and proof | Missing/stale/diagnostic/failed result; source mismatch across profiles | Finalizer positive and each negative evidence set | Acceptance only for complete matching final state; interrupted run stays incomplete | Workflow/runtime | Individual profile pass incorrectly implies total acceptance | Final v15 evidence reconciliation and checker fixtures | lead/scripts |
| 480 MiB covers simultaneous ownership | Disabled SQLite/helper, separate peaks, uncharged failed retirement | Q4 A/B full populations and real refill/fault overlap | Complete static backing inventory plus synchronized observations within all partitions | Allocation/runtime/native | RSS or post-GC retained heap substitutes for ownership | Q4 a/b; slice bounds; pc92-allocation-proof.md | lead/owners |
| Platform/package paths work outside workspace | Repo sibling accidentally masks missing packaged helper; build-only Linux claim | Isolated unpacked package, missing helper control, actual Windows/Linux runs | Expected process lifecycle/data/log behavior and both hashes; no silent skipped platform cases | Build/process/runtime | Cross-compilation called execution | PackageOnly package checks; native platform manifests | lead |

## Review dispositions

- Preserve literal historical collision inputs; make true duplicates suppress
  after both distinct inputs pass. Do not copy the old defect-expecting probe.
- Primary minute-key construction and duplicate-age boundaries are separate
  tests. The cleanup race must use the real processing path and a deterministic
  coordination seam, plus a broken-implementation control.
- No forced GC is permitted during warm load. Optional stage instrumentation
  under V15-04 now has passing 32 MiB backing, token/clock/overflow, lifecycle
  and mutation checks. The original off/on diagnostic stopped after its off
  run because caller-environment restoration failed. Its replacement began at
  2026-10-03 01:09:00 UTC with full pre-run environment presence/value hashes;
  both full 900+660-second runs and byte-identity checks completed at
  2026-10-03 02:02:50 UTC. Both original enqueue verdicts remain failed.
  See `pc92-v15-stage-validation.md`.
- Diagnostic inventory includes every callback/direct-log escape path,
  startup/IPC/path/environment allocation, file maintenance and failed process
  generation. Real blocked-helper kill/join tests run on both operating systems.
- SQLite failed release ownership extends through wrapper, handles, OS mapping
  and memory APIs. An error return without retained ownership does not pass.
- Preserve module graph isolation, loaded logging defaults/explicit zero,
  topology DSN/pragma/deadline semantics and committed logical data. Acknowledged
  commits require new data after crash; old-or-new is only valid without ACK.
- The workflow checker unconditionally rejects create-release.ps1 changes.
  Record this narrow conflict with explicitly approved companion packaging;
  do not silently modify or bypass its global rules.
- WSL's required Windows components are not enabled. No host feature enable or
  reboot was performed. Actual Linux checks instead run in an isolated Debian
  13.7 QEMU guest, with publisher-verified inputs and loopback-only access.
  Native component results remain provisional until the final source freeze.

## Initial executed evidence

Artifacts are under `D:\codex-gocluster-v15-20261002`. Individual worker records
identify their exact checked source; later cross-package changes require final
integration checks and invalidate any broader result they could affect.

- V15-02/03 targeted dedup normal/race/static checks and both mutation controls
  passed their intended assertions; see `pc92-v15-dedup-validation.md`. The lead
  directly reviewed the key construction, map equality, cleanup transaction and
  collision/concurrent-refresh tests. No material finding in that slice.
- The new warm-profile Go checker tests passed in the isolated baseline source:
  profile contract, schedule, missing/late/duplicate/clock faults, cancellation
  and maximal report encoding. The maximal RPC was 1,412,293 bytes, below 2 MiB;
  conservative sample/report/encoding backing was 13,260,160 bytes. This is a
  fixture bound, not the protocol allocation proof.
- `scripts/test-pc92-warm-diagnostic.ps1` passed 19 synthetic positive/negative
  checker cases. An initial fixture attempt was terminated after identifying a
  PowerShell comma/multiplication precedence error in the new checker. Separate
  scalar assignments fixed it before the passing run and warm execution.
- `scripts/test-pc92-qualification-contract.ps1` passed 104 behavioral fixtures,
  including failed companion build and missing/changed companion after execution
  (`wrapper-fixtures.log`). Mock outputs are never qualification evidence.
- Module selection before/after adding the pinned fork differs by exactly the
  SQLite fork, byte-identical local engine and julianday v1.0.0. No previously
  selected dependency version changed (`modules-before.txt`, `modules-after.txt`,
  and corresponding graph files). The Go directive is normalized from 1.26 to
  1.26.0 to meet the module's declared minimum; execution remains Go1.26.4.

The `warm-baseline-source` detached worktree freezes `2413beb` plus only the
diagnostic harness, with exact CTY/H3 inputs copied for isolated execution. Its
26-minute run completed under `warm-baseline`, run
`3bacbb61b1c7448680c1c355b517fcf6`: 900 seconds offered load plus 660 seconds
drain (1,591.46 seconds including setup). Later production edits in the main
workspace cannot alter that baseline. All three actual CPU windows and all
900 one-second samples passed the warm-evidence checker; their hashes are
retained in `diagnostic-evidence.json`. The measurement verdict failed and
overall acceptance remains false.

The baseline offered 150,000 new spot keys, 1,500,000 duplicates and 90,000
PC92 records. Peer spot recipients missed zero deliveries; each of the 100
local clients missed IDs 19367 and 90004 (200 missing reads/enqueues). The
first was traced to primary collision 16953/19367 (`2ad54793`), while 90004 is
the retained SLOW pair. Literal encoded keys, the input-ledger timing bounds
and their limitations are recorded in `warm-collision-analysis/reconciliation.json`
and [TSR-0036](troubleshooting/TSR-0036-spot-collisions-and-peer-ownership.md).
Overall client enqueue p99 upper bounds were 31-35 ms and
first-byte bounds 40-47 ms; worst minute bounds were 52 and 66 ms. All four
protocol caches drained to zero. These diagnostic timings include profiling
and do not replace original unprofiled qualification.

In the 660-780 second CPU window, WhoSpotsMe.Record accounted for 16.72 seconds
of 125.47 sampled CPU seconds (its bucket scrub 15.92 seconds), and
HarmonicDetector.cleanup accounted for 9.55 seconds. This supports investigating
their maintenance work, not a claim that CPU sampling alone proves the latency
cause. Those algorithms fall outside V15-05's permitted mechanisms. The user
then approved the focused [v16 amendment](pc18-pc92-scope-ledger-v16.md),
retaining existing behavior and acceptance criteria. Its two algorithms and
targeted normal/race/static/mutation checks are complete. Clean retained-binary
benchmarks measured about88% less full-capacity WHOSPOTSME maintenance time and
98% less harmonic steady-expiry time, with unchanged allocation counts and no
reproduced sparse-case regression. Three matched end-to-end comparisons are
complete; [the slice record](pc92-v16-performance-validation.md) retains exact
evidence and the limits of adaptive benchmark comparisons.

All three matched runs delivered every required token. With both v16 changes,
overall client enqueue p99 was 4.6–6.0 ms and the worst client-minute was
8.6 ms, still above the unchanged 5 ms limit. First-byte p99 met its 25 ms
limit in every measured client-minute (worst 20.4 ms). These frozen diagnostic
snapshots isolate the history changes and do not contain the complete v15
SQLite/helper implementation. No original acceptance profile has been replaced
by these results.

The isolated `warm-dedup-source` changes only the approved primary/secondary
keys and primary cleanup relative to the frozen diagnostic baseline (plus its
matching existing compact-test adaptation). Its full 900+660 second run under
`warm-dedup` completed in 1,591.98 seconds including setup: 150,000 new keys,
zero missing local enqueues/reads and zero missing peer reads. All 900 samples
and three CPU windows passed the diagnostic evidence checker, and all four
protocol caches drained to zero. Latency still failed: client overall enqueue
p99 upper bounds were 30-34 ms and first-byte bounds 40-47 ms. Actual Linux VM
compilation overlapped part of this run; it is delivery evidence and a diagnostic,
not a controlled performance-improvement comparison. Matched performance reruns
will avoid that competing workload.

Q4 now enables isolated SQLite and the companion, preserves the configured
projection interval, and requires a real full-population projection commit
before measurement. Simultaneous pressure samples include their actual charges,
context occupancy and failure states. Fourteen negative ownership fixtures plus
the positive control pass; a reservation flag alone cannot satisfy enabled
ownership. The complete source-level allocation proof remains a separate open
requirement.

Q6 receive-only now uses its independent immutable per-token requirement (all
11 copies of every offered key), exact total, no-forwarding and zero-cache
assertions. The removed diagnostic callback counter did not establish delivery;
diagnostic coalescing/loss is deliberately separate from frame-loss evidence.

## Current result

The repaired Windows SQLite sustained test completed at
2026-10-02 23:37:52 UTC in 1,801.44 seconds. All 1,800 projection snapshots and
1,800 legacy updates committed; maximum commit gaps were 1.2679519 and
1.7884017 seconds, maximum legacy queue occupancy was one, and final drain
was zero seconds. An independent modernc reader checked every final node and
typed edge. The retained log is
`sqlite/persistence-30min-windows-repaired.log`; the failed 142.27-second
pre-repair run remains evidence of the corrected deficit. This component pass
does not close the native Linux, final-source, allocation-proof or filesystem
compatibility requirements. Subsequent Windows path corrections and newly
demonstrated filename/pragma differences are recorded below. The SQLite slice
record retains each failed and passing generation. Actual Windows symbolic-link
coverage remains unavailable on this host.

The frozen `v15-native-relevant-03` Linux generation failed the unchanged
full-capacity projection gate: 4,096 nodes and 131,072 typed edges reached
`sqlite3: interrupted` at 5.00698015 seconds. One authorized same-binary
diagnostic replay also failed at 5.004954732 seconds. Its CPU profile attributed
90.45% of samples cumulatively to the projection transaction, including
SQLite execution and binding. The host was QEMU TCG with two virtual CPUs,
4 GiB RAM, Go 1.26.4, GOMAXPROCS=2, GOGC=50 and GOMEMLIMIT=1536MiB.
This is an unmet Linux qualification gate; emulated execution does not establish
native-hardware performance or a data-correctness defect. No deadline was
extended. Later native gates and the required thirty-minute Linux workload were
not run. Source, binary, logs, profile, process-retirement witness and hashes
are retained in `linux/native-relevant-03-result.json`. The VM was paused after
the diagnostic, with no test/build process remaining. This source generation
precedes the subsequent Windows helper path repairs.

The same native03 binary subsequently passed on actual WHPX acceleration with
`q35,kernel-irqchip=off`, the same two CPUs, 4 GiB and runtime settings. Its
full-capacity projection took 515.336 ms within the unchanged five-second
deadline; source and binary hashes matched before and after. Default WHPX had
failed boot with VP exit code 4 before any test, so its failure and the earlier
TCG results remain retained. No host feature, mitigation or test deadline was
changed. The VM was positively paused afterward. Evidence is
`linux/native03-whpx-irqchip-result.json`; this source-specific capacity pass
does not replace the still-required sustained Linux run or final-source checks.

The source proof also remains open for standard-library context/network
descendants and SQLite Windows path/failed-release operations. The helper's
owner-local Windows correction passed its full normal suite in 10.359 seconds,
race in 9.262 seconds, and vet. Its native attributes/find/file paths, recursive
directory creation, legacy path conversion and actual failed-close process
retirement have explicit source and fault evidence. The lead reviewed the
consumed metadata semantics, release paths and the 64P+64C path inventory.
Fixed backing is parent 528,776/helper 1,138,872 bytes; the unchanged 3 MiB
reservation includes the other derived/native terms, not just these structs.
Final-source Linux/Windows integration and aggregate qualification remain
required. SQLite pre-VFS directory work and sticky failed-release ownership
remain separate open work. These are proof gaps, not observed 480 MiB overruns.

The frozen helper-only `native-04` snapshot then passed actual Linux normal
and race execution, vet, companion build and the 113-package minimal dependency
check at 2026-10-03 02:56 UTC. Normal and race binaries took 1.66 and 2.93
seconds respectively. Source/file-set and binary hashes matched before/after;
the retrieved evidence archive SHA256 is
`17af9049ebf238d60ed56979d5bfb571adf1ff325fa452f50fe6e8de89e3f2fb`.
The guest had no live build/test/helper processes and was positively paused at
02:56:49 UTC. The lead verified the result, native log, archive hash and QMP
witness in `linux/helper-native-04-run-20261003T025631Z-6b71eddf248142938fb57c93106dc3cc`.
This establishes the helper component result, not SQLite or whole-cluster
Linux qualification.

The first stage-disabled warm diagnostic completed its full load and drain at
2026-10-03 00:52 UTC with no missing tokens. It still failed enqueue latency:
overall client p99 ranged from 4.4 to 5.8 ms and the worst client-minute was
6.8 ms. First-byte p99 ranged from 11.6 to 15.6 ms, with a worst minute of
17.4 ms. The paired run stopped before the enabled run because the PowerShell
wrapper restored originally absent environment variables as present-empty.
The original failed pair remains in `warm-stage-pair`; separate v2 integrity
checks preserve all 1,218 original latency failures. The caller's complete
pre-run environment was not retained, so this run cannot retrospectively
establish a controlled observer-overhead comparison. The fresh pair in
`warm-stage-pair-v2` retains full presence/value hashes before launching either
run, permits restoration only of the demonstrated managed absent-to-empty
change, and checks complete cross-run parity except for the instrumentation
switch. Its stage-disabled run completed the full 900-second load and
660-second drain with strict evidence integrity and no missing tokens. All
1,450 original failures were enqueue-latency conditions; other failure count
was zero. Client overall enqueue p99 ranged from 4.5 to 5.9 ms, the worst
client-minute was 7.4 ms, and 180,021 of 15,000,000 spot-client endpoints exceeded
5 ms. First-byte overall p99 ranged from 11.8 to 16 ms, with a worst minute of
18.6 ms and no violating cohorts.

The enabled run then completed with the same source, executable pair, assets
and caller environment except for its stage switch. Pair closure and integrity
passed; both runs observed all 17,355,750 required reads across 116 recipients.
The enabled run retained 1,337 original enqueue failures, zero other failures,
overall client enqueue p99 of 4.6–6.2 ms and a worst minute of 7.4 ms. Every
first-byte cohort passed (overall 12–16.2 ms, worst minute 18 ms). Its complete
stage chain covered 150,000 spots and 15,000,000 spot-client endpoints. All seven
conditional segments reconcile to the same 190,817 late endpoints. Of those,
88,000 already exceeded 5 ms between publication and primary-ready, and 16,900
exceeded 5 ms between broadcast receipt and worker start. These overlapping
fractions must not be added. Worker-start to client admission had a conditional
p99 upper bound of 0.3 ms.

Publication-to-primary-ready includes driver formatting/send, TCP/peer parsing,
ingestion queue wait and dedup processing; it does not identify dedup CPU as
the cause. Instrumented late endpoints numbered 190,817 versus 180,021 without
instrumentation. A single pair describes observer influence and locates delay
along the pipeline, without isolating every cause or proving observer neutrality.
Quantiles are not subtracted, and independently rounded stage bounds are not
summed into an endpoint duration. Raw reports, every cohort, original verdicts
and the artifact closure remain in `warm-stage-pair-v2/diagnostic-summary.json`
and its accompanying evidence. This archived generation precedes later Windows
path changes and cannot qualify final-source acceptance.

Actual Windows GUID-junction differential fixtures also exposed persistence
compatibility defects: a configured junction followed by `..` selected a
different saved database than modernc, and trailing-dot/space or literal
extended-target aliases differed by `winreadlinkvolume` mode. Ordinary GUID
aliases and their multiprocess WAL controls passed. The failed saved-target
checks are real correctness evidence; the extended-target fixture's initial
seeding error is retained separately. Preparing the initial Windows pathname
before resolving junctions subsequently passed the parent-component and
trailing-dot/space saved-target cases in both tested modes. The literal
extended-target failures remained. A subsequent review of the pinned modernc
driver's Windows VFS established that it uses lexical GetFullPathName output,
without a reparse walk. The Windows-only correction now follows that behavior;
the intervening native reparse-reader draft was removed without claiming an
execution result. The short native gates in `sqlite/windows-lexical-20261003`
passed all GUID/DRIVE saved-target and native dot-component cases, GUID
three-process WAL, and the fixed-path normal/race/checkptr/vet checks. Both the
complete source and fork manifests matched before and after this run.

That same run confirmed two separate compatibility failures. Three malformed
UTF-8 URI filename vectors selected a new empty candidate file instead of the
prior driver's replacement-decoded saved database. Also, a successful
`data_store_directory` pragma changed a later relative modernc open with the
old topology driver but not with the isolated candidate engine. Exact logs and
the passing Unicode/invalid-byte controls are retained. The bounded native
CP_UTF8 conversion subsequently passed every original malformed-name vector,
the existing GUID/drive/dot/WAL cases, and VFS normal/race/checkptr/vet checks.
Peer directory normal/race/vet also passed after the helper constant repair.
Evidence is in `sqlite/windows-native-utf8-20261003`, with matching source
manifests before and after execution (02:26:29–02:27:01 UTC). The pragma
compatibility failure remains against the selected preservation contract; no
cross-driver bridge or changed setting policy has been selected. The required
actual file-symlink gate still failed because this process lacks the Windows
privilege. Neither failure is counted as a compatibility or overall pass.

The subsequent Windows ownership correction passed on stable source in
`sqlite/windows-native-owners-20261003-b`: engine/wrapper normal and race;
VFS normal, race, checkptr=2 and vet; and peer constructor/native-failure
subprocess normal, race and vet. Tests cover failure arising during admitted
SQL work, sticky successful-result handling, deferred allocator unwind,
native lock release, fixed handle admission, actual metadata ownership,
private MkdirAll/junction parity and failed-constructor replacement refusal.
Saved-target, native-dot, malformed-URI and three-process alias-WAL checks
also passed. The lead reviewed the changed ownership paths and these actual
logs. A second bounded reviewer checked SQL entry/unwind and callback ownership;
that was design-aware review, not independent certification.

The earlier generation did not compile its VFS tests because a test-hook
signature used the wrong CreateFile template-argument type. Its failed build
is retained and does not count as executed VFS evidence. After correcting the
signature, the revised generation's complete short lane ran. Its overall
runner still reports FAIL: the separately identified global data-directory
incompatibility and required symbolic-link privilege gate remain unresolved.
The updated allocation inventory and current-source Linux/sustained tests
remain separate obligations.

The writer's ten alternating retained-binary benchmark pairs passed their
complete-output guards. Ordinary spot output fell from 240 bytes/three
allocations to 80 bytes/one allocation, with median time 727.6 to 558.9 ns per
spot. Normal/race/vet and four deliberate broken-implementation controls passed
their intended checks. The lead reviewed the production diff and byte/lifecycle
fixtures. This establishes a local reduction, not an enqueue-deadline fix. See
`pc92-v15-writer-validation.md` for the exact artifacts and other output cases.

That comparison started at 2026-10-03 02:29:01 UTC in `warm-writer`, run
`e32a951fdff848a3a3fbfafea9d74368`. Its frozen source differs from the fresh OFF
baseline only in the two reviewed writer production files; helper, data,
settings and full caller-environment guards matched before launch and at
closure. The full 900-second load and 660-second drain completed, with all
recorded launcher/driver/service/helper processes confirmed exited at
02:55:59 UTC. The diagnostic is valid, but the latency verdict is failed.
All 17,355,750 required reads arrived. Enqueue client-wide p99 ranged from
4.3 to 5.7 ms, the worst client-minute was 7.3 ms, and 1,186 client cohorts
violated the unchanged 5 ms requirement. First-byte p99 ranged from 11.1 to
15.5 ms, with a worst minute of 17.7 ms and no violating cohorts. There were
no other checker failures. The corresponding fresh OFF figures were enqueue
4.5–5.9 ms, worst minute 7.4 ms and 1,450 violating cohorts.

The observed sample-1-to-900 runtime allocation deltas were 17,222,772,752
bytes and 181,037,171 allocations, versus 19,627,375,088 bytes and 211,108,344
in fresh OFF. These are process-wide runtime measurements, not the protocol
ownership bound. The late CPU samples totaled 95.59 seconds versus 96.24 in
OFF; one sequential comparison does not establish latency causality or a
significant total-CPU reduction. Original verdicts, complete cohort metrics,
profiles and provenance are retained under `warm-writer` and
`writer-analysis/runtime-profile-analysis`. The run is a diagnostic on its
archived source, not final-source acceptance.

The final reconciler is `scripts/pc92-final-qualification.ps1`. It requires all
11 original full workload profiles, retained sibling binaries and their exact
hashes, unchanged before/build/after source-and-asset manifests, original test
cases and durations, the pinned receiver, all selected check records and a
source-bound engineering review. The review separately covers every approved
v15/v16 item and all ten allocation subdivisions totaling at most480MiB. It
cannot turn an open ownership claim into a proof: each review entry must cite
retained hashed evidence, and the lead remains responsible for that evidence's
sufficiency. A full-source digest includes tests, scripts and documentation;
use the same frozen source for the entire final run set. Native check records
use a path-independent digest without normalizing source bytes.

Forty-three synthetic positive/negative final-reconciliation fixtures passed,
including separate load/drain durations, mandatory receiver inputs and changes
to previously validated artifacts during reconciliation. Invalid exit-code
types are rejected; changed correction evidence clears correction closeout,
while missing workload evidence alone need not clear it. These are checker evidence
only, never executed workloads or allocation proofs. The reconciler retains
separate `audit_corrections_complete` and `overall_accepted` results and writes
an incomplete initial verdict before checking. No real final bundle has passed.
Artifacts: `final-reconciliation-strict.log`. The updated wrapper previously
passed all104 behavioral fixtures with an unambiguous child PowerShell exit0
(`wrapper-fixtures-final-durations.log`). Its newest receiver-field changes also
passed all104 fixtures with exit0 (`wrapper-fixtures-reviewed.log`). An earlier enclosing-shell stale native
exit value is retained in `wrapper-fixtures-final-fields.log`; it is not the
final script execution result.

The final wrapper records `GOEXPERIMENT` and the effective process `GODEBUG`
alongside the toolchain/build settings; recording those values does not prove
their compatibility with an allocation argument. The latest wrapper and final
reconciler again passed 104 and 43 fixtures respectively with exit 0
(`wrapper-fixtures-build-options.log`, `final-reconciliation-build-options.log`).
The later environment-restoration correction explicitly preserves absent,
present-empty and nonempty values. All 110 wrapper fixtures passed with exit 0
(`wrapper-fixtures-environment.log`), including six same-process cases that
compare the complete caller environment and current directory after successful
and failed execution. These synthetic runs exercise the real wrapper with fake
build/test processes; they are not qualification workloads.

A design-aware packaging review corrected the Linux companion build/deployment
instructions and isolated PGO output from the original executable pair. PGO
now publishes both successful builds together under a unique `.tmp/pgo`
directory, preserving existing executables and recording both hashes. Four
mocked script cases passed after lead review, covering both build failures,
successive successful pairs, preservation, containment and hashes
(`pgo-pair-fixtures-reviewed.log`). These are script checks, not real package
builds or source-provenance proof. Release packaging includes the SQLite
wrapper/engine notices and the Go BSD notice for adapted bounded path code.

A real local `create-release.ps1 -PackageOnly -AllowDirty` run subsequently
passed with current generated code maps. The archive was extracted outside
the repository. Both executable hashes matched `binaries.json`, all three
third-party notices were present, and the packaged cluster's `--version`
command ran successfully. A retained standalone harness exercised the actual
packaged sibling: one event reached its dedicated log, shutdown joined it and
released the charge. Moving only that isolated sibling aside made logging
degrade with zero generations/writes; shutdown still released its charge.
The sibling was restored and both hashes rechecked. Evidence and harness source
are retained under `package`; no tag, push, release publication or cluster
runtime startup occurred. This checks the packaged generation at
2026-10-02 23:57:33 UTC, not later helper changes or final-source provenance.

The lead verified the fixed heap ownership and timestamp callers and recorded
the durable v16 decision in [ADR-0235](decisions/ADR-0235-spot-history-maintenance.md),
linked to [TSR-0036](troubleshooting/TSR-0036-spot-collisions-and-peer-ownership.md).
The troubleshooting index/record checker passes. A separate design-aware
read-only finalizer review found the receiver-closure and change-during-read
gaps; both are repaired and have negative fixtures. This is not independent
certification of the complete allocation proof.

Detailed test review complete. Implementation and final-source evidence remain
in progress. **Audit corrections incomplete; overall acceptance false.**
