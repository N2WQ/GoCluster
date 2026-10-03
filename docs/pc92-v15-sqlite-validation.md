# V15 topology SQLite validation and ownership argument

Status: implementation and targeted checks are in progress. This record covers
V15-09/10/11 and the persistence portion of V15-12. It does not establish overall
acceptance. Final-source qualification, the complete prescribed persistence
exercise on both platforms, and root integration review remain required.
The inherited v9 later-phase list includes Windows and Linux execution. The
repaired Linux full-capacity five-second deadline gate must pass before its
30-minute profile is meaningful; emulation does not authorize a relaxed limit.

## Accepted detailed checker matrix

The postapproval review was design-aware; it is not independent certification.
Each row records the eight review fields, including a way to reject a false pass.

| Contract | Failure stimulus | Fixture boundary | Oracle | Negative control | Resource/lifecycle observation | False-green risk | Disposition/checker |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Own partial initialization and zero-handle wrapper | Too-small engine allowance; invalid path/pragma after acquisition; failed retirement | Pinned fork constructor and production adapter | Original owner remains reachable; no replacement before successful release | Ordinary open/close succeeds after clearing fault | Real reserved extent and scalar counters, global reservation | Returning an error while losing native owner | Fork `PartialInitializationOwnership`, `ZeroHandleClose`; adapter `FailedInitializationRetainsReservation` |
| Serialize one connection with total deadline | Hold admission gate; external modernc writer lock; long SQL | Production `run`, transaction and manager workers | Context expires including wait; live graph continues; joined shutdown closes store | Release lock/perform later operation | No worker/connection multiplication; projection reservations drain | Simulating a queue rather than an actual database lock | `DeadlineIncludesAdmission`, `RealSQLCancellation`, `PC92SlowStorageDoesNotBlockAuthorityAndStop` |
| Recognize only allocation failure and retire poisoned engine | Actual 16 MiB `randomblob` in an 8 MiB engine; native reserve/commit ENOMEM; non-OOM panic values | Generated engine, handwritten allocation boundary, adapter | Ordinary budget refusal, old committed data intact, old owner released before reuse; unrelated panic identity unchanged | New small write after retirement; independent modernc observer | Fixed engine base/capacity; confirmed native release or retained charge | Disk-backed `zeroblob` can stream and need not exhaust memory | `ActualOOMRetiresBeforeReplacement`, fork `OOMClassificationAndRetirement`, `NativeAllocationOOMOwnership`; ineffective zeroblob fixture replaced |
| Lower-layer release ownership | Failing wrapper closer, native free/unmap, pending mapping-handle close, fallback view close and directory close | Each owning layer plus adapter retirement gate | Failed owner and charge retained; only a safely retriable owner may release later; disabled manager observes global old reservation | Clear pre-call fault and confirm release/reopen; consumed File.Close stays terminal | Real native memory/view/file, fixed slots, no overlapping replacement | Error above an already-freed allocation; retrying a consumed descriptor wrapper | Fork release tests plus `CleanupFailure250`, `DisabledManagerObservesFailedOwner`, native terminal-owner cases below |
| Preserve configured SQL behavior and committed files | Repeated/invalid pragmas, configured mmap, large saved schema and oversized temporary path | Previous modernc driver versus new topology adapter | Matching pragma ordering/defaults/acceptance; independent committed-value and integrity checks | Valid ordinary database and temp path | Bounded DSN, streamed schema rows, no destructive migration | Comparing only driver error strings or file existence | `DSNCompatibility`, `ConfiguredMmapReadWrite`, `StartupResourceRefusalPreservesData`, `TemporaryPathResourceRefusal` |
| Native/fallback WAL compatibility | Separate processes, conflicting writers, pinned reader, checkpoints, >64 KiB index, crash before/after acknowledgment | Fork native/fallback and modernc processes | Exact old-or-new whole transaction; acknowledged commit must be new; recovered writes visible | Reader release permits checkpoint; subsequent committed write | Real locks and shared mappings, not process-local mutexes | Calling two connections in one process; accepting old data after acknowledged commit | Portable `qualification` module `WALProcessLocksAndCoherence`, `WALMultipleIndexPages`, `WALCrashAndReopen` |
| Crash recovery begins after writer termination | Kill sent while COMMIT acknowledgment remains unread | Exact helper PID and its single owned Wait | Forced exit witnessed before immediate recovery UPDATE; unchanged whole-transaction phase values | Live writer permits all read/integrity probes but rejects conflicting UPDATE; the same observer succeeds after Kill/Wait | PID, exit status/signal and join timestamp; cleanup reuses Wait result | Treating successful reads or Kill returning as proof of lock release | `WALLiveWriterReadProbes` and `WALCrashAndReopen`; Windows normal/race/checkptr and native06 Linux normal/race |
| Fallback mapping budget across files | 64 slots across two files; 65th request; externally grown 2.5 MiB WAL index | Real native views/private-shadow bridge | Correct 64 KiB mapping alignment; capacity refusal; independent data survives checkpoint/reopen | Released slot reused; small WAL accepted | Global 64 slots; each 64 KiB view + 32 KiB shadow; private copy in engine | Counting only logical 32 KiB index pages or per-file slots | `FallbackMappingAlignmentAndGlobalBudget`, portable `WALExternalGrowthBudgetAndRecovery` |
| Full backing and sustained progress | Maximum population/metadata, alternating generations, 1 Hz each request class, repeated failures | Actual projection/legacy workers and independent database observer | Both classes commit at least every 5 seconds; latest snapshot drains within 5 seconds; exact final rows | Real wire decode and graph admission for both shapes | Source bounds below, 1,000 prescribed lifecycle cycles, 30-minute profile, simultaneous enabled Q4 | A short run, unsupported synthetic rows, different runtime profile or separate peak sums | Pressure fixtures explicitly distinguish diagnostic/full runs; final-source and Q4 evidence remain separate gates |

## Crash fixture termination-barrier correction

Native05 failed the normal Linux native/unacknowledged crash case: the reopened
candidate UPDATE returned `sqlite3: database is locked`. Its original source,
binary and `normal-wal.log` remain retained under the external evidence root's
`linux/sqlite-native-05-run-20261003T110050Z-b8bf281389ec43e3820f9f63ff1eb3de`.
The source review found a definite fixture defect: `Process.Kill` returned
before the test waited for termination; `Cmd.Wait` ran only in cleanup after
the recovery assertions. Go documents that Kill does not wait for exit.
Successful committed reads and integrity checks do not prove the writer has
released its WAL write lock. The original log lacks a per-child exit witness,
so it does not establish the exact kernel scheduling point behind that BUSY.

The correction changes only `qualification/process_test.go`. The test owner
waits once for the exact killed process, verifies forced termination and logs
the PID, Wait result and join timestamp before recovery. Cleanup reuses that
result. The unacknowledged COMMIT response remains unread. The old-or-new whole
transaction, acknowledged-new, integrity and immediate recovery-write checks
are unchanged. No production code, timeout, retry or persistence policy changed.

The new `TestV15SQLiteWALLiveWriterReadProbes` control intentionally leaves a
writer alive with an uncommitted update. The previous read/integrity probes
must succeed, a conflicting UPDATE must return BUSY, and the writer must still
read its own pending changes. After Kill/Wait, the same observer must immediately
write successfully without a sleep, retry or changed timeout. This falsifies
using successful reads as a termination barrier.

Windows normal, race and explicit checkptr=2 gates passed in native and fallback
modes at 11:19:45–11:20:12 UTC on October 3, 2026. Retained binaries, commands and
logs are in `sqlite/wal-crash-wait`; all six build/test commands exited zero.
The complete relevant source manifests before and after match SHA256
`4d4cc8bceddb8a6bd1ec090e7c78fd2f023ddb10da749dddccd47422bfb0071e`.
The corrected fixture SHA256 is
`273e1fcc27835bf2c1754fdb548f82f47addfd709e0c64ec8913007449e53f37`.

Actual Linux native06 normal/race WAL gates also passed. Both runs recorded
SIGKILL/Wait witnesses for the live control and all three crash phases before
successful recovery writes. Uncommitted data stayed old; acknowledged commits
were new; the unacknowledged outcomes observed in these runs were old. The
locks/coherence and multiple-index-page gates passed as well. Linux explicitly
skipped only the Windows fallback external-growth case. The complete short
runner also passed its selected ownership, lifecycle, callback, PSOW and vet
gates; it did not run the separately required 30-minute profile.

Native06 evidence is under
`linux/sqlite-native-06-run-20261003T112209Z-b4af529982374de5b91126473904ca67/retrieved/evidence-v15-sqlite-native-06`.
Its source archive SHA256 is
`a43d38e3cdd35d66b60183d1818b3cf845e6da8ad851c8c87dc1a44bc48cfb20`.
The retrieved normal/race WAL logs were read and their hashes verified:
`5d086fcc20d1a0489ab3dc1705e62e18214f8b9d12f0a4a0abbdae0654e238b4` and
`2ed762638faad9053793acb3a0670996cc83d85193c0bbf411a68baee3729f8e`.

Historical crash-gate passes elsewhere in this record used the earlier fixture
and did not prove process-exit ordering. The corrected checks establish that
property for the observed runs; this incident did not demonstrate a product
VFS failure after confirmed writer exit. The global-pragma compatibility,
required Windows symbolic-link, aggregate allocation and sustained-load
acceptance gaps remain separate. These targeted passes do not establish overall
acceptance.

## Implementation and compatibility boundaries

The topology adapter uses the narrow local `ncruces/go-sqlite3` v0.35.6 fork;
other modernc consumers are unchanged. The generated engine v6.3.35304 is
byte-identical, including its original data and license. Fork manifests record
original source hashes. The three original v0.30.0 fallback source files and
their hashes are retained under `third_party/go-sqlite3/provenance`.
The root dependency graph comparison records only the three pinned additions
and two local replacements, with no pre-existing selected module version change.

There is one process-wide reservation across opening, active and failed
retirement. A second topology constructor refuses immediately; it may retry a
previously closed failed owner without waiting for another operation. A cleanly
retired owner permits a later constructor. This restriction is deliberate: two
engines must not overlap the approved 16 MiB persistence allowance. A failed
constructor cannot orphan allocations merely because its caller receives nil.

The adapter keeps the existing five-second total operation context, created
before serialization/lock waiting. Rows and parameters stream through the one
connection. One transaction-owned statement is reused for consecutive identical
application SQL and finalized when SQL changes, before commit, or before normal
rollback. It does not survive its transaction; there is no dynamic cache or
retained row batch. Allocation failure retires the engine and its fixed statement
table without further SQL cleanup. Unknown
panics are not converted into database errors. Known engine/native allocation
failures poison the connection and retire it before another SQL operation.
Failed release retains the wrapper and global reservation. Synchronous disk
I/O assumes a responsive supported OS; a context is not a promise to interrupt
an indefinitely stuck kernel operation.

Previous defaults `foreign_keys=0` and `trusted_schema=1` are restored before
explicit DSN pragmas. Busy timeout and repeated pragma ordering follow the
previous modernc driver. Parsing uses a 64 KiB input limit and 128 fixed pragma
records. Lowercase sort keys are compared as streams of simple-lowercased runes,
including invalid UTF-8 replacement, with no materialized expanded key. This
preserves modernc's busy-timeout precedence, repeated sorting behavior and
`_txlock` interpretation; Unicode case folding would incorrectly accept long-s.
The adapter clones the admitted path so a direct caller's short substring cannot
retain an arbitrarily large backing string. Oversized configuration is an
ordinary resource refusal. Schema
inspection streams seven presence bits and never materializes a large saved
default expression or an entire result set on the Go heap.

Windows database-file mapped reads differ: the candidate VFS uses ordinary file
I/O and `PRAGMA mmap_size` returns no row, while modernc exposes its mapping
limit and may map database pages. Configured `mmap_size(N)` is still executed.
SQLite defines N as the **maximum permitted** mapped-I/O size, subject to build
and runtime limits; it does not guarantee mapping. Successful configured-N
commit, independent external update, and read-back are tested. WAL mapping is a
separate mechanism and remains required. Generic SQLite API or performance
equivalence is not claimed. Reference: [SQLite mmap_size](https://sqlite.org/pragma.html#pragma_mmap_size).

The existing 1,024-byte VFS filename ceiling now also bounds temporary-file
construction before path composition. Windows uses fixed-buffer environment and
GetTempPath2/GetTempPath calls; an oversized environment value cannot cause a
retry-sized allocation before checking the bound. Empty/default/root directory
semantics are tested against the Go implementation. Oversized temp paths return
the same budget-refusal category; the permanent database remains intact.
The cleaned startup directory is checked against the same 1,024-byte VFS ceiling
before `MkdirAll`, preserving the previous directory calculation and refusing
before filesystem mutation. Both deep and oversized directory refusals have a
normal creation control and an independent saved-value/integrity oracle.
The URI `modeof` metadata-reference filename also has the 1,024-byte owned-path
budget before OS stat/conversion. Its exceptional refusal is a resource policy;
there is no truncation. The already opened partial file remains in wrapper
ownership if closing that refusal fails. Its 1,024/1,025-byte boundary and normal
modernc parity/saved-data fixture must pass before final acceptance.

## Source ownership inventory

### Current Windows lexical fullpath correction

Current source follows modernc's Windows lexical path contract rather than
the generic ncruces Go resolver. It bounds GetFullPathNameW with one1,025-unit
output, validates the complete result before conversion, and returns it without
Clean, case/existence/reparse checks or OK_SYMLINK. Exactly one leading `/` is
removed before an ASCII drive letter/colon or literal `\\?\`, as in modernc.
The OS open follows reparse points; DB/WAL/shared-memory names remain based
on the lexical full name. Unix's resolver and ordering remain unchanged.

The former Windows walker, FindClose owner and unexecuted native reader were
removed after preserving56 exact source files in
`sqlite/windows-resolver-before-lexical/raw-source-manifest.json` under the
external evidence root. Earlier failures below remain evidence. Find/Readlink
implementation-only tests are retired with those removed acquisitions; they
do not count as passes. Saved-target/decoy, actual links, native-reply bounds
and multiprocess WAL obligations remain. The source-bound executions below
verify the implemented lexical boundary; they are not overall qualification.

The malformed-byte differential demonstrated a compatibility defect: native
modernc CP_UTF8/flags0 selected different saved files from Go's WTF-8 conversion
for `ED A0 80`, `E2 82`, and `F4 90 80 80`. The first lexical generation failed
those cases. The corrected boundary uses fixed-buffer native CP_UTF8/flags0
conversion in both directions; all six independently seeded modernc vectors
now pass, including valid Unicode, FF and C0 AF controls.

The separate topology
`_pragma=data_store_directory(...)` can set modernc process-global state before
later relative dashboard/ULS modernc opens. The generated topology engine has
no proved equivalent. Separate subprocesses demonstrated the mismatch:
the prior driver returned the saved `redirected` sentinel on the later relative
open, while the candidate returned `base`; absolute controls were unchanged.
This is a confirmed incompatibility against v9 P05/v15-09's selected DSN
preservation contract. Rejecting, ignoring or narrowing the pragma is not an
unconstrained policy choice. A shared-driver preservation bridge has not been
approved; complete DSN equivalence remains false.

The accepted lexical correction uses these current checkers:

| Contract | Boundary | Stimulus | Observable | Evidence | False-green risk | Checker | Owner |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Saved file preserved | GUID/DRIVE, lexical parent, final dot/space, literal target | Independently seeded modernc DB and decoy | Same prior-selected contents and externally visible commit | Native integration | Test construction removes adversarial suffix | `GUIDPathCompatibility`, `DrivePathCompatibility` | Fork qualification |
| Native dot semantics preserved | Exact dot/parent inside reparse target | Read-back actual native record; modernc before candidate | Same file or same refusal; no new file on refusal | Native differential | Unit parser substituted for OS result | `NativeTargetDotCompatibility` | Fork qualification |
| Fullpath stays lexical and bounded | Missing parent, leading slash, native error/growth/CWD/Unicode | Literal namespace vectors, fixed reply and real long CWD | No filesystem probe or OS-sized allocation; admitted native result unchanged | Unit/native/source | Input-only bound or extra Clean/case walk | `WindowsFullPathSpelling`, `WindowsFullPathAdmission`, `WindowsLongCurrentDirectoryRefusal` | Fork VFS |
| Native UTF-8 behavior preserved | Malformed percent-decoded URI bytes | Prior driver creates and identifies actual saved target | Candidate reads/updates that same database or matches refusal | Native differential | Go WTF-8 used as expected output | `MalformedURIPathCompatibility` | Fork qualification |
| Directory aliases preserve WAL | Direct target and GUID alias | Three real modernc/native/fallback processes | Exact commits, conflicting writer exclusion, pinned-reader/checkpoint recovery | Multiprocess | Main-file equality hides sidecar mismatch | `WALGUIDJunctionAlias`, existing portable WAL cases | Fork qualification |
| Whole-file alias sidecars preserved | Real file symbolic link | Same alias across all three processes | Alias sidecars present; no newly introduced target sidecars | Native/process | Junction treated as file-link evidence | `WholeFileSymlinkWALCompatibility`, required privilege gate | Fork qualification |
| Configured global pragma effect characterized | Later relative modernc open after topology pragma | Each driver in isolated child; same two saved sentinels | Prior and candidate target/result recorded; inequality fails | Process differential | Direct reopen only or leaking state between cases | `DataDirectoryCompatibility` (separate OPEN finding) | Fork qualification |
| Unix resolver unchanged | Real Linux links, accumulation, CWD | Existing bounded resolver/native WAL suite | Original admitted result and refusal behavior | Native/race | Windows results treated as cross-platform proof | Existing Linux resolver/WAL gates | Native validation owner |

On 2026-10-03 02:26:29–02:27:01 UTC, retained generation
`sqlite/windows-native-utf8-20261003` passed saved-target GUID/DRIVE cases,
all eight exact native dot/parent refusal controls, real multiprocess alias
WAL, and all malformed URI filename cases. VFS conversion/admission/spelling,
long-CWD refusal and relative temporary-path checks passed normal, race,
explicit checkptr=2 and vet. Peer directory checks passed normal/race/vet.
Both the whole-repository and fork source byte/file-set manifests matched
before/after. Fork manifest SHA256 is
`0349ca5cc0d9740154abf3c30ec9ddc7ec8131065ac18f2324a43765f858203c`;
qualification binary SHA256 is
`4da650f2e686c69f1fcfcfb986b21cdef00d569a03cf28f7526307335517e96a`.
The runner retains every binary, command, log hash and process setting.
Its overall result remains **FAIL**: data-directory compatibility still fails,
and the required real whole-file symbolic-link fixture fails with
`ERROR_PRIVILEGE_NOT_HELD`. Junction evidence does not replace that fixture.
The earlier `windows-lexical-20261003` generation remains retained, including
the three malformed-name failures and its helper-dependent peer compile
failure; no peer check ran in that earlier generation.

### Windows GUID reparse differential gate

The unchanged modernc v1.36.1 driver is the controlling saved-file oracle.
`HEAD:peer/topology.go` passed the configured path to that driver. Its generated
`lib/sqlite_windows.go:103821` calls GetFullPathNameW, then `:103418` opens the
result with CreateFileW. It does not select Go's `winreadlinkvolume` behavior.
Thus matching Go's private intermediate DOS/GUID spelling is not itself a
topology compatibility requirement. Actual accepted files, committed contents,
sidecar identity and bounded refusal remain requirements.

This further detailed test-strategy review is design-aware, not independent.
The Find/Readlink rows below record the superseded resolver design; those
implementation-only checkers are retired with the removed Windows acquisitions.
Saved-target, actual native-target, constructor and WAL rows remain binding.
The lead authorized baseline fixtures before any native Readlink production
change. The baseline has now **demonstrated compatibility failures** described
below; no native Readlink production change preceded that evidence.

| Contract or invariant | Failure or boundary case | Stimulus or fault | Observable result | Evidence level | False-green risk | Exact checker | Owner |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Inherited saved-file target preserved | GUID junction, lexical parent, final dot/space | Distinct modernc-seeded sentinel databases; raw suffix input | Candidate reads the modernc-selected value; independent observer sees its commit; decoy unchanged | Native integration | Join/Clean removes the adversary or two paths contain identical data | `TestV15SQLiteGUIDPathCompatibility` | Fork qualification |
| Extended target keeps its actual identity | Native-created directory ending dot/space | Real GUID reparse entry pointing to the literal directory | Same modernc open outcome, saved contents and integrity | Native integration | Only ordinary target names tested | Same compatibility test's extended-target case | Fork qualification |
| DB, WAL and shared-memory aliases agree | Ordinary direct path versus GUID directory alias | Separate modernc/native/fallback processes, pinned reader and checkpoint | Mutual writer exclusion, old/new visibility, checkpoint recovery, integrity | Multiprocess | Main-file equality alone hides different sidecars | `TestV15SQLiteWALGUIDJunctionAlias` | Fork qualification |
| No incidental Go setting contract introduced | `winreadlinkvolume=1` and `0` | Same actual native GUID target in both modes | Same inherited modernc file/data contract | Integration/source | Default-only evidence | Both new qualification tests | Fork qualification |
| Existing failed native ownership remains reachable | FindClose fails on actual GUID reparse classification | Real acquired handle plus injected failure; clear fault and retry | One retained wrapper owner, no false successful release | Lifecycle/race | Error injected after a real close | `TestV15SQLiteGUIDFindRetirement` | Fork VFS |
| Native reader bounds before conversion | Truncated reply, odd/out-of-range offsets, oversized target/composition | Fixed reply/parser controls and exact boundary vectors | Error before slicing/copy; original data intact | Unit/source/fuzz | Fixed buffer mistaken for validated offsets or final filename bound | `TestV15SQLiteReadlinkRecordBounds`, `FuzzV15SQLiteReadlinkRecord`, actual oversized-target control; execution pending | Fork VFS |
| Native reader owns CloseHandle failure | Read/normalize fails after acquired handle; CloseHandle fails | Native-close fault seam, real handles and actual junction | Existing fixed wrapper table retains owner until verified release | Lifecycle/race | Existing FindClose test mistaken for new reader coverage | `TestV15SQLiteNativeReadlinkRetirement`, `TestV15SQLiteNativeReadlinkJunction`; execution pending | Fork VFS |
| Native target dot components retain inherited semantics | Actual NT target contains exact dot or parent components | Verified GUID/DRIVE reparse records; modernc open plus independent saved sentinels | Same accepted file or prior refusal; refusal cannot create another file or change saved bytes | Native integration | Test's path construction cleans away target components | `TestV15SQLiteNativeTargetDotCompatibility`; pre-candidate file hashes and execution pending | Fork qualification |
| Constructor directory expansion is admitted before filesystem work | Relative/CWD or native size exceeds existing filename budget | Fixed native reply, UTF-16/WTF-8 boundaries and actual relative directory | Bounded refusal before mkdir; reservation released; normal retry and saved data remain valid | Unit/integration/source | Bounding only raw input while Stat acquires an unbounded absolute name | `TestV15TopologyDirectoryNativeAdmission`, `TestV15TopologyDirectoryRefusalPreservesState`; current normal/race passed below | Peer adapter |
| Constructor keeps inherited directory and namespace semantics | Raw DSN query/URI text, dot skip, literal absolute path, legacy prefix | Observe native input from raw DSN; compare pinned native spelling and explicit prefix vectors | Same selected directory; Linux unchanged; existing1024-byte admission includes prefix | Unit/integration/source | Preparing the parsed SQLite filename silently changes directory selection | `TestV15TopologyDirectorySpelling`, raw-DSN and dot-skip checks in refusal test; current normal/race passed below | Peer adapter |

The qualification-only fixture creates the junction directly with
FSCTL_SET_REPARSE_POINT, using read-only queries for the existing volume GUID.
This prevents a shell/provider from silently changing the requested target.
It rereads the fixed native reparse record, validates its descriptor bounds,
and compares the actual substitute name before using the fixture; the separate
native handle-close result must also succeed.
All paths are verified absolute descendants of an owned TempDir; cleanup removes
the junction entry before its target. It does not mount a volume, change host
settings, or require symbolic-link privilege. Its native descriptor follows
Microsoft's [REPARSE_DATA_BUFFER layout](https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntifs/ns-ntifs-_reparse_data_buffer).

Windows baseline results on 2026-10-03 are retained under
`D:\codex-gocluster-v15-20261002\sqlite\guid-baseline-20261003`:

| Actual configured case | `winreadlinkvolume=1` | `winreadlinkvolume=0` | Modernc control |
| --- | --- | --- | --- |
| Ordinary GUID directory junction | Pass | Pass | Same committed contents |
| Raw `junction\..\kept.db` | Wrong saved database selected | Wrong saved database selected | Lexically normalized original database selected |
| Final filename dot or space | Refuses accepted saved file | Pass | Opens original database |
| Junction into an extended literal directory ending dot/space | Pass | Refuses accepted saved file | Opens literal target |

The lexical-parent failure read a distinct `wrong-resolved-parent` sentinel
where modernc read `committed-original`; it is an observed wrong-file result,
not a performance or telemetry gap. All ordinary GUID multiprocess WAL
lock/pinned-reader/checkpoint controls passed in both modes (0.22 seconds).
The actual GUID FindClose-retirement baseline passed separately. These passes
do not discharge the failing path-selection cases or the planned native-reader
CloseHandle ownership checks.

The first extended-target fixture could not seed through a plain `\\?\...`
modernc DSN; that driver's `sqlite.go:830` splits on `?`. This was a fixture
failure before candidate comparison. The corrected fixture seeds through the
accepted ordinary junction alias and confirms the physical literal target
using native SameFile identity before comparison. Both logs and binaries are
retained; the corrected comparison takes0.30 seconds. Only that fixture source
changed between the two captured source manifests.

Corrected binary SHA256:
`5cc3a5ffb68f0cd617260a617794ca5bf1b6805d5a54e8289f2cac16a9717018`.
Corrected source-manifest SHA256:
`7babc0ca0d721d844900f53342e9182db9d5a788362bb3ae5a9eb81cb617b535`.
Corrected log SHA256:
`68e09a0f5a6b0063d7a7329eff14824cbbe194908f6213742c5f9b163dc5c3ff`.

After reviewing that baseline, the lead authorized one minimal production
step: Windows FullPathname now calls the existing bounded native
GetFullPathNameW helper before link traversal. It applies no additional Clean;
Linux and other platforms retain their original ordering. The strict rerun
proved that lexical-parent selection and final-dot/space cases now pass for
both GUID and DRIVE targets, in both volume modes. The native Readlink remains
unchanged and still fails literal extended-target cases.

An additional `TestV15SQLiteDrivePathCompatibility` baseline is prepared for
the same real target cases using an actual NT DRIVE-form substitute name.
This distinguishes Go's drive-prefix stripping from GUID-specific behavior;
the fixture verifies native readback exactly as for GUID targets. Its executed
baseline found that DRIVE-form literal targets are refused in **both** volume
modes, while the unchanged modernc driver opens the same saved file. GUID-form
literal targets remain refused in mode0 only. Consequently Go's display-path
normalization cannot simply be copied as the native target semantics.

The lexical-input revision retained ordinary GUID multiprocess WAL progress
and lock/checkpoint controls (0.26 seconds), and GUID FindClose retirement
passed. Source manifests matched before and after those checks. Evidence is
`sqlite/lexical-input-20261003` under the external evidence root above:
binary SHA256 `6fc5c7c6ec6f92f77336c84327217451dd84ad0e46bdaefc01bf4d026db7f5fa`,
source-manifest SHA256 `9a509172c30c70ea52ee603b7225c50a33f25a0305c221f853d75753225af131`,
and differential-log SHA256 `0c4a8e86b9511b84caf5f3c33b4533fde1397385f745b629c3d47841869abff5`.
No assertions were relaxed to produce these results; the three remaining
literal-target subcases fail the strict differential checker.

An intermediate native reader was prepared but never built or executed. The
subsequent caller-contract review established that Windows should not resolve
reparse targets here at all. That reader and Windows walk are archived and
removed; their planned parser/release tests are retired with the acquisition.
The added actual native target baseline emits modernc's selected
sentinel or open refusal, all independent saved sentinels and file hashes before
any candidate call. A prior refusal must leave those file bytes and names
unchanged. Until execution establishes those cases, broader extended-target
equivalence remains unproved. Linux retains its existing exact path parity.

There is a separate **OPEN whole-file symbolic-link compatibility question**:
modernc's `sqlite_windows.go:102498` derives shared-memory names from its lexical
database path, and the new Windows fullpath source now preserves that name.
Directory-junction coverage cannot prove whole-file-link sidecar equivalence.
No new refusal, rewriting policy or claim that all aliases are equivalent has
been selected. Actual symbolic-link capability is also still absent on this
Windows account.

### Native release and poisoned-connection correction

The following accepted post-approval checker matrix refines V15-09/10/11/12;
it adds no budget, OS baseline, worker, cache or global-pragma bridge.

| Contract | Falsifier/stimulus | Checker boundary | Required observation | Control | Ownership evidence | False green avoided | Implementation/checker |
| --- | --- | --- | --- | --- | --- | --- | --- |
| A: failed release forbids reuse | Actual metadata File.Close failure; next SQL/adapter call | Fork Conn and production db.run | IOERR_CLOSE, no later operation body, reservation retained | Ordinary new store after verified normal close | Real consumed native handle and fixed retained owner | Error-only test without a later operation | `CleanupFailurePreventsReuse`, `PoisonedEntryBoundary` |
| B: successful engine return cannot hide poison | Poisoned ROW/DONE/OK and deferred arena overflow cleanup | Step result/errorFor/Wrapper.Free | No success or allocator entry after poison | Ordinary step/bind/reset tests | Engine extent remains charged until direct retirement | Testing only initially failed entry | `PoisonedStepSuccessResult`, `PoisonedArenaUnwind` |
| C: two owners fit before acquisition |254/255/256 ordinary occupied slots; emergency occupied | CanAcquireHandles/AddHandle/RetainFailedHandle | Refuse before acquisition or retain main plus metadata; emergency never successful | Free two slots permits operation | Same257-slot backing | Counting an emergency slot as normal successful registration | `MetadataHandleAdmission`, `TerminalEmergencyOwner`, `ModeofRetainsBothOwners` |
| D: cleanup error survives wrappers | Access permission/not-found, journal existence probe, modeof sysError | Real callback routing | Cleanup error and owner outrank ordinary masked error | Ordinary not-found/permission behavior | Nested main and metadata owner retained separately | Only calling error classifier directly | `MetadataAccessAndJournalPrecedence`, `MetadataCleanupErrorPrecedence`, `ModeofRetainsBothOwners` |
| E: poison allows retirement only | Callback read/write/map/lock/control and downgrade | VFS callback entry | No new work; only genuine release/unmap/unlock | LOCK_NONE remains available | No lookup/acquisition in forbidden calls | Treating downgrade with lock acquisition as release | `PoisonedCallbacksDoNotStartWork` |
| F: unwinding cannot publish or free through SQL | Poisoned SHM barrier/close and arena free | Windows fallback and wrapper | No shadow publication or SQLite allocator call | Healthy release still publishes | Fixed private engine backing remains owned | Setting Retiring only after an earlier poisoned unwind | `PoisonedFallbackDoesNotPublish`, `PoisonedArenaUnwind` |
| G: failed unlock is observable | Release, temporary/shared/DMS/probe/downgrade error | Native unlock to callback | Sticky failure; partial state never reported reusable | Positive final File.Close releases remaining locks | Owning file stays charged until verified close | Returning error without poison or losing partially held ranges | `WindowsUnlockFailureStaysOwned` |
| H: bounded native paths preserve ordinary behavior | Native growth/invalid reply, legacy namespace, temp collision | Private Windows filesystem boundary | Fixed refusal before growth; normal flags/names/data preserved | Saved target and literal path fixtures | Fixed buffers and owner before later failure | Second stdlib pathname retry after bounded preparation | `WindowsFilesystemPaths`, `NativeOperationAdmission`, `TemporaryOwnership`, `NativeBudgetCategory` |
| I: metadata follows prior branch order | Attributes success, sharing violation, reparse/console and release failure | Owner-local Windows metadata | Fast path opens no handle; failed close terminal and retained | Pinned metadata Mode/IsDir/Size/ModTime parity | Real acquired File/Find owners | Find-only fast-path guard or fake handle as native proof | `WindowsMetadataParity`, `WindowsMetadataBranchOrder`, `MetadataReleaseOwnership` |
| J: constructor owns failure before Conn exists | Real Find acquisition/failure; private mkdir through junction | Production constructor/retireLocked | Global charge/replacement gate survives Conn=nil; saved data intact | Fresh tree, dot/trailing spelling and both winsymlink modes | One inline pending owner, no engine overlap | Fake123 structural test used as sole native evidence | `DirectoryNativeCleanupOwnership`, `DirectoryMkdirParity`, `DirectoryJunctionParity`, directory admission tests |

Current execution is retained under
`D:\codex-gocluster-v15-20261002\sqlite\windows-native-owners-20261003-b`.
The preceding `windows-native-owners-20261003` lane stopped at a test-hook
CreateFile signature compile error (`Handle` versus the pinned API's `int32`);
that failed log remains retained. The corrected lane ran03:09:48–03:10:43 UTC:
fork engine/wrapper and VFS normal/race, VFS explicit checkptr=2/vet, and peer
constructor/retirement normal/race/vet all passed. Actual saved-target GUID/DRIVE,
native-dot/malformed-byte and multiprocess directory-alias WAL checks passed.
The **overall runner is FAIL**, because `DataDirectoryCompatibility` still
demonstrates the inherited global-pragma mismatch, and the required actual
file-symlink fixture lacks privilege. Both failures remain acceptance blockers.
The manifests confirm stable source bytes/file sets during execution; later
documentation and support-comment edits are separate from that binary identity.
Fork manifest SHA256 is
`4de70a2a65217cd4d3adf5b10db8bedebc33d7ea53478b2901561631b44b643a`;
retained peer binary SHA256 is
`7e62c6f3bd591ace2ed111d25e4477b38ec8481e1d3b6b95d97a2e74ddeadacb`.
Commands, logs and every retained binary hash are in that directory's JSON
records. None of these targeted passes is final all-profile acceptance.

A consumed Windows File.Close failure is terminal: retrying that Go wrapper
cannot certify native release. CleanupFailed therefore remains observable,
the full16 MiB reservation remains charged, new SQL/storage replacement is
gated, and process restart is required to clear that owner. Live topology
authority and local service remain independent of failed projection persistence.
Mapping/view releases and deliberately injected pre-call failures retain their
separate retriable retirement behavior; the250 cleanup/retry fixture does not
claim that a consumed close becomes safely retriable.

**Allocation inventory and qualification are separate.** The source inventory
below now includes the owner-local Windows filesystem corrections. Its arithmetic
is not an overall acceptance result: final relevant-source Linux ownership/WAL
and sustained qualification remain pending, Windows actual file-symlink evidence
is missing, and the confirmed global-pragma compatibility failure is unresolved.
The former 128 KiB transient
coefficient did not cover the pinned Go `filepath.EvalSymlinks` algorithm:
successive `link + pendingSuffix` concatenations can accumulate long unresolved
suffixes even when each OS lookup and the original filename are short. The
existing VFS output-length check happened after that allocation. An attributed
bounded Linux walker checks each composed workspace before allocation, retaining
ordinary resolution and refusing long intermediate paths rather than truncating
them. Its final Linux validation remains pending. This Windows account cannot
create actual symlinks (`ERROR_PRIVILEGE_NOT_HELD`); the required Windows
compatibility gate records failure rather than substituting junction evidence.
Fixed-buffer absolute-path admission now addresses CWD expansion without
excluding its Go backing. Windows fullpath no longer calls
`os.normaliseLinkPath`/`winreadlinkvolume=0` or constructs resolved target strings,
and its targeted Windows validation passed. The formerly open pre-engine
`os.MkdirAll` and VFS Stat/OpenFile/Remove routes are now owner-local native
operations; bounding only their input had not bounded the stdlib's later
pathname retries or proved temporary-handle release.
The private peer `topologyDirectoryPath` admits the OS-facing spelling:
one fixed1,025-unit native output, no size-driven retry, exact WTF-8 length
counted before conversion, and a required absolute result within1,024 bytes.
Already absolute spellings retain their literal namespace. The caller still
selects `filepath.Dir(rawDSN)` and skips empty/dot directories; it does not use
the parsed SQLite filename instead. Non-Windows behavior remains unchanged.
Bounded legacy prefix preparation preserves ordinary/UNC/extended/device
selection and keeps prefix overhead within the same filename budget. The
private attributed MkdirAll branch uses that spelling directly in native
CreateDirectory/metadata calls, with no subsequent stdlib pathname entry.
Recursion and error names borrow the original logical raw-DSN directory.
Metadata follows pinned Go's attributes-fast-path, sharing-violation FindFirst,
then reparse/console CreateFile branch order. Ordinary metadata does not acquire
an exclusive handle. A pending constructor owner is installed immediately after
acquisition, before any later error; retirement checks it even when Conn is nil.

The constructor's new native preparation fits a16KiB opening-phase workspace
inside the existing host reservation. Each1,025-unit input/output allocation
rounds conservatively to2,304 bytes; each returned WTF-8 conversion is at most
3,072 bytes. A first absolute result retained while legacy preparation runs,
the second input/output, its returned conversion, and the final1,024-byte
prefixed result (rounded1,152) sum to11,904 bytes. Counting these mutually
overlapping objects includes scratch phases before the prefixed result exists.
The cloned raw DSN and parser program remain in their separately counted
opening terms. The process-global opening reservation is acquired before
these paths; no second engine or previous failed-retirement owner overlaps.
Exhaustion returns the existing topology-budget error, and constructor cleanup
must release the reservation without creating a directory or altering saved
data. `TestV15TopologyDirectoryNativeAdmission`,
`TestV15TopologyDirectorySpelling`, and
`TestV15TopologyDirectoryRefusalPreservesState` cover fixed native replies,
WTF-8 boundaries, saved-target spelling, raw-DSN interpretation, no-mutation
refusal, successful recovery and committed-data integrity.
VFS metadata/open/remove and temporary creation also use private native
boundaries. Their legacy preparation follows Go's actual OS-version predicate,
uses fixed1,037-unit buffers (including the owned `-shm` suffix and prefix), and
never sends the result through fixLongPath again. Direct syscall.Open is limited
to the VFS's complete read/write/create/exclusive flag set: its hidden
O_TRUNC/O_DIRECTORY partial-cleanup paths cannot be reached. Temp names retain
the historical uint32 decimal random spelling, `.db` suffix,0600 mode and10,000
collision limit, using public math/rand/v2's runtime-backed generator.
The Go originals and hashes are retained under fork provenance.

The shared `internal/poll` operation pool is separately dispositioned from the
active lease. In the pinned Go source, `operation` contains only the native
OVERLAPPED scalar fields, runtime poll-context token and mode. Synchronous I/O
completes before return; unpinning drops temporary Go payload references. A
returned global pool item contains no topology connection, file, callback or
buffer owner. The active lease belongs to the host term; the shared library's
returned no-payload cache belongs to separately reported runtime/library memory.
This does not exclude any retained Go path/error backing. The new owner-local
metadata operations check FindClose and File.Close explicitly. A failed release
is captured in the existing fixed wrapper table, or the constructor's one
pending field; no unbounded retired-owner list is introduced. Removed Windows
normalization acquires no Find/Readlink handles.

This argument concerns owned protocol data and backing. Unreachable Go garbage
awaiting GC, runtime arenas/stacks, and OS bookkeeping are reported separately
under the selected accounting boundary. Dropping the last Go reference is an
ownership release, not a claim that the OS has already reclaimed that heap.
Native allocations require confirmed native release. No failed native owner is
made invisible by zeroing only a counter or discarding its wrapper.

| Owner | Bound and release argument |
| --- | --- |
| Engine backing | At most128 ×65,536 =8,388,608 bytes. Windows native and Unix reserve one fixed extent; Windows fallback allocates one full-capacity Go backing array and only reslices it. No geometric append or second reachable generation. Windows placeholder pieces use fixed129 slots; mapped regions use fixed128 descriptors. Unix uses direct MmapPtr/MunmapPtr behind existing fault seams, avoiding x/sys's process-global Mmap slice registry and its retained Go-map backing. Invalid-length EINVAL, extent/flags and failed-release ownership remain unchanged; current native Linux execution is pending. Growth refuses before the ceiling. Native free/unmap failure retains both extent metadata and charge. |
| Windows fallback WAL | One global 64-slot table across all attached files. Each slot owns at most one 65,536-byte native view and one 32,768-byte shadow: 6,291,456 bytes total. Its private 32 KiB copy is inside the engine's 8 MiB. Mapping offsets round down to Windows' 64 KiB granularity. Failed view or mapping-handle release retains its slot and shadow; no new slot replaces it. |
| Fixed host owner/tables | 64 KiB allowance covers connection, wrapper, Memory, arena, 32 statement slots, the generated 413-entry function table and initialization overlap, method closures, accounting scalars, adapter and fixed bookkeeping. The transaction owns one 48-byte Stmt plus its fixed query/connection headers; SQL/values are copied into the already bounded engine, and its slot disappears before commit or during verified engine retirement. Observed Windows sizes: Conn 608 bytes in production/616 with qualification hook; Wrapper 4,168; Memory 11,000; Arena 168; Stmt 48; generated Module 104. |
| Retained file/callback handles | 256 ordinary slots plus one terminal overflow owner, captured and poisoned before the OOM sentinel. Budget 4,608 bytes/slot: 1,920 VFS/checksum/shared-memory metadata, 2,176 main/shared names including rounding, and 512 for two os.File owners. Total 1,184,256 bytes. Current Windows sizes/classes: vfsFile56 + cksmFile24 + vfsShm1,576 rounded1,792 =1,872; two os.File/os.file pairs are2*(8+192)=400; main name at most1,024 plus shared name at most1,028 rounded1,152 =2,176. The shmOpen os.File borrows its existing path backing. A separately retained metadata owner uses another existing slot: owner24 rounded32, one200-byte File pair, at most1,024-byte name and64-byte error allowance are at most1,320 bytes, below4,608. Potential main-file plus metadata ownership is admitted for two available slots before acquisition; metadata terminal insertion may use the emergency slot without exposing successful registration. Runtime pinning machinery is separate; no new owner table exists. |
| DSN and parse backing | 304 KiB allowance covers the cloned path, URI reconstruction, decoded token text, fixed pragma table and overlapping parse results. The explicit 280,640-byte derivation below leaves rounding margin without relying on the remaining aggregate slack. Lowercase comparison allocates no backing. Total input is 64 KiB, not 64 KiB per key; unrelated keys never form an unbounded map. |
| Generated data/static metadata | 128 KiB allowance includes the 103,092-byte generated data segment and fixed bridge constants/OS entry-point metadata. The data segment's copy inside engine memory is already in 8 MiB. |
| Transient host work/errors | 384 KiB allowance. Explicit components:64 KiB current Exec SQL;66 KiB checksum pragma name/value/lowercase copies;32 KiB bounded resolver/absolute/temp/native-operation conversions and overlapping joins;80 KiB formerly assigned reparse headroom (unused on Windows);32 KiB current and previous SysError/diagnostic/wrapping/active-I/O lease. These sum to274 KiB. Including SQL and callback/path work does not rely on their non-overlap. Engine diagnostics are at most4,096 bytes; ordinary OS errors borrow admitted paths of at most1,028 bytes. Temporary metadata and kernel reply buffers are included in the32 KiB path term below. |

Current Windows lexical acquisition uses fixed CP_UTF8/flags0 input/output
conversion matching the previous driver's native APIs. Rounded backing is
1,152 bytes raw input, 2,304 bytes UTF-16 input, 2,304 bytes UTF-16 output,
3,200 bytes native UTF-8 output, and 1,152 bytes copied admitted result:
10,112 bytes before ordinary fixed/error metadata, within the32KiB term.
No native size return drives allocation or retry. It acquires no file handle.
For a conservative Windows overlap, also count three1,037-unit legacy CWD/input/
output arrays (3*2,304), one3,200-byte WTF-8 result, one1,152-byte prefix result,
the downstream UTF-16 native argument2,304, both caller and syscall FindFirst
reply structs2*640, two FileInfo results2*160, two ordinary metadata results2*64,
one tag reply16 and two temporary owner/File pairs2*(32+200).
Together with the10,112-byte lexical path conversion this is25,888 bytes,
below32 KiB. This deliberately includes alternatives which cannot all execute
at once. Pinned syscall.UTF16ToString returns a string over its one pre-sized
byte array, so it does not add an uncounted conversion copy. Error objects,
diagnostic strings and the previous SysError are in the separate32 KiB term.
Open/remove call one native path operation; metadata closes its first owner
before following a name surrogate. No recursive Windows VFS path walk remains.
The one process-static WideCharToMultiByte lazy entry belongs to the existing
128KiB static allowance: LazyDLL40, LazyProc40, DLL24, Proc32 bytes (rounded
48+48+24+32), fixed names12+19 bytes and one8-byte global pointer. Kernel32's
special-case loader avoids system-directory acquisition. First use adds at
most32-byte UTF-16 module name and24-byte procedure name backing; an API lookup
failure's DLLError48 and message allocation are bounded by fixed names plus
pinned syscall.Errno's300-unit FormatMessage buffer (640-byte rounded buffer,
at most1,024-byte conversion, at most1,024-byte composed message). These fit
the existing32KiB error/transient term; previous SysError overlap remains
counted. Successful resolution retains one module reference for process
lifetime rather than attributing it to a retired connection. The former80KiB
reparse allowance in the conservative table is now unused Windows headroom,
not an owned reader allocation or permission to expand another owner. The
archived reader's36,736-byte source inventory is superseded. The current
owner-local paths passed the targeted Windows lane below; this is not full
cross-platform or compatibility acceptance.

Source inventory arithmetic: `257*4608 + (64+304+128+384)*1024 = 2,085,376` bytes,
below2 MiB by11,776 bytes. This is an ownership derivation, not a replacement
for required final-source execution evidence. The process reserves the full16 MiB before first
acquisition, so concurrent atomic observations cannot turn an acquisition gap
into an uncharged generation. Snapshot/context owners remain in their existing
partitions and are not silently counted twice or excluded.

The DSN coefficient derivation is tied to `peer/topology_sqlite_dsn.go`:
`topologyDSNBytes` bounds the whole source by I = 65,536; `filename.Grow(len(path))`
allocates at most I and never grows again because reconstruction only removes
private query pairs. `url.QueryUnescape` either borrows the input or reserves its
exact decoded length before writing (Go 1.26.4 `net/url/url.go`, `unescape`).
Decoded retained values and the current decoded key/value are disjoint pieces
of that source, with total logical length at most I. At most 132 decoded owners
overlap: 128 pragmas, first txlock/time-format, and current key/value. The pinned
Go noscan size classes obey the deliberately loose allocation bound `2*n+16`;
large allocations round to pages within the same bound. Thus their total backing
is at most `2*I + 132*16`. Allowing 16 KiB for all fixed table/result copies and
sort bookkeeping gives `I + I + 2*I + 132*16 + 16*1024 = 280,640` bytes, below
304 KiB. Borrowed trim/lowercase-comparison keys add no string backing; invalid
UTF-8 is included, with a zero-allocation differential fixture. The prior
`strings.ToLower` implementation could create geometric expansion overlap and
did not justify this coefficient.

Startup directory normalization is a separate phase: `filepath.Dir/Clean` runs
before engine creation or acquisition of any file-handle slot. A conservative
512 KiB allowance covers its input-bounded buffers, Windows path-prefix growth,
copied strings and private directory recursion. At most513 one-character
components fit1,024 bytes; allowing64 bytes for each retained logical-path
error is32,832 bytes, plus the16 KiB native workspace described above. Recursive
frames borrow the original logical string, and each metadata acquisition is
released before recursion continues. A terminal failed owner stops recursion
and remains in the one inline directoryOwner field. Adding320 KiB DSN and192 KiB fixed/static allowances is
1 MiB, below the host ceiling. The cleaned directory guard prevents recursive
OS work on an oversized result. This phase cannot coincide with the table's
257-file maximum; a failed prior retirement blocks the constructor before this
work. The steady-state/transient file-operation proof remains a separate bound.
The sequencing is enforced in `openTopologyDatabase`: its compare-and-swap
claims the opening sentinel before path cloning/parsing; a concurrently refused
constructor does neither. A failed-retirement owner must successfully close all
files/mappings, clear connection/program/path references and release that same
reservation before a replacement can claim it. `db.run` creates the engine only
after directory work finishes. The global failed-owner observation and refused
replacement tests cover the non-overlap boundary.

The callback conversion argument cannot rely on64KiB input SQL: an admitted
short DSN pragma can append SQL which generates a much larger URI value, and a
pragma virtual table generates an internal PRAGMA. Bounded child falsifiers
demonstrated600,000-byte unknown URI key/value/psow copies allocating about
3,665,000 Go bytes, an unknown VFS lookup610,688 bytes, and a generated
`pragma_table_info` value610,248 bytes. Retained allocation profiles attribute
these large copies to Memory.ReadString. These are allocated-byte observations,
not a claim that every allocation remained owned simultaneously; they invalidate
the prior input-size derivation independently of that distinction.

The private production paths now borrow NUL-delimited spans inside the already
owned engine extent. URI scanning skips unknown keys and values without copies;
modeof enforces its existing1,024-byte filename budget before copying. The fixed
OS file's psow parsing now uses the existing SQLite engine getter, as established
by the separate normative file-control differential below. The checksum callback identifies its two
names before reading any value: at most84 bytes are copied/lowercased, unknown
names return NOTFOUND, checksum_verification observes the same bounded boolean
representation, and page_size observes empty versus nonempty. Public copied
URIParameter/URIParameters and generic registered VFS callback behavior remain
unchanged. Borrowed slices never cross an engine entry or escape their callback.
The VFS registry lookup uses a temporary read-only string view; Find only
compares/looks up that argument and does not retain it or enter SQLite.
Production has no custom VFS registrations, so subsequent vfsGet names are
the fixed engine `os` registration; unknown names fail before a VFS opens.

A further1,100,000-byte child exposed the private copied-name limit's panic and
partial-open cleanup failure. Prior modernc children accepted the URI key/value/
psow cases and returned ordinary `no such vfs` for the unknown name. Private
borrowing now scans within the existing fixed engine backing instead of that
one-million-byte copied-string limit; public unused APIs retain their limit.
All ten600,000/1,100,000-byte candidate cases preserve saved data and ordinary
success/refusal, with about20 KiB total Go allocations for URI work and4 KiB
for pragma/unknown-VFS work. Actual psow and checksum-state queries, malformed/
Unicode/duplicate/empty controls and two caught mutations (copying borrowed
parameters; losing numeric-prefix boolean handling) complement the real engine
cases. The callback component now needs at most the bounded short name/value
copies and the1,024-byte modeof result; the conservative66 KiB callback allowance
in the table remains reserved, rather than being reassigned to another owner.

Evidence is retained under `sqlite/callback-backing`: before/after binaries,
profiles, original panic/partial-open logs, independent prior-modernc child
results, mutation binaries/logs and final source/command manifests. The first
final-run wrapper stopped after a successful silent build because it had not
created an empty log; its incomplete records are retained separately. That
harness failure is not a failed or passed runtime gate. The corrected targeted
normal/race/checkptr/vet results are recorded by `final-commands.json`.

Matching upstream ncruces ParseBool did not prove inherited modernc psow parity.
The isolated23-input native file-control oracle demonstrated six differences:
01,256,512,0x101,2147483648 and9223372036854775808. PSOW affects
DeviceCharacteristics and SQLite's write-safety assumptions. The prior oracle
uses the exact pinned modernc generated library and driver's open flags; all
pointer arguments belong to its libc allocator. Candidate observations use
Conn.FileControl on real saved databases. Both children must report every exact
input identity, and independent reopen verifies the saved sentinel for each case.

The fixed OS VFS now invokes its existing generated Xsqlite3_uri_boolean using
the existing engine-owned matched key pointer. This preserves SQLite's integer,
hexadecimal, overflow and uint8 behavior without another parser. Absent keys do
not call the setter; the current file flag supplies the invalid-value default;
the engine retains first-duplicate-key behavior. The getter only scans owned
engine memory and uses16 bytes of engine stack; it does not allocate, move the
extent or call a VFS callback. No Go span is read after that getter. Generic
custom VFS handling and public ParseBool/URI APIs retain their existing behavior.
The generated engine bytes and the8MiB allowance are unchanged.

| Contract | Falsifier | Boundary | Observation | Control | Ownership | False green avoided | Checker |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Inherited psow semantics | Numeric/hex/overflow differences plus empty/absent, malformed, text and duplicates | Isolated pinned-modernc and candidate actual file-control query | Exact23-row equality, same inputs and saved sentinel | Ordinary0/1/default/text plus first duplicate | Existing key pointer and bounded engine getter, no copied large value | Checking only upstream Go parser, empty result arrays, or file contents without the PSOW flag | `qualification.TestV15SQLitePSOWCompatibility`, before/after binaries and logs |

All23 Windows native comparisons passed after the repair. The affected complete
fork normal/race and focused checkptr/peer/qualification checks are retained
under `sqlite/callback-backing/psow-final` with source and command identities.
That lane ran03:59:23–04:00:33 UTC on October3; all11 selected commands exited0.
Its before/after relevant fork manifests match SHA256
`3550d230ebf1b2c7e1a3ca917adb47295f2431f218d93b4b2ba475e35d0b0fe5`.
The retained root race binary is
`3ecd977d01720b6021682d1cae84f6c0af1b0caf6f77fc47eae8c025c9c48ae2`;
the command/log record is
`2a900089fbf26a4f6b40b1a0dd1264de404c0d296d162e809c19bdc4c7f0b09f`.
Platform qualification remains separate: the same cross-platform differential
is selected for the next Linux source. The confirmed data_store_directory
mismatch and suspected temp-directory/allocator-global pragma boundary remain
OPEN and are not resolved by this private URI correction.

The generated engine is not described as allocation-free. Its `New` allocates
the fixed function table and initialization structures above. Source inspection
also found a `strconv.ParseInt` conversion in generated libc that can copy large
error inputs. All twelve references to its `_strtol` entry are confined to
`Xmain_mptest` (4), `_runScript` (7) and `Xmain_speedtest1` (1). `_runScript` is
called only by `Xmain_mptest` and itself. These test programs are absent from the
function table and unused by the handwritten production bridge. The AST guard
`TestV15SQLiteGeneratedHostReachability` checks those references and production
callers; no new SQL value limit or generated-engine modification was introduced.

Dynamic Go extension, authorizer, trace, log, collation and callback registration
is not entered by `OpenTopologyContext`. The topology adapter uses the fixed OS
VFS and engine functions. Native/Go borrowed slices remain within the serialized
operation; no worker survives manager joining. On retirement, wrapper handles
must close successfully before native backing is released. A poisoned engine
does not execute SQL cleanup. Successful adapter close clears retained DSN/path
references. Process-wide resource stats remain visible through a new disabled
manager, while Enabled/commit/watermark fields retain that manager's identity.

A successful close still leaves a small shell while its caller retains the
store. The pinned amd64 source layout, confirmed after the sustained run from
the current executed peer binary's DWARF
(`sqlite/windows-native-owners-20261003-b/shell-layout.json`), is:
`topologyDSN`4,136 bytes, `topologyDatabase`4,264 rounded to a4,864-byte
pointer-bearing allocation, `topologyStore` 16, the zero-element buffered gate's
Go 1.26 `hchan` 112, and five scalar ownership counters 40 rounded to 48.
The retained shell is therefore 5,040 allocated bytes. The process-static
opening sentinel and reservation pointer add4,264+8 =4,272 bytes. The new inline
directoryOwner increased the unrounded database layout by32 bytes, without
changing its heap size class. Combined heap/static ownership is9,312 bytes.
The aggregate proof assigns a conservative 16 KiB shell/static allowance inside
the existing global-other-metadata partition, not above the 480 MiB ceiling.
Successful close has cleared the connection, all program strings and path;
the inline zeroed table and zeroed scalar ledger do not retain an engine,
native mapping, projection snapshot or file. Failed retirement still retains
the full persistence reservation and its actual owners.

The production call chain creates one peer manager in
`clusterRuntime.initializePeerManager`, retains it as `r.peerManager`, and calls
its idempotent `Stop` from shutdown and deferred close. Stop joins the workers
before closing topology storage; the one stopped Manager may retain the shell.
The implementation keeps no history of completed manager generations. An
external caller retaining arbitrary sets of stopped API objects owns that
retention; it is not described as a bounded internal production history.
Former objects with no remaining references are separately reported GC overhead.
The DWARF inspection reads the actual binary rather than constructing a mirror
layout test. Existing retirement checks verify that close clears its references;
no runtime interface changed for this measurement.

## Observed evidence and remaining gates

Artifacts are under `D:\codex-gocluster-v15-20261002\sqlite`, with Linux artifacts
under the sibling `linux` directory. These are interim source snapshots unless
the root final-source manifest explicitly reconciles them.

- The bounded resolver/absolute-path source passed Windows normal and race
  fork checks and `go vet`. Controls cover exact pinned `filepath.Abs` Unicode/
  malformed UTF-8/WTF-8 behavior, ordinary path parity, long-CWD refusal before
  file creation, saved bytes, relative temporary-file placement and native
  find-handle retirement. `bounded-fork-normal.log`, `bounded-fork-race.log`
  and `bounded-fork-vet.log` retain the results. The two actual Windows symbolic
  link cases explicitly skip in ordinary lanes when privilege is absent;
  `GOCLUSTER_SQLITE_SYMLINK_REQUIRED=1` fails them, retained in
  `bounded-path-required-windows.log`. Required Windows link evidence is absent.
- The same Windows adapter generation passed selected normal/race peer gates
  in 27.894/39.476 seconds (`bounded-peer-normal.log`, `bounded-peer-race.log`).
  Full production 4,096-node/131,072-edge replacement took 642.392 ms within
  its unchanged five-second deadline. Native/fallback cleanup-cycle and
  statement-lifetime checks passed. This does not establish sustained throughput.
- The repaired Windows 30-minute profile started at
  `2026-10-02T23:07:50.482Z` with the prescribed two processors, GC 50 and
  1,536 MiB runtime memory limit and passed at `2026-10-02T23:37:52.059Z`
  (1,801.44 seconds). All 1,800 projection offers and 1,800 legacy offers
  committed; maximum commit gaps were 1.2679519/1.7884017 seconds, maximum
  legacy queue depth was one, and the latest snapshot needed no additional
  drain wait. The independent modernc observer checked every final node and
  typed edge. All three prescribed ten-minute phases ran unchanged. Retained binary
  `peer-persistence-repaired.exe` has SHA256
  `8fcc5850cb9cb187182c09086567f742b3f9fa4fc47f3bb527c716e987616e7f`;
  `persistence-repaired-source.json` has SHA256
  `5dc265bbef80db77bb69fa2858612e5cb9ea3880f4bd4ee92c859a25df4413e9`.
  Output is `persistence-30min-windows-repaired.log`. The earlier 142.27-second
  saturation failure remains retained, and this run does not close the separate
  host-proof or required symbolic-link evidence gaps.
- The current bounded-path generation also passed the Windows multiprocess
  WAL suite normally (9.082 seconds) and with the race detector (86.228 seconds),
  retained in `bounded-wal-process-normal.log` and
  `bounded-wal-process-race.log`. This includes native/fallback locks, multi-page
  indexes, crash/reopen, and external-growth refusal/checkpoint/recovery.
- The actual unprivileged Windows junction fixture exposed a compatibility
  defect (`windows-junction-parity.log`). Under `winsymlink=1`, pinned Go's
  `EvalSymlinks` and the candidate refuse the junction path, while unchanged
  modernc opens the same database and reads its committed row. Under
  `winsymlink=0`, both drivers open it. The test passed its pinned-Go parity and
  saved-data/integrity assertions; that is explicitly not a driver compatibility
  pass. The default-mode refusal prompted the correction below. This junction
  evidence does not replace the separately missing Windows symbolic-link gate.
  The approved correction recognizes only native mount-point tags and resolves
  them with the same bounded walker. It deliberately differs from Go's rejecting
  default walker, keeps canonical target/WAL naming, and shares the existing
  retained FindClose owner. Conservatively count two 588-byte public find-data
  values plus the syscall wrapper's 592-byte native struct (each rounded to
  640 bytes), and a 16-byte FileInfo wrapper, inside the existing 32 KiB
  bounded-path transient component. Its Windows normal/race path and failed-close
  gates passed (0.514/1.549 seconds), including immediate propagation of an
  owned close failure from the initial `FullPathname` probe. Full fork vet
  passed. Actual topology junction opens/commits passed normal/race
  (0.379/1.426 seconds). Separate-process direct modernc versus alias
  native/fallback WAL writer locks, pinned readers, checkpoints and integrity
  passed normally (0.940 seconds) and under race (8.011 seconds), in both
  `winsymlink` modes. The `junction-fork-*`, `junction-peer-*` and
  `junction-wal-*` logs retain these results. The earlier refusing source and
  diagnostic log remain evidence; these are focused correction checks rather
  than a final-source qualification verdict.

- Windows Go 1.26.4 targeted fork gates passed: partial initialization, zero
  handle, exact OOM classification, actual engine exhaustion, native reserve/
  commit failure, fixed backing, handle/view/extent release faults, global slots,
  64 KiB alignment, ordinary fallback WAL, temporary-path bounds and inventory.
- A later full fork normal/race pass includes generated-byte provenance,
  large malformed pragma/value controls and real VFS retry after a view was
  unmapped but its native mapping handle failed to close. A closing VFS cannot
  publish or return a partly retired view to SQLite. Native unmap errors are
  surfaced while retaining ownership for retry.
- Fork vet initially reported the direct MapViewOfFile uintptr-to-pointer
  conversion. Lead review confirmed this is an OS mapping rather than a Go
  pointer retained through an integer. The code now uses the pinned native
  Memory implementation's conversion idiom, with explicit lifetime comments;
  full fork vet and affected default-race/checkptr tests pass. The initial
  warning remains in `latest-fork-vet.log`; no blanket checker suppression was
  applied. The explicit `-gcflags=all=-d=checkptr=2` native/fallback mapping
  cases subsequently passed at approximately 22:38 UTC on October 2; retained
  evidence is `fallback-checkptr2.log`. The later path-helper changes do not
  alter that reviewed native-address conversion; this remains interim-source
  evidence rather than a final-source acceptance verdict.
- Production adapter DSN/default/ordering, configured mmap read/write, saved
  3 MiB schema refusal with independent preserved value/integrity, temp spill
  refusal/control, total admission deadline, actual SQL cancellation, and
  global failed-owner observation checks passed.
- Windows native and fallback each completed 250 successful, 250 partial-open,
  250 actual-OOM and 250 cleanup-failure/retry cycles. Each mode additionally
  completed 250 cancellation cycles. Cleanup-failure cycles retain a genuinely
  allocated engine and reject replacement until the injected gate clears;
  lower-layer tests independently inject real release-site failures.
- Separate Windows processes passed lock/coherence, pinned-reader/checkpoint,
  multi-page index, and crash/reopen gates. Acknowledged commits were required
  to survive. A modernc writer expanded the WAL index to 2,621,440 bytes; the
  bounded fallback refused, retired all engine/WAL ownership, and resumed on
  intact data after independent checkpoint. The fixture's exact-OOM boundary
  is separate from the production adapter's ordinary-error tests.
- Actual Debian 13.7 Linux/amd64, Go 1.26.4/GCC 14.2, ran the interim fork normal
  and race gates successfully (`sqlite-diagnostic-01`, seven applicable cases).
  A later portable WAL snapshot first exposed a missing Linux-only uuid sum;
  its fixture module was completed. The later portable native WAL gate passed
  in 7.664 seconds; its Windows-only fallback-growth case was explicitly
  skipped. Production adapter/lifecycle checks passed except the full-capacity
  projection: the real five-second SQL deadline interrupted it at 5.0068
  seconds under QEMU TCG (2 virtual CPUs). This is a qualification failure on
  that environment, not a Linux acceptance pass. Windows fallback is never
  silently labeled exercised on Linux.
- The initial full projection measured 1.3756 seconds uninstrumented. Race
  instrumentation exceeded the unchanged production five-second deadline;
  full-capacity timing is explicitly `!race`, with a smaller real production
  path/race fixture. No race-only production timeout exists.
- The admitted-shape diagnostic ran 3 × 20 seconds at GOMAXPROCS=2, GOGC=50,
  GOMEMLIMIT=1536MiB: 60 offered projections, 37 projection commits, 60 legacy
  commits, maximum gaps 2.1133/2.1127 seconds, latest-snapshot drain 2.8237 seconds.
  An independent modernc observer checked every final node and typed edge.
  Full-count and max-metadata graphs were admitted through actual encoding,
  decoding and graph plans (51,773,440/53,264,288 graph bytes); maximum wire size
  was 64,603 bytes and two max-metadata projections exactly fill 36 MiB.
  An earlier synthetic-call timing result is diagnostic only: its calls were
  rejected by the added wire oracle and corrected before the admitted run.
- The first prescribed long run failed after 142.27 seconds: the unchanged
  64-entry legacy queue filled while each request class was offered once per
  second. The short diagnostic above had not exposed that accumulated backlog.
  The failed binary, source manifest and log are retained; a fresh diagnostic
  records per-class progress/queue depth and a CPU profile before choosing the
  narrowly approved transaction-owned statement repair. No queue, offer-rate
  or deadline relaxation is authorized by this finding.
- The retained before-repair CPU profile attributed 30.38 of 62.86 sampled
  seconds (48.33%) to statement preparation. Its 60-second admitted workload
  accumulated 22 legacy requests by 59 seconds (37 commits in each class during
  load). The single-statement repair reduced the full production projection to
  470.8 ms. The matched after-run committed all 60 projections and all 60 legacy
  requests, with sampled backlog zero (maximum enqueue depth one), maximum
  commit gaps 1.1308/1.6222 seconds and no pending latest snapshot at drain.
  The independent observer compared every final row. Ordinary SQL/prepare/
  binding errors roll back without replacing the engine; real allocation
  exhaustion with an active reused statement retires it before replacement.
  These short results justify the fix but do not substitute for the rerun of
  the prescribed 30-minute exercise or the failed Linux capacity gate.

The Linux 30-minute three-phase exercise, final-source normal/race/static/fork
and platform reruns, enabled-store Q4 simultaneous ownership evidence and root
fresh verification remain required. A stopped or superseded-source long run
does not satisfy those requirements. Audit corrections and overall acceptance
remain separate results.
