# V15 context and diagnostic ownership evidence

Status: implementation and targeted checks completed for the fixed context
parents and diagnostic isolation; complete allocation acceptance remains open.
This is the V15-06/07/08 slice record, not the final PC18/PC92 verdict. The lead
accepted the detailed gate before mutation. The reviews were design-aware;
they are not an independent certification. Approval and inherited obligations
are recorded in [the approved ledger](pc18-pc92-scope-ledger-v15.md).

## Contract-to-test matrix

Each row has the eight fields required by `test-strategy-adversary`. Findings
were covered or checker-only refinements before the first slice. Subsequent
native-launch and global-owner refinements preserve the approved single
generation, bounded reservation, diagnostic refusal and cleanup contracts.

| Contract or invariant | Failure or boundary case | Stimulus or fault | Observable result | Evidence level | False-green risk | Exact checker | Owner |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Exactly N+128 permanent transport parents | Parent map grows with lifetime churn | N=1 and64;20 full-pool reuse cycles | Parent identities fixed, no extra slot, canceled operation before reuse | Unit/race | Occupancy alone is not descendant/backing proof | `TestV15ContextParentsStableAcrossChurn` | peer/context_ownership_test.go |
| Standard context ancestry preserved | Earlier deadline bypasses operation root; values/cause lost | Start with Value, earlier deadline and CancelCause | Explicit operation root, identical value/deadline/cause | Unit/race | Testing background-only parent misses early-deadline path | `TestV15OutboundContextPreservesParentContract` | peer/context_ownership_test.go |
| Transport credit released only after operation cancellation | Dial/auth/replay/EOF/Stop partial return retains authority | Actual TCP refusal and handshake/session harnesses;192 real pipe handshakes with full mailbox | Session workers retire; owner/candidate credits zero; diagnostic overload does not block | Integration/race | Callback mock can miss actor I/O or real closure | `TestSessionOwnerReservationSurvivesDiagnosticOverload`, `TestPC92V14RetryDialFailure`, existing `TestSession*`/`TestOutboundHandshake*` | peer/session_ownership_test.go, peer harnesses |
| Entire context backing fits metadata partition | Detached DNS/racer retains old context after credit release | Pinned Go1.26.4 Dialer/resolver source inventory plus Q4 | Old descendants accounted or explicit open proof | Source/runtime | Root count and successful cancellation falsely imply joined descendants | Descendant inventory below; lead Q4 | peer/context_ownership.go and lead proof |
| Reserve before copying/formatting | Full mailbox allocates hostile input or calls formatter | Full256 slots, multi-megabyte borrowed fields | Refusal with zero allocations; fixed record owns copied bytes | Unit/race | Checking length after an unbounded format/copy | `TestV15DiagnosticReserveBeforeEncoding`, `TestV15DiagnosticEventOwnsFixedBacking` | internal/peerdiag/mailbox_test.go |
| No arbitrary logger on peer actor | Bad-call/storage/handshake path reaches callback or log I/O | Source escape search plus literal diagnostic consumers | Only typed primitive fields reach mailbox; peer log sink constructed only in helper | Source/consumer | Removing one callback leaves another escape path | `TestHandleFrameReportsBadCallParseDrop`, `TestManagerReportsConnectionEvent`, `TestSpotLeaseInvalidDXAndDEDiagnosticConsumers`, source search below | peer and internal/cluster |
| Concurrent close has exact known loss | Producer races shutdown/full mailbox |16 concurrent producers, close,16000 events | Every refused/discarded event counted; closed queue never admits | Race | No concurrent stimulus or counting only dequeued records | `TestV15DiagnosticConcurrentCloseAccounting`, `TestV15DiagnosticStartStopRace` | internal/peerdiag |
| Exact512-key diagnostic dedupe contract | Hash collision, wrong boundary, changed zero default/oldest rule | Literal repeated line,59/60-second boundary,513 identities, explicitzero | Exact equality, suppression count, oldest replacement,zero bypass | Unit | Expected output derived by same dedupe algorithm | `TestV15DiagnosticDedupeContract` | internal/peerdiag/mailbox_test.go |
| Authenticated bounded startup IPC | Malformed lengths allocate before validation |32-byte header with4GiB declarations, bad flags/scalars, truncation, nonASCII paths | Reject before payload read/allocation; valid values round-trip; fixed deadline | Unit/fuzz/integration | Per-frame bound leaves options unbounded or resets deadline per step | `TestV15HelperOptionsIPC`, `TestV15HelperOptionsRejectBeforePayload`, `TestV15HelperReservationArithmetic`, `FuzzV15HelperOptionsHeader` | internal/peerdiag/options* |
| ACK distinguishes known loss from unconfirmed write | Partial write, failed sink, wrong seq, missing ACK, process crash | Pipe prefix failure; invalid ACKs; actual process exits after full record | Unconfirmed increments; no false Written/durable claim | Unit/integration/race | Failed write may have written a prefix; mock-only crash | `TestV15HelperIPCFailureMatrix`, `TestV15PartialWriteIsUnconfirmed`, `TestV15ActualProcessCrashBeforeACKIsUnconfirmed` | internal/peerdiag/service_test.go |
| Recovery summaries acknowledged before progress | Summary itself lost after enqueue | Missing ACK then next generation summary | Reported counters stay pending until successful matching ACK | Unit/race | Enqueue wrongly marks a summary delivered | `TestV15LossSummaryRequiresWriteAcknowledgement` | internal/peerdiag/service_test.go |
| Real blocked sink cannot freeze protocol shutdown | Child blocked in OS pipe write | Actual subprocess fills an unread pipe; Kill/Wait | Bounded retirement and terminal process witness | Native integration/race | Cancel-only mock; Unix Exited excludes signal termination | `TestV15HelperBlockedWriteTerminatesAndJoins` | internal/peerdiag/service_test.go |
| No overlapping uncertain generation or constructor | Failed Wait/Close; manager discarded and reconstructed | Delayed Wait, failed-Wait witness, setup Close failure, GC, repeated New | Strong global owner retains charge; zero-allocation fixed refusal handles; no new queue/worker | Unit/native/race | Retired pointer can disappear with discarded manager; constructor allocates second queue before gate | `TestV15HelperFailedTerminationRetainsGeneration`, `TestV15FailedWaitOwnsServiceAcrossReplacement`, `TestV15SetupCloseFailureRetainsLease` | internal/peerdiag/service_test.go |
| Native startup remains owned on every partial failure | Windows second attribute init fails; CreateProcess fails | Actual invalid init flags;100 missing executable launches | Bounded attribute allocation freed; all duplicated/thread/process handles tracked | Native integration/race | Standard launcher hides partial native allocation and free failures | `TestV15WindowsNativeAttributeOwnership`, `TestV15WindowsFailedCreateReleasesNativeStartup` | internal/peerdiag/launch_windows_test.go |
| Only intended standard handles inherited | Basic STARTUPINFO inherits unrelated handles | Extra inheritable event exists while real child launches | Child cannot use that handle under HANDLE_LIST | Native integration | Passing stdhandles alone does not limit other inheritable handles | `TestV15WindowsInheritsOnlyOwnedHandles` | internal/peerdiag/launch_windows_test.go |
| Existing enabled/disabled/file semantics and independent status | Disabled daily log accidentally disables overlong or offline status | Actual helper disabled daily/overlong; rotation/retention; disconnected Peers | Overlong written, daily directory absent; status visible offline | Consumer/integration | Testing only enabled in-memory sink | `TestV15HelperDisabledDailyStillWritesOverlong`, `TestV15HelperDailyRotationAndRetention`, `TestV15HelperOverlongRotation`, `TestV15HelperLargeDirectoryBounded`, `TestV15PeerDiagnosticStatusVisibleWithoutSessions` | internal/peerdiag, internal/cluster |
| Both binaries and Linux execution qualified | Windows-only behavior or stale companion | Actual Linux run and lead packaging/source manifests | Matching source/binary provenance and actual platform behavior | Platform/workflow | Cross-compilation or helper from different source treated as qualification | Lead packaging lane and retained Linux commands below | Lead and Linux agent |

The later Windows path/retention correction received a design-aware detailed
gate before edits. The lead dispositioned the following checker refinements;
the old-OS long-device proof caveat was explicitly left open at that stage, with no new
behavioral refusal or OS-baseline change selected for that caveat.

| Contract or invariant | Failure or boundary case | Stimulus or fault | Observable result | Evidence level | False-green risk | Exact checker | Owner |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Admit native acquisition before allocation | Zero/error/oversize OS result or second-read growth | Inject exact native size returns | No oversized allocation, conversion or retry | Unit/allocation | Checking final strings after native allocation | `TestV15WindowsNativePathAdmission` | internal/peerdiag |
| Exact byte count without cwd copy | Paired/unpaired surrogates, embedded terminator | Literal UTF16 vectors | Count equals pinned syscall WTF8 result | Unit | Replacement UTF8 hides surrogate bytes | `TestV15WindowsWTF8ByteCount` | internal/peerdiag |
| Parent executable fits acquisition and launch phases | Native truncation/error, large environment | Native control and injected returns | Fixed output; admission before conversion/derived sibling; phase within1MiB | Native/unit/source | Ordinary short executable only | `TestV15WindowsExecutableBound` | internal/peerdiag |
| Preserve path targets and logical log naming | Dot/root, relative/drive/UNC, device/reserved/trailing-dot/extended/empty | Modern identity, legacy native preparation and real files | Same target/empty failure; current.log remains current.log | Native/source | Blind extended-prefix changes OS semantics | `TestV15WindowsPreparedPathParity`; later owned-operation correction below | internal/peerdiag |
| Fixed retention preserves deletion policy | Many unrelated files, cutoff, directory, junction, locked old file | Actual2,000-entry directory and literal expected retained set | Fixed scan state; exact expiration; ignored Remove failure; junction target survives | Native/unit | Small directory or fake IsDir only | `TestV15HelperLargeDirectoryBounded`, `TestV15WindowsRetentionSemantics` | internal/peerdiag |
| Acquired find handle remains owned | First/Next/Close fails | Native handle plus exact fault hooks | No owner after failed First; one Close; failed Close poisons and retains, no rescan | Native/unit | Report error after already freeing handle | `TestV15WindowsFindOwnership` | internal/peerdiag |
| Failed find release ends whole generation | Helper cannot close acquired find handle | Actual companion runs injected failure through RunHelper | ackFailed/unconfirmed; actual Wait precedes discharge | Process/race | Cancel-only fixture or false Written ACK | `TestV15WindowsFindFailureRetiresHelper` | internal/peerdiag |
| Existing resource ceiling retained | Largest native workspace and phase overlap | Source inventory, concrete struct/array sizes and arithmetic checks | Parent1/helper2MiB; no new generation/cache/worker | Source/unit | Sampled heap mistaken for bound | `TestV15DiagnosticBackingEnvelope`, `TestV15HelperReservationArithmetic`, native phase assertion | internal/peerdiag and lead |

The metadata-only absolute-preparation draft was an approved intermediate
V15-08/12 refinement, but its new checks were never executed. It was superseded
by the owner-local Windows filesystem correction below. That draft removed
modern Stat's unbounded saveInfoFromPath call but did not own ignored temporary
handle-close failures or the second legacy long-device normalization. Its
proposed query/read-count tests are historical, not current acceptance evidence.

The lead accepted the following helper-only detailed matrix before production
changes. Tests were written first. This is design-aware review, not independent
certification. After the timing slot was released, Windows full normal/race
and vet passed on this correction; exact retained evidence is recorded below.
The current relevant-source Linux helper checks also passed as recorded below;
the lead's aggregate review and final overall qualification remain open.

| Contract or invariant | Failure or boundary case | Stimulus or fault | Observable result | Evidence level | False-green risk | Exact checker | Owner |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Existing native workspace/retained credits | Zero, oversized or changing DWORD; excessive WTF8 capacity | Native reply controls and surrogate pair | Refuse before oversized allocation; no unbounded retry; ordinary native-error fallback distinct | Unit/source | Counting string length only | `TestV15WindowsOwnedPathAdmission`, `TestV15WindowsMetadataSurrogateBacking` | Helper Windows paths |
| Actual targets unchanged | Relative/drive/device/extended/dot/space and junction-parent spelling | Actual files, native input capture, independent open/file-ID comparison | Same target, consumed metadata and error class | Native integration | Comparing strings or private FileInfo via os.SameFile | `TestV15WindowsOwnedFilesystemTargetParity`, `TestV15WindowsMetadataJunctionParentParity` | Helper Windows metadata |
| Attributes fast path | Unnecessary exclusive open | Restricted-sharing real file; reject CreateFile | Correct metadata without open | Native/unit | Unrestricted fixture | `TestV15WindowsOwnedMetadataAttributeFastPath` | Helper metadata |
| Sharing fallback | Find result replaced by exclusive open | Only attributes forced to sharing error; actual FindFirstFile/close | Metadata matches control; exactly one closed find owner | Native fault | Fully mocked metadata | `TestV15WindowsOwnedMetadataSharingFallback` | Helper metadata |
| Required handle branch | Wrong flags, target follow, console retry or metadata | Actual files/directories/NUL/junction; controlled attributes error | Pinned access/share/flags; consumed metadata preserved | Native | Ordinary fast paths only | `TestV15WindowsOwnedMetadataHandleParity`, `TestV15WindowsOwnedMetadataJunctionRelease` | Helper metadata |
| Failed close remains owned | FindClose/File.Close ignored | Failure before releasing real acquired handle | Sticky slot; no retry/new acquisition/write | Native fault | Failure injected after real release | `TestV15WindowsOwnedMetadataReleaseFailure` | Helper metadata |
| Linear recursive ownership | Expanded names retained at every level | Deep short components; native expansion; prefix error checks | Error paths borrow original logical backing; bounded recursion | Unit/source | Only final string length tested | `TestV15WindowsOwnedMkdirBacking`, `TestV15WindowsOwnedMkdirParity` | Helper mkdir |
| Fatal ownership beats ordinary errors | Overlong Stat or MkdirAll fallback ignores release failure | Real file and metadata close fault | Failed ACK; no subsequent file mutation | Consumer | Metadata-only unit test | `TestV15WindowsMetadataFailureStopsSink` | Helper sink |
| Native file operations | Changed append/rename/readonly removal/error behavior | Separate Go-control and helper files | Matching bytes, outcomes, replacement and deletion | Native | Existence-only assertions | `TestV15WindowsOwnedOpenRemoveRenameParity` | Helper filesystem |
| Failed helper generation retires | Parent releases charge before Wait | Actual fault companion and authenticated IPC | Unconfirmed ACK; charge retained until positive process retirement | Process/race | Mock process exit | `TestV15WindowsMetadataFailureRetiresHelper` | Helper lifecycle |
| Fixed backing | Larger structs or additional owner/escaped name | Type-size guards and explicit inventory | Fixed buckets and unchanged3MiB admission hold | Source/unit | RSS/sample as proof | `TestV15WindowsOwnedMetadataBacking`, `TestV15DiagnosticBackingEnvelope` | Helper accounting |
| Existing sink behavior | Rotation/retention/disabled regressions | Full existing helper suite | Existing names, contents, retention and ACKs | Native/race | New targeted tests only | `go test [-race] ./internal/peerdiag -count=1` | Helper package |

## Implemented ownership and operational behavior

`peer/context_ownership.go` creates fixed parents once under the Manager context.
Inbound admission and outbound dialing reserve an operation before constructing
workers. The outbound operation is explicitly cancellable even when its parent
deadline is earlier. Projection and legacy storage workers have separate fixed
parents. Private `session.Run()` consumes that owned operation; there is no
alternate parent, custom Context or cancellation bridge. Nil connection,
stopped manager, dial failure and failed retry establishment paths return their
operation/transport ownership. Normal Run cancels, closes, joins, unregisters,
releases metadata and then returns credits.

Peer actors use `*peerdiag.Service` and fixed `Fields`; there are no arbitrary
diagnostic callbacks. The mailbox does not retain sessions, errors, Stringers,
raw frames or producer string headers. String fields are bounded and sanitized
after admission; spaces/control bytes become underscores in the structured
record. PC11/61/26 rejection diagnostics carry bounded DX/DE identity fields.
This changes diagnostic representation, not protocol data.

The parent no longer constructs a peer_connections sink. The sibling helper is
the sole daily/overlong file owner. It has no cluster/config/database imports.
Overlong remains active when daily logging is disabled. Rotation uses fixed
64KiB scratch; Windows directory cleanup uses one fixed native find record,
while Linux retains the original one-entry ReadDir behavior. The helper uses
GOMAXPROCS=1 and a minimal explicit environment, not inherited PATHEXT/PWD or a
full parent environment. Existing nonpeer sinks remain in the parent.

Startup uses a250ms deadline established before setup, random32-byte
authentication, a fixed32-byte options header with pre-allocation length and
scalar validation, and a fixed options acknowledgement. Record IPC is2048bytes
with16-byte sequence/outcome/count acknowledgements. Record acknowledgements
mean an accepted file write, not fsync or durable storage. A failed sink write
is unconfirmed because a prefix might have been written. Successful loss-summary
ACK, not enqueue, advances recovery-summary progress.

The process-wide owner is reserved before allocating the queue. A concurrent or
replacement constructor while that owner remains returns a fixed no-queue
degraded handle; it copies no configuration and starts no worker. Disabled and
enabled refusal handles have separate fixed counters. They also expose the
prior owner's cleanup failure, charged backing and generation, while retaining
their own producer counters. Such a refused handle stays degraded; a fresh
constructor can acquire ownership after confirmed retirement.

Normal Stop closes producer admission, discards/counts queued records, joins or
terminates the helper, detaches queue/options, then releases the owner. Stop
waits at most2seconds. A delayed successful Wait can discharge a generation;
failed Wait, unconfirmed native Close/LocalFree or permanently blocked cleanup
keeps the strong global owner and full charge. A stopped Service object and
fixed refusal/status objects still exist; ChargedBytes=0 means its large owned
allocation reservation was released, not that the object has zero bytes.

Windows uses a narrow owned CreateProcess adapter. It queries the one-attribute
HANDLE_LIST size, refuses above4096bytes before LocalAlloc, takes ownership
before second initialization, and tracks every duplicated standard/thread/
process handle. Every CloseHandle and LocalFree outcome is checked. The void
DeleteProcThreadAttributeList API has no failure return. Hidden-window flags
and exact absolute executable selection remain. Linux uses os.StartProcess;
successful Wait requires nonnil ProcessState. A signal-killed process is a
valid terminal witness even though Unix ProcessState.Exited() returns false.

File write or Close failure produces an unconfirmed outcome and ends the
helper generation. The parent joins that whole process before replacement.
This also bounds a failed helper-side Close whose native ownership is uncertain;
there is no continuing stream of replacement files inside that generation.

## Diagnostic allocation inventory

The allocation proposal remains parent1MiB + helper2MiB =3MiB within the32MiB
metadata partition. Its complete derivation remains **OPEN pending fresh aggregate review and
final overall qualification**. It is a reservation to prove,
not an RSS limit or proof by measurement. `allocation.go` rejects conservative reservation exhaustion before
large option payload copies/file operations; this is diagnostic degradation,
not a measured OOM and never a protocol admission failure.

The fixed-size checker records exact current struct/array sizes in
`fixed-backing-windows.txt`; these are inputs to the inventory below, not a
substitute for it. The native attribute list was48bytes on this host; the
enforced production maximum is4096bytes regardless of that observation.

| Owner/backing | Bound and lifetime | Disposition |
| --- | --- | --- |
| Queue |256 x2048 =524288 bytes, allocated only after process-wide owner admission; detached before replacement | Implemented and tested |
| Deduper |512 entries with2048-byte exact key plus bounded timestamps/count/length metadata | Fixed struct checked |
| Record and file scratch | Fixed event/IPC/ACK/options arrays,4096-byte line,65536-byte copy/tail buffer | Fixed struct checked; concurrent phase inventory required in aggregate |
| Directory buffers | Existing196608-byte reservation retained. Windows owns fixed find-data, one scan owner and one metadata owner;8KiB is suballocated to metadata/path constants below. No ReadDir pool or FinalPath call | No directory-cardinality-sized slice; failed find release retained until process exit |
| Option payload | Header validated before allocation; received bytes and immutable strings charged simultaneously | Implemented; overflow/refusal and round-trip gates pass |
| Input-derived paths |64 x maximum path bytes +64 x cwd on Windows (16 x cwd on Linux) +2 x sum option bytes | Source coefficient inventory below; payload and immutable copies overlap |
| Windows native path workspace | Enforced229376-byte workspace for bounded cwd/full-path queries, their native input/output and conversion. No OS-sized retry selects an allocation | Modern Windows spelling unchanged; legacy prepared names consumed directly by native operations, Windows and current relevant-source Linux helper normal/race passed; aggregate review remains |
| Parent executable/env |2 x executable bytes +3 x sibling bytes +5 x environment bytes, plus fixed argv metadata | Exact sibling; environment string, temporary WTF-16 entry and destination block all charged |
| Helper environment |7 x minimal environment bytes | Native environment, GetEnvironmentStrings copy and worst Go conversion charged; startup cannot borrow spare parent bytes |
| Native launcher |4096-byte hard attribute allocation limit; three inherited duplicates, one thread/process handle; fixed ownership flags | Partial init/create, inheritance and release checks pass on actual Windows |
| Setup/retirement | Listener, accepted socket, devnull and native process owner retained on uncertain release; no second generation/queue | Explicit ownership and reconstruction tests pass |
| Other metadata |64KiB per process with the five explicit subpartitions below | Source inventory; actual wrapper/native structure tests detect pinned layout changes |

Go stacks/GC overhead and unchanged configuration must be reported separately
as selected by the user. No unproven allocation has been moved into those
categories. In particular, introduced native/library bookkeeping must be
classified explicitly in the aggregate proof. Missing-helper and failed-start
paths remain charged before attempts; native/socket release uncertainty gates
further attempts instead of accumulating abandoned generations. This is a bound
on owned protocol backing, including the listed caller-owned native allocation,
not all memory inside the OS or the process RSS. Kernel socket/handle resources
and generic shared runtime/library storage are reported separately.

### Fixed64KiB bookkeeping inventory

The inventory uses Go1.26.4 Windows/Linux amd64. Size classes come from
`internal/runtime/gc/sizeclasses.go` (8192-byte large-allocation pages). All
subpartitions are charged simultaneously; alternatives within the32KiB phase
row are mutually exclusive call phases, not independent simultaneous owners.

| Subpartition | Concrete owners and maximum source population |
| --- | --- |
|32KiB phase temporaries | Windows `internal/poll.InitWSA` has32 WSAProtocolInfo plus WSAData: actual pinned structure sum20504bytes, tested. This precedes file work. Native process startup has at most4096 attribute bytes, short escaped argv/UTF-16 buffers bounded by the512-byte private argv contract, authentication/endpoint arrays, and at most three large environment-conversion rounding tails. File phases instead use this allowance for the fixed sink's large-allocation tail plus at most three independent native-path/conversion rounding tails and constant prefixes. Linux's PathMax cwd/readlink/startup temporaries are smaller. |
|8KiB wrappers | At most16 simultaneously live network/file/address/info objects, each below512bytes. Parent has listener, accepted socket, devnull, their addresses, native/process state. Helper has three standard-file wrappers, one socket, and at most two log/archive/directory files; sequential phases do not accumulate descriptors. Pinned tests check netFD224, file184, fileStat104, TCPListener64, TCPAddr48, OpError80 and PathError48. Active leased poll operations and FD pinners fit the unused wrapper slots. |
|8KiB lifecycle/control | At most32 nodes at256bytes: parent stop/done/notify/exited channels, overlapping Stop/join timers and their channels, retry timer in a separate phase, process attributes/Wait control. Helper literal IPv4 dialing has one deadline context, one AfterFunc/interrupter, their small child map/control/channels/timer; there is one address and no DNS or Happy Eyeballs race. Windows stops/joins its AfterFunc. Linux may return before that callback completes: this one-shot helper dials only once, includes that callback/fd backing, and the parent joins the whole process before replacement. There is no repeated in-process dial generation. |
|8KiB record copies | Four extra2048-byte value-copy slots cover producer initialization, Mailbox.Next return, decodeEvent return and helperSink.write argument overlap beyond the two explicit wire/event records. They are owned protocol bytes even if the compiler places them on a Go stack. |
|8KiB small metadata | Fixed flag/parser and two-entry environment-map state, fixed error objects, UTC timestamp/parser scratch, native lazy-procedure wrappers, string/slice headers, the two global refused Service/Mailbox objects, owner guard, and one stopped/current constructor transition. Stopped objects release queue/options; no refusal registry or refusal worker exists. Retaining arbitrarily many stopped API objects externally is caller-owned state, not a production Manager construction path. |

Returned Windows operation-pool storage is generic shared runtime/library
retention. `internal/poll/fd_windows.go`'s operation has only Overlapped scalars,
runtimeCtx uintptr and mode. execIO waits for completion (including cancel-and-
wait), returns the operation, and unpins. `runtime/pinner.go` clears refStore
and resets refs before caching. It retains no FD, payload or closure reference.
Active leased operation/pinner backing is charged above; charging every other
socket's shared pool to this helper would duplicate the runtime owner. Runtime
pollDesc explicitly has no heap pointers and is reused through pollcache after
closure. No parent GOMAXPROCS setting or P-based diagnostic refusal was added.
The previous Windows directory buffers retained filename bytes. Replacing
ReadDir removes that pool from the helper path; its conservative196608-byte
reservation remains available rather than increasing other admitted limits.

### Path and environment coefficient derivation

Let P=max(directory bytes, overlong bytes), C=cwd bytes. The received payload
and immutable option copies remain in the separate2*(directory+overlong) term.
Windows admits64P+64C; Linux retains its previous64P+16C predicate. The
Windows helper now recurses over the original directory/Dir(overlong) slices,
L<=P+1, not an expanded P+C spelling. At most ceil(L/2)+1 levels retain one
initial metadata error each; a conservative64-byte bucket (48-byte PathError
plus boxed errno/rounding) and leaf errors cost at most32L+256 bytes. Metadata,
File.Name and returned errors borrow logical names; normalized native strings
never escape an operation. This prevents quadratic recursive path backing.

Conservative variable terms are8P+128 for derived logical names/construction,
11P+7C+1024 for prepared names and native conversions, and3P+2C+256 for one
rename operand overlapping the other's preparation. Active-log names can be
2P plus a fixed suffix. Adding recursion gives at most54P+9C+2048, covered by
64P+64C and fixed constants. This accounting deliberately adds phases that need
not coexist. Native acquisition is separately charged below; unreachable
intermediate allocations are GC overhead, not retained generations.

Within the already charged196608-byte directory allowance,4KiB covers fixed
metadata work: two640-byte rounded native/public find records, two512-byte
file-owner buckets and two512-byte metadata buckets conservatively coexisting,
64-byte attributes and64-byte handle records,16-byte tag record,32-byte native
mode query,64-byte owner slot and128-byte temporary error/header allowance:
3696 bytes. Another4KiB covers path/error constants above. The new owner itself
also appears in helperSink's directly measured fixed size; this deliberate
overcount does not enlarge admission. The concrete-size guards passed in both Windows normal and race checks;
these checked buckets remain inputs to the lead's aggregate proof. Logical Options remain unchanged: '.' still
names current.log and existing log names are derived before native preparation.

The former7*32768 term was an unsupported proposed OS maximum. It now enforces
an explicit229376-byte workspace. Native GetCurrentDirectory first returns a
required size; both native storage and the minimum possible cwd charge are
admitted before make. The second read cannot grow the allocation. Exact WTF8
length is counted directly, including unpaired surrogates, before payload
admission; no whole cwd string is copied. Native full-path preparation reserves
2*(input bytes+1) plus5*(native output units+prefix) before allocation. The
five-unit term includes output UTF16 and syscall.UTF16ToString's conservative
three-byte capacity per unit, including surrogate-pair overestimation.

Windows cleanup no longer calls ReadDir, eliminating its reachable uncapped
FinalPath acquisition. FindFirst/Next use fixed native data; the public and
native structs and possible returned copy fit within the retained directory
allowance. One16-byte scan owner captures the handle immediately. First failure
owns nothing; every acquired handle gets one FindClose. Failed Close keeps the
handle/poison state, disallows another scan or write, returns ackFailed and
exits the entire helper. Only actual parent Wait discharges that generation.
Names must be exactly15 ASCII bytes before conversion/date parsing. The pinned
Go directory/name-surrogate predicate preserves junction entry deletion without
traversing its target. Cutoff and ignored Remove failures remain unchanged.

The exact Go1.26.4 runtime sets CanUseLongPaths solely from RtlGetVersion
(Windows >=10.0.15063). The public x/sys call supplies the same native version
without changing OS state. On that branch all OS-facing strings remain as
supplied for Open/Remove/Rename/retention, and Go's fixLongPath returns
immediately. Older supported Windows
uses the bounded attributed addExtendedPrefix adaptation at OS-facing calls,
including ordinary short/relative, UNC and already extended forms. No GODEBUG,
OS baseline or logical option policy changed.

Pinned Go's modern Stat used saveInfoFromPath for relative names; legacy
long-device paths could reenter fixLongPath. Both are avoided by the new
owner-local helper operations. Modern native path spelling is unchanged.
Legacy helperOperationPath follows pinned Go's short-path threshold and prefix
rules, then calls the bounded native preparation once. Ordinary native errors
return the original spelling, matching Go; resource refusal is propagated.
Prepared device names gain no changed namespace prefix and never reenter an
os pathname operation. The existing parent NUL opener and its reservation are
unchanged. No universal32767-unit bound or idempotence assumption is used.

For actual logical operand length L and admitted cwd C, normalization output
must fit L+C+8 bytes (including prefix credit), with UTF16ToString backing at
most1.5 times that credit. Acquisition remains inside229376 bytes and checks
its input, output and conversion before allocation. A native drive-relative
expansion beyond admitted credit is ordinary diagnostic resource refusal, not
a newly excluded pathname class or an assumed impossible OS result.

Windows metadata retains GetFileAttributesEx first and FindFirstFile on sharing
violation. It owns every temporary find/file handle before the next operation.
Plain non-reparse results store only fixed attributes and logical basename.
Already-required handle branches use os.File.Stat so Go's effective winsymlink
interpretation remains intact. A fixed tag query selects the name-surrogate
follow branch because FileInfo does not expose that tag; this adds no open,
path resolution or atomic-snapshot claim. The fresh synchronous handle has no
outstanding I/O when os.NewFile performs its fixed native mode probe.

One failed metadata close poisons the helper generation and retains its slot.
No retry, next open or success ACK follows. MkdirAll and the overlong Stat-ignore
path test this state before interpreting ordinary missing/permission errors.
The helper exits and only the parent's positive process Wait discharges that
native generation. Opaque kernel bookkeeping remains separately reported, but
its byte exclusion is not substituted for a release witness. This correction
addresses a source escape; no measured native-handle leak was claimed.

Open uses only the sink's actual flag set, excluding O_TRUNC/O_DIRECTORY's
partial-handle cleanup branches. Native append/share/access behavior comes from
syscall.Open; no helper caller uses append-mode WriteAt. Remove preserves
Go's file/directory/readonly fallback; Rename uses its existing replace flag.
MkdirAll follows the pinned recursion and fallback algorithm with the owned
metadata calls. Linux continues to use its existing os operations. No shared
filesystem abstraction, worker, cache, configuration, OS baseline or SQLite
interface is introduced.

Parent executable acquisition uses one65536-unit buffer (131072 bytes) and at
most196608 conversion bytes. Fixed parent528776 + metadata65536 + acquisition
327680 =921992 bytes, below parent1MiB before argv/environment construction.
Truncation or error refuses without retry; conservative conversion/sibling
backing is admitted before conversion. Later launch admission charges2*executable
+3*sibling+5*environment. On legacy Windows it additionally reserves229376 bytes
for OS-facing NUL preparation and its retained os.File name while argv,
environment and startup owners overlap. This is parent credit, not borrowed
helper credit; the earlier executable scratch is no longer owned in that phase.
Modern Windows and Linux receive no such extra legacy reservation. The literal
legacy-parent44877-admitted/44878-refused environment boundary subsequently
passed normal and race execution; modern Windows still admits44878.
Parent environment construction keeps the explicit string (R), temporary
WTF-16 entry (<=2R) and destination UTF-16 block (<=2R). The helper can overlap
its native block (<=2R), GetEnvironmentStrings copy (<=2R) and Go string
conversion (<=3R); hence5R and7R. Native terminators, headers and allocation
rounding use the phase metadata row. SYSTEMROOT is captured once before the
parent reservation check; the helper receives only SYSTEMROOT/GOMAXPROCS on
Windows and GOMAXPROCS on Linux. No arbitrary inherited environment copy exists.

The complete3MiB diagnostic envelope remains conditional under this ownership
boundary. Current relevant-source helper checks passed on Windows and Linux;
the lead's fresh aggregate review and final overall source qualification remain
required before overall acceptance. The independent context-descendant
gap below is not discharged by this diagnostic proof.

## Exact remaining context descendant gap

Pinned sources are the installed Go1.26.4 `src/context/context.go`,
`src/internal/runtime/maps/{map,table}.go`, `src/net/{dial,lookup,net}.go`,
`src/net/{cgo_unix,lookup_windows,rlimit_unix}.go`.

The fixed root has N+128 transport parents plus two optional projection
parents, created once, and no lifetime churn inserts at that root. Each fixed
transport parent has at most one operation child, synchronously removed before
reuse. These establish structural counts; they do not establish all reachable
descendant backing.

- Dialer dialParallel starts primary/fallback racers (dial.go689/700). Returning
  a winning connection closes a returned channel and cancels contexts; it does
  not join the losing racer. The racer can still retain old contexts, resolved
  addresses, errors and a connection until it observes cancellation/return.
- dialSerial defers partial-deadline cancels for its per-address loop
  (dial.go751). Address population affects simultaneously retained timerCtx
  descendants; the active-parent count alone does not bound these.
- lookupIPAddr creates lookupGroupCtx under onlyValuesCtx and a singleflight
  lookup (lookup.go330). onlyValuesCtx retains `lookupValues` pointing to the
  old context even after Value begins returning nil on cancellation. It does
  not erase that ancestry reference.
- Canceled lookup callers can return while a dnsWaitGroupDone goroutine waits
  for completion (lookup.go352 onward). There is no public per-operation join.
- Native resolver calls acquire a global semaphore:500 on Windows and at most
  min(500, adjusted RLIMIT_NOFILE) on Unix. cgo's doBlockingWithCtx documents
  that its blocking worker can outlive the caller. This semaphore does not
  bound pure-Go DNS or all singleflight/waiter goroutines outside it.
- Existing runOutbound is sequential per configured peer, with finite active
  transport credits and reconnect/controlled-retry delays. It waits for the
  DialContext caller, not those hidden workers. Positive delays reduce observed
  churn but cannot prove a lifetime retained-generation bound without a
  completion bound or join for the detached workers.

Consequently V15-06's fixed-parent implementation is present and behavior-tested,
but its complete13MiB metadata proof is **OPEN**. Q4 parent counts or a stable
heap profile cannot substitute for this missing source bound. Read-only options
for the lead to assess are a bounded ownership/join integration around the
existing resolver/racer semantics, a separately approved resolver/connection
policy change, or an explicitly accepted reduced claim. No resolver selection,
Happy Eyeballs, context wrapper, bridge worker or retry policy was changed here.

A bounded public-API design search found no positive join witness. Resolver.Dial
sees only pure-Go DNS socket creation; Dialer.Control/ControlContext sees socket
setup, not racer retirement. DNSDone tracing is invoked on canceled caller
return. dnsWaitGroup is unexported, global and test-only. The existing sequential
outbound loop does not observe any of these hidden completions.

The smallest behavior-preserving architecture fork is an owned resolver/dial
implementation (or a pinned toolchain hook) exposing worker joins, including
native lookup and both Happy Eyeballs racers, plus bounded resolved-address
storage. A wrapper using WithoutCancel loses existing Deadline/Cause behavior;
it also does not join workers canceled by the dialer's own timeout. Permanently
retaining an owner after every ambiguous cancellation/successful race could
bound generations but changes retry/admission policy and has no positive resume
witness. These are read-only alternatives, not approved implementation changes.
The combination of complete descendant proof, unchanged net behavior and only
the current public net APIs cannot presently be satisfied by the fixed-parent
implementation alone.

Further pinned-source review identifies a general deadline retirement gap,
even if Windows outbound endpoints were restricted to numeric addresses.
`context.WithDeadlineCause` creates a `time.AfterFunc` closure capturing its
`timerCtx` (`context.go:646-654`). `timerCtx.cancel` stops that timer but does
not join an already started callback (`context.go:679-692`, `time/sleep.go:98`).
The deadline callback can cancel children, release the context mutex and pause
before parent removal; a concurrent deferred cancel can finish and permit slot
reuse while that original callback still retains the old context ancestry.
Without a completion witness or a scheduler assumption, active counts do not
bound these retired generations. This applies to the outbound dial, parse-budget
waits and topology operation deadlines. Timers already supplied by an external
caller are distinct from these newly owned deadlines.

Numeric Windows endpoints avoid resolver/singleflight and, for a single result,
the Happy Eyeballs race; Windows also joins its connect cancellation callback.
That conditional restriction therefore does not close the general timer gap.
No IP-only policy, DNS replacement, patched toolchain, scheduler assumption or
new accounting exclusion is selected or implemented. The lead verified the
source argument; this was design-aware review, not an independent certification
or an observed memory-ceiling breach.

## Executed checks and limitations

Windows source was changing during component work. These results prove their
named component state only; final frozen-source qualification belongs to the
lead. Evidence directory: `D:\codex-gocluster-v15-20261002\ownership`.

The owner-local Windows filesystem correction passed full helper normal in
10.359s, full helper race in9.262s, and vet with exit0. Both suites executed the
actual junction/follow and no-follow cases, failures at each junction close,
attributes/sharing fast paths, native append/rename/readonly removal, fresh-root
MkdirAll parity, logical-error backing, oversized first/second native replies,
overlong fatal-error priority and actual metadata-failure companion retirement.
No test was skipped. Fixed backing was parent528776/helper1138872; concrete
metadata buckets and the unchanged admission thresholds passed.

Evidence: `ownership/filesystem-20261003/normal-02.log`, `race.log`, `vet.log`.
The relevant source/file set was identical before and after race/vet, including
all helper/command/logutil source and root module files. Manifest SHA256:
`53528088b5c83d672558ec161146661c27bd1c4c65c8ecacaa1c63c746aed45d`.
Normal log SHA256:
`98676796d0ac1648d62f03ae10cf3347bcf8a693a473a30ddbb4905eb988aa0c`;
race log SHA256:
`d66e495d0bdcd2b7817df1d031b781fcfe9f6390567d9dd53b1d3d64a4e857c6`.
The first normal attempt did not compile because test hooks used a Handle
instead of the pinned CreateFile int32 template argument; it ran no tests and
is preserved as `normal-build-failure.log`. The earlier SQLite peer compile
caught two Windows constant namespaces; that separate failed compile is retained
by the SQLite owner. These failures were corrected without weakening checks.
The legacy predicate was exercised on this current Windows host; this is not a
claim of execution on an older Windows installation. The subsequent Linux helper gates are recorded next. Refreshed whole-release
companion packaging and final overall qualification remain required.

Actual Linux helper validation completed at2026-10-03T02:56:49UTC on the
current helper-only source packet `source-v15-helper-native-04.tar`, SHA256
`a2492b942815326be946808bc634299bed12ac365ed33d62f6e9827a585374b4`.
Its38-file raw-byte manifest includes helper/logutil/command source, root module
files, local replacement-module metadata and this evidence record as it stood
before execution. It is explicitly not a whole-repository/SQLite snapshot.
The reviewed wrapper checked current source bytes/file sets before execution;
the guest verified packet/source bytes and file sets before and after checks.

Native Go1.26.4 linux/amd64, CGO_ENABLED=1, GOMAXPROCS=2/GOGC=50/
GOMEMLIMIT=1536MiB ran inside the existing WHPX guest:2vCPU,4GiB,
q35,kernel-irqchip=off. Full helper normal and race each passed47 run entries,
with zero skipped/failed tests; Bash real elapsed times were1.66s/2.93s.
Native vet passed. The minimal companion dependency check passed113 packages,
without cluster/database/model initialization imports. Separate retained normal
and race binaries were executed from the package directory; each built and
exercised the actual companion. Package and both test-built companion hashes
were identical:
`c687d1eb9aad1aee778ebd16c4b88fb77c53a44fbec6c9d9584a43fec2c0020d`.
Normal test binary SHA256:
`e3d279804fcecdbd74331f13f581b5310d09409a1532250a088a76a99e622bbd`;
race test binary SHA256:
`76f4ada8420f317d30255e6ef8bf82244d10edd3ac6a546385c4bb83805f420f`.
All are verified ELF64 x86_64 artifacts; these were executed natively in Linux,
not counted from cross-compilation.

All raw logs, build metadata, dependency output, binaries and source witnesses
were retrieved under
`linux/helper-native-04-run-20261003T025631Z-6b71eddf248142938fb57c93106dc3cc`.
The result archive SHA256, checked on both guest and host, is
`17af9049ebf238d60ed56979d5bfb571adf1ff325fa452f50fe6e8de89e3f2fb`.
Normal/race raw logs have SHA256
`8a7aea3197ac7c79197d13f9a68ee41b221ba3334e8953a377bce187871b005c` and
`897a55b1ce82d2b24cd37156dc296af2e512ae41140a661f22e71cc5621ea451`.
Before each QMP mutation the wrapper verified PID/executable/creation/argv,
QEMU preflight hash and exclusive loopback listener ownership. Final process
inspection found no live Go/cgo/GCC/link/test/companion; the VM then positively
reported paused with unchanged resources. These are component results, not
whole-cluster qualification or a resolution of the context-descendant proof.

The later bounded native-path/retention repair used logs in
`D:\codex-gocluster-v15-20261002` itself. Full Windows helper normal/race passed
9.436/7.088 seconds (`helper-native-path-full-normal.log`,
`helper-native-path-full-race.log`), including the actual failed-FindClose
companion,2,000-entry directory and all prior lifecycle checks. After a
same-value constant extraction and executable-phase size assertion, affected
targeted race passed2.721 seconds and vet passed. Windows64C admission then
passed its affected normal/race checks in0.530/1.476 seconds
(`helper-cwd-charge-normal.log`, `helper-cwd-charge-race.log`), including the
literal7077-admitted/7078-refused cwd boundary. At that earlier stage fixed backing was
parent528776/helper1138848, including the16-byte scan owner. The final
metadata-owner correction adds24 bytes, producing the measured1138872 above. The subsequent
legacy-only parent workspace reservation then passed its literal boundary test
in normal and race binaries. Evidence is retained under
`sqlite/guid-baseline-20261003/helper-parent-{normal-corrected,race}.log`;
the initial unquoted PowerShell argument failure ran no tests and is retained
separately as `helper-parent-normal.log`. Earlier Linux
snapshots predate these Windows helper changes and do not prove their final
cross-platform integration.

- Latest targeted race lane: helper11.209s, peer17.829s, cluster1.159s, all pass.
  Command: `go test -race ./internal/peerdiag ./peer ./internal/cluster -run
  'Test(V15|PC92BookkeepingDiagnosticReasons|Session|OutboundHandshake|OutboundBackoff|PC92V14RetryDialFailure|ManagerReports|HandleFrameReports|Overlong|DirectionLabel|EventFileLogger|SpotLeaseInvalid)'
  -skip 'TestV15Topology' -count=1 -timeout=180s`. SQLite tests belong to the
  separate slice; this is an ownership lane, not a full-package waiver.
  Retained log: `ownership-targeted-race-windows.txt`.
- Latest full helper race passed6.084s, including actual-crash/disabled-daily,
  all-fields zero-allocation, native startup, sink/Close failure retirement,
  and constructor/retained-charge cases.
  Retained log: `helper-race-windows.txt`.
- Helper vet/staticcheck and lint passed on that same helper state; lint
  reported0issues in `helper-lint-windows.txt`.
- Event fuzz:201731 executions in5seconds, pass. Options-header fuzz:220352
  executions in5seconds, pass. Both used2 fuzz workers on the current helper
  code after native adapter, constructor guard and reservation corrections.
- Fixed-size/native-allocation check passed; retained
  `fixed-backing-windows.txt` contains parent528776/helper1138832 fixed bytes,
  native attributes48bytes and Winsock startup structures20504bytes.
- Source escape search for peer log calls, old diagnostic setters and Error()
  calls found no peer diagnostic escape. The remaining `func(string)` hook is
  the authorized raw protocol broadcast consumer, not a logging callback.
- An earlier combined race run included the full4096-node/131072-edge SQLite
  projection and failed its five-second context with `sqlite3: interrupted`.
  This was reported to the SQLite owner/lead; it is not relabeled as a pass.
- Actual initial Linux helper race run found a test assertion defect:
  ProcessState.Exited is false after signal termination. The test now checks
  the successful platform Wait witness. Source changed afterward; final Linux
  helper/context execution remains required. Linux snapshot/log owner retains
  `helper-diagnostic-01.log` and its manifests separately.
- The next actual Linux helper race passed10.556s under Go1.26.4, with exact
  snapshot archive SHA256
  `eba018bf07513ee13d729f4c2d8dc44eb3a98f22bcb931bf3acf6f5e60bf6e34`.
  Its source preceded the final environment coefficients, retained-charge
  Snapshot and sink failure additions; it remains diagnostic evidence.

Remaining lead acceptance includes fresh helper-allocation review and complete
context allocation proof,
final actual Linux peer/context/integration execution, matching whole-release
companion packaging/source manifests,
final full selected normal/static/race/tagged lanes and original Q1-Q6 workloads.
