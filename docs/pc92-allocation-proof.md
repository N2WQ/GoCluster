# PC18/PC92 owned-allocation proof record

## Session cancellation publication correction (2026-10-03)

The session-local cancellation mutex and terminal flag restore safe close-before-
installation behavior without adding a context, goroutine, timer or retained
payload. On Go 1.26.4 windows/amd64, the session grows from 2,792 to 2,808 bytes;
both round to the existing 3,072-byte allocation class. The current layout gate
reports 5,632/8,192 bytes of per-owner fixed inventory and 3,312/4,096 bytes of
active-only inventory. No reservation is enlarged. See
[TSR-0037](troubleshooting/TSR-0037-peer-session-cancellation-publication.md).

This structural check does not close the remaining aggregate allocation,
retirement or long-running qualification obligations below.

The selected ceiling is **480 MiB of owned protocol data and backing storage**, including overlapping active generations. Go stacks, GC/runtime bookkeeping and unchanged configuration storage are reported separately. This is not a process RSS limit. The aggregate proof remains open for the final enabled-SQLite inventory, unjoined standard-library dial/resolver descendants and final-source qualification. Approved v15 replaces the historical persistence and diagnostic owners described below; its current accounting is summarized first. No final 30-minute Q4 acceptance is claimed.

The derivation targets Go 1.26.4, windows/amd64. `allocationBytes` covers small-object classes and whole 8 KiB pages; `pointerAllocationBytes` includes an allocation type header. Rounded owned storage is included. Unreachable garbage awaiting GC, runtime heap arenas and stack backing are separate runtime overhead. A toolchain/architecture change requires rechecking the source and allocation oracles.

## Current v15 ownership changes

[Approved v15](pc18-pc92-scope-ledger-v15.md) subdivides the existing32MiB
metadata ceiling into SQLite16MiB (engine8, WAL6, host2), diagnostics3MiB
(parent1, helper2), and other metadata13MiB. No partition or aggregate increase
is implied. The historical modernc and direct-logger descriptions below record
why earlier proofs remained open; they no longer describe the v15 implementation.

The topology-only SQLite fork has one serialized connection and one global
reservation covering construction, operation and failed retirement. Its source
inventory and actual native/fallback/Linux evidence are tracked in
[the SQLite record](pc92-v15-sqlite-validation.md). The generated engine remains
byte-identical to its pinned source. Constructor path work precedes engine/file
owners; simultaneous phased maxima, rather than independent measurements, govern
the host proof. The full prescribed sustained gate and final-source Linux
capacity result remain required.

A successfully closed topology store leaves a small zeroed shell in the single
production Manager. The pinned amd64 source inventory is 5,040 bytes of heap
shell/gate/scalar accounting plus 4,272 bytes of process-static reservation state.
A 16 KiB allowance includes these inside the existing other-metadata global
allowance; their combined 9,312 bytes are not added outside 480 MiB. The current
Windows owner layout was read from the executed native-owner binary's DWARF;
the new directory owner changes the static term while the heap allocation
still rounds to the same size class. Successfully released engine, file,
path and statement backing is absent from this shell. Failed retirement remains
fully charged through its strong owner. Arbitrary sets of stopped API objects
retained by an external caller are distinct from the production Manager's
bounded owner history. This source inventory does not close the remaining
aggregate proof; the SQLite record retains its derivation and validation state.

Peer diagnostic records now enter a fixed mailbox and one companion generation.
All callbacks into the shared logger were removed from the peer diagnostic path.
Failed process/native cleanup keeps a strong charged owner and gates replacement.
[The ownership record](pc92-v15-ownership-validation.md) inventories fixed records,
path/environment expansion, file maintenance, process startup and retirement.
Helper Linux connect cancellation may finish after DialContext returns, but the
helper dials only once and the parent joins the whole process before replacement.

Fixed context parents remove manager-root map churn and limit each parent to one
active operation child. They do not join hidden Go resolver/Happy Eyeballs workers
or Linux connect-cancellation callbacks. Standard deadline timer callbacks also can outlive cancellation and slot reuse, including parse and topology deadlines. Retained old generations remain an open
source proof; fixed parent counts, a stable heap and successful cancellation
cannot close it. No networking behavior, retry policy or accounting exclusion has
been changed to hide this gap.

V16's harmonic expiry index belongs to ordinary shared ingestion history. Its
live-count/spare-capacity/overlap inventory is separately reported in
[the history record](pc92-v16-performance-validation.md), consistent with the
existing separation of shared ingestion owners from protocol partitions.
## Partitions

| Owner | Ceiling | Included storage |
| --- | ---: | --- |
| Spot dedupe | 96 MiB | Cloned keys, fixed buckets, linked entries, fixed expiry array. |
| Graph/freshness | 96 MiB | Nodes, edges, users, ingress, watermarks, strings, spare/index-overlap storage and 5 MiB controller transaction reserve. |
| Other dedupe | 32 MiB | Independent PC92, PC93 and bulletin cache storage. |
| Queues/readers/writes | 160 MiB | Output backing/payloads, active writes, input mailboxes, reader storage and shared scratch. |
| Candidate staging | 16 MiB | Fixed candidate arrays and rounded wire clones, including active establishment drain. |
| Snapshots/projection | 48 MiB | 12 MiB local publication work and 36 MiB across all optional projection generations. |
| Remaining metadata | 32 MiB | Session/controller/registry structures, contexts/timers/waiters, retry records and persistence working storage; final inventory open. |

These sum to 480 MiB. Driver sockets, fixture wire and evidence objects are qualification owners. Ordinary telnet, archive, unrelated logging and ingestion owners are distinct from the protocol partitions. This does not exclude heap copies of peer protocol diagnostics merely because the standard logger owns them; those copies require an explicit bound. Independent heap peaks are never summed and called a simultaneous observation.

## Cache proof

Each cache has fixed bucket and expiry arrays and one allocated entry per key. Hash collisions cannot grow an index directory. Deletion clears the entry so a popped heap pointer cannot retain a key alongside its replacement. For cardinality N and logical key-byte limit B, the independent oracle uses:

```
ceil(5*B/4) + 7*N       rounded key storage
+ 48*N                 pointer-bearing entries
+ 16*N                 fixed bucket and expiry arrays
+ 2*8192 + 4096         array rounding, structs and tiny-allocation slack
```

`dedupe_allocation_test.go` checks the string inequality independently against every allocation class and larger page transitions. The fixed slack includes tiny-allocation fragments on the qualified two-P runtime.

| Owner | Entry / logical byte limits | Complete bound |
| --- | --- | ---: |
| Spot | 131,072 / 64 MiB | 93,212,672 bytes |
| PC92 + PC93 + bulletin | 65,536 / 8 MiB each; 8,192 / 2 MiB | 33,542,144 bytes |

Full count/byte, collision, constant-occupancy churn and expiry/refill tests support these source bounds. TotalAlloc evidence is supporting evidence, not a substitute for a live-ownership proof.

## Graph and sole-controller scratch

Explicit bucket arrays retain high-water capacity charges until compaction and include old/new arrays during grow/shrink. Storage does not depend on a runtime map remaining proportional to current len.

| Owner | Maximum count | Fixed bytes each |
| --- | ---: | ---: |
| Node | 4,096 | 512 |
| Membership edge | 131,072 | 256 plus rounded entry strings |
| User reference | 65,536 | 132 |
| Origin/ingress observation | 262,144 | 160 plus both rounded strings |
| Freshness record | 16,384 | 196 |
| Controller transaction/static work | 1 | 5 MiB |

Fixed coefficients plus scratch total 94,699,520 bytes, leaving 5,963,776 bytes for variable strings under 96 MiB. The full short-key fixture charges 5,341,184 variable bytes and 100,040,704 total. Metadata admission includes old/new replacement overlap and removed strings retained by the current plan. Received numeric metadata is charged, not narrowed to publication's ten-digit limit.

The independent graph envelope tests cover every high-water count to each cap, allocator rounding and both bucket generations. Preparation deliberately overcombines these maxima:

| Working backing | Bytes |
| --- | ---: |
| Frame fields | 139,264 |
| Decoded and addition arrays | 1,310,720 |
| Typed desired entries + bucket generations | 1,163,136 |
| Planned nodes + bucket generations | 518,144 |
| Subtotal | 3,131,264 |

C removal uses two bounded traversals and retains no population-sized removal
slice. Including rounded normalized strings, fixed headers/direct index and the
active input wire, the independent combined bound is **3,467,143 bytes**, below
5 MiB. The reachable sparse-origin fixture replaces 65,536 users with 8,191
mixed members and measured 3,408,392 bytes cumulative decode/prepare allocation.
The [v11 graph evidence](pc92-v11-graph-validation.md) preserves the detailed
arithmetic, receiver metadata semantics and limitations.

Decoding precedes those large plan arrays. Its retained frame/member/string storage is about 1.01 MiB; only one entry's colon array, numeric builder, regex and IP work is active. Colon arrays have at most four elements. The callsign regex has 75 instructions and an acyclic longest path 70; newly required backtracker storage fits 40 KiB. Even an eleven-wire-size normalization allowance plus active input/encoding copies fits the 5 MiB envelope. Existing larger objects borrowed from process-wide regexp pools remain shared-library retention, not graph generations.

## Transport proof

| Owned term | Reservation |
| --- | ---: |
| 64 established normal and 64 control lanes | 128 MiB |
| 64 active established writes | 4 MiB +128 bytes |
| PC92/PC93 input backing and rounded payload | 4 MiB |
| Shared reader/parser temporary pool | 8 MiB |
| 192 stable reader/raw/remainder/read/writer owners | 14.25 MiB |
| 128 pending control-channel backings | 770,048 bytes |
| 128 pending owners × 128 native replies × 8-byte allocation | 131,072 bytes |
| Pending startup output, including active transfer | 128 × 2 KiB |
| Pending PC18 formatting overlap | 128 × 2 KiB |
| Optional Ziutek input buffers | 192 × 256 bytes |
| Total | 159.65625 MiB +128 bytes |

The remainder inside 160 MiB is 360,320 bytes. Session structs and compact queue-age rings belong to metadata, not this wire-buffer table. Each existing 1 MiB lane quota includes channel backing and rounded payload. The configured normal count remains an upper bound; the byte quota derives storage before make, including extreme integer configurations. Only the registration winner allocates a normal channel. Terminal cleanup releases buffers after workers join and before replacement registration.

A 192-owner reservation is acquired before session construction/dial and held through terminal callback/cleanup. Pending handshakes retain their separate 128 cap; established identities retain 64. Slow retirement callbacks cannot accumulate additional transport owners.

Each stable reader permits 64 KiB rounded raw plus 4 KiB remainder (or aggregate), 4 KiB read and 4 KiB writer buffers. The first terminator is in the newest chunk, bounding remainder. The 65,537-byte lookahead and old/new growth overlap occur only inside a temporary lease. Large consumed backing and terminal closures are released.

Each reader batch leases 288 KiB after socket read and before Feed/growth/copy. Its 128 KiB native-parser allowance plus 73,728-byte aggregate, 65,536-byte raw copy and 4,096-byte remainder total 274,432 bytes. It releases before blocking read or frame parsing, so these leases do not nest. Cancellation and original phase deadlines bound waits.

Startup retains one PC18, one initial A and K (or legacy PC19), small prompts/login and completion markers. A successful initial A is recorded; a timestamp-rate retry resumes K without repeating A. Maximum initial A is 154 wire bytes (160 allocated), K at most 83 (96 allocated), and complete PC18 at most 1,024. Two KiB covers finite retained startup output plus its active transfer. Outbound passwords borrow the exact immutable configuration string: logical queue accounting remains, but there is no extra large clone. Unchanged configuration backing is reported separately.

## v8 spot scratch

PC11/PC61/PC26 are charged before splitting:

```
65,536 +64*F +32*L +24*C +128*T
```

L is total wire bytes; F caret fields; C payload-field-four comment bytes; T ASCII-space/tab token count. Header case/trimming and malformed/extra fields follow ordinary parsing. Since F<=L-C+1 and 2 T<=C+1, a 64 KiB input requires at most 7,929,984 bytes; another 288 KiB reader batch can progress beside it.

The parser's exact arrays contain 80*T token bytes, T consumed bytes and 16*T output headers. Streaming scanner cursors allocate no match slice/index and preserve original transitions, precedence and Unicode-offset behavior. The remaining coefficients cover input/output strings, case conversion, report/time parsing, fixed regexp scratch, rounding, and invalid-call callback normalization. Invalid-call diagnostic parsing is mutually exclusive with successful parsing, not nested. Persistent application logging remains a distinct owner.

Frozen-oracle tests cover 12,288 parser cases; maximum-taxonomy adversarial measurements support the source table. Standalone TotalAlloc is not the peer lease proof. Non-peer callers retain their input-relative O(L+T) work and existing admission owners; 64 KiB is the peer transport envelope, not a new universal spot parser restriction. Peer tests cover real parsing, cancellation, deadline, handler error and Stop, zero final usage, pool peaks and reader progress beside a maximum spot lease.

## Staging and generation ownership

The first staged record reserves a 256-string array (4,864 bytes); each wire owns a rounded compact clone. Individual 256-record/512 KiB and global 8,192-record/16 MiB bounds all apply. Up to64 active replay batches retain their original full charge until release.
The sole controller services one record per replay turn; no live input reader
starts before its replay-ready reply. Cancellation retires the batch before its
transport permit is released. Replay changes ownership, not storage partitions. Failed/losing candidates cannot advance global topology or freshness.

| Independent boundary | Population | Charge |
| --- | --- | ---: |
| Global count | 32 candidates × 256 records × 128-byte wire | 8,192 records /1,204,224 bytes |
| Global bytes | 80 candidates × 62 +48 × 61 records; 2,016-byte wire rounds 2,048 | 7,888 records /16 MiB |
| Individual count | 256 × 128-byte wire | 256 records /37,632 bytes |
| Individual bytes | 253 × 2,016 +one 1,280-byte wire | 254 records /512 KiB |

Local snapshots include at most 1,000 users and 64 peers. Current/published/temporary fixed indexes, sorted/member/delta arrays, membership adapter copies and encode work share 12 MiB. Existing login syntax bounds admitted calls at 15 bytes. Remote session metadata uses explicit oversized markers; fitting version/build values are cloned at no more than ten bytes each. Unpublishable received values are not retained in publication snapshots. Optional absent metadata preserves its own marker; later valid replacement clears it.

A deliberately overcombined local inventory reserves 1 MiB for all indexes, 270,720 bytes for five generations of compact entry strings, 1,187,840 bytes for provider/adapter slices and borrowed login/address backing, 2,228,224 bytes for member/delta/decoded arrays including growth overlap, and 1 MiB for encoding intermediates. This baseline is 5,783,936 bytes. V11 conservatively adds64 maximum64KiB
immutable recovery wires and512KiB for one decoded recovery/baseline generation:
**10,502,528 bytes**, below12MiB. Recovery wires commonly share storage within a
fanout, but the proof does not depend on that sharing. The address allowance is 512 bytes per local owner, including a Windows interface zone name; ordinary global IP literals use at most 39 bytes. The final platform audit must retain that address-source premise. Encoding bounds use at most 1,128 entries, 512 bytes of intermediate material per entry, 64 KiB field headers, three 96 KiB wire/join copies and 64 KiB fixed formatter/regexp work.

Projection reservations include arrays and every string a snapshot can retain after live replacement. All queued/building/active generations share 36 MiB. Oversized projection is refused whole; live authority remains intact. Stop joins producers/consumer before draining. Native SQL working storage is a separate dependency below.

## V14 changed-owner accounting

`peering.max_peers` is required and ranges1�64. Logical established/replay/retry
owners use N; pending candidates remain128 and transport owners are N+128.
Worst-case qualification and this ceiling still use N64. Publication backing
uses1000+N, N+1 and1000+2N, reserving actual enabled identities only.

The v14 layout gate on Go1.26.4 windows/amd64 conservatively charges **28,032
incremental bytes**, below32KiB in the existing metadata partition:

| Changed owner | Incremental rounded backing |
| --- | ---: |
| Manager/coordinator, including MaxPeers and timer pointers | 9,472 |
|24 receipt bytes/session across641 retained identities; session2808 stays in3072 class | 0 |
|One lifetime-owned reusable timer per identity;288 bytes each �64 | 18,432 |
|Two overlapping samples containing six retry phase counters | 128 |

The timer charge includes runtime timeTimer, hchan and time.Time element backing.
Each identity keeps the same timer across candidates, history resets and later
overload episodes. Stop alone can leave a runtime-heap zombie; per-attempt
allocation would not prove a bound from live waiter count. Synchronous handshake
waiting returns/stops before terminal owner release, so replacement cannot
concurrently reset the same timer. Runtime per-P timer heap bookkeeping remains
reported as runtime overhead under the existing accounting boundary.

No control-channel item was widened, no additional wire generation/graph plan
is retained, and one5MiB graph scratch owner remains. Normal-build observations
are scalar phase counts; qualification events remain callback-bounded. The
[test matrix/evidence](pc92-v14-validation.md) distinguishes this changed-owner
check from outstanding aggregate ownership proofs.

## Historical v12 incremental proof and remaining aggregate dependencies

The superseded v12 design added **16,064 bytes** of conservatively rounded fixed recovery bookkeeping
on Go 1.26.4 windows/amd64, below its approved 32 KiB addition inside the existing
32 MiB metadata partition. `TestPC92V12RecoveryFixedStateEnvelope` accounts for
64 active episode entries, 128 overlapping pending-handoff entries, actual
controller/manager/cache allocation-class growth, one cache interval tracker
and an 8 KiB allowance for the sole receiver method-value backing. It does not
add a wire generation, decoded-plan collection or graph owner. Normal and
qualification-tagged race checks passed this layout bound.

This is an incremental fixed-state bound, not a complete ownership proof.
The new scratch-lifetime/retirement experiment and overlapping global-gate
regression remain open. V12's sustained service gate failed before long
qualification, so no aggregate memory or runtime acceptance follows from this
small fixed-state result; see [v12 evidence](pc92-v12-validation.md).

The pre-v14 fixed-index inventory conservatively charged 954,320 bytes across small indexes, including five publication generations
and the64-entry active-replay index and both failure indexes. The superseded design reserved12MiB for three refused-wire generations; v14 retains none. This removal does not grant unproved SQLite headroom. The layout oracle reports a 2,784-byte session (3,072 rounded), 144-byte reader wrapper (160 rounded), 224-byte TCP descriptor (240 rounded) and 80-byte cancel context (96 rounded). Including fixed channels, closures, compact metadata and address material gives 5,632 bytes per live/stale identity, reserved as 8 KiB. Active-only wrappers, timers, semaphore waiter and I/O control work total 3,312 bytes, reserved as 4 KiB per live owner.

A provisional inventory includes 192 live owners, 256 queued-input references, 128 lifecycle references, one active actor reference and 64 outbound-loop retirement references: 641 distinct session identities. The pre-v14 combination with12MiB retry wire,1MiB indexes and1MiB global metadata totaled19.7578125MiB; v14 removes refused-wire ownership and adds the separately proved fixed coordinator/timers above. This is not yet a final headroom grant: outer goroutine retirement and standard-library context-child-map high-water backing still need explicit disposition. Heap context/timer/closure data cannot be silently excluded as stack overhead.

V11 also bounds new fixed global owners: copied active configuration (at most64
peer structs plus canonical identity allocations), up to128 queued requests,
192 active callers,64 replay replies and two startup/shutdown calls; request
messages/attempts/timers/channels,192 candidate/replay headers and fixed
controller storage. The deliberately overcombined layout inventory is351,536
bytes inside the existing1MiB global allowance. It does not prove context child
backing or unrestricted outer goroutine retirement. The cumulative PC93 input
refusal counter adds no history. These changed-owner checks cannot grant the
remaining provisional SQLite allowance.

Separately reported unchanged configuration ownership includes the raw loaded configuration and the baseline's normalized ACL/registry storage: parsed global/per-peer IP ACLs, allowed-call normalization/map, credentials and endpoint formatting. V11 removes disabled-row-dependent
active registry capacity; newly copied normalized configuration is charged above. Source comparison against the pre-change baseline established that these owners were not introduced by this work. Newly introduced publication keys, wire clones and remote numeric metadata remain charged above.

Enabled SQLite topology persistence is not yet proved inside the remaining allowance. Two serial worker paths can own two DB transactions. Each Exec binds only one row; bindings/statements are released before the next. However the default approximately 2 MiB page-cache target is soft, existing file headers can alter it, and transaction/WAL/native allocator backing is not covered by snapshot reservations. On Windows, modernc uses 64 KiB VirtualAlloc granularity outside Go MemStats. Global hard-heap limits would affect all application SQLite users. No setting, accepted-file policy or exclusion was silently introduced. Default configuration disables this optional store; that does not prove its enabled case.

The isolated [v9 persistence experiment](pc92-persistence-feasibility-v9.md)
rejected its proposed driver candidate at the early allocation/WAL gates.
Its provisional 10 MiB allowance is not proven production headroom, and the
production modernc driver remains unchanged. Enabled persistence and the
remaining metadata inventory still block the complete aggregate claim.

## Q4 execution and evidence

Current v11 results are in the [implementation record](pc92-v11-closeout.md).
The original v11 B diagnostic exposed cross-socket ordering in the pressure
fixture: small refill records could fill count slots before the intended large
frames. Correcting fixture admission order preserves the required byte/count
populations and original write deadlines. It does not close the aggregate proof.
The corrected A/B diagnostics passed their measurement and provenance checks;
the implementation record preserves their populations and diagnostic limits.
The earlier preflights described below are historical evidence.

The script records source hashes, runtime/hardware settings and JSON observations. Full A/B require 30 minutes after setup and at least three real-expiry cache fill/release/refill windows, alongside queue/active-write/reader/candidate pressure. Authentication, duplicate ownership and handshake/write deadlines remain active. The narrow writer latch pauses an already dequeued write; it changes no admission/count/deadline and ends on cancellation or the original deadline.

A uses 1,000 real local logins, 64 established peers and 128 prelogin candidates. B uses 1,000 locals, 63 peers and 128 authenticated candidates for the last identity; its reachable ingress maximum is 258,048. Graph/freshness/cache population enters through ordinary wire; only the agreed historical setup clock seam prepares old freshness cohorts. Staging compares actual origin watermarks and verifies only the winner acquires authority.

The first preflight-a reached all cache entry limits, full graph/freshness, 192 owners, 8,127 control records, 8,190 data records, 64 active writes and 13,369,344 reader bytes together, then released candidates. ProcessHeap 736,646,128 and StackInuse 52,166,656 include driver/application/GC owners; they are not the protocol bound. Evidence is in `%TEMP%/gocluster-q4-preflight-a-current`. Source changed during that diagnostic and timed pressure lasted 10.16 seconds.

The first preflight-b also passed: 1,000 local users, 63 established peers and 128 authenticated candidates, full reachable graph/freshness and cache entry counts, exactly 16 MiB/7,888 staged records, 7,998 control records, 8,062 data records, 63 active writes and 13,299,712 reader bytes together. The staged origin watermark remained unchanged. A separate completion race selected one winner and advanced its staged freshness, then returned to 63 established peers. ProcessHeap was 736,642,336 bytes and StackInuse 51,806,208 bytes at the simultaneous pressure sample. Evidence is `%TEMP%/gocluster-q4-preflight-b-current`; source changed during setup, and the pressure phase lasted 10.00 seconds.

These are reachability diagnostics, not final-state or 30-minute acceptance. Diagnostic profiles never substitute for full phases; reports retain Qualified=false while aggregate proof is open.


## Continued ownership review (2026-10-02)

The current application supplies a no-deadline cancellation parent to
Manager.Start. Sessions and outbound dial roots jointly consume N+128 transport
permits; the two serial optional projection paths add at most two immediate
timeout children. Thus this caller bounds live direct manager children by
N+130 (194 at N=64). An arbitrary earlier-deadline Start caller needs its own
subtree inventory. This is a live-child bound, not a backing-allocation proof.

Go1.26.4 context.removeChild deletes entries without rebuilding the child map.
Source review of Swiss-map insertion, tombstone pruning, table splitting and
directory growth found no fitting hard backing bound from that cardinality.
A symbolic legal-hash arrangement can force a 1024-slot table split with at
most 188 live keys during construction, below 194. Deletion neither merges
empty siblings nor shrinks the directory. This is not an observed context-map
runaway, a remote hash-control capability, or a measured 480 MiB breach. The
source argument and its actual-pointer-hash reachability limits are retained in
`D:\codex-gocluster-v14-remaining-20261002\ownership-probe\context-child-map-review.md`.

The blocked-logger diagnostic disproved whole-session retention for its tested
rejected-inbound path: all measured session and original error objects became
unreachable while 160 post-Run wrappers waited with zero transport permits.
That does not reclaim the distinct formatted log buffers. Standard log.output
formats into an exclusively owned heap buffer before acquiring outMu, and only
returns that buffer after Write finishes. manager.go's terminal inbound log is
after Run releases the owner permit. A blocked standard sink with a responsive
connection-event reporter therefore permits additional formatted peer-error
buffers to wait outside the transport-owner cap. The pool's per-buffer return
limit and downstream line-buffer limit do not cap their concurrent count.
These peer-derived heap copies need accounting; they are not runtime stacks.

The external diagnostic and source review narrow the retirement concern rather
than closing it. Enabled SQLite, context backing and the formatted diagnostic
ownership remain unresolved. See [v14 evidence](pc92-v14-validation.md#continued-overall-qualification-on-2026-10-02).
