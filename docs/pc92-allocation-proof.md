# PC18/PC92 owned-allocation proof record

The selected ceiling is **480 MiB of owned protocol data and backing storage**, including overlapping active generations. Go stacks, GC/runtime bookkeeping and unchanged configuration storage are reported separately. This is not a process RSS limit. The aggregate proof remains open for enabled SQLite persistence and the final remaining-metadata inventory. No final 30-minute Q4 acceptance is claimed.

The derivation targets Go 1.26.4, windows/amd64. `allocationBytes` covers small-object classes and whole 8 KiB pages; `pointerAllocationBytes` includes an allocation type header. Rounded owned storage is included. Unreachable garbage awaiting GC, runtime heap arenas and stack backing are separate runtime overhead. A toolchain/architecture change requires rechecking the source and allocation oracles.

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

These sum to 480 MiB. Driver sockets, fixture wire and evidence objects are qualification owners. Ordinary telnet, archive, logging and ingestion owners are distinct from the protocol partitions. Independent heap peaks are never summed and called a simultaneous observation.

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
| Decoded members | 663,552 |
| Desired entries + bucket generations | 1,032,080 |
| Planned nodes + bucket generations | 518,144 |
| Removal slice | 1,122,304 |
| Addition slice | 663,552 |
| Subtotal | 4,138,896 |

At most 24,577 normalized call/version/build strings partition 64 KiB of input; rounded storage is at most 253,959 bytes. A further 16 KiB covers fixed graph/plan/index headers and the 64-peer direct index. Include another 64 KiB for the active input wire after its mailbox reservation is released. Preparation is bounded by 4,474,775 bytes, below 5 MiB. The sparse-origin test exercises 65,536 existing users replaced by a maximum mixed C, independently of the full-node fixture.

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

The first staged record reserves a 256-string array (4,864 bytes); each wire owns a rounded compact clone. Individual 256-record/512 KiB and global 8,192-record/16 MiB bounds all apply. The active establishment batch retains its charge until release. Failed/losing candidates cannot advance global topology or freshness.

| Independent boundary | Population | Charge |
| --- | --- | ---: |
| Global count | 32 candidates × 256 records × 128-byte wire | 8,192 records /1,204,224 bytes |
| Global bytes | 80 candidates × 62 +48 × 61 records; 2,016-byte wire rounds 2,048 | 7,888 records /16 MiB |
| Individual count | 256 × 128-byte wire | 256 records /37,632 bytes |
| Individual bytes | 253 × 2,016 +one 1,280-byte wire | 254 records /512 KiB |

Local snapshots include at most 1,000 users and 64 peers. Current/published/temporary fixed indexes, sorted/member/delta arrays, membership adapter copies and encode work share 12 MiB. Existing login syntax bounds admitted calls at 15 bytes. Remote session metadata uses explicit oversized markers; fitting version/build values are cloned at no more than ten bytes each. Unpublishable received values are not retained in publication snapshots. Optional absent metadata preserves its own marker; later valid replacement clears it.

A deliberately overcombined local inventory reserves 1 MiB for all indexes, 270,720 bytes for five generations of compact entry strings, 1,187,840 bytes for provider/adapter slices and borrowed login/address backing, 2,228,224 bytes for member/delta/decoded arrays including growth overlap, and 1 MiB for encoding intermediates. This is 5,783,936 bytes, below 12 MiB. The address allowance is 512 bytes per local owner, including a Windows interface zone name; ordinary global IP literals use at most 39 bytes. The final platform audit must retain that address-source premise. Encoding bounds use at most 1,128 entries, 512 bytes of intermediate material per entry, 64 KiB field headers, three 96 KiB wire/join copies and 64 KiB fixed formatter/regexp work.

Projection reservations include arrays and every string a snapshot can retain after live replacement. All queued/building/active generations share 36 MiB. Oversized projection is refused whole; live authority remains intact. Stop joins producers/consumer before draining. Native SQL working storage is a separate dependency below.

## Remaining aggregate dependencies

The latest fixed-index inventory conservatively charges 813,904 bytes across small indexes, including four publication generations and both failure indexes. Retry wire can occupy three 64-entry generations (blocked, active drain, new manager failures): 12 MiB. The layout oracle reports a 2,752-byte session (3,072 rounded), 144-byte reader wrapper (160 rounded), 224-byte TCP descriptor (240 rounded) and 80-byte cancel context (96 rounded). Including fixed channels, closures, compact metadata and address material gives 5,632 bytes per live/stale identity, reserved as 8 KiB. Active-only wrappers, timers, semaphore waiter and I/O control work total 3,312 bytes, reserved as 4 KiB per live owner.

A provisional inventory includes 192 live owners, 256 queued-input references, 128 lifecycle references, one active actor reference and 64 outbound-loop retirement references: 641 distinct session identities. Combining these reservations with 12 MiB retry wire, 1 MiB indexes and 1 MiB global metadata totals 19.7578125 MiB. This is not yet a final headroom grant: outer goroutine retirement and standard-library context-child-map high-water backing still need explicit disposition. Heap context/timer/closure data cannot be silently excluded as stack overhead.

Separately reported unchanged configuration ownership includes the raw loaded configuration and the baseline's normalized ACL/registry storage: parsed global/per-peer IP ACLs, allowed-call normalization/map, disabled-row-dependent outbound slice capacity, credentials and endpoint formatting. Source comparison against the pre-change baseline established that these owners were not introduced by this work. Newly introduced publication keys, wire clones and remote numeric metadata remain charged above.

Enabled SQLite topology persistence is not yet proved inside the remaining allowance. Two serial worker paths can own two DB transactions. Each Exec binds only one row; bindings/statements are released before the next. However the default approximately 2 MiB page-cache target is soft, existing file headers can alter it, and transaction/WAL/native allocator backing is not covered by snapshot reservations. On Windows, modernc uses 64 KiB VirtualAlloc granularity outside Go MemStats. Global hard-heap limits would affect all application SQLite users. No setting, accepted-file policy or exclusion was silently introduced. Default configuration disables this optional store; that does not prove its enabled case.

## Q4 execution and evidence

The script records source hashes, runtime/hardware settings and JSON observations. Full A/B require 30 minutes after setup and at least three real-expiry cache fill/release/refill windows, alongside queue/active-write/reader/candidate pressure. Authentication, duplicate ownership and handshake/write deadlines remain active. The narrow writer latch pauses an already dequeued write; it changes no admission/count/deadline and ends on cancellation or the original deadline.

A uses 1,000 real local logins, 64 established peers and 128 prelogin candidates. B uses 1,000 locals, 63 peers and 128 authenticated candidates for the last identity; its reachable ingress maximum is 258,048. Graph/freshness/cache population enters through ordinary wire; only the agreed historical setup clock seam prepares old freshness cohorts. Staging compares actual origin watermarks and verifies only the winner acquires authority.

The first preflight-a reached all cache entry limits, full graph/freshness, 192 owners, 8,127 control records, 8,190 data records, 64 active writes and 13,369,344 reader bytes together, then released candidates. ProcessHeap 736,646,128 and StackInuse 52,166,656 include driver/application/GC owners; they are not the protocol bound. Evidence is in `%TEMP%/gocluster-q4-preflight-a-current`. Source changed during that diagnostic and timed pressure lasted 10.16 seconds.

The first preflight-b also passed: 1,000 local users, 63 established peers and 128 authenticated candidates, full reachable graph/freshness and cache entry counts, exactly 16 MiB/7,888 staged records, 7,998 control records, 8,062 data records, 63 active writes and 13,299,712 reader bytes together. The staged origin watermark remained unchanged. A separate completion race selected one winner and advanced its staged freshness, then returned to 63 established peers. ProcessHeap was 736,642,336 bytes and StackInuse 51,806,208 bytes at the simultaneous pressure sample. Evidence is `%TEMP%/gocluster-q4-preflight-b-current`; source changed during setup, and the pressure phase lasted 10.00 seconds.

These are reachability diagnostics, not final-state or 30-minute acceptance. Diagnostic profiles never substitute for full phases; reports retain Qualified=false while aggregate proof is open.
