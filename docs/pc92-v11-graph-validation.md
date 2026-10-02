# V11 graph, projection, and ingress evidence

This records the approved R03/R04/R05 implementation and targeted evidence on
2026-10-01. It is slice evidence, not completion of v11 or overall qualification.
The final repository lane, combined timing workload, complete owned-allocation
proof, and required long qualification profiles remain separate obligations.

## Contract and ownership

`graphNode.Members` uses `(canonical callsign, node-kind)` keys. A node and user
with the same callsign can coexist under one parent. Here/external flags remain
metadata. D removes exactly the requested kind; C replaces exactly the typed
subject population. User-reference accounting, expiry, external-subject edges,
and existing local/direct protections use the typed relationship consistently.

`pc92_graph_plan.go` prepares without changing authority. C traverses existing
membership to determine removals and later deletes only the currently yielded
missing entry. Addition and compaction occur after that traversal. The plan has
input-sized desired/addition storage and no population-sized removal slice.
D scans only input keys; A/K have no removal traversal. The controller remains
the sole graph owner between preparation and commitment.

The pinned receiver revealed metadata details that the old Go expectations did
not model. The lead dispositioned their corrections within R03:

- C processes metadata for a callsign when either typed relationship is new or
  the same kind repeats in the input. An ordinary retained member is not an
  unconditional metadata update. Tests distinguish all three cases.
- Repeated member entries preserve the last explicit IP; omission does not
  erase it. A missing node IP cannot reassert an older edge IP over a newer
  address learned from another parent.
- New member routes default Here to true, including clear wire Here bits.
  Existing node-member additions preserve Here established by an explicit node
  subject. Explicit node-subject updates remain separate.
- New node members retain the first version, defaulting to `5401` when absent or
  zero. Member entries do not establish node build. Existing node-member
  additions preserve authoritative node version/build; explicit subject
  updates can change them.
- Numeric version/build slots are ignored for stored user membership, matching
  `Route::User`. Original transit fields remain unchanged.

The oracle is unmodified DXSpider commit
`3e9b3621d94dd45c68702e4a0f896aac33f2a91d`, especially `DXProtHandle.pm`
`_add_thingy`, `pc92_handle_first_slot`, and `handle_92`, plus
`Route::Node::calc_config_changes`, node/user constructors, and add methods.
The adapter now reports node and user routes separately; `Route::get` alone
cannot prove simultaneous kinds. This is receiver-component evidence, not a
deployed daemon exchange or CCCluster claim.

## SQLite diagnostic generations

The current edge table is `peer_pc92_typed_edges`, with the former diagnostic
columns plus integer `kind` (`0` user, `1` node), and primary key
`(parent, call, kind)`. Schema creation is transactional and idempotent.

`peer_pc92_nodes` and `peer_pc92_typed_edges` are replaced together in one
transaction. A later insert failure retains both complete previous current
sets. `peer_pc92_edges` remains historical and is no longer updated by this
writer. Do not join historical edges with current nodes as a current snapshot.
Rows never restore live routing authority.

Upgrade/failure/retry and SQL-compatible rollback/upgrade tests pass. The
rollback test exercises the previous writer's table shape; it does not claim
execution of a retained old Go binary. Native SQLite working storage is not
proved bounded by these tests.

## Duplicate ingress lifetime

Fresh and duplicate external records use the same fixed pair of origin and
external-subject observation identities. Both missing slots and rounded string
charges are checked before either observation is added. Known observations
remain unchanged on duplicate reception.

`dedupeCache.firstAdmission` reads the existing elapsed-time offset under the
cache mutex after strict expiry pruning. It adds no cache or retained history.
Alternate observations use that original admission instant rather than a newer
PC92/PC93 watermark. Fresh ingress observations also use elapsed time, separate
from controlled UTC authority. Duplicates do not replay topology, relay, advance
freshness, or renew node liveness. Existing admission refusal still closes/gates
the affected source and invalidates its ingress knowledge.

## Changed allocation proof

The retained graph remains inside its 96 MiB partition, including 5 MiB reserved
transaction scratch. The typed membership entry has 112 logical bytes and a
conservative 128-byte allocation charge. The existing 256-byte edge coefficient
still covers every count through 131,072, including bucket growth/shrink overlap.
Node/index header and other coefficient checks also pass.

The scratch checker deliberately combines independent maxima:

| Temporary owner | Conservative bytes |
| --- | ---: |
| Typed desired entries and both bucket generations | 1,163,136 |
| Planned nodes and both bucket generations | 518,144 |
| Frame field backing | 139,264 |
| Decoded and addition arrays | 1,310,720 |
| Rounded normalized strings, fixed headers/direct index, active input wire | 335,879 |
| **Combined** | **3,467,143** |

The reachable sparse-origin fixture replaces 65,536 users with 8,191 mixed
members in a 65,368-byte C. Normal-build decode/prepare cumulative allocation
was 3,408,392 bytes against 5,242,880 reserved bytes. This complements the owner
arithmetic; cumulative allocation is not itself a proof of peak ownership.
Race execution retains semantic assertions but does not compare regexp pool
cumulative allocation with production, as documented by the existing test.

All active, queued, and building diagnostic projections still share 36 MiB.
No new per-edge projection field is retained: `kind` comes from existing flags.
Tests retain an active typed generation across graph replacement, verify both
its old strings and typed edges, and verify combined charges and exact release.
The 12 MiB local-publication reservation remains separate within the existing
48 MiB combined partition.

These are changed-owner results. They do not close enabled-SQLite engine,
context-child backing, or outer retirement ownership gaps in the aggregate
480 MiB claim.

## Contract-to-check mapping

| Contract / false-green avoided | Checkers |
| --- | --- |
| Both kinds, replacement/deletion, wrong-kind isolation | `TestPC92GraphTypedMembershipAndDelete`, `TestDXSpiderReferenceTypedMembership` |
| C metadata selection; repeated IP, numeric and Here behavior | `TestPC92GraphCMetadataSelection`, `TestPC92GraphOrderedMetadata`, `TestPC92GraphMemberHereAndExplicitSubject`, `TestPC92GraphMemberNodeNumericAuthorityAndAlternateIP`, corresponding `TestDXSpiderReference*` cases |
| Typed external parent edge, user references, expiry and collision-safe removal | `TestPC92GraphExternalSubjectPreservesSameCallUser`, `TestPC92GraphTypedExpiryAndCollisionReplacement`, existing protection/full-capacity/collision fixtures |
| Count, byte, rounded metadata and replacement overlap admission | Updated graph capacity/allocation tests and controller resource tests; refusal retains membership/freshness/cache authority |
| No retained C removal array; bounded typed scratch/index history | `TestPC92GraphTypedScratchEnvelope`, `TestPC92GraphSparseOriginReplacementScratch`, `TestPC92GraphIndexAllocationEnvelopes`, shrink/churn tests |
| Additive schema and late DDL failure roll back | `TestTopologyTypedSchemaUpgradePreservesHistoricalRows`, `TestTopologyTypedSchemaFailureIsAtomic` |
| Complete old nodes and edges survive failed replacement | Upgrade test compares every selected column in both previous current sets, plus `TestTopologyProjectionAtomicReplacementAndFailure` |
| No restore; rollback-compatible SQL; projection overlap and shutdown | `TestTopologyProjectionDoesNotRestoreAuthority`, `TestTopologyTypedProjectionRollbackUpgrade`, `TestPC92ProjectionRetainsTypedGenerationThroughGraphReplacement`, existing bound/storage-stop tests |
| Duplicate external observations admitted together | `TestPC92DuplicateExternalIngressAtomicCapacity`, `TestPC92DuplicateExternalIngressByteBoundary`, `TestPC92DuplicateExternalPreservesKnownObservation` |
| Original admission age survives newer PC92/PC93, ingress loss and UTC faults | `TestPC92DuplicateIngressUsesPayloadAdmissionAge`, `TestPC92DuplicateIngressElapsedClockDomain`, `TestDedupeFirstAdmissionDoesNotRefresh`, expired-payload test |

## Executed targeted validation

The following passed after the final functional changes in this slice:

```text
go test ./peer -run 'Test(PC92Graph|PC92Projection|Topology|PC92Controller|PC92Resource|PC92Duplicate|DedupeFirst)' -count=1
go test -tags qualification ./peer -run '^Test(PC92DuplicateIngressElapsedClockDomain|QualificationClockSeparatesAuthorityAndPayloadExpiry)$' -count=1
go test -race ./peer -run 'Test(PC92Graph|PC92Projection|Topology|PC92Duplicate|DedupeFirst|DedupeConcurrent|PC92SlowStorage)' -count=1
go test ./peer -run '^TestDXSpiderReference(TypedMembership|CMetadataSelection|RepeatedMetadata|MemberHereAndExplicitSubject|MemberNodeNumericAuthority)$' -count=1 -v -timeout=2m
go test ./peer -run '^TestPC92Graph(TypedScratchEnvelope|SparseOriginReplacementScratch|IndexAllocationEnvelopes)$' -bench '^BenchmarkPC92Graph(RemovalTraversal|TypedDelta)$' -benchmem -benchtime=100ms -count=1 -v
```

Observed package durations were 2.789s, 0.150s, 17.095s, 10.582s, and 0.816s,
respectively. Receiver environment used the pinned checkout and existing
`gocluster-dxinterop-perl-54021` Perl/dependency tree. Go build cache, temporary
files and test state were redirected to `D:/codex-gocluster-v11-20261001` after
the system drive filled during parallel compilation; no global Go environment
setting was changed.

On Windows/amd64, Intel Core i9-10900, removal traversal A/D/C reported
0 bytes and 0 allocations per operation. The 8,192-member C traversal measured
312,750 ns/op; the single-member A prepare/commit fixture measured 1,479 ns/op,
584 bytes/op and 8 allocations/op. A separate one-second CPU profile measured
1,501 ns/op with the same allocation counts. Prominent sampled work included
hashing, allocation-charge calculation, metadata merge, and preparation.
These are diagnostics without a comparative baseline, not whole-controller
latency qualification or a performance-improvement claim.

Profile and retained test executable:
`D:/codex-gocluster-v11-20261001/evidence/graph-cpu.pprof` and
`D:/codex-gocluster-v11-20261001/evidence/graph-profile.test.exe`.
