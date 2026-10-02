# PC18/PC92 approved v11 execution ledger

The user authorized **Approved v11** on 2026-10-01. V11 incorporates the
complete v10 R01-R10 corrective scope proposed in the conversation and the
explicit selection that the one-second membership deadline also applies to
healthy established peers during recovery. Approval preceded implementation.

Baseline: branch `p92`, commit
`0d8a728354cc423f3d7929208aac2b72b05f9d5f`. Receiver reference:
`3e9b3621d94dd45c68702e4a0f896aac33f2a91d`. Existing v9 documentation changes
are preserved. This record does not supersede the inherited D1-D8/S01-S14,
[v6 contract](pc18-pc92-scope-ledger-v6.md),
[v7 qualification amendment](pc18-pc92-scope-ledger-v7.md),
[v8 parser/accounting amendment](pc18-pc92-scope-ledger-v8.md), or the negative
[v9 experiment](pc18-pc92-scope-ledger-v9.md).

## Authorized boundary

Correct all thirteen implementation findings F01-F13 and qualification
findings V01/V02 in Astra's audit of the baseline. Preserve IP publication,
A/C/D/K support, unsupported-action exclusion, selected CCCluster startup and
direct publication/transit boundaries, truthful PC18 identity, local user
admission, literal inbound authentication, private current-owner checks, and
all resource/no-eviction requirements. Keep production modernc SQLite.

No deployment, running-cluster restart, commit/push, database-driver replacement,
Pebble migration, new preference command, broad chat feature, or unrelated
cleanup is authorized. Complete C/A recovery remains mandatory with periodic
C/K disabled.

## Corrective slices

| Item | Approved implementation and distinguishing evidence |
| --- | --- |
| R01 / F01,F09 | Shared pure receiver-compatible wire normalization; validity and normalization stability; canonical publication/collision/private ownership; unrepresentable users remain local-only. Enforce active loader and constructor identities, argument/config agreement, root/login agreement, peer collisions/count, numeric metadata and publication limits before storage/workers. Local flags4/5; received external6/7 remain supported. Preserve literal allowlist/login/IP/password boundaries. Actual receiver alias/SSID/normalized-startup and unique/ambiguous/unique/IP tests. |
| R02 / F02,F08 | Grammar-aware transport-hop extraction and encoding; preserve positional empties, K revision and PC93 fields; reject malformed whole records without authority/cache/relay effects or terminal-hop backtracking. Collapse only unambiguous transport stacks. Implicit/empty self C preserves absent metadata. Hash all positional validated payload, excluding actual transport hop only; bounded keys retained. Repair peerprobe re-stripping. Literal field oracles, consumer tests, unaffected protocol regression and fuzzing. |
| R03 / F05,F06 | Membership key is canonical call plus node/user kind. Preserve simultaneous kinds, typed D and external node edges, user references and receiver-ordered metadata. Prepare bounded complete C without authority mutation, then delete absent typed keys and add/update under sole-controller ownership. No full graph clone or population-sized removal/undo array. Preflight cardinality, metadata and backing overlap; no hypothetical node reclamation. Receiver typed-state, multiparent, collision traversal, expiry, exact/one-over and atomic refusal tests. |
| R04 | Add `peer_pc92_typed_edges` with existing columns plus kind and PK(parent,call,kind). Preserve old edge table/rows as historical diagnostics, not a collapsed current view. Transactional/idempotent schema creation and atomic current nodes+typed edges replacement. Never restore live authority. Preserve36MiB active/queued/building projection reservation, deadline, cancellation/join. Upgrade/interruption/failed replacement/restart/rollback-upgrade tests compare complete generations. |
| R05 / F07 | At most two planned alternate-ingress additions for origin/external subject, combined count/byte preflight, all or none. Reuse dedupe first-admission elapsed offset with nonrefreshing lookup. No topology reapplication, relay, watermark/liveness renewal, or new retained side table. Test newer PC92/PC93 between original/duplicate, first-path loss, 0/1/2 slots, byte boundaries and UTC/elapsed separation. |
| R06 / F03,F11 | Restore normalized nonzero backoff base, saturating cap; enforce inherited300s maximum before duration conversion while preserving valid defaults/normalization. Fixed monotonic phase deadlines through reads/scratch/controller waits/startup retry/establishment; startup bounded by phase and existing5s allowance. One expiry-versus-commit winner. Commitment is separate from replay readiness: live reader stays parked until bounded FIFO replay completes. Reservations/replies retire exactly once before transport permit release. Preserve completed initial A during K retry; cleanup works after phase expiry. Real reconnect, queue/lease/reader expiry, commit races, replay/cancel/Stop/churn tests. |
| R07 / F04,V01 | Sole-controller bounded servicing covers lifecycle/input/replay/publication/clock/maintenance/cancellation. Every local timestamp consumer shares legal100-values-per-UTC-second progression with bounded membership progress. Immutable matching-revision C/A finishes before catch-up; retain bounded needed payload generations inside12MiB. No unrelated churn/recovery/K deadline reset. Include whole expiry/projection/stats work and<=1s cleanup lag. Unsafe clock closes/gates<=5s from actual fault including detection/scheduling; safe UTC beyond issuance and continuous advancing health for1s, recovery observed in next1s. Combined workload and per-recipient deadline tests. No extra authority owner or relaxed deadline without revised approval. |
| R08 / F10,F12,F13 | Restore named PC93 local announcements, distinguish invalid private candidates so they never broadcast. Preserve freshness/dedupe/private ownership. Consistent pure exclusions across normal/staging/full-mailbox paths and current-session check before gate. Persistent class-specific PC93 mailbox-refusal count, separate cache refusal, fixed bounded diagnostics. Count/byte saturation, after-drain stats, stale owner, local-origin alias and no cross-class failure tests. |
| R09 / V01,V02 | Qualification-only independently applied regression/freeze clock with real monotonic deadlines. External fault/closure/refusal/recovery observations and terminal reasons; observer failure/late evidence cannot pass. All five wrappers build once/run retained binary, hash inputs before/after build/after run, include scripts/config/assets/reference, fresh run identity/directory. One authoritative initially-unqualified final verdict; Go observations provisional. Per-wrapper behavioral positive/negative fixtures cover every failure and preserve open overall dependencies. No tamper-proof or edit-and-restore guarantee. |
| R10 | Re-derive changed storage/layout/scratch/overlap/retirement proofs without new headroom. Update support-critical comments, operator/config/peer docs, qualification/allocation evidence and support-agent cards/routing. Preserve durable ADR history and update ADR0050/0230 chain and relevant TSRs. Fresh final review maps each finding to code and distinguishing evidence. Audit correction completion and overall acceptance are separate results. |

## V11 timing clarification (controlling)

Every healthy established PC9x peer, including one recovering, must receive
successful outbound-control-queue admission of all records needed for a stable
eligible join, withdrawal or IP change within **one second of the eligible
local change/revision becoming available**. Scheduling/discovery delay counts.
Coalescing superseded intermediate states is permitted; unrelated churn cannot
restart the oldest stable obligation. Recovery alone is not an exemption.

Pending matching-revision C/A precedes catch-up. The five-second recovery
allowance never extends, suspends or restarts a membership deadline. If C starts
at0.0s and withdrawal becomes eligible at0.1s, matching A then withdrawal must
be queued by1.1s. A withdrawal at1.2s fails even if recovery finishes within5s.
One second measures queue admission, not receiver processing. The separate
externally observed complete C/A deadline remains5s after handshake completion.

Actual unsafe clock and complete-publication overflow close/gate established
and pending PC9x, stopping their transit/spot traffic while local users and
established legacy links continue. Authority admission failure closes/gates
the affected link and marks knowledge incomplete. Other class refusals neither
close sessions nor create replay backlogs. Real recovery condition must hold
continuously1s, then be observed within the next1s; reconnect backoff and fixed
handshake deadlines still apply. Remote completeness requires a valid remote C.

## Resource and qualification contract

The480MiB owned-data/backing/overlap ceiling includes SQLite engine storage.
Partitions remain spot96, graph/freshness96 (including5MiB transaction scratch),
other caches32, transport/parsing160, staging16, snapshots/projection48
(local12/database36), metadata/persistence32 MiB. Runtime stacks/GC overhead
and unchanged configuration are separately reported, not silently charged out
of protocol copies. This is not an RSS ceiling.

Existing caps remain:64 peers,128 candidates,192 transport owners,1000 locals,
4096 nodes,65536 users,131072 typed edges,262144 ingress observations,16384
freshness records. Candidate staging256 records/512KiB and global8192/16MiB
include active replay. Separate dedupe classes preserve strict age>600s expiry,
no unexpired eviction and no duplicate age renewal.

Unchanged workload:10000 new PC11/61/26 forwarding keys/minute (40/40/20),
100000 duplicates/minute; PC92 100new/sec (45%A/45%D/2%C/8%K), including two
8000-user C/sec; PC93100/min half private/announcement; bulletins20/min.
Q1:100 clients/16 peers/half topology45+11min plus shipped repeat.
Q2:100 clients/64 peers/full topology30+11min. Q3:six fixed5min burst cycles,
specified slow-peer windows and drain. Q4A/B:separate30min reachable pressure
phases. Q5:>=three real600s windows plus drain. Q6:repeated faults with periodic
timers enabled/disabled, receiver recovery, and20min receive-only.
Q1-Q3 retain per-client/per-minute/full-run p99<=5ms enqueue and<=25ms first byte.
Short diagnostics do not qualify.

Targeted checks precede one final relevant-state lane:
`go test ./...`, `go vet ./...`, `staticcheck ./...`,
`golangci-lint run ./... --config=.golangci.yaml`, `go test -race ./...`.
Parser/protocol fuzz, affected benchmarks/profiles, actual pinned receiver and
eligible unchanged long qualification are separately required. Do not infer
deployed-daemon or CCCluster evidence from the receiver component harness.

## Post-approval test review and dispositions

Three design-aware read-only specialist passes completed before source edits;
these are not independent normative reviews. All material findings were
accepted as refinements within approved scope; no new product decision arose.
The detailed matrices are retained with the implementation slice evidence.

- Replace wrong slash-preservation and hop-like-payload fuzz expectations; use
  valid active-constructor fixtures without weakening production validation.
- Use unmodified receiver normalization before channel construction and
  explicit Node/User queries so dual-kind state cannot be hidden by Route::get.
- Test ordinary C metadata, repeated same-kind C and adding a second kind
  against actual receiver selection; neither unconditional refresh nor blanket
  preservation is a sufficient oracle. A repeated metadata applies in order.
- Invalid callsign-like private destinations cannot fall through to named-chat
  broadcast; empty private targets are rejected defensively.
- Failed SQLite replacement changes proposed node identities/metadata too;
  compare both complete old sets. Inspect retained projection strings in
  addition to reservation counters.
- Record eligibility before actor handling and successful admission per
  recipient. Use actual elapsed service time rather than old ticker timestamps.
- Keep Session.Run parked until replay-ready, retain replay/request ownership
  through cancellation, and test complete maintenance under synchronized expiry.
- Apply clock faults outside the actor; replace erroneous minimum5s tests with
  maximum5s evidence, including worst sample phase and independent observer
  error classification. Fail closed in the authoritative final artifact.

## Execution status

Implementation followed approval and the detailed-review dispositions. The
production corrections and targeted audit checks are present, the normal full
repository lane passed, and full Q5/Q6 profiles were accepted. Corrected Q4 A/B
diagnostics passed with valid provenance. Runtime latency preflight failed;
overall acceptance is incomplete. Preserve two closeout results: audit
corrections and overall acceptance. See the [implementation and validation
record](pc92-v11-closeout.md) for item mapping, commands and limitations. The pre-existing
enabled-modernc native allocation, context backing and outer retirement proof
dependencies remain open. V11 authorizes changed-owner evidence, not a new
persistence backend/allocator or a waiver of the complete proof.

Stop for revised scope if a new authority owner, increased resource limit,
broader authentication, destructive schema migration or persistence redesign
becomes necessary. Missing required evidence remains incomplete.
