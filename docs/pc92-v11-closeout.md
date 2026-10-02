# PC18/PC92 v11 implementation and validation record

Authority: `Approved v11`, branch `p92`, baseline
`0d8a728354cc423f3d7929208aac2b72b05f9d5f`. The
[approved ledger](pc18-pc92-scope-ledger-v11.md) controls scope. Existing v9
experiment documentation remains preserved; production keeps modernc SQLite.
No commit, push, deployment or running-cluster restart occurred.

Implementation is present. Audit-correction validation and overall acceptance
are separate results. The final normal repository lane, targeted audit checks,
full Q5 and Q6, cache-memory profile, and corrected Q4 A/B diagnostics passed.
Runtime preflight failed its enqueue-latency target. Overall
PC18/PC92 acceptance and the complete 480 MiB owned-allocation claim remain open.

## Approved item mapping

| Item / audit finding | Implementation | Distinguishing evidence |
| --- | --- | --- |
| R01 / F01,F09 | `config/peering_contract.go`, manager constructor, canonical publication/private ownership | Actual receiver normalization and startup; loader/direct constructor rejection before storage; alias collisions, current IP and literal authentication tests. [Wire evidence](pc92-v11-wire-validation.md). |
| R02 / F02,F08 | `peer/protocol.go`, `keys.go`, `pc92_codec.go`, peerprobe | Literal positional payload/hop vectors, queue/staging round trips, complete key distinctions, implicit C metadata, actual receiver, both fuzz targets. Wire evidence. |
| R03 / F05,F06 | Typed graph keys and `pc92_graph_plan.go` prepare/commit | Actual receiver dual-kind and ordered metadata comparisons; exact/over limits, atomic refusal, expiry/collision removal, sparse C scratch. [Graph evidence](pc92-v11-graph-validation.md). |
| R04 | Additive typed SQLite edges and atomic projection replacement | Upgrade/DDL failure/rollback-compatible SQL; complete old nodes and edges survive late insert failure; retained projection strings and reservation retirement; no authority restore. Graph evidence. |
| R05 / F07 | Dedupe first-admission lookup and atomic alternate ingress plan | Newer PC92/PC93 watermarks cannot renew original payload age; 0/1/2-slot and byte boundaries; authority UTC separated from elapsed expiry. Graph evidence. |
| R06 / F03,F11 | Backoff base/reset, fixed phase deadline, establishment winner, bounded FIFO replay owner | Real TCP failure/reconnect sequence; queued/reply expiry, late committed reply, parked live reader, two-owner replay, cancellation and Stop retirement tests below. |
| R07 / F04,V01 | Sole-owner rotating scheduler, reserved membership timestamps, immutable C/A then catch-up, maintenance phases and clock gate | Captured-pair tests; all64 recipients under combined service pressure; actual Q6 regression/freeze/publication closure and recovery; actual receiver timestamp burst. [Qualification evidence](pc92-v11-qualification-validation.md). Sustained qualification is separate. |
| R08 / F10,F12,F13 | Named PC93 local delivery, private rejection, shared pure PC92 eligibility, persistent PC93 mailbox refusal counter | Consumer callbacks, stale-owner/full-mailbox exclusions, count/byte/concurrent refusal and sampled diagnostic tests. Wire evidence; qualification guards include mailbox refusals. |
| R09 / V01,V02 | External qualification clock/observers; five build-once wrappers and authoritative fail-closed finalizer | 64 wrapper behavioral fixtures, 33 report-type fixtures, external terminal-reason/deadline tests; actual profile results recorded separately. Qualification evidence. |
| R10 | Changed owner arithmetic, operator/config/support docs, ADR/TSR history and final review | Allocation layout/scratch/overlap tests; this mapping and final diff review. Enabled SQLite, context backing and outer retirement proofs remain open. |

## Lifecycle and publication checks

`peer/pc92_handshake_deadline_test.go` exercises real controller and session
boundaries. A queued establishment that expires cannot register or replay.
An establishment committed before its deadline remains the winner when its
replay response arrives later. The actual `Session.Run` reader cannot consume
live PC92 while replay is pending. Two staged owners interleave service turns
without reordering either batch. Sixteen cancellation cycles retire the retained
wire batch and reply exactly once. `Stop` joins a parked session and controller
retirement before the transport permit is released. Existing active-drain and
192-owner retirement tests cover staging/transport overlap.

`TestOutboundTCPBackoffAfterEstablishedDisconnect` uses a real TCP listener and
the outbound loop: dial failure, handshake failure, establishment, disconnect,
and another failure. Both a positive configured base and a nonpositive sentinel
produce nonzero retries after reset. Constructor validation bounds positive
milliseconds before conversion; nonpositive sentinels are clamped before
conversion so extreme negative integers cannot wrap into positive delays.

The seven new lifecycle/reconnect tests passed in 8.243 seconds. The same cases
plus active staging and retirement ownership tests passed under race detection
in 9.975 seconds. These are targeted results, separate from the final full lane.

Recovery is reserved at registration commitment, before any replay service turn.
This matters because the new owner immediately becomes a publication recipient:
deferring recovery marking until replay completion could admit an ordinary delta
before C/A. Fresh review found that integration error and the implementation was
corrected before final validation. The combined scheduler test verifies the
original complete C/A precedes membership catch-up at each recipient.

`TestPC92PublicationRecoveryFinishesCapturedPairBeforeCatchup` retains the old A
after C and then checks withdrawal/join/IP catch-up without restarting recovery.
`TestPC92InitialRateRetryResumesAfterAcceptedA` crosses UTC rollover without
repeating the accepted initial A. The actual receiver burst test separately
checks all100 legal values, timestamp exhaustion, periodic-K coalescing and the
next-second emission; it does not incorrectly require periodic requests to send
synchronously. Zero-periodic-C tests still require complete recovery.

## Changed resource owners

Final changed-owner arithmetic reports 10,502,528 bytes for local snapshot,
delta and frozen recovery overlap within the existing12MiB reservation;
954,320 bytes for the conservative small-index inventory; and351,536 bytes for
the deliberately overcombined fixed global inventory. A lifecycle request is
charged640 bytes. Session layout is2,784 logical bytes, charged3,072; the
overcombined per-owner bookkeeping allowance is5,632 within8KiB, and active
owner bookkeeping3,312 within4KiB. These calculations do not replace the open
context/retirement proofs. Graph transaction scratch is3,467,143 bytes within
5MiB; projection active/queued/building reservations remain36MiB.

The candidate's existing staged record array and wire reservations transfer to
the bounded replay owner. Replay does not create another staged-wire generation.
The existing buffered reply channel also serves as the terminal retirement
fence. Fixed64 replay slots/index entries and bounded immutable recovery wires
are included in the changed metadata/local-publication inventory.

## Documentation and review

Support-agent docs impact: **Required**. Peer operation, diagnostics and
qualification interpretation changed; the support card and routing index were
updated alongside the authoritative documents.

Operator and support changes are in `peer/README.md`, `docs/domain-contract.md`,
`data/config/README.md`, the peer bulletin support card and troubleshooting
index. They distinguish one-second queue admission from receiver processing,
preserve literal authentication, describe typed historical/current SQL tables,
and retain both qualification/proof limitations. ADR0231 records the durable
corrections; ADR0050/0230 remain historical with explicit refinement links.
TSR0035 and the decision/troubleshooting indexes preserve the negative v9
experiment and explain evidence interpretation.

The final code-quality review is lead-owned, supplemented by bounded
design-aware specialist review. It is not an independent normative review.
The unmodified pinned DXSpider receiver supplies the external compatibility
oracle; receiver-component results do not establish deployed-daemon or
CCCluster interoperability. Source, callers, tests and the final diff were
reviewed for authority, bounded ownership, cancellation, authentication,
ordering and diagnostic behavior. Review findings were fixed within v11;
remaining proof and workload gaps are not converted into passes.

## Final execution results

Final normal `go test ./...`, `go vet ./...`, `staticcheck ./...` and
`golangci-lint run ./... --config=.golangci.yaml` passed. Lint reports0 issues.
The normal test run used the pinned receiver environment. Its first attempt
exposed the obsolete immediate-periodic-K assumption; the targeted receiver test
and complete normal rerun passed after correcting that checker. The failed log
is retained alongside the rerun. `go test -race ./...` also passed on the final
relevant production Go state. Later Q4 changes affect only tagged cluster test
fixtures; their own targeted checks and actual reruns are recorded separately.

The retained-binary wrappers observed the following. All listed completed runs
passed their source/binary provenance checks; a negative measurement remains a
negative result even when provenance is valid.

| Artifact directory under `D:/codex-gocluster-v11-20261001` | Result and interpretation |
| --- | --- |
| `runtime-preflight` | Failed: 71 of 100 clients exceeded the exact enqueue-tail allowance during 20 seconds of load. This separate run retained CPU/allocation profiles. |
| `runtime-preflight-plain` | Failed: 90 of 100 clients exceeded that allowance; client enqueue p99 upper bounds were 4.5-6.5 ms against 5 ms. First-byte bounds were 11.8-16.6 ms against 25 ms. All required deliveries arrived without renaming, cache/mailbox refusal or peer gates. |
| `cache-memory` | Accepted cache-only profile. Three full spot-cache cycles retained at most 56,622,976 bytes; all four classes together retained 82,182,776 bytes, including 25,559,584 non-spot bytes. This does not prove the entire subsystem ceiling. |
| `q6-full` | Accepted full Q6 profile, 1,578.43 seconds total. All 36 fault cases passed, including real handshake expiry and normal/disabled periodic timers. Maximum clock-fault closure was 4.2237418 seconds; maximum observed gate recovery was 1.7111948 seconds. Actual pinned-receiver recovery checks passed. The 20-minute receive-only phase admitted all 2,200,000 arrivals from 200,000 distinct keys, forwarded none and retained zero spot-forwarding keys/bytes. |
| `q5-full` | Accepted full Q5 profile, 2,605.14 seconds total. All four classes completed three real 600-second windows plus drain, preserving class isolation, expected saturation refusal, expiry and refill. Exact short cleanup-deadline evidence is supplied by the separate combined scheduler diagnostic; the soak is not the Q1-Q3 latency measurement. |
| `q4-preflight-a` | Initial diagnostic passed in 589.81 seconds, with 1,000 clients and 64 peers. Its queue contents nevertheless exposed the same mixed-record setup race diagnosed in B; corrected-fixture verification is recorded below. |
| `q4-preflight-b` | Initial diagnostic failed in 516.74 seconds: one full-count data queue held only 929,952 bytes. The winner-race cycle released all candidate/staged reservations correctly. A cross-socket fixture ordering race prevented the later required byte-pressure population; this is not an observed production quota violation. |
| `q4-preflight-a-barrier` | Corrected diagnostic passed in 606.65 seconds including setup; the diagnostic phase lasted 10.1626485 seconds. Reached 1,000 clients, 64 peers, 128 candidates, full graph/freshness and all four cache limits, with 8,127 control records, 8,190 data records and 64 active writes simultaneously. |
| `q4-preflight-b-barrier` | Corrected diagnostic passed in 606.58 seconds including setup; the diagnostic phase lasted 10.0001118 seconds. Winner selection/release passed, followed by 63 peers and 128 candidates with 7,888 staged records occupying exactly 16 MiB, full reachable graph/freshness and cache counts, 7,998 control records, 8,062 data records and 63 active writes simultaneously. |

Runtime preflight used 3,334 new spot keys, 33,340 duplicate arrivals, 2,000
PC92 records, 34 PC93 records and seven bulletins. Each client had 3,334 required
spots, so at most 33 observations could exceed 5 ms. The exact counters failed;
no threshold-uncertainty crossings explained those failures. Both preflights
used the same executable hash. Sampled CPU concentrated in the telnet writer
and Windows socket-write path; controller work accounted for 4.42 percent and
publication/maintenance for 0.88 percent. These overlapping CPU samples do not
identify the cause of tail latency. The enqueue observer conservatively includes
scheduling before its post-admission timestamp; these profiles cannot separate
that delay from the preceding pipeline. A bounded read-only review found no
concrete v11 defect explaining the misses. No runtime tuning or wider telnet
optimization was performed under this corrective scope.

Q5 and Q6 ran concurrently, with Q4 diagnostics and the brief cache-memory run
also overlapping parts of their elapsed-time windows. Each wrapper retained its
declared runtime settings; these are isolation/recovery/capacity observations,
not uncontended ordinary latency measurements. The two ordinary runtime
preflights ran separately, before the soak runs. All protocol production source
remained unchanged throughout. Subsequent Q4-only test corrections and final
documentation updates do not replace the preserved manifests for these runs.
The corrected Q4 A/B diagnostics ran concurrently with each other after the
other profiles finished; both passed all before/build/after provenance checks.

The Q4 correction requires actual queue admission of the intended large frames
before sending small cache-refill frames on other sockets. The initial failure
reconstructs exactly as 2,560 backing bytes plus 113 records of 8,194 bytes and
15 records of 98 bytes. Successful socket writes alone could not establish the
required cross-socket order. The correction preserves the one-million-byte
targets, count targets, original held-write deadline and later simultaneous
cache/reader/transport checks. No production or peer API change is required.
The admission barrier rejects owner loss, quota overflow, cancellation and late
observations. It reserves half the unchanged two-second held-write lifetime for
the later checks. Its 21 regression subcases passed normally in 0.142 seconds
and under race detection in 1.171 seconds. Scoped tagged cluster lint retains
30 existing findings; none is in the new barrier or oracle tests.

Both corrected socket diagnostics observed at least 1,035,004 data bytes and
1,038,460 control bytes at every established recipient in the pressure sample.
Candidates and staged reservations returned to zero after release. The A/B
reader-backing samples were 13,377,536 and 13,307,904 bytes respectively. The
wrapper verdicts correctly leave `profile_accepted=false` for these diagnostic
runs and `overall_accepted=false`; passing their measurement is not full Q4.

Full Q1-Q3 and the shipped-Q1 repeat were not executed after the failed latency
preflights; those required results remain open. Full Q4A/B cannot establish
acceptance while the enabled-SQLite, context-backing and outer-retirement
allocation proofs remain unresolved. Short A/B diagnostics do not substitute
for their 30-minute phases. The cache-sustained profile was not repeated; the
actual Q5 soak and targeted changed-cache checks are reported at their own scope.
None of these missing results is waived or marked passed.

Targeted logs are retained under `D:/codex-gocluster-v11-20261001/evidence`;
profile directories above also retain executable, manifests and verdict.
Per-process
cache/scratch variables use D: because C: lacked build space. No global Go or
runtime-cluster configuration changed. Existing tagged qualification lint
findings remain separately reported in the qualification evidence; the normal
repository lane does not compile those tagged files.

Final documentation cross-reference and troubleshooting-record checks passed,
as did `git diff --check`. No required tool was unavailable. The final review
found no further in-scope correction; failed and missing qualification evidence
still prevents an overall passing closeout.

## Reviewed Go file inventory

The changed/new Go files in the final review are listed here explicitly. The
three slice evidence records above identify their material test oracles;
test-fixture source under scripts is included in the script fixture checks.

```text
cmd/peerprobe/main_test.go
cmd/peerprobe/main.go
config/peering_contract_test.go
config/peering_contract.go
internal/cluster/pc92_q4_pressure_oracle_test.go
internal/cluster/pc92_q4_pressure_test.go
internal/cluster/pc92_q4_runtime_qualification_test.go
internal/cluster/pc92_runtime_driver_test.go
internal/cluster/pc92_runtime_load_test.go
peer/backoff_test.go
peer/backoff.go
peer/dedupe.go
peer/inbound_handshake_harness_test.go
peer/inbound_handshake_test.go
peer/inbound_listener_test.go
peer/keys_test.go
peer/keys.go
peer/manager_test.go
peer/manager.go
peer/outbound_backoff_tcp_test.go
peer/pc92_allocation_test.go
peer/pc92_codec.go
peer/pc92_controller_test.go
peer/pc92_controller.go
peer/pc92_duplicate_clock_qualification_test.go
peer/pc92_duplicate_ingress_test.go
peer/pc92_eligibility.go
peer/pc92_graph_allocation.go
peer/pc92_graph_benchmark_test.go
peer/pc92_graph_index_test.go
peer/pc92_graph_interop_test.go
peer/pc92_graph_plan.go
peer/pc92_graph_scratch_test.go
peer/pc92_graph_test.go
peer/pc92_graph_typed_test.go
peer/pc92_graph.go
peer/pc92_handshake_deadline_test.go
peer/pc92_handshake.go
peer/pc92_identity_interop_test.go
peer/pc92_initial_retry_test.go
peer/pc92_interop_test.go
peer/pc92_lifecycle_regression_test.go
peer/pc92_mailbox_v11_test.go
peer/pc92_membership.go
peer/pc92_metadata_allocation_test.go
peer/pc92_process_restart_test.go
peer/pc92_projection_typed_test.go
peer/pc92_projection.go
peer/pc92_publication_test.go
peer/pc92_publication.go
peer/pc92_q5_qualification_test.go
peer/pc92_q6_fixture_test.go
peer/pc92_q6_qualification_test.go
peer/pc92_receive.go
peer/pc92_resources.go
peer/pc92_scheduler_qualification_test.go
peer/pc92_scheduler.go
peer/pc92_staging_allocation_test.go
peer/pc92_test_config_test.go
peer/pc92_wire_v11_test.go
peer/pc93_test.go
peer/pc93.go
peer/protocol_bookkeeping_test.go
peer/protocol_fuzz_test.go
peer/protocol_test.go
peer/protocol.go
peer/qualification_clock_test.go
peer/qualification_clock.go
peer/qualification_disabled.go
peer/qualification_enabled.go
peer/qualification_test.go
peer/qualification_topology.go
peer/registry_test.go
peer/session_keepalive_test.go
peer/session_transport.go
peer/session.go
peer/timestamp.go
peer/topology_apply_test.go
scripts/testdata/pc92qualificationtool/main.go
```
