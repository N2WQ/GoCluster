# V15 optional stage diagnostic validation

Authority: approved V15-04. This extends the diagnostic warm workload only.
Implementation and check results below remain provisional until explicitly
recorded. It changes no formatter, protocol, queue, worker, runtime setting,
model or production Spot API. Design-aware review occurred; this is not an
independent certification.

## Completed controlled diagnostic pair

The retained fresh pair at
`D:\codex-gocluster-v15-20261002\warm-stage-pair-v2` completed on
2026-10-03 at 02:02:50 UTC. Its external verdict is `diagnostic_complete`, with
`comparison_valid=true` and `overall_accepted=false`. Both full 900-second load
plus 660-second drain runs passed source/assets, exact runtime/helper executable,
environment, complete delivery, 900-sample and three-profile-window integrity
checks. Original failed measurement reports remain unchanged. This archived
source precedes later helper/proof corrections and is not final-source acceptance.

| Observed metric | Stages off | Stages on |
| --- | --- | --- |
| Required/observed reads, all 116 recipients | 17,355,750 / 17,355,750 | 17,355,750 / 17,355,750 |
| Client spot enqueue endpoints | 15,000,000 | 15,000,000 |
| Client overall enqueue p99 upper-bound range | 4.5–5.9 ms | 4.6–6.2 ms |
| Worst client/input-minute enqueue p99 upper bound | 7.4 ms | 7.4 ms |
| Endpoints above five milliseconds | 180,021 | 190,817 |
| Violating enqueue cohorts | 1,450 | 1,337 |
| Client overall first-byte p99 upper-bound range | 11.8–16 ms | 12–16.2 ms |
| Worst client/input-minute first-byte p99 upper bound | 18.6 ms | 18 ms |
| Violating first-byte cohorts / other failures | 0 / 0 | 0 / 0 |

The enabled trace completed all 150,000 spot tokens. Every conditional segment
has the same 190,817 late client-spot endpoints. PC93/WWV account for 105,750
separate required client control reads; peer delivery and controls retain the
original checks and are excluded from the seven-stage spot chain.

| Segment | Ordinary observations | Conditional late p99 upper bound | Late endpoints whose segment exceeds 5 ms |
| --- | --- | --- | --- |
| Input publication to primary dedup ready | 150,000 | 23.7 ms | 88,000 (46.12%) |
| Primary ready to output queue receipt | 150,000 | 3.8 ms | 1,400 (0.73%) |
| Output receipt to delivery entry | 150,000 | 4.9 ms | 1,900 (1.00%) |
| Delivery entry to broadcast admission attempt | 150,000 | 1 ms | 200 (0.10%) |
| Broadcast admission attempt to receipt | 150,000 | 8 ms | 5,900 (3.09%) |
| Broadcast receipt to worker start | 1,500,000 | 12.2 ms | 16,900 (8.86%) |
| Worker start to client admission | 15,000,000 | 0.3 ms | 51 (0.03%) |

The first interval includes driver frame construction/send, TCP and peer parsing,
ingestion queue wait and dedup processing. It does not identify dedup CPU cost.
Percentages overlap and cannot be summed. Raw per-endpoint deltas telescope
before conversion; separately rounded segment bounds do not. The persisted
report has no raw histogram bins or sums, so no weighted mean or percentile
subtraction is asserted. A single controlled pair records observer influence
and observed intervals without isolating every runtime cause. Reducing writer
allocations could affect shared scheduling/GC, but this trace does not prove
that causal link or establish the five-millisecond enqueue criterion.

`diagnostic-summary.json` retains all fifteen cohort distributions, exact original
denominators, uncertainty crossings and artifact hash closure. The original
aborted `warm-stage-pair/off` remains separate standalone evidence; its missing
environment snapshot was not reconstructed. The strict fresh runner records
full environment presence/value hashes before each launch and allows only the
previously reproduced absent-to-empty restoration defect in fourteen managed
variables, then requires exact restored parity.

## Contract-to-test matrix

| Contract/invariant | Failure/boundary | Stimulus/fault | Observable result | Evidence level | False-green risk | Exact checker | Owner |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Original token and QPC | Reset clock or wrong deltas | Literal ticks 100/110/125/140/155/170/190/220 | Deltas 10/15/15/15/15/20/30, original input minute | Unit | Quantile subtraction or refreshed timestamp | TestPC92StageTraceLiteralChain | child stage owner |
| Complete token evidence | Missing/duplicate/unknown/unpublished/descending/overflow | Each common/worker slot and malformed token faults | Diagnostic invalid; no imputation | Unit and mutation | Surviving tokens become denominator | TestPC92StageTraceRejectsCorruptEvidence | child final pass |
| Actual fanout | Empty shard, changed mapping or dropped dispatch | Actual shard snapshot and dispatch markers | Exact expected mask and one worker timestamp per bit | Integration | Hardcoded ten workers | TestPC92StageTraceObservedFanout; TestQualificationStageMarkers | telnet |
| Total optional backing <=32 MiB | Oversized dimensions or report copies | Maximum rows/scalars/combined warm packet | Refuse before allocation; complete packet fits existing RPC | Allocation/unit | Row JSON copies or counting only lengths | TestPC92StageTraceBackingAndPacketBound | child owner |
| Retired ownership | Concurrent callback or second arm | Active drain, stale loaded callback, repeated constructor | No retired row access or second backing allocation | Deterministic/race | Removal falsely claimed to join preloaded callbacks | TestPC92StageTraceRetirement; TestPC92StageTraceObserverAllocation | stage guard |
| Causal accounting | Fast and late endpoints | Literal chain and original endpoint counters | Raw segments sum to original interval; late counts reconcile | Unit | Aggregate p99 differences claimed causal | TestPC92StageTraceAccountingAndLateAttribution | enqueue oracle |
| Observer overhead | Installed/absent observer | Actual QPC and synthetic-clock controls | Zero observer-core allocations; actual QPC overhead measured separately | Benchmark | Synthetic clock conceals real clock allocation | BenchmarkPC92StageTraceObserver; TestPC92StageTraceRealClockAllocation | fixture |
| Actual diagnosis | Real unchanged offered load | Same binary with stage switch off/on, 15+11 minutes | All original per-client/minute checks retained | Matched diagnostic | Microbenchmark or diagnostics promoted to acceptance | TestPC92RuntimeQualification | root |

## Stages and lifetime

Common indices are primary dedup ready to send; output-pipeline channel receipt;
delivery decision entry; BroadcastSpotOwned ready to send; broadcast consumer
entry. Worker slots mark each existing broadcast worker immediately before
delivery. The existing enqueue observer supplies the final endpoint QPC tick.
The output marker deliberately excludes toxicity or delayed-stage re-entry.

The diagnostic captures the actual shard/session assignment when arming and
checks it at completion. This stable-membership requirement applies only to the
warm fixture. Each dispatch publishes its expected bit before the existing
channel send; a consumer needs only its own published bit. A failed channel send
therefore leaves missing worker evidence and cannot silently qualify.

One process-wide reservation precedes all row allocation. Hooks are published
only after initialization. Closure removes the hook, closes the guard and drains
active users before validation. A callback loaded earlier may enter afterward,
but its closed recheck prevents row access; it is not described as joined.
Rows and retained oracle references are cleared before owner replacement.
The report reference is also cleared after finish captures the returned scalar
report. The actual warm service can arm only once: its nonzero measurement epoch
rejects subsequent arm requests. Thus preloaded callbacks retain at most that
single cleared controller shell. This bound is for that runtime fixture, not
arbitrary repeated calls to the generic test observer registry. Registry
reservation precedes backing allocation as well, so an occupied hook cannot
cause a rejected second array allocation. Publication is atomic after setup.

Rows never leave the child and retain no strings, Spots, raw frames, clients or
error interfaces. Summaries contain fixed numeric arrays only. Input-minute
cohorts use the original published input tick, not stage arrival time. Common
stage distributions count tokens; worker distributions count actual dispatched
workers; final-stage distributions count client admissions. Late attribution
counts each original over-five-ms endpoint across all seven segments.

## Backing derivation

Current amd64 layouts give ProtocolStats 296 bytes, WarmSample 376 bytes and
WarmProfile 338,624 bytes. Existing warm sample/report/RPC reservation is
`2*338624 + 6*2097152 = 13,260,160` bytes. The combined maximal packet must still
be checked; historical packet evidence alone is insufficient.

At 151,928 input slots and the actual ten workers, timestamps use 18,231,360
bytes; dispatch masks use 607,712 bytes. Two-array page rounding reserves 8,192
bytes. Two groups, sixteen cohorts and seven 2,068-byte histograms use 463,232
bytes, plus 8,192 rounding. Four fixed report owners are bounded by 16 KiB each;
two controllers/guards/membership snapshots by 4 KiB each. The reviewed maximum
is 32,652,576 bytes, below 33,554,432. Constructor size and dimension checks reject
larger layouts before allocation. Existing RPC buffers cover the combined packet
encodings; raw rows are never encoded or copied into a second process.

The inherited real QPC wrapper allocates two eight-byte objects per marker call.
The warm fixture has one primary dedup owner, one output owner, one broadcast
dispatcher and ten broadcast workers: thirteen simultaneous marker callers.
Conservatively allowing two separate sixteen-byte tiny allocation blocks gives
416 bytes of transient marker backing. This is included in the two-controller
8 KiB reservation and explicitly checked for the actual worker count. Returning
from a marker releases that ownership; runtime allocator/GC machinery remains
separately reported, as in the approved boundary.

## Execution evidence

Targeted normal and race checks passed, including all corruption, missing-slot,
literal arithmetic, input-minute, lifetime, actual dispatch and allocation gates.
The current backing checker measured the reviewed 32,652,576-byte reservation;
the maximal combined warm/stage packet is 1,451,076 bytes, below the unchanged
2 MiB RPC limit. Controller/report sizes are 1,472/7,408 bytes.

Three external source-overlay mutations each failed their intended assertion:
omitting missing-slot counts, omitting sticky counter-overflow failure, and
removing the actual telnet worker-dispatch marker. Original sources were not
modified by these controls.

The retained real-clock benchmark measured absent observers at 5.2-5.5 ns/op,
zero allocations, and installed markers at 132-135 ns/op, 16 B/op and two
allocations. The earlier synthetic-clock check isolates zero-allocation observer
logic; it does not establish a zero-allocation real marker. Shared QPC behavior
is unchanged. These microbenchmarks do not establish runtime latency or harmless
instrumentation. The same-binary off/on full warm diagnostic is still required.

Artifacts are under `D:\codex-gocluster-v15-20261002\stages`. No instrumented warm
workload or overall acceptance is claimed. The VM remains delegated to the
native validation owner; CPU slots are coordinated by the root agent.

The qualification-tagged vet warning in the inherited Windows mapping fixture
was corrected by reinterpreting the stored native address with the same pinned
native-memory idiom as the SQLite wrapper. Mapping size, scalar record layout,
publication and unmap/close behavior are unchanged. Tagged vet passed afterward;
the actual cross-process mapping/clock checks passed normally (0.146 seconds)
and with the race detector (2.218 seconds). Logs and exact commands are retained
in `mapping-checks.json`, `mapping-normal.log`, `mapping-race.log` and
`vet-mapping-fixed.log`.

The refreshed component binary and companion helper were built between
2026-10-03T00:04:59Z and 00:05:05Z. Full source/assets manifests before and after
that build were identical (SHA256
`2B0C5791FB79D41C4D0A3C7AFC4CDDF6AB935162EF87149EC19EF5E311F0B962`).
`final-build-manifest.json` records commands, sizes and artifact hashes:

- `runtime-stage-final.test.exe`:
  `D9F1B0AFE8A6B263C2D6D6E024BBC6B95CD42E2063701B9DEFC80EDBD5048B6C`.
- `peerdiag.exe`:
  `F4E72B77E5EFA95AE246ED2E4CEDAB6EB131A838220B51EBE520EA6E36366AFB`.

These identify the recorded component generation. This evidence paragraph was
added afterward; it does not claim that later source changes were in that
binary or establish final-source acceptance. The root owns the frozen source
and matched off/on diagnostic execution.
