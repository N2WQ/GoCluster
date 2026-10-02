# PC18/PC92 qualification contract and evidence

**Current authority:** [approved v14](pc18-pc92-scope-ledger-v14.md) replaces
admission headroom simulation with controlled retries and a required YAML peer
cap. See [v14 evidence](pc92-v14-validation.md) for current results. Overall
acceptance remains open; historical passes below do not qualify the final v14
source.

**Historical V12 evidence status:** the2c06079 re-audit invalidated the earlier ordinary
admission-recovery timing oracle. Its combined five-second Q6 wait could accept
late recovery; Q5 observed after the last expiry and its lookups could prune.
Historical run results remain below, but do not prove corrected recovery timing.
The [v12 record](pc92-v12-validation.md) tracks fresh evidence. Its sustained
63-blocked/one-live service gate failed: repeated exact recovery checks exhausted
the input mailbox and closed the healthy source, while the zero-blocker control
passed. V12 stopped at its explicit design boundary. Overall acceptance remains open. The
[retained CTY refresh](cty-refresh-2c06079.md) also requires exact new-asset
manifests for future runtime qualification.

The approved spot-forwarding target is **10,000 distinct new PC11/PC61/PC26
keys per minute across all peers**, with 100,000 duplicate arrivals per minute.
Copies delivered to different peers do not count as additional keys. PC92 uses
its separate target of 100 new records/second. These rates are acceptance
inputs, not measured claims.

## Historical v11 execution status (2026-10-01)

V11 corrections are implemented with targeted validation; full acceptance is not complete.
V11 authority was the [v11 ledger](pc18-pc92-scope-ledger-v11.md); the subsequent
v12 stop was superseded for admission recovery by [v14](pc18-pc92-scope-ledger-v14.md).
Slice evidence: [wire](pc92-v11-wire-validation.md),
[graph/projection](pc92-v11-graph-validation.md),
[qualification checker](pc92-v11-qualification-validation.md).
Current command results and limitations are in the
[v11 implementation record](pc92-v11-closeout.md): the normal full lane, full
Q5/Q6 and cache-memory profile passed; two runtime preflights failed enqueue
latency. Corrected Q4 A/B diagnostics passed with valid source/binary provenance;
their short duration does not qualify the full Q4 profiles.
Older measurements below are historical and do not qualify the v11 final source. The inherited scope and current
implementation mapping are in [the v6 execution record](pc18-pc92-scope-ledger-v6.md),
with the controlling qualification amendments in [v7](pc18-pc92-scope-ledger-v7.md)
and the shared-parser amendment in [v8](pc18-pc92-scope-ledger-v8.md).

The subsequent [approved v9 persistence experiment](pc18-pc92-scope-ledger-v9.md)
is complete with a negative feasibility result. Its proposed driver candidate
failed the early ownership/allocation/compatibility gates; see the
[evidence report](pc92-persistence-feasibility-v9.md). Production modernc SQLite
is unchanged. This result does not replace Q1-Q6 or complete the 480 MiB proof.

V11 wrappers build once, retain the executable, and compare manifests before
build, after build and after execution. The sole authoritative final JSON begins
unqualified and can accept a profile only after every required checker passes. Go case
reports are provisional. Script/config/assets/reference inputs and the binary
are included. Reported overall acceptance remains false while complete owned
allocation proof and required final-source profiles are missing. This detects
persisting changes, not adversarial edit-and-restore or tampering.

Clock regression/freeze is applied atomically outside the controller. Measure
five seconds from actual fault to closure/gating, including detection and
scheduling. Continuous safe advancing UTC must hold for one second, then be
observed within the next second. Membership queue admission is measured from
producer-side eligibility per healthy established recipient, including recovery;
no unrelated publication or five-second recovery window resets that deadline.

Historical component evidence (before v11; current results are linked above):

- The actual pinned DXSpider receiver modules accepted both startup directions,
  the complete 62,171-byte C fixture, and C/A repair of user IP metadata. The
  harness also inspected PC18 channel/user/route/capability state. This is
  receiver-component interoperability, not a deployed full DXSpider daemon or
  a CCCluster interoperability result.
- Two separate sender OS processes exercised production Manager startup, dialing,
  handshake and C/A recovery against one persistent reference receiver. The
  receiver replaced K1OLD with K2NEW and advanced its retained watermark. This
  complements deterministic same-second controller and midnight tests; it does
  not claim a deployed full-cluster restart. The reference suite passed in
  12.213 seconds after adding this check.
- The cache-memory run measured a 61,516,184-byte spot retained-heap delta and
  30,758,088 bytes for the other three full pools concurrently (92,274,376 bytes
  combined). These pass their retained-heap partition checks, but do not prove
  transient growth or the total governed allocation ceiling.
- Targeted protocol, ownership, queue, identity, resource-bound and lifecycle
  checks have passed, including targeted race checks. Final-state full-lane
  validation and the sustained cache run remain outstanding.
- The earlier cache implementation completed its 45-minute load and 11-minute
  drain in 3,360.10 seconds: 450,000 new spot keys, 4,500,000 duplicates,
  270,000 PC92 keys, 4,500 PC93 keys and 900 bulletin keys, with no cache
  refusal. Evidence is under `%TEMP%\gocluster-pc92-20261001-135539`.
  Its retained executable predates the bounded-index replacement; this is a
  baseline component result, not final-source acceptance. The replacement
  still requires its sustained run.
- The strengthened Q5 preflight filled all four cache classes through actual
  TCP sessions, verified refusal without eviction and service for the other
  classes during and after filling, and passed in 70.501 seconds. Its three-second
  hold does not satisfy the required real 600-second windows.
- Q6's reduced preflight exercised publication, clock, authoritative capacity,
  staging, stalled-write and candidate-race faults with periodic C/K both enabled
  and disabled. Normal and race runs passed; captured recovery C/A records were
  replayed through the actual reference receiver. The repeated full-duration
  profile and 20-minute receive-only run remain outstanding.
- Runtime diagnostics have accounted for every declared delivery. Separating
  the external driver from the two-processor service removed their shared Go
  scheduler. The first plain split run still failed ten clients' enqueue
  threshold; a later profiled run recorded no threshold failures. Different
  source and instrumentation prevent a causal comparison. Neither short run
  qualifies Q1 or establishes sustained performance; details follow below.

V8 addresses shared spot-comment parser scratch growth while preserving its
results. An earlier overlap-growth extrapolation was disproved by execution and
is not evidence; the actual accepted single-alias counterexample is recorded in
v8. Implementation and validation remain in progress. The complete 160 MiB
transport and 480 MiB aggregate bounds are not yet proven.

The allocation review now also identifies an enabled-persistence dependency:
Go projection reservations do not by themselves bound SQLite's native page
cache and transaction allocations. Shipped topology persistence is disabled;
its enabled case remains in the approved scope and needs a complete ownership
disposition. Native SQLite storage cannot be silently excluded as Go runtime
stacks or GC overhead under the user's selected accounting boundary.

The user selected isolated latency profiles and reachable Q4 pressure phases.
Q1-Q3 disable batching, stabilization holds and temporal decoding only in the
isolated test configuration. Shipped behavior remains effective in a separate
45-minute Q1 repeat plus 11-minute drain; its timings are reported without the
5/25 ms thresholds. Observer seams exist only in `qualification` builds.

Diagnostic short profiles are never Q1-Q6 acceptance. The earlier smoke lacked
stable correlation and an enqueue observer; its surviving-output measurements
remain diagnostic evidence only.

## Runtime diagnostic method and observations

The `qualification` harness runs the actual cluster runtime in a child OS
process with `GOMAXPROCS=2`, `GOGC=50`, and `GOMEMLIMIT=1536MiB`. The parent owns
the external source and recipient sockets, with its CPU and Go memory reported
separately. The child performs normal admission, parsing, correction, routing,
filtering and successful telnet queue admission. There is no synthetic delivery
path. The split runner currently supports Windows; unsupported platforms fail
explicitly without changing ordinary builds.

The start boundary is the external counter reading in `qualificationOracle.add`,
before constructing and writing the source frame. It precedes the application's
`peer.Manager.ingestSpot` admission. Consequently the reported arrival-to-enqueue
interval includes source construction, socket transport and parsing as well as
application queues. It is a conservative external measurement, not a timestamp
taken exactly at the internal ingest channel. The enqueue endpoint is recorded
on entry to the callback immediately following successful `c.spotChan <- env`.
The first-byte endpoint is the first receiver read containing the record marker;
it includes receiver scheduling/read completion, not just kernel arrival. Split
markers and login-prompt prefixes retain their first read's timestamp.

Every endpoint uses the same Windows performance-counter domain. The launcher
checks parent/child frequency and launch-order brackets, retains raw ticks in
the shared input table, and converts intervals once using integer arithmetic.
It adds one counter tick and rounds upward to cover the documented cross-thread
ordering uncertainty; see [Microsoft's QPC guidance](https://learn.microsoft.com/en-us/windows/win32/sysinfo/acquiring-high-resolution-time-stamps).
Immutable input metadata is atomically published before the source write. A
failed write remains a required input rather than disappearing from the
denominator. Child enqueue bitsets and fixed histograms remain bounded; only
final summaries cross the control socket. Cross-process visibility, rounding,
missing-output, duplicate, split-read and shutdown fixtures exercise this oracle.

These runs used Windows, Go 1.26.4, an Intel i9-10900 (10 cores / 20 logical
processors), and 34,124,038,144 bytes of physical memory. Each ordinary short run
used 100 clients, 16 peers, the full Q1 graph, 20 seconds of load and a five-second
drain: 3,334 new spot keys, 33,340 duplicates, 2,000 PC92 records, 34 PC93 records
and seven WWV records. Every required delivery, including all 30,000 PC92 relays,
was observed, with no unknown/duplicate deliveries, gates or capacity refusals.
P99 values below are histogram upper bounds across clients, not raw percentiles.

| Artifact suffix on 2026-10-01 | Driver / instrumentation | Enqueue p99 upper | First-byte p99 upper | Threshold result |
| --- | --- | --- | --- | --- |
| `133355` | Colocated optimized driver, plain | 7.2-8.9 ms | 20.2-28 ms | 100 clients failed enqueue; 20 failed first byte |
| `140447` | Separate process, plain | 3.4-5.1 ms | 9.9-15.3 ms | 10 clients failed enqueue; none failed first byte |
| `141439` | Separate process, load-only CPU profile, v8 parser source | 3.3-4.3 ms | 9.4-13.1 ms | No recorded threshold failure; still diagnostic |

Evidence lives under `%TEMP%\gocluster-pc92-runtime-20261001-<suffix>` and
includes observations, machine/settings, source manifests and the retained test
executable. The `140447` source manifest was unchanged. The only manifest change
during `141439` was addition of `spot/comment_parser_stream_test.go`; production
source did not change during capture. Its executable SHA256 is
`5D8F0B80A357C7B0B5000FC021D795BB0A637E3CFF4B0A4D7E29BCC96180F724`.
A cache-only sustained component test ran concurrently with both split
diagnostics. Final acceptance requires isolated runs on the final relevant
source; these observations do not satisfy that requirement.

The `140447` failures were real exact-counter failures, not an artifact of
displaying the 5.1 ms histogram bucket. Each affected client had 34 of 3,334
observations above 5 ms, where the nearest-rank p99 requirement permits at most
33. In `141439`, the enqueue counts above 5 ms ranged from 16 to 26; first-byte
counts above 25 ms ranged from two to ten. New diagnostic counters found zero
observations crossing either limit solely because of the one-tick allowance
(100 ns at this machine's 10 MHz counter). That counter was absent in `140447`,
so it cannot establish the precise margin of that earlier run's 34th observation.

The profiled child recorded 13.69 seconds of CPU samples over 20 seconds.
`WSASend` accounted for 7.35 seconds cumulatively (53.69%); the enqueue observer
accounted for 0.26 seconds (1.90%). These sampled totals do not identify the
queue or scheduling wait for any particular token. Total child allocation during
load was 517,050,792 bytes; sampled allocation attributed 114.13 MiB to graph
preparation and 6.50 MiB to spot tokenization. These are cumulative allocations,
not simultaneously retained ownership or proof of any allocation ceiling.
The child reported 93,502,168 bytes of live Go heap and the parent 34,367,672;
the separately reported shared input mapping was 143,280 bytes. Observer and
clock overhead remains included: the guarded native-counter callback benchmark,
including its accounting, measured 242.6 ns/event, 16 bytes and two allocations.

Each short run contains only one partial input-minute cohort. The 100 client
results share source inputs and scheduling and are not independent replications.
Short-sample variance can affect p99 near the threshold, but does not waive a
failure. Source changes, profiling and concurrent work also prevent attributing
the differing runs to one cause. Sustained per-minute and full-run acceptance
still requires the exact Q1-Q3 durations and separate shipped-Q1 repeat.

The earlier `135152` full-topology diagnostic reached 64 established peers,
4,096 nodes, 65,536 users, 131,072 edges, 262,144 ingress observations and 16,384
freshness entries. Its ten-second load delivered all 63,000 required PC92 relays
without a delivery, gate or refusal failure, but failed latency. Initial setup
had exceeded the bounded input mailbox by sending 64 large copies together;
setup now waits for observed admission of each large copy. This changes setup
pacing only; live traffic still includes two 8,000-user C records each second.
This proves fixture reachability, not the full Q2 pressure or allocation contract.

Run the short diagnostics from the repository root:

```powershell
./scripts/pc92-runtime-qualification.ps1 -Profile preflight
./scripts/pc92-runtime-qualification.ps1 -Profile preflight -CPUProfile
./scripts/pc92-runtime-qualification.ps1 -Profile diagnostic-full
```

The same wrapper exposes `q1`, `q2`, `q3` and `shipped-q1` with their fixed
approved durations. Full runs await final source freeze and coordinated
qualification; a passing short test still records `Qualified: false`.

## Available cache evidence

Run from the repository root with the qualified runtime settings
`GOMAXPROCS=2`, `GOGC=50`, and `GOMEMLIMIT=1536MiB`:

```powershell
./scripts/pc92-qualification.ps1 -Profile cache-memory
./scripts/pc92-qualification.ps1 -Profile cache-sustained
go test ./peer -run '^$' -bench '^BenchmarkPC92Cache(Duplicate|AdmitAndExpire)$' -benchmem -benchtime=1s
go test ./peer -run '^$' -bench '^BenchmarkPC92CacheConcentratedExpiry$' -benchmem -benchtime=3x
```

The wrapper records the source revision, working-tree status, machine/runtime
information, settings, and unabridged test output in a timestamped temporary
directory. Long evidence must be collected on the final relevant source state.
Profiling, race instrumentation, and ordinary latency evidence use separate
runs. No duration override is available for the sustained profile.

`cache-memory` fills 131,072 spot keys of 373 bytes, tests refusal without
eviction and duplicate recognition at exactly 600 seconds, and performs three
fill/expire/refill cycles. It checks both the primary map and the expiry index.
It then fills the spot, PC92, PC93, and bulletin pools concurrently. Required
retained heap deltas are at most 96 MiB for the spot cache, 32 MiB independently
for the other classes, and 128 MiB for all four caches concurrently.
The sampled process heap includes test-driver garbage and is reported
separately. Retained-heap checks do not prove transient map-growth or the full
480 MiB subsystem ceiling; those remain full qualification obligations.

`cache-sustained` runs for **45 minutes of real time plus 11 minutes of
cleanup**. It admits 10,000 new spot keys/minute with the 40% PC61, 40% PC11,
20% PC26 mix; 100,000 duplicate arrivals/minute; 6,000 new PC92 keys/minute;
100 new PC93 keys/minute; and 20 new bulletin keys/minute. Every admission
must have its expected result, duplicate hits must not prolong retention,
no pool may refuse required traffic, and all pools/indexes must drain to zero.
This exercises cache contention and cleanup, including more than four TTL
windows. It uses cache keys rather than network protocol frames.

**These cache tests are only a subset of the approved evidence.** They do not
qualify session delivery, topology, handshakes, broadcast fan-out, per-client
latency, whole-subsystem memory, or live DXSpider compatibility. A skipped
opt-in test is not qualification success.

## Required complete workload

Use real protocol frames, `forward_spots=true`, and externally correlated
arrival/output observations. Ordinary spot frames are at most 512 bytes and
use valid CW/SSB data without intentional mode delay. Add separately
parser-reachable large numeric keys and maximum-frame fixtures: ordinary
small-key traffic alone cannot establish the memory contract.

PC92 traffic uses 45% A, 45% D, 2% C, and 8% K over a known origin pool.
At 100 new records/second, both C records each second contain 8,000 users
(approximately 64 KiB), preserving the same complete membership. The current
PC92 key builder hashes member data: key-byte occupancy is measured separately
from the full wire and parsed working-set allocation.
PC93 is half announcements and half
private messages, with canonical keys at most 1 KiB. WWV/WCY keys are at
most 512 bytes. All required deliveries must be counted, including missing
or refused ones; computing latency only for surviving output is invalid.

| Case | Population and stimulus | Duration and acceptance |
| --- | --- | --- |
| Q1 | 100 healthy local clients; 16 peers; 2,048 remote nodes, 32,768 users, 65,536 edges, 32,768 ingress observations; standard mixed stream | 45 min load + 11 min drain. No capacity refusals, unintended disconnects, or missing required deliveries. |
| Q2 | 100 clients; 64 peers; full graph/freshness occupancy; updates use existing origins; standard mixed stream | 30 min + 11 min drain; same delivery requirements. |
| Q3 | Q2 population; each five-minute cycle has 24 seconds at 1,000 new spot keys/sec, 120 seconds without new keys to replenish the excess allowance, and 156 seconds at 10,000/min. Duplicate arrivals continue at 100,000/min. Stall the last eight peer readers during minutes 5-8 and 15-18; reconnect and restore ingress before each window ends. | Six cycles over 30 min, 300,000 new keys total. Healthy peers retain service; only stalled peers close; control/messages are not starved. |
| Q4A | 1,000 local sessions, 64 established peers, 128 prelogin candidates; maximum reachable graph/cache/queue/reader/active state and maximum keys. | 30 min fill/release/refill. Observe concurrent peaks and prove all complete allocation bounds. |
| Q4B | 1,000 local sessions, 63 established peers, 128 authenticated candidates for the one remaining configured identity; per-candidate/global staging, cleanup, deadlines and winner races. | Separate 30 min fill/release/refill. Preserve authentication and ownership; same 480 MiB and partition limits. |
| Q5 | Saturate each traffic class independently while other classes remain healthy; concentrated expiry; TTL boundary; existing duplicates and new keys | At least 3 real 600 sec windows plus drain. No unexpired eviction, cross-class capacity theft, or growth with input history. |
| Q6 | Cause each publication/clock/admission/staging/stall gate at least twice, with normal timers and periodic C/K disabled; candidate authority races | Externally timed recovery; complete C followed by required A metadata observed at the actual DXSpider receiver. Add 20 min receive-only run proving spot-forwarding cache remains unused. |

For each healthy client, Q1-Q3 must meet p99 **5 ms arrival to enqueue** and
**25 ms arrival to first byte**, in every fixed one-minute window and over the
full run. Keep the original external arrival timestamp through internal queues.
Use stable comment identifiers across callsign correction; predeclare required
recipients and reconcile all missing, unknown and duplicate observations.
The existing small recent-sample UI metric is not this acceptance oracle.
Report the hardware and Go/runtime settings with results; do not silently tune
an unsuccessful required profile.

## Capacity and recovery boundaries

| Owned state | Required bound |
| --- | --- |
| Spot dedupe | 131,072 entries / 64 MiB key bytes / 96 MiB complete allocation |
| PC92 dedupe | 65,536 entries / 8 MiB key bytes |
| PC93 dedupe | 65,536 entries / 8 MiB key bytes |
| WWV/WCY dedupe | 8,192 entries / 2 MiB key bytes |
| Graph and shared freshness | 4,096 nodes / 65,536 users / 131,072 edges / 262,144 origin-ingress observations / 16,384 freshness entries; 96 MiB allocation |
| Queues, transport readers, active writes | 160 MiB allocation |
| Pending handshake staging | Per candidate: 256 records or 512 KiB charged bytes. Global: 8,192 records or 16 MiB. At most 128 candidates. |
| Other caches | 32 MiB allocation |
| Snapshots/projection | 48 MiB allocation |
| Other metadata | 32 MiB allocation |

The user selected the accounting boundary on 2026-10-01: the aggregate ceiling
is 480 MiB for all owned protocol data, backing allocations and overlapping
active generations. Report Go runtime stacks, GC overhead, unchanged
configuration and whole-process heap/RSS separately. New protocol copies of
configuration still belong to their protocol owner. Expire payload keys only at age strictly
greater than 600 seconds. Duplicate traffic must not refresh their ages.
Maintenance must run independently of topology persistence and periodic K/C,
with at most one-second cleanup lag under qualified load.

Publication overflow and unsafe clock close/gate PC9x links while retaining
local users and established legacy service. Spot/message-class exhaustion
refuses new untrackable work in its own class while keeping sessions open.
Authoritative PC92 admission failure closes affected sessions and marks the
affected knowledge incomplete. Failed, losing, timed-out, or capacity-refused
handshake candidates release all staged state without advancing global
freshness or topology.

Global clock/publication recovery requires the external condition to remain
valid for one second, then gate observation within the next second. Admission
refusal instead uses the [v14 controlled-retry contract](pc18-pc92-scope-ledger-v14.md):
shared backoff, one candidate per identity, one-second global startup pacing,
controller invalidation before startup and60-second healthy history reset after
matching recovery C/A Flush and establishment/replay. Original handshake
deadlines remain absolute. Complete local C plus required A metadata must
arrive within five seconds of externally observed handshake completion,
including when periodic C/K are disabled. Local recovery does not establish
complete remote membership: a valid authoritative remote C is required.

The reference interoperability oracle remains the pinned DXSpider revision
`3e9b3621d94dd45c68702e4a0f896aac33f2a91d`, inspecting actual receiver
channel/user/route/membership state. Codec round trips alone do not demonstrate
that state. CCCluster compatibility claims require separate CCCluster evidence.

## V14 qualification status

The [v14 matrix/evidence](pc92-v14-validation.md) governs correction validation.
The short service gate is necessary, not sufficient. The full retry profile
requires at least30 minutes of actual retries under approved mixed traffic, then
release/recovery/reset and another failure. Expected scheduled work and actual
offered input, lateness, missing output and observer overflow must reconcile;
a producer dropping timer ticks cannot qualify reduced load. Final-source
binaries/manifests and the exact CTY hash remain required. Full aggregate
acceptance remains open pending SQLite, context and retirement proofs.

The retry wrapper requires three cases: 30 minutes of63-peer overload with
periodic C/K disabled,75 seconds of63-peer overload using the enabled600/1800-
second settings, and75 seconds of64-peer overload with maintenance work. Each
qualification case continues its mixed workload or maintenance for a fixed
600-second tail through recovery, healthy reset and another failure. Case load
durations are therefore2400/675/675 seconds. The enabled case proves prompt
recovery independent of periodic firing; its total675 seconds do not span a
scheduled periodic C. The mixed cases change local
membership between recovery C and A and verify the original one-second queue
admission obligation. All wire recovery pairs must decode, retain their matching
baseline, and complete within five seconds; startup expiry cannot excuse a
post-handshake recovery failure. The65-minute wrapper timeout accommodates the
combined3750 seconds of load plus setup. Individual recovery observation bounds
and all protocol deadlines remain unchanged. Healthy recovered recipients must
receive mandatory traffic throughout the tail; only an explicitly scheduled
fault can stop new obligations, after prior obligations have drained. Preflight
retains a short diagnostic load and quiet recovery tail; it cannot prove loaded
reset behavior or the real300-second cap.
