# Path Reliability First-Wave Execution

Authority: owner token `Approved v2`, received 2026-10-14, with the S4 measurement
amendment `Approved v3`, S4 performance exception `Approved v4`, and S7 timing
exception `Approved v5` and observer repair `Approved v6`, received
2026-10-15. This record maps the
seven approved serial slices to implementation and evidence. It does not
authorize the gated backlog, production deployment/restart, push or firewall
changes. The complete approved ledger and raw evidence are retained locally at
`.tmp/path-reliability-first-wave/`.

Only timestamp-valid reports may advance freshness. Fractional support must
remain unrounded through qualification. Forecast precedence is deferred; the
glyph system remains enabled. Recent report units describe usable volume and
aging, not independent samples, calibrated confidence or QSO probability.

| Slice | Contract | State |
| --- | --- | --- |
| S1 | Deterministic persisted retention | Accepted, `3eb9c84` |
| S2 | Bounded incremental persistence | Accepted, `08ba7f7` |
| S3 | Distributed-contraction compaction | Accepted, `9145209` |
| S4 | Fine-key sharding | Accepted, `258e4a8`, with the Approved v4 performance exception |
| S5 | Present zero-dB PSKReporter reports | Accepted, `e82cec1`; all nonwaived gates passed |
| S6 | Nonduplicated common-resolution selection | Accepted, `da1ab80`; all nonwaived gates passed |
| S7 | Recent support and receiver diversity | Accepted at this local checkpoint under Approved v6; timing exception retained |

No later slice begins before the preceding slice's retained required evidence
passes or receives an explicit owner exception. Approved v4 excepts S4
performance gates; Approved v5 separately excepts S7 comparative elapsed time.
Each accepted slice receives a local checkpoint. Reverting an optimization
must preserve any earlier slice's accepted correctness contract.

## S1: Deterministic Persisted Retention

`voacap_forecast_cache_store.go` now orders eligible persisted entries by
descending storage time and ascending canonical key for exact ties. The full
scan was deliberately retained for this checkpoint. Equal-time refresh receives
no priority; a one-nanosecond newer timestamp does. Live-memory eviction is
unchanged. ADR-0303 records this decision.

`voacap_retention_test.go` asserts literal survivor keys across startup,
completion, tied overflow, refresh, reinsertion and reopen. Raw disk is checked
before hydration can repair it. The worker-persistence fixture now cancels and
waits before reading disk; a blocked-persistence fixture proves publication
can precede disk while shutdown still waits. An overlay using the old comparator
failed both mixed startup ordering and runtime raw-survivor oracles.

Final targeted checks passed all 20 exact required identities. Both complete
native ordinary and race lanes passed 9,611 cases across 70 packages: 53 test
packages and 17 build-only packages, with 77 existing optional skips. No required
S1 test skipped. Vet, Staticcheck, golangci-lint, support-agent checks, changed
crawler comments and final code-map checks passed. Final source/diff verification
was lead-owned; this localized slice required no independent scientific review.

## S2: Bounded Incremental Persistence

Implementation: `voacap_forecast_cache_index.go`, `_heap.go`, `_scan.go`, the
existing store, fallback snapshot forwarding and overview diagnostics. ADR-0304
records the ownership, actual-hour subtree certificate and recovery decision.
The map and both indexed heaps own one payload-free node per maintained key;
refresh reuses it and every deletion removes it from all three structures.
Ordinary replacement/removals share an atomic batch. Unknown/failed hydration
does not certify empty disk. Uncertain commits discard trust and recover from
actual disk. Recovery candidates are capacity-bounded; prune chunks hold at
most 256 keys. Cached diagnostics never acquire the disk mutex.

The separate steered Go quality review found no confirmed blocking production
defect and requested stronger heap movement/search tests. Those refinements
include independent subtree oracles, a 50,000-entry certified search with
descendants poisoned, both left/right invalid paths with certified siblings
skipped, and varied-window heap sifts. This was not independent scientific
review. Lead verification remains authoritative.

Final targeted ordinary and race checks each passed 145 cases across
`pathreliability` and `internal/cluster`, with no skips. All 88 exact required
leaf identities passed before the final payload/presentation refinements.
The final full lanes additionally require the daily-payload regression case.
Coverage includes raw disk before repair/reopen,
initialization and unavailable SSN, failed hydration/pruning, exact time/context
boundaries, invalid versus unencodable replacement, applied-then-error commits,
churn/bijection, nonblocking snapshots, concurrent closure and shutdown fencing.

The first final selector omitted one required S1 restore case; the runner
rejected that selection. The corrected selector reran the complete targeted
set ordinarily and with race. The initial commit-failure fixture also needed
an unsigned-arithmetic correction before final verification. Both rejected
attempts remain in the local evidence record.

Frozen matched benchmarks used 12 balanced AB/BA process pairs per fixture,
Go 1.27.1 windows/amd64, GOMAXPROCS=2 and GOGC=50. Complete synchronous persistence
was timed; seed creation and hydration were excluded. No measurement was
discarded. Every small-cache fixture passed its individual ratio limit of 1.05:

| Occupancy | Refresh candidate/baseline median ratio | Insert-at-capacity ratio |
| ---: | ---: | ---: |
| 1 | 0.065827 | 0.044244 |
| 64 | 0.019579 | 0.017438 |
| 256 | 0.005617 | 0.006464 |
| 10,000 | 0.000092 | 0.000103 |
| 50,000 | 0.000017 | 0.000021 |

Each large-cache fixture passed its separate 0.50 limit. The final paired
retained-heap oracle settled seeded Pebble memtables before hydration. Additional
heap at 50,000 entries was 10,403,952 bytes, below 20 MiB. The earlier helper,
built before that setup refinement, measured 9,293,968 bytes and is retained as
diagnostic evidence. Persistence-source hashes match the measured state.

Presentation refinement after the targeted run: the overview disables wrapping,
so index diagnostics use their own compact line. Final validation covers this
adjustment. It changes no measured persistence code or worker caller behavior.

Controlled profiles use actual fallback workers, cache publication and Pebble
persistence with a deterministic local forecaster. They do not run the external
VOACAP engine or establish propagation truth. Each warm bundle excludes seed
creation and hydration, then completes 256 new forecasts at capacity 50,000 and
checks raw disk after Wait. GOMAXPROCS=2, GOGC=50, 24 rows per forecast and the
same test fixture apply to every bundle. All timestamps below are UTC 2026-10-15;
none crossed the UTC hour.

| Bundle | Workload interval | Seconds | Workload allocated bytes | Disposition |
| --- | --- | ---: | ---: | --- |
| S1 baseline | 01:02:05-01:15:34 | 808.303181 | 147,589,528,632 | Reference |
| Initial S2 | 01:15:51 | 0.238796 | 7,262,328 | Rejected payload retention |
| Corrected S2 | 01:20:59 | 0.247809 | 7,259,976 | Worker/raw-disk checks passed |

The first candidate retained JSON growth capacity in long-lived full-day
payloads. Sampled heap identified that gap in the one-row oracle. Compact owned
copies restore the prior payload shape. A new paired live-cache fixture measures
the complete 50,000-by-24-row shape after GC, preserving the fallback:

| Variant | Retained hydration delta bytes | Additional bytes versus S1 |
| --- | ---: | ---: |
| S1 | 146,394,600 | Reference |
| Deliberate alias overlay | 226,686,928 | 80,292,328: rejected |
| Corrected S2 | 156,314,488 | 9,919,888: passes 20 MiB |

The alias also fails the ordinary value/compactness case with len 24/cap 39.
This negative witness prevents a visible-capacity-only assertion from replacing
physical retention measurement. TSR-0096 records the root cause and fix.

CPU attribution supports the mechanism: baseline `scanCurrentLocked` accumulated
770.54s (93.77% of 821.76s sampled CPU); corrected S2 has no ordinary scan samples.
Its existing memory eviction accounts for 190ms of 240ms sampled CPU and remains
excluded CPU-P05 scope. The short corrected capture limits detailed rankings.
Allocation-profile intervals also include capture/compression overhead beyond
the exact workload MemStats counters above. Block profiles reflect waiting
workers, test waiters and Pebble idle loops; cumulative delays are not additive
wall time. Sampled whole-process heap has unrelated allocations and sampling
noise; the exact paired retained-cache oracle owns the 20 MiB numerical gate.

The compact-copy fix and cold sort-loop cleanup affect excluded hydration and
recovery, not timed ordinary writes. Heap-interface lint annotations and removal
of a dead benchmark assignment change no timed behavior. The original matched
ordinary timing evidence therefore remains applicable; both versions retain
the same final benchmark body. The complete ordinary lane passed 9,691 cases
across 70 packages, including all 89 exact required identities. Its 80 skips
comprise 77 existing optional cases and three artifact fixtures executed
separately. Vet, Staticcheck, golangci-lint (zero issues), support-agent,
crawler-comment and troubleshooting-record checks passed. The complete race
lane also passed 9,691 cases across the same 70 packages, with the same 80
explained skips and every required identity verified. Code-map generation and
freshness checks passed. Fresh lead verification reviewed the final scope,
diff, invariants, failures and evidence; S2 is accepted. No production saving is
established.

The controlled fixture executes only at the
existing approved `.tmp/windows-tests/dxcluster/internal/cluster/package.test.exe`
path, with isolated databases and artifacts; no firewall rule is modified.

Support-agent impact: operator diagnostics and troubleshooting changed; the
path support card, package README and operator guide were updated. The disk
format, configuration values, model admission, forecast precedence and observed
glyph qualification remain unchanged through S2.

## S3: Compaction After Distributed Contraction

`Store.Compact` adds a bounded advisory scan of existing shard counts and
peaks. It permits contracted sub-minimum shards to rebuild when total peak
reaches the minimum and total occupancy is strictly below the configured
ratio. Every rebuild rechecks live occupancy under its shard's write lock.
Existing large-shard integer-floor boundaries and bucket pointers survive.
There are no new retained structures, update counters, workers or intervals.
ADR-0305 records the decision; existing bucket counts cover primary cardinality.

New contract tests cover aggregate 999/1000/1001 boundaries, strict ratio,
normalization, large-shard compatibility, nil/empty handling, peak reset,
refill after the advisory snapshot, concurrent refill and exact retained keys
and pointers. A connected public Predictor fixture grows 1,088 coarse buckets,
purges 576 stale buckets, compacts all 64 maps and retains the same nonempty
-10.5 dB prediction and 512-entry active report/histogram. No S4 placement is
included. The old S2 Compact overlay fails both distributed public witnesses.

The opt-in map-only resource fixture grows 64 maps to 512 entries each and
retains eight. All entries refer to one live bucket without receiver sidecars,
so bucket deletion cannot explain the measured release. It rebuilt all 64
maps and preserved every key/pointer. Logical bucket size remained 448 bytes.
HeapAlloc fell by 1,172,992 bytes. The pre-compaction map-allocation estimate
was 1,133,344 bytes; its post-compaction estimate was -39,648 bytes, showing
background GC noise. The frozen minimum release of 50% passed comfortably;
these signed process estimates do not establish exact remaining map bytes,
an exact percentage, or immediate Windows working-set reduction.

Targeted ordinary and race checks each passed 173 cases; the opt-in heap fixture
was skipped there and passed separately. The complete ordinary lane passed
9,723 cases across 70 packages, including all 119 required identities. Its
81 skips are the previous 80 optional/artifact cases plus the separately passed
S3 heap fixture. The complete race lane also passed 9,723 cases across the same
70 packages with the same skips and all 119 required identities verified.
The boundary fixtures gained literal public Compact rebuild counts during the
race lane, before this package built. These stronger assertions passed in the
complete race run and in a final targeted ordinary rerun (173 passes). Production
source did not change after the complete ordinary lane. The existing ordinary
evidence therefore remains applicable alongside the rerun for the changed test
body. Vet, Staticcheck and lint passed for the complete source and again for the
package after that assertion change. Crawler and support-agent checks passed.
Test implementation was partitioned; the fresh source/contract review was
lead-owned. No scientific independence claim is made for this maintenance slice.

## S4: Sharding and Owner-Accepted Performance Exception

The candidate introduces one private fixed full-key selector and uses it for
both updates and captures. It changes map/lock placement without changing
packed keys, H3 mapping, configuration, bucket layout or model arithmetic.
Routing-aware helpers and the connected S3 compaction fixture follow the
selector. New literal vectors, real mapped populations, public ordinary/beacon
readback and concurrent maintenance fixtures cover routing consistency.

Targeted core ordinary/race each passed 197 cases, including 57 required
identities with the test clock checks. The Windows skip is the cgo-only mapper
test; the verbose Linux/cgo run passed that mapper and the applicable corpora.
Connected consumer ordinary/race each passed 13 cases, including all three
required receiver-mode preflights. Loopback wire ordinary/race each passed all
three modes with 192 ordinary and 192 beacon glyph frames per mode, no drops,
and observed server/client goroutine retirement. Vet, Staticcheck and lint
passed; lint reported zero issues. The old selector and old capture-route
overlays failed the intended routing/read-after-write witnesses.

Engineering reviews corrected checker contention, incomplete maintenance-tail
classes, purge-only overlap claims, warm-seed admission slack and wall-time-only
post-cycle success. Test intervals use owner-local Windows performance counters;
local clock checks measured 100 ns minimum positive increments and zero steady
allocations. Workload/profile setup and post-run oracles are excluded from
scoring. The controller passed 84 positive/negative checker expectations.
These reviews were steered engineering evidence, not scientific independence.

The first comparison was rejected before scoring because baseline and candidate
YAML bytes differed only in five CRLF sequences. Its two observations, source
snapshots and binaries remain in `s4/campaign-v1/`; none is pooled with the valid
comparison. Preparation for `campaign-v2` verified semantic text equality,
made isolated baseline YAML bytes identical, and verified both H3 table hashes.
Owner configuration remained untouched. All common S3/S4 preflights passed.

The valid Serial/off fixture completed all twelve balanced process pairs with
one million samples per role per process and matching frozen identities. The
median paired candidate/baseline normal-writer p99.9 ratio was **1.1666666667**,
above the **1.10** limit. Recalculation directly from all 24 raw manifests
reproduced it. Elapsed ratio was 0.9970709699 (within 1.05); the other three
role-tail guards passed. Their gains cannot waive the failing writer guard.

Individual writer-tail ratios varied widely, approximately 0.07 to 31.67. This
is a failed controlled acceptance result; it does not establish a production
regression or identify its cause. No limit was relaxed, valid observation
discarded, or repeated attempt selected to force acceptance. All raw evidence
is retained under `.tmp/path-reliability-first-wave/s4/campaign-v2/`.

At that stage S4 remained uncommitted and unaccepted. The campaign stopped at this gate; the
remaining timing fixtures, comparative profiles, real 66-minute soak and full
S4 acceptance lanes were not run. S5-S7 have not begun. S1-S3 remain accepted at
their local checkpoints. No push, deployment, production restart, firewall edit
or new listening executable path occurred.

### S4 measurement investigation and Approved v3

The owner approved v3 on 2026-10-15. It amends the test measurement and failure
lifecycle only; the original serial failure, numerical limits and S5-S7 serial
gate remain. See [TSR-0097](troubleshooting/TSR-0097-path-sharding-measurement-clock.md)
for source/profile evidence, hypothesis limits and the retained first probe.

The original same-binary control completed twelve balanced pairs with elapsed
ratio 0.9768992589 and normal-writer p99.9 ratio 0.8839869281. It passed the
original one-sided guards, but unchanged code's apparent 11.60% tail improvement
exposes variability. The original contention diagnostic measured elapsed ratio
0.3547503355 for its declared stress population. Neither diagnostic qualifies
S4 or establishes production savings.

V3 uses a separate detached S3 baseline, common repaired harness, controllers,
saved binaries and artifact directories under `s4/v3/`. Both original source
sets were archived byte-for-byte and hash-checked before modification. The
preapproval and postapproval test-strategy findings are dispositioned in the
ignored implementation design. Reviews were steered engineering reviews, not
independent scientific-model evidence.

The repaired helper ordinary/race checks each passed 30 cases; targeted core
checks each passed 74, and targeted connected checks each passed 15. Required
lifecycle leaves, concurrent reads and retirement witnesses passed. The targeted
Linux/cgo check passed. Vet, Staticcheck and final lint passed; controller-only
checks passed 123 positive/negative expectations. All four rebuilt baseline/
candidate common preflights passed before the source/controller/binary freeze.
These are targeted results, not complete S4 acceptance lanes.

Both prescribed twelve-pair Serial/off same-binary controls passed all ten
symmetric guards. New qualification passed Serial/off and Serial/shadow, then
stopped at Serial/enforce: normal-writer p99.9 ratio **1.2361111111** exceeded
**1.10**. Elapsed ratio **1.003743236** and the other three tails passed.
Recalculation from all 24 raw manifests reproduced the failure; identities,
one-million sample populations, test execution and timing receipts are valid.
Neither total elapsed gains nor passing off-mode controls waive this failure.

Source review also found that Go's integer-averaged AllocsPerRun assertion can
hide fractional allocations. All shadow/enforce serial receipts record the
same 372 allocations in both variants; one-time receiver-state growth is a
source-supported explanation, not established allocation attribution. Exact
zero-allocation claims required a stronger checker. Complete frozen sources
were archived and hash-verified before that checker was refined. The warmed
unit tests now count a whole measured batch exactly, with a distinct warm
batch. Only measured reports fund their admission witness. The fixed timing
campaign and its failed result remain unchanged; no report warmup was added
to qualification.

The enforce-mode same-binary follow-up is diagnostic only and kept separately
under `s4/v3-enforce-investigation-20261015/`. Both full series completed:
baseline writer B/A tail ratio1.1984; candidate writer3.7682 and path-only
writer2.8171, each comparing identical code. Both elapsed results stayed
within5%. This demonstrates failed enforce-mode tail repeatability for those
observations; it cannot qualify S4 or establish the candidate failure's cause.
Four separate diagnostic runs retained all 16 million original-position
samples after timing, using isolated source copies and an explicitly unscored
protocol. Their quantiles, maxima and zero counts reproduce the successful
receipts. Slow operations occur beyond receiver growth and bucket-aging work;
their cause remains unknown. The p99.9 rank also crosses a sparse slow-sample
boundary, contributing to unstable reported tail values.

After the allocation-oracle repair, helper ordinary/race each passed 31 cases
with 29 exact required identities and no skips; core ordinary/race each passed
74 with 64 required identities. Fresh Linux/cgo allocation cases, full-repository
vet/Staticcheck/lint and TSR/support checks passed. These do not convert the
failed performance comparison into acceptance.

Before Approved v4, remaining qualification fixtures, final acceptance
profiles, real soak, full acceptance lanes and S5-S7 remained blocked. Work was
uncommitted on voacap; no push, deployment, restart or firewall change occurred.

### Approved v4: owner-accepted S4 performance exception

The owner selected acceptance of S4's unresolved performance risk and supplied
exact Approved v4 before this amendment. Original and repaired failed timing
results remain failed; unfinished comparisons remain unrun. Remaining S4 timing
and final S4 matched profiles are deferred to assessment after S5-S7. No timing
pass, profile-completion marker or production-savings claim is synthesized.

The retained correctness/race/resource/lifecycle checks and real 66-minute
maintenance soak still precede S4's local checkpoint. The new versioned v4
driver replaces both old timing and profile prerequisites with the explicit
approval, preserving source/binary/input identity and actual soak assertions.
It uses the existing approved executable path and shared runner lock. All
nonwaived S5-S7 contracts and per-slice checkpoints remain unchanged. ADR-0306
records the selector decision and accepted uncertainty; TSR-0097 retains the
measurement history.

Fresh steered source reviews found no outstanding production or instrumentation
issue. One wire-test early-dial cleanup gap was fixed before new builds: Stop
and explicit server-retirement wait now register immediately after startup,
before dialing. A failed dial therefore receives the same retirement boundary.
The affected wire ordinary/race reruns passed in the retained validation below.

The retained full native ordinary and race lanes subsequently passed: each
executed 9,824 cases across 70 packages, with 207 required case identities.
Actual loopback glyph ordinary/race checks passed all three receiver modes.
After a test-only allocation-check refinement, the full path-reliability
package ordinary/race reruns each passed 669 cases with 170 required identities;
the Linux/cgo targeted checks and affected analyzers passed too. The earlier
single process-wide allocation remains failed and unattributed. Each warmed
allocation mode now runs in a bounded child of the same stable executable,
with unchanged exact batches and zero limit. A deliberate escaping allocation
inside the measured beacon batch verifies the actual rejection path.

The real 66-minute maintenance soak passed from the approved executable path.
Both actual compactions rebuilt 64 shards, one hour apart. Growth to 20,002
buckets contracted to 180, then regrew, purged and compacted again before a
refill to 435 buckets. The two measured compactions released 8,194,016 and
607,472 retained heap bytes respectively. These candidate-only readings do not
establish a comparative memory saving. All four work roles completed nearly
299 seconds of successful activity after the second compaction; all 13 dated
periodic reports were available, and logger/maintenance owners joined.

The Go test passed in 3,960.04 seconds. Its original post-run PowerShell checker
then failed because it expected Go field names instead of the snake_case JSON
tags. That controller and failure log remain unchanged. A separately reviewed
completion verifier corrects only those property names and retains every soak
assertion. Its 22 positive/negative cases use the actual Go-serialized proof.
It verified the exact RUN/PASS and package PASS, all 1,430 frozen input hashes,
approval/controller/tool/configuration/binary identities and unchanged branch/
HEAD before writing `s4/v4/soak/accepted-retained-validation.json`. No workload
was rerun to clear this reporting error.

S4's retained safety gates and fresh final verification passed. The accepted
change maps `keys.go`'s selector to both `store.go` update/capture routes;
literal/mapped/concurrent contracts, connected ingress, actual glyph wire,
allocation falsifiers and real maintenance cover those boundaries. Support
documentation impact is Required and is reflected in the package guide,
support card, ADR-0306 and TSR-0097. Timing failures and deferred S4 profiles
retain their recorded status. S5-S7 remain serial follow-on work with their
own nonwaived gates; no production performance outcome is established.


## S5: present numeric zero reports

S4 was checkpointed as `258e4a8` on `voacap` before S5 began. S5 removes only
zero-value rejection from `pskreporter/client.go`'s presence check. A present
numeric zero now follows the existing normal/path-only taxonomy route. Missing,
null and malformed reports retain their established handling. Neither decoder,
mode offset, subscription, queue bound nor overload policy changed.

The owner-package ordinary/race checks each passed 360 cases with 212 required
identities. They cover presence, sign, duplicate fields, malformed inputs,
normal/path-only/ignored modes, both parser paths and unchanged compatibility
boundaries. Both bounded 15-second differential fuzz campaigns passed.

Connected MQTT ingress exercises the actual public client, normal output,
path-only learning, archive and peer exclusion, timestamps and metadata. A
separate consumer replays the actual captured broadcast snapshots through a
real telnet connection. Ordinary and race captures passed; literal wire rows
and all mask branches agree. Restoring the old presence gate through a compiler
overlay fails the intended first-zero learning assertion after positive
controls and owner retirement. This is delivery evidence, not continuous
MQTT-to-client latency measurement.

Full native ordinary and race lanes each passed 10,177 cases across 70 packages
(54 executed, 16 build-only), with all 433 required identities and 90 opt-in
skips. The new opt-in wire replay was exercised separately; all 125 parser fuzz
seeds also ran and passed. Subsequent fixture-only lint corrections received
ordinary/race targeted reruns (15 cases, 13 required, no skips) and refreshed
actual wire captures. No production/shared contract changed after the full
lanes. Final full-repository vet, Staticcheck and lint passed with zero issues.

All four nonzero handler controls passed their own fixed 1.10 paired-median
regression limit, using 12 balanced AB/BA process pairs and one million measured
operations per process:

| Control | Candidate/baseline paired median |
| --- | --- |
| FT8 fast | 0.974648 |
| FT8 compatibility | 1.005062 |
| WSPR fast | 0.982082 |
| WSPR compatibility | 0.968479 |

All 96 scored observations and eight separate matched CPU/allocation/live-heap
profiles were verified against frozen inputs and saved binaries. Fast controls
retained 917 B/op and 5 allocs/op; compatibility controls retained 1081 B/op and
8 allocs/op in both variants. Sampled allocation profiles attribute most churn
to existing Spot construction. Sampled live heap is a different measure; no
production latency, memory saving or sustained-load conclusion follows.

Two controller failures remain preserved. One rejected native empty branch
output before measurement. Another rejected the successful first baseline
because verbose Go logs did not emit the expected bench action. A reviewed
versioned continuation adopted that exact observation without rerunning or
discarding it, using the same binaries and timestamp. The interrupted first
pair has a recorded 614.6226298-second gap; the original protocol specified no
gap limit. This remains an interpretation limitation. No timing limit was
relaxed, no process-exit receipt was invented, and S5 needed no performance
exception. TSR-0098 records the actual schema, refusal controls and evidence.

Local evidence is under `.tmp/path-reliability-first-wave/s5/`: final connected
logs, `split-wire-v3/accepted.json`, `performance-v3/accepted-controls.json`,
`performance-v3/final-raw-verification.json` and their immutable predecessors.
The final proof/checker passed 104 schema/source/metric expectations plus 36
adoption/freshness controls and actual native branch checks. A lead-owned fresh
verification reviewed scope, source, raw observations, profiles and final diff.

Support documentation impact: Required. The PSKReporter guide, path support
card, troubleshooting route and ADR-0307 are updated. TSR-0098 records durable
checker learning without a new runtime decision. All new listening/live
fixtures used the existing approved cluster test executable path. No push,
production launch/restart, deployment, host tuning or firewall change occurred.

S6 and S7 remain serial follow-on work. Whole-cluster profiling must account
for S5's additional legitimate report volume; these controls do not compare
old zero-report discard cost with new successful processing as equivalent work.


## S6: physical populations and common resolution

S5 was checkpointed as `e82cec1` before S6 began. `candidate_query.go` plans
four logical roles as at most four unique keys. Each physical key is captured
and evaluated once at the maximum captured/query reference. Receiver values,
including hashes, are copied under shard locks; no sidecar pointer escapes.
The query is bounded temporary scratch, not an atomic multi-bucket snapshot.
Retained bucket ownership and its 448-byte logical layout are unchanged.

`predictor.go` selects one common resolution before numerical usability checks.
Both distinct fine directions require fresh active weight at least 20; otherwise
available coarse directions are used without fine fill. Fresh fine hints are
allowed only after all coarse candidates are unavailable. Shared populations
receive noise and the ordinary signal/histogram discount once. Beacons remain
receive-only and undiscounted. Selected failures are terminal; excluded failures
do not poison the chosen layer. `normalize.go` no longer adds overlapping
histograms or creates a blended age. Existing lifetime-floor and epsilon-OR
receiver liveness remain for S6; receiver hashes are unioned once across distinct
selected physical populations, with configured capacities kept separate.

The frozen policy and identity corpora cover qualification at/adjacent to 20,
aged weights 20 to 10 and 40 to 20, all coarse-availability pairs, 540/541 age boundaries,
store expiry, epoch/backward/common clocks, actual H3 boundary mapping,
concurrent shared-key updates, exact receiver intersection and slot lifecycle.
Literal stored-bin medians and materialization faults distinguish a removed
fine/coarse overflow from remaining real shift/directional failures. Connected
telnet delivery/filter/formatting gives LOW for 16 local strong plus 21 regional
weak reports; PATH LOW passes, PATH HIGH fails and DIAG retains 78 columns.
The 532-line selection corpus is a cohesive set of 12 policy vectors with shared
public-admission/fault helpers; every function remains below 80 lines.

Targeted final native checks passed 169 cases with no skips. Exact isolated
warmed-read allocation checks cover 21 funded shapes and detect a deliberately
escaping allocation in the measured batch. Existing warmed-update admission witnesses
remain required. Linux/cgo focused checks passed. The complete native ordinary
lane passed 10,345 cases across 70 packages (54 executed, 16 build-only), with 602
required identities and 90 opt-in skips. The complete native race lane passed
the same cases, packages and required identities. Final vet and Staticcheck
passed; full lint passed with zero issues after
removing an obsolete production freshness helper and an unused test parameter.

All 21 performance controls passed their individual 1.10 limits against the
actual S5 checkpoint. The common harness uses 100,000 predictions per process,
12 balanced AB/BA pairs and exact stopped-timer elapsed cost. All 504 scored
observations reported zero B/op and zero allocs/op. No observation was discarded
or rerun to seek a favorable result.

| Population | Off ratio | Shadow ratio | Enforce ratio |
| --- | ---: | ---: | ---: |
| All aliases | 0.789845 | 0.753346 | 0.702844 |
| Shared coarse, distinct fine | 0.694661 | 0.743961 | 0.681228 |
| Distinct fine pair | 0.952002 | 0.947387 | 0.998950 |
| Coarse twelve-by-twelve, disjoint | 0.794708 | 0.990586 | 0.983323 |
| Coarse twelve-by-twelve, shared hashes | 0.767666 | 0.866202 | 0.928962 |
| Beacon fine | 1.021957 | 1.070971 | 1.085262 |
| Beacon coarse | 0.929352 | 0.904557 | 0.978816 |

The fine-beacon increases, including 8.53% in enforce mode, remain recorded
costs despite passing their limits. These fixed API controls do not establish
whole-cluster latency or production impact. The largest scored pair gap was
2.647882 seconds.

All 42 matched, unscored CPU/allocation/live-heap captures have prediction-work
samples and the exact expected live bucket objects at the actual bucket
allocation site, retaining 448 bytes per bucket. Complete fixture ownership,
coarse sidecars, binaries, Go build IDs, synthetic Windows pprof mapping IDs and
raw receipts were checked. Profiling starts after seeding, stops and joins
before heap capture, and keeps the complete Store alive through post-GC capture.
Profile-mode observer overhead is diagnostic, not scored timing evidence.

The original harness completed 42 unscored preflights and no scored work before
the live-heap observer correction. Those artifacts remain intact. Version 2
passed all 42 native preflights, two observer-enabled probes and 55 damaged-proof
controls before scoring. Its benchmark-only correction received ordinary/race
targeted reruns: 169 cases, no skips, plus clean lint. The final lead audit
reconstructed ratios from raw integer elapsed values; its initial exact binary
equality check was corrected for one-ULP arithmetic-order differences without
changing the 1.10 guard or rerunning observations. No timing gate is waived.

A steered read-only Go quality review found no blocking source/scope issue;
lead verification retains authority. Independent mathematical evidence was
obtained separately before implementation, without the preferred ledger/design.
It establishes volume/identity arithmetic, not RF accuracy or QSO probability.
ADR-0308 supersedes ADR-0170 selection/scalar/age semantics. Support documentation
impact is Required. Package/config/operator and support guidance are updated;
YAML parsed values are unchanged. No new TSR is needed for this design repair.
Final code-map and diff checks complete this checkpoint. Local evidence is under
`.tmp/path-reliability-first-wave/s6/`, including
`performance-v2/accepted.json` and `performance-v2/final-raw-verification.json`.
S7 remains a separate serial slice. No push, deployment, production restart,
host tuning, new listening path or firewall change occurred.

## S7: Recent Support and Receiver Diversity (Accepted Under V6)

`report_support.go`, the existing bucket/capture owners and predictor carry
recent usable and cap-admitted report units separately from lifetime counts
and signal weight. Qualification uses fractional units before display weights;
tracked receiver quantities combine by identity before the one-unit minimum.
The configured receiver requirement remains separate from user PATHSAMPLES.
Observed support survives forecast replacement. ADR-0309 supersedes ADR-0134's
lifetime-floor policy and refines ADR-0308's temporary receiver qualification.

The steered implementation review added decisive shadow cases with admitted
support just below the floor while legacy CapLimited is false, exact joined
maintenance quantities, and large configured-floor arithmetic boundaries.
DIAG now shows recent units after the lifetime count. Report-present ordinary
and beacon witnesses preserve the 78-column line and document possible reason
clipping. No SNR threshold, forecast precedence or configured value changes.
Support documentation impact is Required; command HELP, operator guidance,
package/config explanations and support routing have been updated.

The full native ordinary and race lanes each passed 10,601 cases across all
70 packages: 54 tested and 16 build-only. All 832 required identities passed;
92 opt-in skips per lane were expected. The ordinary wrapper initially rejected
an inventory that mistakenly required the opt-in resource fixture. Only that
inventory entry was removed; all original package executions remain preserved,
and strict raw-output revalidation passed without rerunning them. Vet,
Staticcheck, golangci-lint, parsed configuration-value equality, and the focused
Linux/cgo run passed. That Linux run passed 236 cases with one expected resource
opt-in skip; an earlier shell invocation failed before Go execution because of
unquoted PATH spaces and remains recorded separately.

The actual original `5d40e7f` and final-candidate consumer comparison passed all
36 literal-oracle records. Before: 36 observed results. After: 16 observed,
10 blank and 10 existing CLOSED forecasts. Six retained observed results change
HIGH to LOW after duplicate local evidence is removed. These deliberately
selected cases demonstrate behavior, not production coverage or RF accuracy.
Full inputs, base/final outputs and binary/source identities remain in
`s7/whole-wave/` under the local evidence root.

The frozen native performance campaign stopped at its tenth guard:
`ordinary_coarse_12x12_disjoint/off` had a paired-median candidate/baseline ratio
of 1.1731677803 against the nonwaived 1.10 limit. Its median paired added cost
was 209.5965 ns per prediction. The first nine guards passed. All 240 scored
observations remain retained; the remaining 408 scored observations and
54 final matched profiles were not run. The 54 correctness preflights and
four profile-observer probes passed, but cannot replace those missing gates.
This fixture does not measure client delivery or establish a production effect.

The separate retained-memory campaign stopped on its first accepted-S6 baseline
fixture, before any candidate observation. It recorded exactly 65,536 separate
448-byte bucket allocations plus one unexplained 16-byte live allocation in
both immediate and post-GC deltas. The captured heap contains the exact bucket
owners and a 16-byte Go background-scavenger timer allocation. The observer
rejects the residual; it is not subtracted after seeing the result. No paired
memory guard passed or failed in that stopped campaign.

Read-only source/compiler/disassembly investigation excludes the receiver
intersection scan in the failing off-mode leaf. It confirms two new support
gate calls and larger value-copy/stack shapes, but does not establish the cause
of the full timing increase. A bounded optimization or new observer must retain
all numerical, fractional-support and ownership contracts, use a separately
frozen evidence version and preserve both stopped campaigns. No unchanged
retry, threshold relaxation or S7 acceptance is recorded by that campaign.

Approved v5 excepts S7 comparative elapsed-time acceptance, bound to the exact
600-file production/configuration fingerprint
`3dc0a4e9590fc75e7dc63f141271252727858e1643a3a5a7410f89c903b55b0a`.
It preserves all stopped results and unrun comparisons. It does not waive
correctness, race, zero warmed allocations, logical bucket size, the 40-byte
incremental retained-memory budget or the 54 matched native profile processes.

The v5 observer permitted only zero residual or exactly 16 bytes, one malloc,
one live object and zero frees, identical in both accounting epochs and solely
in class 16. A nonzero event requires the complete captured runtime scavenger
timer stack. Candidate residual bytes count against the memory budget; baseline
residuals give no credit. Every other discrepancy stops qualification. New
controllers, source/test builds and evidence are separately frozen under
`s7/v5/`; the original stopped campaigns remain immutable.

The affected resource observer policy and exact read-allocation tests pass
ordinary and race: 17 cases each, no skips. Production source/configuration
remains unchanged; only the resource fixture and its added policy test differ
from the completed full-lane source. The full ordinary/race evidence therefore
remains applicable, with observer-specific verification recorded separately.
Vet, Staticcheck and golangci-lint have also passed on the repaired observer.
All 54 v5 profile processes completed and their raw outputs, matched inputs,
CPU/heap artifacts, live owners, binary mappings and retirement were revalidated.
These are 27 pairs, 14 AB and 13 BA. Every CPU profile contains Predictor API
samples; sampling still provides no causal or production-latency verdict.
The separate allocation campaign passed same-reference off and shadow, then
stopped in enforce with one allocation across 100,000 calls. No allocation
profile was captured for that process, so its source is unattributed. The three
half-life allocation cases were not run. The profile campaign has no synthesized
completion marker; `v5/profile-partial-audit.json` records only the evidence
actually present. Earlier exact read allocation gates remain passed.

The new resource campaign also stopped at its first S6 baseline, before any
candidate observation. Seed counters contain exactly the expected 65,536
448-byte buckets plus one 32-byte allocation. Post-GC counters also include a
16-byte free. This differs from v5's permitted identical zero-or-16-byte event
and is correctly rejected. The post-only heap contains runtime scavenger timer
allocation stacks; it does not date individual allocations or frees. No paired
resource gate was established. Both stopped versions and all raw evidence remain
preserved; no unchanged retry or broader residual allowance was used.

[TSR-0099](troubleshooting/TSR-0099-global-heap-counter-observers.md) records the
global-counter limitation and the still-unattributed allocation failure. A
conservative allocation-based memory bound and standard single-processor API
allocation tests required separate approval. V5 alone left S7 uncommitted and
acceptance blocked at S6 `da1ab80`. Its failed and unrun observations remain
unchanged; the v6 evidence below establishes the replacement gates.

### V6 observer qualification and acceptance

Approved v6 changes measurement boundaries only. The 600-file production/config/
H3 fingerprint above remains exact. The conservative bound charges every byte
allocated by the candidate between Start and PostGC, then subtracts only verified
NEW baseline bucket/sidecar bytes. Temporary allocations, candidate frees,
baseline runtime noise and preexisting warm owners supply no credit. This is
Go heap-object accounting, not RSS or allocator span overhead. A bound above
40 bytes per bucket would block acceptance without proving actual retained growth.

All 40 planned native resource processes and 20 independent AB/BA paired guards
passed. The conservative incremental bounds were 32.000000 to 32.317871
bytes per new bucket, each below 40. Exact named live bucket/sidecar owners,
allocator classes, all five counters, class/global lower bounds, inputs,
source/build identities and process retirement passed raw revalidation. Logical
bucket size is 456 bytes; its measured allocator class is 480 versus S6's 448.

The first native baseline passed, but the original v6 report parser rejected
Windows CRLF output. A separately frozen transport adapter validates that exact
saved process and profile, then executes only the remaining 39 planned processes.
No native observation was repeated or discarded. The original parser stop stays
immutable. The first pair's wall-time gap is recorded; these measurements make
no timing claim. Synthetic checks rejected 124 resource-proof faults and 44
transport/adoption faults before their respective native campaigns.

The six replacement warmed update controls passed with exactly zero allocations
per measured 100,000-call callback. Each has a separately seeded 100,000-call
warm callback, exact completed-state/recent-support assertions and a deliberate
one-allocation witness that must fail. Ordinary and race each passed 87 targeted
cases with no skips, including all six positive and six fault children. The helper
uses one scheduler processor and restores four; it neither attributes the old
four-processor allocation nor proves concurrent production allocation behavior.
Focused Linux/cgo passed 85 cases. The final targeted runs follow an equivalent
test-only comparison simplification requested by lint; production, resource and
benchmark fixtures stayed unchanged. Vet, Staticcheck and lint pass on final Go.

The earlier complete 70-package ordinary/race lanes remain applicable: production
and existing functional tests are unchanged, with the three affected/new observer
test files verified separately. All 54 original v5 CPU/heap profile processes
were freshly raw-revalidated against their original binaries, symbols and input
identities. They are evidence for unchanged production, not v6 executable captures.
The original v5 profile campaign remains stopped, with its allocation failure
unattributed; no completion record was manufactured for it.

Fresh final source/diff verification is lead-owned, not an independent review.
The earlier non-steered S6 mathematical review remains the scientific evidence;
this observer repair introduces no new scientific or RF claim. Support impact
remains Required, covered by the HELP/config/operator/support updates. ADR-0309
records the accepted support and qualification contract; TSR-0099 preserves the
observer failures and repair. S7 is accepted at this local `voacap` checkpoint.
Timing exceptions remain explicit; practical delivery impact awaits whole-runtime
profiling. No push, deployment, production restart, firewall change or full-cluster
runtime capture occurred.

Local evidence: `s7/v6/builds/`, `s7/v6/resource/`,
`s7/v6/transport/accepted.json`, `s7/v6/profile-reuse-audit.json`,
`s7/v6/v6-targeted-final-audit.json` and final analyzer/source verification receipts.

## Whole-Cluster Profiling Preparation

The [profiling plan](path-reliability-runtime-profiling-plan.md) isolates the
effect of S4 while retaining final S5-S7 behavior in both variants. It specifies
the approved stable executable path, separate target/driver owners, complete
delivery accounting and source/config/binary provenance. Current wildcard
telnet binding and always-active reference refresh require explicit experiment
adapters before a private replay can launch. Recorded inputs and the bounded
launch/controller implementation remain prerequisites. This is preparation;
no whole-cluster runtime profile, deployment or firewall change has occurred.
The accepted S7 source checkpoint is complete. The actual configured-runtime
binary, concrete corpus, experiment adapters and bounded controller remain the
next separately reviewed execution boundary.
