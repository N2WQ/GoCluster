# PC18/PC92 v16 performance validation

This record maps [approved v16](pc18-pc92-scope-ledger-v16.md) to its detailed
postapproval checks. It is a design-aware review, not independent scientific
certification. The two spot-history owners preserve their existing decisions;
the harmonic model is not recalibrated. The matrix below is planned evidence
until execution is explicitly recorded.

## Detailed contract-to-test matrix

| Contract or invariant | Failure or boundary case | Stimulus or fault | Observable result | Evidence level | False-green risk | Exact checker | Owner |
| --- | --- | --- | --- | --- | --- | --- | --- |
| V16-01/02: remove exactly the selected victim's bucket increments | Country-local deletion misses records or removes another key | Dense and sparse buckets; many countries; repeated increments across seconds; frozen old full scan | Exact remaining bucket maps match the frozen scan; unrelated records unchanged | Unit differential | Oracle repeats new traversal | `TestV16WhoSpotsMeScrubMatchesFrozenScan` | `spot/who_spots_me_v16_test.go` |
| V16-01/02: eviction cannot affect a later generation of the same key | Old buckets subtract from reinserted totals | Evict A, reinsert A, advance through A's original seconds, then its new expiry | Literal country counts remain correct; no stale bucket record | Unit consumer | Only checks victim disappearance immediately | `TestV16WhoSpotsMeVictimBucketCoupling` | spot |
| V16-01/02: keep normalization, country refusal, window and victim rules | Boundary changed; backward record alters recency; new tie order | Exact cutoff, large jump, in-window backward times, country cap; unique oldest victim and tied oldest set | Same accepted counts and victim eligibility; no deterministic identity promised among ties | Unit | Monotonic-only time or one-country fixtures | `TestV16WhoSpotsMeTimeAndVictimRules` plus existing store tests | spot |
| V16-02: no new retained index; both traversal branches are valid | Sparse-many-country case is slower or untested | Bucket cardinality below, equal to and above victim-country count | Both branches produce identical maps; no added retained fields | Unit, diff, benchmark | Warm fixture always chooses one branch | `TestV16WhoSpotsMeSparseDenseScrub`; `BenchmarkV16WhoSpotsMeScrub` | spot |
| V16-01/02: existing locking and detached query ownership | Query observes corrupted totals during eviction/expiry | Concurrent Record, query and explicit cleanup with bounded identities | Race-clean execution and final fresh query; caps and bucket coupling hold | Race | Concurrent tests merely call read-only methods | `TestV16WhoSpotsMeConcurrentQueriesCleanup` | spot |
| V16-01/03: harmonic output and retained payload order remain unchanged | Heap changes detector decision or corroborator order | Deterministic mixed-call traces with backward now, source timestamps, repeated fundamentals and suppressed harmonics | All four returns and retained per-call entries/lastSeen match the frozen pre-change detector after each step | Unit differential | New implementation supplies expected output | `TestV16HarmonicReferenceTrace` | `spot/harmonics_v16_test.go`, frozen reference |
| V16-01/03: preserve different global and per-entry expiry boundaries | Uses <= for global expiry or >= for entry retention | Literal exact-boundary and adjacent-nanosecond cases | Global equality retained; entry equality pruned; hand-derived decision tuple | Unit | Differential oracle alone shares a boundary mistake | `TestV16HarmonicExpiryBoundaries` | spot |
| V16-03: update every existing recency assignment in both directions | Refresh on a dropped harmonic is missed, or backward update fails to move earlier | Surviving future-dated fundamental; suppressed harmonic refresh; backward now; unrelated cleanup then rollback | Same decisions and index root/removal as literal trace and old detector | Unit | Tests only append new calls with increasing times | `TestV16HarmonicRefreshAndBackwardTime` | spot |
| V16-03/04: exactly one expiry item per live owner | Lazy duplicates, stale index after swap/delete, retained string references | Repeated refresh of one call, root/interior/last removal, full expiry, reinsertion and long churn | Owner/index cardinality equality; valid reverse indexes/order; cleared unused slots | Unit retained state | Heap length checked without scanning backing references | `TestV16HarmonicHeapOwnershipChurn` | spot |
| V16-04: backing converges with live population | Empty/drained heap retains peak backing | Fill, refresh, partially expire, fully expire, refill | Empty heap releases backing; nonempty capacity stays within documented spare bound; snapshots report exact capacity bytes | Unit, allocation accounting | Count drops while backing remains high | `TestV16HarmonicHeapBackingConverges` | spot |
| V16-04: equal-call refresh releases obsolete string backing | A heap item pins the first frame after the owner map receives a new equal key | Two explicitly normalized spots with equal callsigns but distinct backing; first is a substring of a 4 KiB frame | Heap and refreshed owner share the new header; omission mutant fails | Unit retained ownership, mutation | Constructor normalization cache silently interns both fixture inputs | `TestV16HarmonicRefreshReleasesOldCallBacking` | spot |
| V16-01/03: mutex and cleanup-worker lifecycle remain unchanged | New index accessed outside lock; cleanup runner silently changed | Concurrent ShouldDrop and explicit cleanup; start/stop existing runner | Race-clean state and joined test goroutines; unchanged shared cleanup runner | Race, direct diff | New test claims the existing Stop joins when it does not | `TestV16HarmonicConcurrentCleanup`; existing lifecycle contract | spot |
| V16-05: improve actual warmed maintenance, preserve allocations | Cold/small benchmark bypasses cap and expiry pressure | WSM full 32768-key/64-shard churn plus sparse/many-country cases; harmonic warmed steady expiry and repeated refresh | Baseline/candidate CPU and allocation comparisons; existing-owner maintenance adds no per-update allocations | Benchmarks, CPU/mutex profile | Initial-fill microbenchmark mistaken for steady-state evidence | `BenchmarkV16WhoSpotsMeRecordAtCapacity`, `BenchmarkV16WhoSpotsMeScrub`, `BenchmarkV16HarmonicSteadyExpiry`, `BenchmarkV16HarmonicRefresh` | spot, external artifacts |
| V16-06: checks falsify known broken repairs | Tests pass with stale scrub/index or wrong boundary | External overlays restoring stale bucket references, <= global expiry, skipped refresh index update, or duplicate heap insertion | Named targeted regression fails at intended assertion | Mutation | Compile failure or unrelated failure counted as sensitivity | Retained overlay, mutant build and targeted logs | worker artifacts |
| V16-05/06: measured runtime completion remains original acceptance | VM/build contention, loss or a diagnostic result is reported as a latency pass | Matched frozen 15-minute load/11-minute drain; same hardware/settings; no heavy concurrent guest/checks | Complete tokens and each-client/each-minute original latency targets; complete profiles and allocation proof | Runtime | Only averages or best windows reported | Lead-owned matched warm runs and final-source qualification | lead |

## Ownership and allocation checks

WHOSPOTSME keeps its existing entry map, country totals and bucket maps. Victim
selection remains unchanged. Its scrub operation chooses between the existing
bucket scan and direct deletion for the victim's existing countries, according
to the smaller cardinality. No side index, worker, setting or policy is added.

The harmonic index has one callsign per live lastSeen owner. The
implementation stores an index beside the existing timestamp and a typed
callsign heap, under the existing mutex. On 64-bit builds the logical increase
is 8 bytes per lastSeen value and 16 bytes per occupied heap slot, before map
spare capacity, heap spare capacity and overlapping replacement arrays.
That is not a complete allocation bound. Removed slots must be cleared.

Lead-accepted backing rule: empty heap releases its slice;
after removal, nonempty capacity is at most max(8,4×live), compacting excessive
capacity to max(8,2×live). Growth and compaction allocations are explicitly
reported; they cannot be described as allocation-free. Existing-call refresh
and maintenance without resizing must add no per-update allocation. These
ordinary ingestion allocations remain separate from the peer protocol budget.

The existing harmonic store has no hard population cap. This amendment adds
none. Index cardinality is coupled to that live population, with the same
recency deletion, rather than accumulating every historical update. The accepted
diagnostic API reports live calls and expiry cardinality/capacity/backing bytes
under the existing mutex; harness observation remains lead-owned.

The live heap is the only new backing owner. On growth, the old capacity and
new capacity coexist while append copies the headers; on explicit compaction,
the same overlap exists until assignment releases the old owner. Neither is
hidden by the steady-state capacity figure. The 1,000-call drain fixture
observed capacity 1,023 before partial drain, new capacity 200 for 100 live
calls, and 1,223 slots (19,568 logical array bytes on amd64) during that
replacement. Full drain sets the slice to nil. The map value changed from 24
to 32 bytes; this is an 8-byte increase per slot, including unused allocated
map slots, not just live entries. Go map tables, size-class rounding and GC
retirement are separate from those logical sizes. No absolute whole-ingestion
byte ceiling or inclusion inside the peer 480 MiB allocation claim is made.

Refreshing an existing call updates the heap string header as well as its
timestamp, so it does not independently pin an obsolete input frame after the
owner maps adopt the current equal key. Removal clears the retired slice slot.
The capacity test checks every unused slot, not only the visible slice length.

The production caller in `internal/cluster/output_pipeline_stages.go` and the
existing harmonic cleanup worker both pass `time.Now().UTC()`. Those recency
timestamps have no monotonic component. The heap preserves that common
wall-clock order, including forward/backward UTC jumps in the differential and
literal tests. No timestamp normalization was added. This evidence does not
claim equivalence for an arbitrary mixture of monotonic and nonmonotonic time
domains with inconsistent clock ordering.

## Execution status

The read-only baseline profile review reproduced 15.92 sampled CPU seconds for
WHOSPOTSME scrub and 9.55 for harmonic cleanup in the retained 660-780s profile.
The lead accepted the capacity, allocation and scalar-observation refinements
before implementation. All matrix findings are covered or checker-only
refinements within v16; no new product-policy choice is selected.

Both production changes are implemented. Artifact root is
`D:\codex-gocluster-v15-20261002\v16-hotpaths`.
Original production files and their SHA256 values were retained under
`baseline` before the first patch, together with `baseline/spot.test.exe` and
the warmed baseline benchmark logs. WSM-only production was snapshotted before
the harmonic patch in the sibling `linux` directory:

| Snapshot | SHA256 |
| --- | --- |
| `source-v16-whospotsme-only-01.tar` | `f7c1ddba9b4aa5ee5d469c8e0c7af87c8992418a33bad28c9da8e0148ce9aa99` |
| `source-v16-both-helper-diagnostic-02.tar` | `eba018bf07513ee13d729f4c2d8dc44eb3a98f22bcb931bf3acf6f5e60bf6e34` |
| Current `spot/who_spots_me.go` | `591c5439ed04485bad515f65d67478ad11f74b1c351dd3e7552e8bce6087f829` |
| Current `spot/harmonics.go` | `07bcd72c5c54f2b7b0842c70ecba477bc8e0fe73bd14595ff16ac9c8d0d36d3a` |
| Current `both-final.test.exe` | `16590642906bf8820d1c377cf06f5a82ac7ca8e29db14bbd4cedcc3529944043` |

Each archive has a sibling JSON file manifest and records unchanged files
before/after capture. The lead uses isolated production overlays on the same
warm source for matched baseline, WSM-only and both-change runtime runs.

Executed on Windows amd64, Go 1.26.4, using the retained v12 environment script:

```powershell
go test ./spot -count=1
go test -race ./spot -count=1
go vet ./spot
staticcheck ./spot
golangci-lint run ./spot/...
```

The final slice lane passed: normal 2.422s, race 7.505s, vet/staticcheck clean,
lint zero issues (`spot-final-lane-02.log`). Earlier normal/race also passed;
the first staticcheck run found a test-only struct-conversion simplification,
which was corrected before the complete slice lane was repeated. The frozen
old-detector oracle is a compatibility reference copied from the retained
baseline, with type/constructor names changed; it is not independent scientific
evidence. The literal expiry, refresh and rollback cases supply separate
hand-derived expectations.

All six retained external mutation controls built successfully and failed the
intended assertion (`mutants/results.json`, each overlay/source/binary/log):

| Broken control | Falsifying checker/result |
| --- | --- |
| Omit WSM victim scrub | `TestV16WhoSpotsMeVictimBucketCoupling`: old bucket corrupts new generation |
| Delete harmonic global equality | `TestV16HarmonicExpiryBoundaries/global_equal`: literal decision tuple changes |
| Omit refresh for retained fundamentals | `TestV16HarmonicRefreshAndBackwardTime`: suppressed harmonic fails to refresh recency |
| Omit bidirectional heap fix | `TestV16HarmonicHeapOwnershipChurn`: invalid heap order |
| Append duplicate index on refresh | `TestV16HarmonicHeapOwnershipChurn`: ownership cardinality mismatch |
| Retain old equal-call string header | `TestV16HarmonicRefreshReleasesOldCallBacking`: obsolete input-frame backing retained |

The initial string-backing fixture falsely passed its mutant because the spot
constructor reused the normalization cache. It was corrected to provide actual
distinct normalized headers directly, verify that precondition, and then
exercise real `ShouldDrop`. The final mutant fails and normal/race passes.
Compilation failures were never counted as mutation sensitivity.

The two concurrent mutation/query/cleanup tests also passed 100 repetitions
with mutex sampling enabled. `concurrent-mutex.pprof` and its retained top
report contain both owners' lock paths (13.05ms total sampled delay in this
short run). This demonstrates exercised contention and retained profile
evidence; it is not a production latency ceiling or a baseline comparison.

Preliminary warmed benchmark ranges below are diagnostic only. VM compilation
overlapped these measurements. The harmonic candidate here also precedes the
final equal-call header refresh, so it is not final-source performance evidence.

| Case | Original ns/op | Preliminary candidate ns/op | Allocations/op |
| --- | ---: | ---: | ---: |
| WSM full 32,768-key churn | 413,127–419,524 | 56,218–59,685 | 6 before/after |
| WSM dense, one country | 371,008–396,092 | 81,864–84,830 | 0 before/after |
| WSM sparse, many countries | 18,804–22,350 | 23,626–25,011 | 0 before/after |
| WSM dense, many countries | 51,792–61,470 | 18,868–25,914 | 0 before/after |
| Harmonic 10,000-call steady expiry | 107,009–116,859 | 2,761–2,775 | 1 before/after (existing 48-byte fundamental allocation) |
| Harmonic single-call refresh | 210.8–212.9 | 257.4–290.0 | 0 before/after |

Those preliminary sparse WSM and single-call harmonic readings prompted a
clean retained-binary comparison; they were not dismissed as noise. Before the
clean comparison, the guest's build driver was stopped and checked for absence
of any active test executable, then QMP paused the guest. SQLite's owner also
released its heavy-work slot. No timed test was paused. The exact paused state
and benchmark commands/timestamps/hashes are retained.

The three-round clean comparison ran at 21:49:28–21:50:03 UTC, alternating
baseline/candidate order. Final-source candidate results are in
`clean-comparison/{commands,measurements,summary}.json` and twelve raw logs.
These are diagnostic microbenchmarks, not a substitute for runtime acceptance.

| Case | Baseline median | Final candidate median | Allocations/op, before/after |
| --- | ---: | ---: | ---: |
| WSM full-capacity churn | 423.357 µs | 49.900 µs | 6 / 6 |
| WSM dense, one country | 281.826 µs | 69.498 µs | 0 / 0 |
| WSM dense, many countries | 51.364 µs | 14.790 µs | 0 / 0 |
| Harmonic 10,000-call steady expiry | 106.584 µs | 1.858 µs | 1 / 1 (48 B) |
| Harmonic single-call refresh | 212.0 ns | 192.0 ns | 0 / 0 |

The three-round sparse case differed by +1.35% with overlapping ranges. A
focused ten-pair run with 10,000 fixed iterations per sample, again alternating
order with the VM paused, completed at 21:51:08–21:51:18 UTC. Its median was
19.7355 µs baseline versus 19.1615 µs candidate (-2.91%); both remained zero
bytes/zero allocations per operation. `clean-sparse` retains every raw log,
command/hash and result. These measurements did not reproduce a sparse or
single-call regression, so no additional production optimization was made.

Adaptive one-second benchmarks execute different iteration counts. The WSM
full-capacity fixture advances simulated time per iteration, so the faster
candidate traverses more window rotations while both retain 32,768 keys.
Its lower bytes/op is not claimed as a retained-memory reduction or an
identical-duration simulated trace. The fixed-count sparse comparisons have
matching iteration/time advancement. Original whole-runtime gates remain
necessary even with the large warmed maintenance improvements observed here.

The matched runtime comparison uses three detached source snapshots based on
`2413beb47551d428d96d06a3f9178e2577d8ec9d`, each with the same v15 exact-key
repair and warm diagnostic fixture. They differ only by the two v16 production
algorithms. These are diagnostic snapshots, not the final v15 SQLite/helper
implementation. The VM remained paused and other heavy validation was deferred
during each measured load. Each completed run offered 150,000 new keys,
1,500,000 duplicates and 90,000 PC92 records to 100 clients and 16 peers over
900 seconds, followed by 660 seconds of drain.

| Diagnostic snapshot | Run ID | UTC interval | Overall client enqueue p99 range | Overall client first-byte p99 range |
| --- | --- | --- | --- | --- |
| Exact-key baseline | `cab4967f927444f2b519496fcc9ac08b` | 2026-10-02 21:51:58–22:18:37 | 13–15 ms | 20.6–26 ms |
| WHOSPOTSME change | `46ca720f219f429b8228f84383712063` | 2026-10-02 22:18:37–22:45:32 | 6.7–8.4 ms | 14.4–18.4 ms |
| Both v16 changes | `e746d2353d0841139b09db3c439ed40c` | 2026-10-02 22:45:33–23:12:25 | 4.6–6.0 ms | 11.9–16.1 ms |

All three completed runs delivered every required token, retained all three CPU
windows and 900 observations, and passed provenance checks. All still failed
the unchanged latency contract: the WHOSPOTSME run's worst per-client minute
was 14.3 ms enqueue and 27 ms first byte (1,490 failing cohort conditions).
The combined run met the first-byte threshold in every measured client-minute
(worst 20.4 ms), but enqueue still failed (worst minute 8.6 ms; 1,287 failing
cohort conditions). All four protocol dedupe caches drained to zero. No overall
or diagnostic acceptance pass is implied by these improvements.

In their matched 660–780-second CPU windows, total sampled CPU fell from
116.23 to 99.08 seconds. `WhoSpotsMe.Record` fell from 15.61 to 1.57 seconds;
`processOutputSpots` fell from 30.57 to 18.11 seconds. Harmonic cleanup remained
9.67 seconds in the latter run, and telnet writer cumulative CPU remained
45.26 seconds. Samples are diagnostic observations, not a causal decomposition
of every latency tail. Exact binaries, manifests, observations, verdicts and
profile reports are retained in the sibling `warm-matched-baseline`,
`warm-matched-wsm` and `warm-matched-both` artifact directories.

The combined run's late window sampled 107.11 CPU seconds. Harmonic cleanup
fell to 0.24 seconds, and `processOutputSpots` to 11.61 seconds. Other costs
varied: WHOSPOTSME Record was 2.32 seconds and telnet writer 51.29 seconds.
Consequently the exact maintenance reductions are supported, while total CPU
did not improve monotonically between every run. The full repeated-window and
delivery results remain necessary; neither sampling variance nor a proposed
explanation of changed write batching can waive the measured enqueue failures.

Final runtime CPU/allocation evidence and original each-client/each-minute
acceptance remain open. Direct slice review found no changes to detector
thresholds, normalization, victim selection, workers or shutdown semantics.
A fresh design-aware read-only review also checked exact encoded-key equality,
atomic expiry/deletion, country-total/bucket coupling, every harmonic recency
assignment and all four detector returns, strict expiry boundaries, cleared heap
references and spare-capacity compaction. It found no additional implementation
defect. No commands or independent certification are implied by that source review.
V15 actual native Linux validation continues separately; the completed helper
race pass is retained independently of the later SSH timeout while its next
command was paused during compilation. That context/session command never
started its timed tests and has no verdict. A fresh final-source native run is
still required after the lead's matched runtime series.
