# TSR-0134 - Reputation Lookup Cap and Overflow

Status: Monitoring
Date Opened: 2026-11-15
Date Resolved: n/a
Owner: Cluster maintainers
Technical Area: reputation lookup caches and overflow retention
Trigger Source: Chat request
Led To ADR(s): ADR-0355
Tags: reputation, cache, bounded resources, FIFO, expiry, string ownership

## RCA Summary
- What happened: A configured 50,000-entry lookup limit applied independently to
  each of 32 main-enrichment and 16 Cymru DNS shard maps. Full-shard insertion
  scanned that shard for expired records under its mutex before arbitrary
  capacity eviction.
- Why: Both cache constructors received the full configured maximum, and each
  shard compared its own size against that maximum. TTL was checked on reads or
  overflow, rather than by a background sweep.
- What fixed it: The replacement shares one namespaced owner with exact shard
  quotas summing to the configured maximum, reusable oldest-written FIFO slots,
  and owned string payload admission of at most 1,024 bytes per record.
- How we know: Original source at main revision `f813c88b` establishes the
  multiplication and sweep paths. Replacement source establishes shared wiring,
  quota distribution, pre-insert eviction and bounded copied payloads. Both
  native ordinary and race lanes passed 13,082 cases, including all 78 reputation
  cases with no package skips. Production improvement is not established.
- Operator/support answer: The limit is combined cached records, not concurrent
  clients or distinct IPs. The same IP can occupy both namespaces. Inspect shard
  pressure and retained payload separately from RSS; do not infer a current
  production incident from this source defect.

## Triggering Request
- Request date: 2026-11-15.
- Request summary: Correct the combined lookup-cache bound and avoid full-shard
  overflow work without changing source precedence or DNS request ownership.
- Request reference: Owner-selected combined-record meaning and exact
  `Approved v1` implementation scope.

## Symptoms and Impact
- At the shipped 50,000 setting, the old 48 shard maps could collectively retain
  up to 2.4 million records. This is a source-derived allowance, not measured
  production occupancy.
- A new address in a full shard triggered a scan even when no entry had expired.
  The lock covered that scan, so work grew with the selected shard's population.
- Approximately 50 concurrent clients, with up to 1,000 expected, does not
  determine address-history size or lookup pressure. No current production
  latency or memory problem was measured for this investigation.

## Timeline
1. 2026-11-15 - Current constructors, cache operations and consumers were traced.
2. 2026-11-15 - The owner selected one combined record budget across both caches.
3. 2026-11-15 - Shared ownership, fixed shard budgets, FIFO and payload admission
   entered the approved implementation and local validation scope.

## Hypotheses and Tests
1. The configuration already bounded the two caches together.
   - Evidence: `NewGate` gave the maximum to both constructors; `set` compared
     only the chosen shard's size with that same maximum.
   - Outcome: Rejected by source. The allowance multiplied by 48 shards.
2. TTL automatically removed idle cached addresses.
   - Evidence: `get` removed its requested expired key; overflow scanned the
     selected shard. Neither cache owned a background sweep.
   - Outcome: Rejected by source. Idle expired records could remain retained.
3. A strict record cap alone also bounds copied metadata bytes.
   - Evidence: Lookup results and address zones have variable string lengths;
     substrings can retain backing larger than their visible text.
   - Outcome: Rejected. Separate payload admission and owned backing are needed
     for the approved cache-owned string-byte guarantee.

## Findings
- Root cause: A nominal cache maximum was implemented as a per-shard threshold
  in two independently sized caches; overflow combined capacity enforcement
  with exhaustive expiry cleanup.
- The replacement enforces one total allowance without a global hit lock,
  record scans, append-only eviction history or new cleanup workers.
- Fixed quotas can evict before total occupancy reaches the cap. FIFO can evict
  a live head while another expired record remains. Large payloads remain usable
  but uncached; oversized replacement removes the prior cached value.
- The string bound excludes fixed structures, maps, allocator overhead,
  `time.Time` locations, upstream stores and caller-held results. It is not RSS.

## Decision Linkage
- ADR created: [ADR-0355](../decisions/ADR-0355-shared-reputation-lookup-retention.md).
- Decision delta: Combined ownership, exact shard quotas, deterministic FIFO,
  bounded payload admission and a detached diagnostic snapshot.
- Source order, tuple/negative promotion behavior, prelogin's no-network path,
  defaults, pending DNS ownership and background lifecycle are unchanged.

## Verification and Monitoring
- Source and tests received a separate read-only substantial Go review. Its
  findings were resolved: expired-transition benchmark calibration is bounded,
  and consumer worker fixtures use a long TTL while owner tests use literal
  expiry times. The lead reviewed the final source and owns executed validation.
- `pwsh -NoProfile -File scripts/test-windows.ps1 -Timeout 10m` passed:
  59 executed and 15 build-only packages, 13,082 passed cases, 97 skipped.
- `pwsh -NoProfile -File scripts/test-windows.ps1 -Race -Timeout 20m` passed
  the same 74-package/13,082-case lane with the same 97 skips and no race report.
  All 78 reputation cases ran and passed in both complete lanes, including the
  real constructor/consumer wiring, namespace separation, concurrent state,
  exact TTL and cumulative payload boundaries, cleared slots and owned backing.
- Skips comprised 56 external DXSpider checks, 20 opt-in profile/qualification
  fixtures, 12 subprocess helper entry points and nine archive/live/native or
  controller prerequisites. They are not evidence of external interoperability,
  live VOACAP/CC operation, native Windows H3 support or a deployed load test.
  No cache/reputation case was skipped.
- The public config-loader contract selection passed eight cases: shipped,
  missing, null, zero, negative, tiny and uneven values plus their parent.
  An initial new fixture wrongly assumed legacy normalization applied at the
  public loader boundary; strict presence/positive checks disproved that
  assumption, and the corrected fixture preserves existing loader behavior.
- `go vet ./...`, `staticcheck ./...` and
  `golangci-lint run ./... --config=.golangci.yaml` passed (lint: zero issues).
  Go 1.27.1, Staticcheck 2026.2.1 and golangci-lint 2.14.0 were verified locally.
- YAML header/comment-only checks passed for all 20 files; troubleshooting,
  support-agent and changed support-critical crawler-entry checks passed.
  YAML values are unchanged. Logs are retained under `.tmp/reputation-cache/`.

Approved v1 traceability:

| Approved obligation | Implementation | Distinguishing validation |
| --- | --- | --- |
| Combined cap and bounded overflow | `reputation/cache.go`, `gate.go`, `cymru.go` | Actual Gate shared-owner wiring, tiny/nondivisible quotas, namespace/FIFO vectors, cross-shard concurrent invariants and matched overflow benchmarks |
| Bounded copied payload and reusable state | `reputation/cache.go` | 1,023/1,024/1,025 cumulative bytes, separate string backing, oversized own-namespace removal, expiry-position and repeated churn invariants |
| Diagnostics and preserved consumers | `reputation/cache_stats.go`, existing Gate/worker paths | Coherent detached snapshots, exact pressure counters, controlled worker completion, lookup tuple/source/age behavior and admission without live network fallback |
| Operator/config/support contract | `data/config/`, `docs/domain-contract.md`, ADR-0355, this TSR and support indexes | Public loader vectors, YAML token comparison, record/link/support checks and source review |

### Local benchmark evidence

Native Windows/amd64, Go 1.27.1, Intel Core i9-10900, GOMAXPROCS=20. These are
medians of three samples from the same fixtures before and after the production
change. Regular samples use 200 ms; the complete-expiry transition uses a fixed
10 operations because each untimed fixture rebuilds all 50,000 records.

| Fixture | Original | Corrected | Allocation evidence |
| --- | ---: | ---: | --- |
| Warm hit, 50 addresses | 93.68 ns | 96.89 ns | Both 0 B/op, 0 allocs/op |
| Warm hit, 1,000 addresses | 167.2 ns | 110.5 ns | Both 0 B/op, 0 allocs/op |
| Unchanged overwrite, 1,000 addresses | 74.87 ns | 130.3 ns | Both 0 B/op, 0 allocs/op |
| Parallel mixed read/write, 50 addresses | 30.21 ns | 25.35 ns | Both 0 B/op, 0 allocs/op |
| Parallel mixed read/write, 1,000 addresses | 38.20 ns | 28.68 ns | Both 0 B/op, 0 allocs/op |
| Full single-shard insertion, all live | 3.748 ms | 349.1 ns | Original 149 B/2 allocs; corrected 31 B/4 allocs |
| Full single-shard insertion, all expired | 9.180 ms | 1.720 us | Original 144 B/1 alloc; corrected 32 B/3 allocs |

The overflow comparison deliberately uses one shard holding 50,000 records on
both implementations, isolating removal of population-dependent work. A separate
corrected workload uses the real 32-shard owner and its 50,000 combined cap: both
namespaces pressure one 1,563-record shard, with a verified eviction on every
timed write. Its median is 271.6 ns, 32 B/op, 3 allocs/op with prebuilt input keys.
Shared-main/DNS parallel medians are 29.31 ns at 50 addresses and 27.58 ns at 1,000;
both report zero allocations. These address sets approximate ordinary client
working sets, not a relationship between concurrent clients and cache occupancy.

Unchanged overwrites cost about 55 ns more in the isolated fixture: they now
validate retained payload and update FIFO/accounting. Warmed reads are similar
at 50 addresses and lower in this 1,000-address sample. Distinct insertion needs
owned string copies, so allocation counts rise even though measured bytes fall;
no claim of universally lower allocation count or latency is made. Fixed backing
can also reserve more structural memory at low occupancy than the former maps.

Because one matched CPU/mutex-profile run showed higher parallel cost, a longer
unprofiled comparison alternated the retained original and corrected binaries
for three pairs of exactly 100 million mixed operations at 1,000 addresses.
Original samples were 33.92/33.09/33.05 ns; corrected samples were
34.63/37.01/34.86 ns, all zero allocation. The corrected median is about 5.3%
slower (1.77 ns/op), unlike the favorable short-sample median above. Treat
ordinary parallel cost as broadly similar with a small measured regression,
not a proven throughput improvement.

Matched overflow CPU profiles use the same full single-shard fixture and a
two-second benchmark duration. The original profile attributes 96.8% inclusive
CPU to `sweepShardLocked`; the replacement has no such path and spends its time
in bounded payload/slot operations, map work and ownership copies. Profiles
include setup and calibration, and process different operation counts, so
sample percentages are not per-operation CPU ratios. A separate matched
10-million-operation parallel profile reports aggregate mutex delay of 745.8 ms
original versus 837.5 ms corrected, consistent with additional write work rather
than a contention improvement. No production p99 or whole-process memory gain
is established.

Artifacts and reproducible native commands are under `.tmp/reputation-cache/`:
`baseline-bench.txt`, `baseline-expired.txt`, `final-bench.txt`,
`final-expired.txt`, `final-shared.txt`, and the retained original/final test
images and profiles. Run the stable reputation binary with `-test.run=^$`,
`-test.bench=BenchmarkLookupCache(Hit|Overwrite|ParallelMix|OverflowLive)$`,
`-test.benchtime=200ms`, `-test.count=3`, `-test.benchmem`; run
`BenchmarkLookupCacheOverflowExpired$` separately with `-test.benchtime=10x`.
The expiry-transition fixture explicitly skips adaptive counts above 100 to
prevent excessive untimed rebuilding; the recorded fixed-count runs all passed.
The matched profiles are `paired-*-overflow-cpu.pprof` and
`paired-*-parallel-{cpu,mutex}.pprof`; longer alternating results are
`parallel-long-{baseline,final}-{1,2,3}.txt`. Baseline image SHA-256 is
`59D8710B5E48D8FAEDC65776A403D5F9CF7F37589ACA7CAB6F18B091965A2E7B`;
the corrected profile image is
`C2924AF26A66A8F9DAC6148EB4EE10A02D7D5EB589DF6F3CE5DA60A3FCA68466`.

### Operational monitoring

- Signals: `Gate.LookupCacheStats()` reports namespace/shard occupancy, quota,
  allocated slots, owned string bytes, capacity evictions, read-observed expiry
  and oversized-write bypasses. Aggregate bypasses include oversized keys that
  never select a shard. No new UI or logging stream is added.
- Rollback/reassessment triggers: Broken combined bounds or source isolation,
  missing freshness/queue ownership, unacceptable measured hit contention, or
  repeated legitimate lookup pressure after cache bypass/eviction. Rollback
  restores the old excessive allowance and scan behavior; no cache data migrates.

## References
- Original source: main revision `f813c88b`, `reputation/cache.go`, `gate.go`,
  `cymru.go`; replacement also includes `reputation/cache_stats.go`.
- Related ownership record: [TSR-0121](TSR-0121-cymru-pending-ownership.md).
- Related docs: [lookup retention](../domain-contract.md#station-reputation),
  [operator configuration](../../data/config/README.md).
