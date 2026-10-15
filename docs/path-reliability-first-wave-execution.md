# Path Reliability First-Wave Execution

Authority: owner token `Approved v2`, received 2026-10-14. This record maps the
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
| S2 | Bounded incremental persistence | Accepted; local checkpoint |
| S3 | Distributed-contraction compaction | Not started |
| S4 | Fine-key sharding | Not started |
| S5 | Present zero-dB PSKReporter reports | Not started |
| S6 | Nonduplicated common-resolution selection | Not started |
| S7 | Recent support and receiver diversity | Not started |

No later slice begins before the preceding slice's required evidence passes.
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
