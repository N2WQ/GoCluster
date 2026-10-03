# V15 writer allocation investigation

Authority: approved V15-05, with the detailed implementation and evidence plan
reviewed by the lead. This is design-aware review, not independent certification.
The narrow writer change, targeted checks and retained runtime comparison below
are complete. The candidate reduced observed allocation traffic, but runtime
enqueue measurements still fail. This investigation does not establish an
enqueue-latency fix or overall acceptance.

## Narrow design

Retain the unconditional `Spot.FormatDXCluster` call after read-pause suppression
to preserve base-cache priming. Construct its trailing-LF string only for the
nil-server fallback; client-specific formatting remains unchanged. Append the
old newline transform directly into the writer's existing byte batch, with
capacity for the complete normalized record before appending. Treat every
message independently, including when the existing batch ends with CR.

Shared formatter and `normalizeOutboundLine` APIs, raw-control bytes, control
priority, complete-record threshold overshoot, timers, queues, scheduling,
read pause, errors, shutdown, client settings and prediction-age checks remain
unchanged. No new cache or worker is proposed.

## Contract-to-test matrix

Execution evidence below distinguishes component checks from the completed
diagnostic runtime comparison and unresolved overall acceptance.

| Contract/invariant | Failure/boundary | Stimulus/fault | Observable result | Evidence level | False-green risk | Exact checker | Owner |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Exact outbound bytes | Double CRLF, removed lone CR, merged message boundary | Literal empty/LF/CRLF/CRCRLF/repeated LF/NUL/invalid UTF-8; existing prefix ending CR; capacity edges | Independent literals and frozen two-ReplaceAll oracle agree, prefix preserved | Unit/property | Only implementation-derived expectations | TestWriterV15AppendNormalizedBytes; TestWriterV15AppendNormalizedOracle | telnet |
| Whole records and batch threshold | Partial record, changed overshoot or control order | Queued control plus two spots at threshold minus/equal/plus one | Exact bytes and expected flush boundaries | Consumer | Helper tests omit writer loop | TestWriterV15RecordBoundaryOvershoot | telnet |
| Base cache priming | Omit base call for diagnostic client | Alternate-comment writer, then joined writer and changed source time | Base formatter retains original time | Consumer/mutation | Oracle primes the tested Spot | TestWriterV15BaseCachePriming | telnet |
| Read-pause boundary | Prime or emit suppressed queued spot | Paused spot plus deliverable control | Control only, one suppression, base remains unprimed | Consumer/race | Only checking absent bytes | TestWriterV15PausedSpotDoesNotPrimeCache; TestWriterLoopDropsQueuedReadPauseSpotsButWritesControl | telnet |
| Formatting branches | Lost fallback LF or changed diagnostics | Normal, source-diagnostic and nil-server writer | Exact old transformed output | Consumer | Testing normal branch alone | TestWriterV15FormatterBranches | telnet |
| Prediction reuse | Reuse after expiry/state change or recompute at inclusive boundary | Actual writer at zero/one-second/one-second-plus-ns age, rollback and changed noise setting | Exact independent-fixture output and lookup/display counts | Consumer | Testing only formatter in isolation | TestWriterV15PredictionStateAndAge; TestPathPredictionEnvelope* | telnet |
| Control and close semantics | Normalize raw bytes, conflate nil/empty raw, consume spot after close control | Binary raw control, whitespace line, empty nonnil raw, closing control before queued spot | Literal output and queued spot left unconsumed | Consumer | Closing benchmark hides the spot path | TestWriterV15ControlBytesAndClose; TestWriterLoopBatchesAndPrioritizesControl | telnet |
| Failure lifecycle | Writer survives failed output or leaves a goroutine | Existing spot/control failure fixtures | One failure/close and joined writer | Normal/race | Race command without live writer stimulus | TestWriterLoopDisconnectsOnSpotSendFailure; TestWriterLoopDisconnectsOnControlSendFailure | telnet |
| Actual allocation reduction | Compiler already removed work or benchmark skips spots | Retained executable assembly, unchanged source plus guarded benchmark overlay | Exact observed record count, CPU/bytes/allocations per operation | Assembly/benchmark | Existing close-first BenchmarkWriterLoopBurst never drains its spot | BenchmarkWriterV15SpotDrain; BenchmarkWriterV15AppendNormalized | writer fixture |
| Runtime usefulness | Microbenchmark promoted to latency acceptance | Comparable retained workload after measured local improvement | Original per-client/minute checks and failed results preserved | Matched diagnostic | Subtracting p99s or changing offered load | TestPC92RuntimeQualification and external diagnostic reconciliation | lead |

## Evidence retained before production changes

`D:\codex-gocluster-v15-20261002\writer-analysis\prepatch\manifest.json`
records the unchanged writer source and adjacent formatter/test contracts.
`telnet/server.go` SHA-256 is
`934F792974F30301A971443BFCAE6AA63AB032931B9DB9A864844467D1FC9A2B`.
It matches the frozen stage-diagnostic source byte-for-byte.

Read-only `go tool objdump` inspection during the completed OFF load's drain
confirmed the current binary calls `FormatDXCluster` and `runtime.concatstring2`
before testing the server pointer. The client-specific branch then overwrites
that temporary string. The compiler passes stack scratch to concatenation, but
the pinned Go 1.26.4 runtime scratch is only 32 bytes. An ordinary 76-byte line
plus LF therefore uses heap backing. The binary also calls the two-ReplaceAll
normalizer and copies its returned string into the byte batch with `memmove`.

Exact commands, executable/tool hashes and assembly output hashes are in
`writer-analysis\assembly-provenance.json`. The inspected runtime executable
SHA-256 is `54B8B5EB933D1EF7247C11131BFC1EBBBE10121ABA075545CD81FF4CC048B3AB`.
The retained late CPU profile attributed 0.59 of 107.11 CPU seconds to outbound
normalization. That observation does not establish it as the enqueue bottleneck.
The expected ordinary saving of two string allocations was subsequently
confirmed by the guarded before/after benchmarks below. There is no retained
allocation-stack profile establishing a stronger runtime attribution.

## Execution plan and status

The real-writer fixture checks every output byte and counts every spot. Shutdown
occurs only after all required spots reach the sink, and the writer is joined
before counters are read. An independent Spot supplies expected formatter bytes;
cache-priming tests never preformat the tested Spot. The new fixtures use only
the old production API. The isolated baseline overlay adds the same append
checks with a test-only shim reproducing the old normalization-plus-append;
the candidate removes that shim and uses the production helper. Every common
test and benchmark fixture is byte-identical across the two frozen sources.

The unchanged guarded baseline was captured before the production patch. The
following targeted checks then ran on the candidate:

```text
go test ./telnet -run 'Test(WriterV15|WriterLoop|PathPredictionEnvelope)' -count=1
go test -race ./telnet -run 'Test(WriterV15|WriterLoop|PathPredictionEnvelope)' -count=1
go vet ./telnet
```

Baseline normal checks passed in 0.216 s; candidate normal checks passed in
0.210 s, race in 1.320 s, and vet passed. Four overlay corruption controls each
failed its intended assertion: missing base priming, naive LF replacement,
cross-message CR merging, and the close-first benchmark that delivered no spots.
No compile failure was accepted as mutation evidence. Logs and exact overlay
hashes are retained under `writer-analysis\mutations`.

Ten alternating before/after benchmark pairs used retained binaries, Go 1.26.4
on Windows/amd64, Intel i9-10900, `GOMAXPROCS=2 GOGC=50 GOMEMLIMIT=1536MiB`.
Each invocation used `-test.bench=^BenchmarkWriterV15 -test.benchmem
-test.benchtime=300ms -test.count=1`; all nine cases checked their observed record
counts. `run-matched-bench.ps1`, all twenty logs and `matched-bench/summary.json`
retain exact commands, hashes, samples and paired ratios.

| Case | Before median | After median | Before bytes/allocations | After bytes/allocations |
| --- | --- | --- | --- | --- |
| Actual ordinary spot drain | 727.6 ns/spot | 558.9 ns/spot | 240 / 3 | 80 / 1 |
| Actual source-diagnostic drain | 1,330.5 ns/spot | 1,137 ns/spot | 405 / 6 | 245 / 4 |
| Actual nil-server drain | 633.35 ns/spot | 534.25 ns/spot | 160 / 2 | 80 / 1 |
| LF normalization, retained batch | 83.095 ns/op | 31.94 ns/op | 80 / 1 | 0 / 0 |
| CRLF normalization, retained batch | 154.8 ns/op | 37.605 ns/op | 160 / 2 | 0 / 0 |
| Multiline control, retained batch | 149.75 ns/op | 60.865 ns/op | 40 / 2 | 0 / 0 |

All three growth cases also improved and retained only their necessary output
buffer allocation. This is a local writer measurement; the guarded sink is not
a real network workload. No cluster enqueue result is inferred from it.

Baseline test executable SHA-256:
`E5F643667C3B3DC4269365FEDF06080AE34C4404437F92682D51D79E4DF111AB`.
Candidate test executable SHA-256:
`902A1B101715CA438FD8C66C39DB74BF8D2CF5F1471A327B3B07AFDDFC17F32A`.
`writer-before-source.json` and `writer-after-source.json` prove the isolated
source difference consists only of `telnet/server.go`, the new
`telnet/writer_normalize.go` and removal of the baseline test shim. The initial
direct-binary invocation failed PowerShell argument parsing before executing
benchmarks; its log remains retained, followed by the successful quoted-argument
baseline run.

Final direct diff review found no changes to shared normalization/formatting
APIs or writer scheduling. Overall final-source acceptance remains separate work.

## Completed stage-disabled runtime comparison

The candidate ran from 2026-10-03 02:29:01.9354957 UTC through
02:55:46.8742294 UTC, including the complete 900-second offered load and
660-second drain. It used the unchanged public diagnostic wrapper, with stages
disabled, Go 1.26.4, service `GOMAXPROCS=2`, `GOGC=50` and
`GOMEMLIMIT=1536MiB`. The driver retained its original 20-P setting. The VM
remained paused, and no competing builds or benchmarks ran during offered load.
Read-only analysis of completed profiles occurred during the drain.

The frozen candidate source differs from the fresh OFF baseline only in
`telnet/server.go` and `telnet/writer_normalize.go`. Exact source and runtime
asset sets, complete environment presence/value snapshots, runtime settings,
prebuilt/executed binary identities, and before/after artifact closure all passed.
The helper executable is byte-identical to the baseline. These archived sources
precede later unrelated helper/proof corrections and are diagnostic component
evidence, not final-source acceptance.

Baseline RunID: `b80d2f73e1744dadbf5e5bbcd4bd7e69`.
Candidate RunID: `e32a951fdff848a3a3fbfafea9d74368`.
Candidate runtime SHA-256:
`DAFE2D3B7FA11EBE67019BD707B3C5E9BEEE5FF7E3E159713B6A016D71D2D4AF`.
Identical helper SHA-256:
`43024FACA616319F07AE4E6B09D6AF7F5E06BECBE8274307237CEF7295B484E2`.

Both runs delivered all 17,355,750 required reads across 116 recipients, including
15,000,000 client spot endpoints and 105,750 separate client control reads.
Each offered 150,000 new spot keys, 1,500,000 duplicate arrivals, 90,000 PC92,
1,500 PC93 and 300 WWV records. Each retained all 900 scalar samples and three
complete CPU windows. Per-minute cohorts were derived from each run's exact
publication ledger; small boundary differences were preserved rather than
replaced by assumed 10,000-token cohorts.

| Original measure | Fresh OFF baseline | Writer candidate |
| --- | --- | --- |
| Missing required deliveries | 0 | 0 |
| Overall client enqueue p99 upper range | 4.5–5.9 ms | 4.3–5.7 ms |
| Worst client/minute enqueue p99 upper | 7.4 ms | 7.3 ms |
| Violating enqueue cohorts, including overall cohorts | 1,450 | 1,186 |
| Client spot endpoints above 5 ms | 180,021 / 15,000,000 | 170,899 / 15,000,000 |
| Overall client first-byte p99 upper range | 11.8–16.0 ms | 11.1–15.5 ms |
| Worst client/minute first-byte p99 upper | 18.6 ms | 17.7 ms |
| Violating first-byte cohorts | 0 | 0 |
| Other original checker failures | 0 | 0 |

The original `TestPC92RuntimeQualification` result remains **FAIL** for both
runs. The external verdict is `diagnostic_complete`, `comparison_valid=true`,
`overall_accepted=false`; it validates the evidence without replacing the
original latency verdict. One sequential comparison does not establish the cause
of the observed latency differences. No percentile subtraction is used.

Global runtime sample 1-to-900 counters separately show the allocation effect:

| Runtime counter delta | Fresh OFF baseline | Writer candidate |
| --- | --- | --- |
| Total allocated bytes | 19,627,375,088 | 17,222,772,752 |
| Allocation count | 211,108,344 | 181,037,171 |
| GC cycles | 241 | 215 |
| Total GC pause | 36.611 ms | 36.933 ms |

These are whole-runtime observations, not a proof of protocol-owned memory or
of an enqueue cause. In the late 120-second profile, total sampled CPU was
96.24 seconds before and 95.59 seconds after. The old normalizer accounted for
0.40 cumulative seconds; the new append helper accounted for 0.33 seconds.
The remaining large sampled costs are network writes and owners outside the
approved V15-05 normalization/formatting changes. The profiles do not establish
another material candidate inside that boundary. The earlier seven-stage
diagnostic cannot correlate individual late endpoints with PC92 C processing;
its first interval includes driver/send/parsing/queue work as well as dedup.

All original verdicts, full client/minute metrics, publication ledgers, binaries,
profiles and manifests remain under `D:\codex-gocluster-v15-20261002\warm-writer`
and `warm-stage-pair-v2`. `writer-analysis\run-writer-runtime.ps1` and
`writer-runtime-source.json` retain the exact invocation and two-path overlay;
`runtime-profile-analysis\provenance.json` retains profile commands and hashes.
`warm-writer\diagnostic-summary.json` rechecks its raw artifact closure and
records its own analysis source hashes. Its SHA-256 is
`D4304970022D0176DD5263565E1B4D857506149955E479E4E5904FF4C1E5FF2E`.
The comparison verdict SHA-256 is
`B64441A016E9ECF753334643184581FC373D1C8A2900C971636817A0F74C7AD6`.

At 02:55:59.4597868 UTC, all recorded launcher, driver, service and helper
processes had exited; `writer-analysis\runtime-process-exit.json` retains that
check. The quiet CPU slot was then released. No further production changes or
reruns followed this comparison.
