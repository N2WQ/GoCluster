# VOACAP flux and daily forecast execution

This records the local implementation and qualification of Scope Ledger v4,
authorized by the owner's exact `Approved v4`. The starting revision was
`2e50d6ad994ec5d16541b59a1050572b1eb83916`, on
`voacap-flux-24h-v3`. Deployment, restart, merge and push are outside this work.
The runtime contract is [ADR-0347](decisions/ADR-0347-voacap-flux-and-daily-forecast.md).

Qualification is still in progress. The functional lanes passed; the current
candidate has two inconclusive protected tail metrics. Performance acceptance
remains unmet.

## Scope and ownership

| Approved requirement | Implementation and evidence |
| --- | --- |
| Correct native receiver longitude; retire affected predictions | `internal/voacap/deck.go`, independent fixed-column tests, generation-1 retirement/fault tests; [TSR-0131](troubleshooting/TSR-0131-voacap-receiver-longitude-columns.md) |
| Noon flux, elapsed-time smoothing, blend and final-only rounding | `internal/voacap/solar_flux.go`, `solar_input.go`, independent numeric vectors and parser fuzzing |
| Compatible continuity, independent watermarks and last-good health | `sunspot_monitor.go`, `sunspot_monitor_state.go`, `sunspot_monitor_health.go`, restart/late-mean/error/cancellation tests |
| Absolute daily calendar ownership | `pathreliability/voacap_forecaster.go`, literal month/year/leap fixtures, missing-row negative witness and isolated native worker sample |
| Stable key, frozen validity, requested renewal and bounded coalescing | `voacap_fallback.go`, `voacap_v4_state_test.go`, ordinary and command lifecycle fixtures |
| Frozen persisted bounds and bounded retirement/recovery | Forecast store and coupled index/heap files; uncertain-commit, raw-record, malformed-band and restart tests |
| Truthful overview and command SSN | `internal/cluster/bootstrap.go`, coherent-publication fixtures, `telnet/show_prop.go`, zero/mixed-SSN fixtures |
| Resource and timing guards | Physical metadata, warmed allocation, paired daily hydration, row-growth, common serial/reader-overlap fixtures and full-worker qualification |

One worker, queue capacity 1000 and the two 50,000-entry live limits remain.
Migration markers share the delay owner. No generation history, second forecast
cache, idle refresh, additional worker or all-band engine reuse was added.

Support-agent documentation impact is required: solar freshness, cache
retirement, requested renewal and the new diagnostics change operator answers.
The path-reliability support card and operator/troubleshooting indexes are
updated alongside runtime/configuration documentation. Final source review also
corrected two documentation-only references to the lifetime setting; its actual
YAML name remains `cache_ttl_seconds`.

## Scientific and native evidence

A separate, non-steered scientific review derived the native coordinate
contract and ordinary-path geometry. It exposed the receiver hemisphere shift;
an inherited-context review or a second lead pass is not called independent.
The lead verified and dispositioned those findings. Later implementation and
test-controller reviews were separate source reviews, not independent physical
model validation. Final verification remains the lead's responsibility.

The native installation was copied to `.tmp/voacap-v3/engine`, excluding its
live `run` directory. Every native qualification invocation used this clone.
The baseline archive, module hashes and static-installation identity are in
`.tmp/voacap-v3/baseline/`.

The production-builder gate passed 36 invocations (28 distinct and eight
repeats), 192 rolling windows and ten negative witnesses. Its receipt is
`.tmp/voacap-v3/native4-production/report.json`; the controlling gate receipt is
`.tmp/voacap-v3/GATE_STATUS.json`. Earlier echo-only and prototype results remain
preserved and do not substitute for the production-builder geometry gate.

Qualified cases include both directed paths, Methods 20/30, zero SSN and paired
73/74 inputs across the selected calendars. Adjacent SSN values can change
sampled native output; this does not establish a universal change threshold,
physical accuracy, a calibrated daily R12 input or a production glyph benefit.

All 395 static files in the isolated installation were verified against the
frozen installation before and after the final native worker profile. Only
`run/` is mutable invocation work. Two artifact-checker errors (the work-folder
name and the small native fixture's capacity) were corrected; their failed
receipts remain separate from the successful native test and final receipt.

## Observed functional and worker evidence

Targeted configuration, forecast-calendar and display checks passed. The removed
hold setting is also rejected through raw YAML merge maps, aliases and merge
sequences. A separate source review found that loophole; the negative fixture
now preserves YAML syntax and checks its CRLF-safe insertion.

Bounded parser fuzzing passed 497,460 solar-feed executions and 317,400 native
output executions. These are local parser robustness results, not validation of
every future NOAA representation or native engine result.

The first full worker workload restored a mixed 50,000-owner population and
completed 2061 jobs: 2060 successes and one injected failure. It exercised
requested legacy rebuilding, final-hour renewal, two daily input changes,
partial command traffic, failure cooldown/command retry, a post-expiry gap,
1000 queued jobs, one rejected request and successful later admission. The
synthetic service took 1 ms; the real two-second backpressure gate was included
in whole-worker duration. The CPU profile covers hydration and worker phases,
not the initial disk seeding. This run is accounting/queue evidence; the
subsequently strengthened cache/raw-persistence assertions require a new run.

That strengthened run subsequently passed on the final frozen normal binary,
with 23 representative live/raw/index checks. It completed the same 2061 jobs
in 9.090 seconds, including hydration and validation. The actual native sample
also passed its two raw-persistence checks. Its instrumented cycles took about
195.0 and 94.1 ms. The observed parent-process OS peak working sets were
317,640,704 and 20,148,224 bytes respectively. These samples include process
setup and exclude native children; they are not Go heap or production ceilings.
The captured after-GC full-capacity Go heap profile is a sampled retained state,
not an exact process peak. Mutex/block samples aggregate goroutine waits and
must not be added to derive elapsed time or delivery p99.

The older cluster-level opt-in profile was adapted to generation-2 wire setup
and the actual-dequeue hour contract. Its unchanged 50,000-hydration/256-job
workload passed explicitly, including full raw persisted rows and frozen
metadata. It measured 0.398 seconds for the instrumented completion phase.

The isolated native worker sample completed a month-crossing job in about
184.3 ms and its ordinary replacement in about 91.5 ms, including persistence.
These are two actual worker cycles using controlled SSNs. The larger synthetic
workload is not native throughput evidence, and neither establishes production
capacity without production demand and arrival patterns.

## Memory and protected timing

The final representation uses a 128-byte inline map value and a 96-byte immutable,
pointer-free input/origin object. Against the original 96-byte entry, the added
32 inline bytes plus the allocated 96-byte object total 128 bytes. The physical
allocation fixture measured those sizes. Warm cache hits, replacement and delay
bookkeeping, including capacity insertion, measured zero allocations.

The 50,000-key row-growth fixture allocates actual compact backing arrays. Its
eight-row live owner retained 61,663,616 bytes; the 24-row owner retained
151,178,232 bytes, a growth of 89,514,616 bytes. Logical row contents alone are
41,600,000 and 124,800,000 bytes respectively (104 bytes per row). This fixture
has an attached empty disk store; it is not the complete hydrated disk index or
a whole-process memory ceiling. The separate identical-24-row baseline/candidate
hydration campaign applies the 20 MiB additional-retained-heap gate.

The first inline-metadata design failed the protected half-expiry timing gate:
candidate/baseline geometric mean 1.464889. A separate calculation of all 72 raw
observations confirmed chronological controls AA=1.017121 and BB=1.021605, both
inside the required interval. The first controller summary had reversed some
control orientations; its raw observations remain intact and the corrected
arithmetic still establishes failure.

A pointer-metadata repair removed map-element allocations but its controlled
half-expiry result was inconclusive: B/A=1.455747, AA=0.899364, BB=0.969959. The AA
control fell outside the accepted interval. It is not reported as a pass or a
binding failure. Diagnostic profiles then attributed 460 ms inclusive CPU to
the candidate expiry predicate within 910 ms of pruning, versus 570 ms of
baseline pruning. These profiles include setup and state oracles; inclusive
samples overlap and are not independent elapsed-time gates.

The subsequent repair moved deadlines inline and calculates the scan clock
once. It adds no index or policy. The current serial campaign uses 52 fixed
shapes, 12 balanced baseline/candidate pairs and symmetric chronological AA/BB
controls. Duration-only pilots freeze scales before scored observations. The
1.10 limit, control interval `[1/1.10, 1.10]`, per-observation 1 ms resolution
floor and actual-overlap floor of 1000 remain binding. No failed observation is
dropped, pooled, topped up or silently retried. Controller source, fixture and
binary fingerprints, raw intervals and append-only hashed receipts are retained
under `.tmp/voacap-v3/`.

## Remaining qualification

The subsequent critical-inline serial campaign completed all 72 observations:
47 metrics passed, one failed and four were inconclusive. Rotating delay
updates at 50,000 owners failed at B/A=1.207122 with qualifying chronological
controls AA=0.977545 and BB=1.049063. Expiry and cache-hit guards passed. The
four inconclusive metrics were small-cache controls; the controller also
revealed a case-order confound in which every chronological first process ran
the forward matrix and every second process ran the reverse matrix. Future
campaigns balance first case direction independently from binary order.
These observations are retained; neither failed nor inconclusive metrics are
treated as passes.

Delay profiling attributed the regression primarily to map lookup/update
costs. The next repair represents the delay key's fixed bidirectional identity
with a private typed ID: key size falls from 48 to 32 bytes, while its full
timestamp/state value remains 56 bytes. It changes no warm-up, retry or
retirement policy. The baseline is not padded, and the common timing adapter
changes only construction of that private key representation. Ordinary and
race targeted checks passed 210 cases each, with two explicit worker opt-ins
skipped. Timing qualification of this repair remains pending.

The final focused rotating-delay test subsequently passed all twelve pairs:
B/A=1.005986, chronological AA=0.987270 and BB=1.000885. This is the focused
regression result, not acceptance of every protected metric. A new complete
serial campaign uses the independently balanced case-order schedule and final
binary `901696B383EBB4C72023995B564A596A3F146BC8BCA87D9ACDBB56F48A16A935`.

The complete ordinary and race Windows lanes each passed 12,770 cases across
74 packages (59 executed, 15 build-only), with 96 skips. After private dead-code
and unused-result cleanup, the complete path-reliability ordinary/race package
checks each passed 1,267 cases with 12 skips (11 opt-ins and a platform H3 skip).
The affected solar/display ordinary/race selection each passed 78 cases without
skips. Vet, Staticcheck 2026.2.1 and golangci-lint 2.14.0 passed; lint reported
zero issues under Go 1.27.1. Intentional single-copy inline map scans have local
lint explanations; no timing or allocation guard was waived.

The final 52-shape serial campaign on binary `901696B3...A935` passed every
metric with qualifying controls (72 observations). Both half- and full-expiry
reader campaigns also passed all five metrics. The cache-insertion reader
campaign failed its actual-overlap p99 guard: B/A=1.100965, AA=0.956284 and
BB=1.036414. The other four insertion metrics passed, including trigger p99
at B/A=1.056603. The binding limit remains 1.10; this is a failure, not a rounded
pass. Those raw results remain under `.tmp/voacap-v3/scored-final-*`.

The remaining reader campaign was stopped after that failure. Its partial delay
observations remain preserved and are not an acceptance campaign. Diagnostic
CPU samples of the unchanged insertion algorithm attributed most work to the
50,000-owner scan, value copying and map iteration. Profiles include setup and
state assertions and do not supersede the paired timing gate. A compact private
cache-direction representation is being evaluated without changing the disk
direction, tie order, bounds or admission policy.

Final protected timings on the repaired representation, repeated retained heap
and final generated-map checks remain required. Production capacity, deployment
effects and before/after five-minute log comparisons remain unqualified. No
live deployment is authorized.

## Current compact-key result

The private cache key now uses the same compact direction ID as the delay key:
both keys are 32 bytes. Disk direction strings, database keys, ordering, limits
and admission policy are preserved. Literal wire-key/round-trip checks and
invalid-ID refusals passed. The complete path-reliability ordinary and race
checks each passed 1,272 cases with 12 skips. Final vet, Staticcheck and lint
passed; lint reported zero issues. The normal executable is
`0C9A6116A0E40CE40AE0D8EEBDA0410732F7A6262C08BF16A62362959FD901F4`.

The repaired cache-insertion reader campaign passed all five guards:
actual-overlap p99 B/A=1.040331, AA=0.958087, BB=1.005704; trigger p99
B/A=0.992157, AA=0.968410, BB=1.076202. The 72-observation repeated daily
hydration campaign also passed. Every paired additional delta remained below
20 MiB; the largest was 9,177,448 bytes (8.75 MiB). Chronological heap controls
were AA=0.999991 and BB=1.002794. No outlier was discarded.

All three full-worker profile workloads and the row-growth fixture passed on
the compact-key source. The latest fake/native receipts bind the new executable
and verify the isolated installation's 395 static files. These local results
retain the earlier workload and production-capacity limitations.

The next scored campaign, tied delay insertion, completed all 72 observations.
Three metrics passed. Actual-overlap p99 was inconclusive at B/A=0.963137,
AA=1.163291, BB=0.872093; trigger p99 was inconclusive at B/A=0.948738,
AA=1.248280, BB=0.806793. Both same-binary control pairs fall outside the
required interval. The pipeline stopped before the remaining four reader
campaigns and the new full serial matrix. Earlier serial/expiry passes belong
to the prior executable and do not close these remaining obligations.

A separate, non-steered test-strategy reviewer verified all 72 raw/log hashes,
receipt completion, chronological pair orientation and raw arithmetic. Actual
overlap counts were 2,036-2,048 per observation; insufficient overlap is ruled
out. The controls' refusal is correct. Trigger p99 is the third-highest of only
256 maintenance events, and overlap tail samples are clustered by those events.
Scheduler/GC involvement remains a hypothesis without event telemetry. A
measurement-design reassessment is in progress; no threshold, observation or
required workload has been waived or silently rerun.

Two separately retained diagnostic runtime traces used the existing executables
and unchanged 256-pass workload. Both completed with valid witnesses and binary
identities. Trace stacks show 49 baseline and 54 candidate GC mark assists in
reader allocation while holding the fallback mutex. This demonstrates a real
reader-side cost, but does not attribute the original control variation to GC.
The raw benchmark origin is absent, and a single trace/benchmark clock offset
does not satisfy the observed event-order bounds. Exact tail attribution would
need a separately instrumented diagnostic fixture. Artifacts and hashes are in
`.tmp/voacap-v3/diagnostic-reader-trace/`; neither trace is replacement scoring.

A separate design comparison and scope challenge found that preserving this
stress workload with a prospectively larger fixed event population is a
coherent next amendment. A resident-map benchmark would protect a different
workload and cannot silently replace this obligation. No larger scored campaign
is authorized or running at this checkpoint. The remaining validation requires
the owner's applicable revised approval; the implementation remains unqualified
for performance acceptance.

Current-state support-agent checks, tracked support-critical crawler comments,
YAML header/comment audit, generated-map freshness and diff whitespace checks
passed. Local link checking found 272 resolving relative file targets and 261
unchanged repository-root ADR hyperlinks in the two generated maps. Those ADR
files exist, but their hyperlinks do not resolve relative to the map directories.
This inherited generator issue is outside this runtime change; no newly broken
local file target was found. Generated-map freshness does not establish link
correctness.
