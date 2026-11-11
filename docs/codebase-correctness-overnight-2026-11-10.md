# Overnight correctness work — Scope Ledger v1

## Authority and execution identity

The owner approved the chat's complete Scope Ledger v1 with the exact token
`Approved v1` before implementation. This file preserves that authority and
records execution; it does not authorize additional work.

- Planning baseline: `7417a99fb971`.
- Subsequent owner instruction: create and switch to `Nov-11`.
- Execution baseline: `b21528357ba7` on `Nov-11`. Its only additional commit is
  the completed HELP update; the approved production repair targets are unchanged.
- Isolated worktree: `C:\src\gocluster-overnight`. The original checkout remains
  at the same clean HELP commit on `main`; no owner changes were discarded.
- Window: 2026-11-10 23:20 through 2026-11-11 07:20 America/New_York
  (2026-11-11 04:20–12:20 UTC). Stop starting slices by 10:20 UTC or earlier
  when validation needs a larger reserve.
- Deliver an ordered validated subset, with deferred and unsuccessful items
  explicit. Correctness and evidence take priority over slice count.
- Keep results local. No merge, push, release, deployment, production service or
  database operation, firewall change, or unrelated HELP/workflow edit is authorized.

## Approved queue

| Slice | Bounded behavior | Status |
|---|---|---|
| S1 | Grid pending merge keeps locator/provenance coherent; actual overrides derived; preserve CTY, observation, timestamp and expiry rules | Targeted validated; final lane pending |
| S2 | Flood cleanup preserves threshold-plus-one live history and existing configured actions, modes, partitions, expiry and overflow behavior | Targeted validated; final lane pending |
| S3 | Cymru pending reservations follow admitted queue/worker ownership; failed admission retains no new identity; old completion cannot remove newer ownership | Targeted validated; final lane pending |
| S4 | Documented propagation retention zero inherits the parent through the existing loader/normalizer and consumer | Targeted validated; final lane pending |
| S5 | Periodic server keepalive emits exact CRLF through bounded raw control, outside membership locks; generic whitespace and blank-command behavior unchanged | Targeted validated; final lane pending |
| S6 | Following S1, terminal grid-hook admission, lookup-before-writer joins, grid-only background cancellation/completion and store closure after all grid owners retire | Targeted validated; final lane pending |
| S7 | Stable session worker affinity preserves ordinary broadcast order across membership churn; direct SELF remains separate and immediate | Targeted validated; final lane pending |
| S8 | Optional measured DX normalization once per worker job and SELF match once per recipient using existing exact helpers, without new retained state | Queued, conditional |
| S9 | Optional temporal no-due return before allocation, with unchanged deadlines, force, order and model decisions | Queued, conditional |

S8–S9 require all feasible correctness work to pass and sufficient final-validation
reserve. S6 requires S1 to pass. One implementation slice writes at a time;
read-only investigation/review can proceed concurrently.

## Implementation and validation gates

For each slice: verify source and dependencies; derive observable failing cases;
add a red regression where feasible; implement the smallest complete repair;
run owner package ordinary/race and distinct integration/performance evidence;
review actual normal, failure, overload and retirement paths; then checkpoint.
Targeted validation is provisional until the final whole-branch lane passes.

Failed work is diagnosed within scope. If a new policy or material expansion is
required, preserve investigation, restore only the current slice's owned edits
to the last passing checkpoint, defer that slice/dependents, and continue safe
independent approved work. Do not waive checks or weaken tests.

Final relevant state requires full native Windows ordinary/race lanes,
`go vet ./...`, Staticcheck, golangci-lint, relevant transport/lifecycle/fuzz seed
checks, connected CustomSCP ordinary/race when runtime/delivery changes warrant
it, and before/after benchmarks/profiles for retained performance changes.
Update affected operator/support documentation and appropriate decision records;
generate/check code maps after final source/docs, review the actual diff, and
perform fresh final verification. Raw evidence lives in `.tmp/overnight-20261110/`.

## Exclusions and limits

Do not change reputation cache-cap meaning, enrichment wait policy, history
decoding/concurrency, batching/GC/worker defaults, CW caching, envelope ownership,
peer parsing, model/scientific semantics, or previously persisted grid records.
Small private helpers and deterministic operation seams are permitted; public
API redesign, broad reorganization and general testing frameworks are excluded.

Fixed queues and ownership-coupled deletion do not certify a process memory
ceiling. Grid hooks gain synchronization; stable affinity can leave skew after
departures; preserving flood history retains the intended live window. Measure
relevant costs. Existing synchronous storage and arbitrary callbacks remain
owned until return under ADR-0275; the work window is not a hung-disk shutdown
deadline. Blocked OS execution is a reported gap, not authority to modify policy.

## Review and falsifiability record

Three fresh read-only design investigations and a separate scope challenge were
completed before approval. Material findings were dispositioned: periodic sends
must occur outside membership locks; writer provenance regression must force
one pending batch; all grid maintenance/recovery owners must join; exact DX
normalization helpers must be reused; ordered subset replaces an inconsistent
prefix promise. The periodic-only raw-control refinement was re-challenged.

The lead owns implementation, source completeness, validation, disposition and
final claims. Review evidence is not transferred authority. No scientific/model
validity change or claim is part of this work.

### S1 detailed contract-to-test gate

| Contract | Distinguishing stimulus | Observable oracle | False-green risk | Checker/owner |
|---|---|---|---|---|
| Locator and provenance move together | Derived then actual; CTY-only then derived; actual then derived | Literal expected grid and flag, not a production-derived expectation | Direct store calls bypass broken pending merge | Cluster owner tests of pending merge and real writer/reopened store |
| Actual precedence survives batching | Conflicting records in one pending window | Reopened actual grid is unchanged and non-derived | Timer flush accidentally separates records | Long flush interval plus controlled producer completion and final drain |
| Other merge fields remain intact | CTY-only records, observations, out-of-order times and expiry | Explicit CTY/count/min-first/max-update/expiry values | Assertions cover only locator | Cohesive owner semantic cases |
| Existing lookup identity remains intact | Base calls and CTY-derived miss | Existing lookup results remain identical | New tests alone omit callers | Existing grid lookup and gridstore owner packages, ordinary/race |

## Execution evidence

- 04:20 UTC: verified clean `Nov-11`, current AGENTS.md and unchanged target
  source relative to the planning baseline. No production changes made yet.
- Native prerequisites: Go 1.27.1 windows/amd64, CGO enabled, Staticcheck 2026.2.1,
  golangci-lint 2.14.0. Isolated stable-path build preparation started.
- Initial preparation invocation passed a comma-list as one external PowerShell
  argument; discovery correctly refused it. Corrected to an explicit array in
  native PowerShell `-Command`; no test or source failure was involved.
- Baseline owner suites: `gridstore`, `floodcontrol`, `reputation`, `config`;
  661 passed cases, zero skips. Evidence: `baseline-owner-tests.log`.
- S1 red: unchanged extracted coalescer failed all three tuple orderings in
  helper and same-batch persistence tests (`s1-red.log`).
- S1 ordinary: `-Packages ./internal/cluster -Run
  '^Test(MergePendingGridRecord|GridWriterPendingProvenancePersistence|GridLookup)'`;
  15 passed cases, zero skips (`s1-green.log`).
- S1 race: corresponding cluster/gridstore selection plus existing durable
  precedence/merge cases; 17 passed cases, zero skips (`s1-race.log`).
- S1 source completeness: lead comparison and a separate read-only reviewer
  found no material issue. Extracted production writer retains batching, TTL,
  error/drop, metrics and final-flush behavior. TSR-0119 and support routing
  explain the repaired boundary and exclude old-data repair. Troubleshooting
  index validation and `git diff --check` passed. Final broad lane remains open.
- S2 detailed falsifiability: fixed-clock controller process/cleanup tests cover
  thresholds above one, all three actions, repeated cleanup, separate source
  thresholds, inclusive cutoff/expiry deletion and each selected DX mode.
  Existing full package tests retain forwarding/overflow/dedup coverage. Tests
  synchronously inspect actual decisions and output, not a copied prune algorithm.
- S2 red: all new cleanup regressions exposed live-history loss (`s2-red.log`).
  Full owner ordinary/race lanes each passed 17 cases, zero skips after repair
  (`s2-green.log`, `s2-race.log`). Lead source review verified the sole behavior
  change is cleanup's partition-specific retained count; existing mutex, action,
  cutoff, actor cap and arrival paths are unchanged. TSR-0120 preserves ADR-0062.
- S3 detailed falsifiability: actual queue admission and production worker loop
  are exercised with controlled local lookup completion. A read-only specialist
  identified timestamp ABA; the approved identity alternative uses request-held
  nonzero objects instead. Fullness is checked before allocation under the same
  producer mutex, and only successful admission publishes ownership. The count
  bound is queue capacity plus actual live workers; existing Start/Close and
  cancellation/drain behavior are preserved, without process-byte claims.
- S3 red: 3,016 pending keys for a 16-slot queue and refused same-time retry
  (`s3-red-admission.log`); unconditional completion deletion removed newer
  owners (`s3-red-completion.log`). Full reputation ordinary/race each passed
  42 cases, zero skips (`s3-green.log`, `s3-race.log`). Cleanup refinement passed
  three race cases. Reviewer required literal five-minute expected queue counts;
  those final boundary assertions passed ordinary/race four-case selections.
- S3 source completeness: lead and separate read-only review found no remaining
  material issue after the literal TTL oracle refinement. Positive/negative
  cache, timeout/lookup order and queue/worker policies are unchanged. TSR-0121
  records overflow, generation identity, churn and cancellation limitations.
- S4 detailed falsifiability: two complete-directory loads explicitly set parent
  retention 13, propagation zero, and enabled true/false. Literal expected 13
  prevents fallback-seven false greens. An independent cluster consumer fixture
  copies only the required public files, runs the real loader and asserts the
  propagation destination's literal retention 13 and daily kind.
- S4 red: both full-loader cases rejected the documented zero (`s4-red.log`).
  Full config ordinary/race each passed 627 cases, zero skips; destination
  ordinary/race each passed five cases (`s4-config*.log`, `s4-consumer*.log`).
  Lead source review confirms exactly one allowed-zero key changed, with existing
  normalizer/consumers and all unrelated config rules untouched. TSR-0122 records
  restoration of the existing contract; no new ADR/default is needed.
- S5 detailed falsifiability: actual native/ziutek sessions consume literal
  CRLF plus a following sentinel; fixed raw ownership, nil/closed/full clients,
  writer priority/failure, and virtual-time actual-loop cadence/shutdown have
  distinct checks. A write TryLock in the overflow reporter detects retained
  membership locks without relying on a recursively successful read lock.
- S5 red: periodic admission and actual timer ticks produced zero queued
  heartbeats (`s5-red.log`). Production repair passed 46 ordinary/race cases,
  zero skips (`s5-green.log`, `s5-race.log`). Read-only review refined pause tests
  to seed distinct preferences, establish pending through beginHumanReadback,
  and wait for real post-flush quiescence; four affected cases passed ordinary/
  race (`s5-pause-final*.log`). The final virtual cadence completion assertion
  passed ordinary/race (`s5-cadence-final*.log`).
- S5 source completeness: lead and separate source review found no production
  blocker. Generic whitespace/blank-command paths remain untouched; temporary
  snapshot/payload ownership is population/queue-coupled and no worker is added.
  TSR-0123 and operator/support routing explain periodic-only behavior and the
  existing synchronous callback limitation.
- S6 detailed gate: component hook admission holds through cache/enqueue work;
  terminal calls miss without cache/metric mutation. Controlled CTY callbacks
  must prove async and timed-out sync derived sends persist before writer close;
  cache-shard barriers prove admitted hook completion, and repeated stop uses
  the actual production Once. Real background loops with per-instance operation
  seams must prove checkpoint/integrity/recovery plus warning-child completion
  before concrete store closure, including partial startup and recovery races.
  Existing runtime output/HTTP retirement witnesses remain mandatory.
- S6 red: the old retired lookup returned cached `FN20` rather than a terminal
  miss (`s6-red-terminal.log`). Initial admission/lookup/provenance selection
  passes 16 ordinary cases after hook fencing; this is not yet a slice verdict.
- S6 baseline hook benchmarks use a frozen pre-fence test binary, GOMAXPROCS=2,
  GOGC=50, memory target 1536MiB, eight samples (`s6-hook-baseline.txt`). An initial
  native dotted-flag invocation was rejected before execution; quoted argument
  arrays corrected it, with failed invocation retained separately.

- S6 source completeness: separate read-only production review found no material
  lifecycle defect. Grid-only cancellation precedes recovery launch; complete
  background joins and concrete Close share the runtime Once. Existing output,
  peer and global-cancellation order remains intact. Close failures now log a
  warning; ADR-0338 and TSR-0124 record the decision and support boundary.
- S6 final lifecycle selection passed 49 ordinary/race cases, zero skips
  (`s6-complete*.log`), including existing output/HTTP retirement. Earlier race
  selection also built gridstore but its regex selected zero gridstore tests;
  no gridstore execution coverage is attributed to that invocation.
- S6 review refinements: individual return channels reject any early repeated
  caller; overflow requires all 4,097 admitted async callbacks; failure cleanup
  releases and joins warning/startup owners; one blocked derived scenario now
  routes through real runtime retirement and actual Close. Affected ordinary/
  race reruns are retained as `s6-repeat-final*`, `s6-backlog-final*` and
  `s6-runtime-writer-final*`. The final whole-branch lane remains mandatory.
- S6 matched hook benchmarks (ten 500ms samples each, sequential quiet runs,
  fixed environment) measured cache hit 78.02 to 92.72 ns (+18.84%) and unchanged
  update 76.77 to 89.44 ns (+16.51%), zero B/op and zero allocs/op throughout
  (`s6-hook-*-final.txt`, `s6-hook-benchstat.txt`). These isolate the correctness
  fence, not full-pipeline/p99 cost. An earlier overlapping exploratory run is
  retained but is not the matched result. No production latency claim is made.

- S7 red: removing only worker 0's client forced the surviving worker-1 session
  onto worker 0 on the original rebuild. Actual workers then delivered literal
  newer before older in immediate and batched modes (`s7-red.log`). Exact batch
  fillers and sentinel completion establish causality without an arbitrary tick.
- S7 ordinary/race selection each passed 29 cases, zero skips (`s7-complete.log`,
  `s7-race.log`), including concurrent cached access, 128 session replacements,
  SELF, dedupe, worker batching and overflow. Review added a trailing worker-1
  sentinel before no-extra/drop assertions; three affected cases passed ordinary/
  race (`s7-order-final*.log`). Source review found no production blocker.
- S7 retention: one fixed-size integer per already-owned Client object; no new
  references, historical keys, timers, queues, counters or side maps. Existing
  membership/snapshot/job owners control lifetime; active client count already
  observes the primary population. This does not establish a process byte cap.
  Frozen-binary verbose benchmark setup reports Client 912 to 920 bytes on amd64
  (`s7-size-{baseline,candidate}.txt`), an eight-byte per-object increase.
- S7 local cost (eight 300ms samples, matched environment): 100-client rebuild
  3.238 to 3.446 microseconds; 1,000-client rebuild 26.70 to 35.91 microseconds.
  Cached paths remained about 2.25–2.45ns; their source is unchanged. Bytes and
  allocations are unchanged in all four shapes, cached zero/zero. The deliberate
  second map pass costs rebuild CPU; it prevents reordering without per-spot
  synchronization. No production throughput/p99 claim. Evidence:
  `s7-affinity-{baseline,candidate}.txt`, `s7-affinity-benchstat.txt`.
- S7 ADR-0339/TSR-0125/operator/support routing record stable ordinary affinity,
  possible departure skew and the separate immediate SELF lane.

## Closeout

Pending. No implementation or validation pass is claimed by this initial record.
Support-agent documentation impact: Required for implemented operator/config/
connection behavior; exact routing changes and ADR/TSR disposition follow slices.
