# ADR-0300: Path Observation-Time Accounting

- Status: Accepted
- Date: 2026-10-13
- Decision Origin: Troubleshooting chat

## Context

Late path reports exposed conflation of observation, arrival, decay reference,
and newest-observation time. Float32 storage and individually clamped query
references introduced additional numerical and sampling boundaries. Independent
mathematical review established the arithmetic; the owner selected product
semantics and authorized the three-slice scope with exact `Approved v2`.

## Decision

Use observation time for report aging and a separately supplied ingestion arrival
for validation. Missing observation uses arrival; missing arrival rejects.
Positive future skew through 120 seconds clamps to arrival, while larger skew
rejects path learning/metrics without rewriting the shared spot timestamp.
Original weights must be finite and positive before beacon limiting.

Maintain monotonic accounting reference R'=max(R,arrival) and a distinct maximum
validated effective observation across eligible raw reports. Advancing R cannot
refresh observation recency. Raw count remains a saturating, nondecaying count
of valid reports, including capped and numerically unusable reports.

With half-life H, age incoming count p=2^(-(R'-observation)/H) and weight w*p.
Apply the same aged-state admission fraction to both incoming capped quantities
and the active histogram. Retain the existing receiver-slot/count/weight-cap
approximation and arrival-order dependence.

Check numerical eligibility at float32 precision for the weighted aged raw
contribution and the final cap-scaled contribution, including safe prospective
writes. Representable increments absorbed by existing-bin rounding remain
admissible. An unusable aged raw contribution adds no raw/capped mass or capped
count/histogram trust and cannot allocate/replace a receiver slot; its raw
count/effective-observation metadata remains eligible. Do not round a decay
factor prematurely when its weighted product is representable.
If the aged raw contribution is usable but only its cap-scaled increment is
unrepresentable, retain the usable raw mass and reject the capped increment:
no capped mass/count or receiver allocation/replacement. The owner explicitly
clarified this boundary during slice-1 validation.

Copy bounded candidate snapshots under existing individual shard locks, including
coarse slot values. Evaluate selected candidates at common T=max(query,captured
references) before selection/expiry/freshness. Expose T with explicit availability;
Unix epoch zero is valid. This is a present-state evaluation, not a historical
query or an atomic multi-bucket event snapshot. Distribution failure in a needed
component makes the complete observed estimate unavailable, rather than yielding
a partial median.

Storewide diagnostic scans use a common reference, at most one retry, and honest
availability/as-of metadata. Preserve dated prior stats if a new coherent scan
cannot complete. Read total and per-band counts from one cached publication per
report. Discover the horizon during the provisional first scan; finish that
scan even if provisional evidence is unsafe, since it may expire at the final
reference. A lagging request consumes the sole retry reaching that reference;
further advancement makes reporting unavailable. No bucket-count-sized scratch
history is introduced.

Roll out sequentially: admission/callers/metrics; coherent accounting/precision/
sampling; consumer provenance/reporting/performance closeout. Complete the full
selected validation lane before each next slice. No intermediate deployment.

## Alternatives considered
1. Arrival-based aging: would treat old reports as fresh observations.
2. Strict-query abstention: coherent but owner selected common later evaluation.
3. Exclude unusable reports from raw diagnostics or require measurable bin change:
   owner selected the validated-input population and representable increments.
4. Ordered multi-shard locking: adds contention without atomic fine/coarse event
   inclusion because writers remain separate; unnecessary for time coherence.

## Consequences
### Benefits
- Exact uncapped scalar accounting within its usable numerical domain and
  declared tolerance, with coherent fractional capped count/weight/histogram.
- Freshness describes validated inputs without conflating reference advancement.
- Fixed-cardinality diagnostics distinguish rejection/unavailability from zero.

### Risks
- Float32 histogram rounding remains an approximation; no calibration or RF
  accuracy claim follows from arithmetic correctness.
- Common-time samples can be later than the requested time.
- Constant reference churn can make a fresh storewide report unavailable.
- Wider median accumulation can correct previous float32-created false ties.

### Operational impact
- Bucket target is at most 448 logical bytes (440-byte prior layout).
- Preserve slot bounds, no new retained history/index/worker, zero steady hot-path
  allocations, and the agreed measured performance gate.
- Original spot serialization, request/reuse/forecast/solar clocks, source-parser
  policies, YAML values, glyph thresholds, and fallback policies remain intact.
- Ordinary late-report hourly-window rewinding remains deferred.

## Implementation evidence

Slice 1 timestamp admission and caller/metric wiring is implemented. Targeted
cases and all 70 native Windows packages passed ordinary and race validation;
vet, Staticcheck, lint, YAML/comment/support checks also passed. The ordinary
lane completed in two segments after aligning test-only shipped-budget oracles
with the owner's twelve checks; no production stabilizer code changed. Owner
configuration retains twelve checks and a 0.95 receiver reliability floor.
Matched benchmark medians remained within the 10% review threshold, with zero
steady update/prediction allocations. That gate preceded accounting and consumer
implementation in slices 2/3. No intermediate deployment.

Slice 2 accounting, numerical guards, common sampling, and reporting are now
implemented. Native targeted package checks passed 372 cases, with two existing
opt-in skips. All 70 root packages passed both complete native ordinary and race
lanes (9,151 passing cases and 77 existing helper/opt-in skips in each).
Vet, Staticcheck, lint, YAML/comment/support and decision-record checks passed.
Logical bucket size is verified at 448 bytes. Production
retains directional float32 materialization but computes the final median once;
connected literal oracles cover blend shape, directional merging, unsafe
components, and quantile diagnostics when an output floor fails.

Six interleaved 500-ms benchmark pairs against the original revision retain
zero steady update/prediction allocations and the prior 16-byte reporting
allocation. The 10% review threshold is exceeded by partial shadow admission
(+21.6%, about 87 ns), elapsed-second aging (+14.6%, about 64 ns), and populated
beacon sampling (+10.3%, about 109 ns). Enforce partial-admission measurements
are noisy; no confident timing verdict follows for that case. Required checked
admission and complete-component validation are retained. This is a reviewed
accounting cost, not a production latency conclusion; controlled runtime
profiles and the final consumer gate were reserved for slice 3.

Slice 3 preserves observed sampling metadata through normal/beacon fallback
replacement and keeps request/reuse/forecast/solar clocks separate. The overview
consumes one dated publication; periodic logs explicitly distinguish unavailable
snapshots from known zero bands. The connected propagation-log reader requires
complete, same-emission count/weight pairs with equal available references,
groups modern samples by actual UTC date/hour and deduplicates equal references.
Its explicit JSON variance-availability field prevents missing or malformed
modern variance from creating a false zero-based degeneracy conclusion. Legacy
log assumptions remain documented. The live variance window accepts advancing
references only and retains at most one hour of fixed-band samples.

Final targeted checks passed 252 cases without skips. An instrumented bounded
fuzz run passed 112,902 executions with 74 new coverage inputs. Controlled
serial/four-worker process fixtures retained 16,384 buckets and 8,192 populated
coarse sidecars, with finite distribution/raw-count/cap guards and zero steady
allocations. Three 20,000/40,000-bucket retained-heap measurements plus disjoint
refill/purge checks passed; current live heap is about 575 bytes per bucket
including map/sidecar/cache overhead, comparable to the original implementation.
CPU/mutex/live-heap/allocation profiles are mapped to exact executable identities.

Six final matched benchmark pairs show partial shadow/enforce admission costs
of +19.85%/+21.28% (about 68/70 ns) and elapsed-second aging +11.13% (about 39 ns).
These exceed the 10% review threshold; required correctness checks are retained.
Final beacon timing is uncertain. There is no production latency, RF accuracy,
or calibration claim. All 70 root packages passed complete ordinary and race
lanes (9,268 passing cases and the same 77 existing helper/opt-in skips in each).
Every required case passed without skips. Vet, Staticcheck and lint passed.
Lint required removal of one constant-valued private test-helper parameter;
all 30 affected cases passed ordinary and race reruns, followed by the full
analyzers. This test-only cleanup does not invalidate other package results or
the unchanged production/profile binaries.

## Links
- Related tests: `pathreliability/observation_times_test.go`,
  `internal/cluster/path_timestamp_ingest_test.go`,
  `pathreliability/observation_accounting_test.go`,
  `pathreliability/accounting_math_test.go`,
  `pathreliability/store_reporting_test.go`,
  `pathreliability/snr_histogram_precision_test.go`.
- Related docs: [path README](../../pathreliability/README.md),
  [operator guide](../../data/config/PATH_PREDICTIONS.md).
- Related TSR: [TSR-0092](../troubleshooting/TSR-0092-path-timestamp-accounting.md).
- Refines time accounting in ADR-0065/0130; retains ADR-0134/0139/0170 policies.
- Supersedes / superseded by: none.
