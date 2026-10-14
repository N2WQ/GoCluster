# TSR-0094 - Path Median Speed Retention

Status: Monitoring
Date Opened: 2026-10-14
Date Resolved: n/a
Owner: Repository owner and Codex
Technical Area: pathreliability, exact median CPU cost
Trigger Source: Chat request
Led To ADR(s): none
Tags: exact median, performance, Windows, zero allocations

## RCA Summary
- What happened: the owner reported higher CPU use after the exact-median
  repair and approved one bounded speed experiment, v6, on `path_relia`.
- Why: exact multiword arithmetic costs more than approximate sums. That
  explains a calculation cost, but does not establish the cause of the whole
  production CPU increase. The attempted sparse/certified dispatch also adds
  work before integer fallback.
- What fixed it: the initial candidate passed correctness and resource checks
  but exceeded four declared regression limits, so the existing exact
  implementation was restored. The owner subsequently requested restoration
  of the same v6 candidate on `path_relia` for long-running cluster profiling.
  Overall cluster CPU improvement remains unverified.
- How we know: one frozen native Windows campaign completed 1,060 matched pairs
  and 2,120 valid processes. Raw-duration analysis and a separate lead
  recalculation agree. All twelve prescribed profile processes passed.
- Operator/support answer: compare matched running builds and their actual
  traffic before attributing overall CPU to p50. A dense-median speedup alone
  does not establish a production CPU reduction.

## Triggering Request
- Request date: 2026-10-14.
- Request summary: optimize calculation speed while preserving exact results,
  zero steady allocations, bounded state, and full testing between slices.
- Request reference: owner chat, exact `Approved v6`.

## Symptoms and Impact
- The existing method preserves tiny stored float32 contributions with five
  stack-local integer words. Earlier floating sums could change p50 and glyphs;
  restoring that approximate method was outside this experiment.
- Production CPU increase and production histogram frequencies remain unmeasured.
- The candidate preserved exact outputs, but some direct sparse/aged cases cost
  more than the existing exact method. The initial campaign did not deploy it;
  the later restoration prepares the owner's profiling build.

## Timeline
1. 2026-10-14 - Separate non-steered mathematical and statistical reviewers
   derived the arithmetic certificate and confidence contract before approval.
2. 2026-10-14 - Owner approved v6; detailed test-plan and source reviews required
   full validation, rational oracles, immutable inputs, and fixed measurements.
3. 2026-10-14 - Candidate passed ordinary/race lanes, analyzers, guided fuzz,
   allocation checks and matched corpus preflights. Exploratory versions were
   archived separately and were not pooled into confirmation.
4. 2026-10-14 09:31 EDT - Binaries, fixtures and randomized schedule were frozen.
5. 2026-10-14 10:03 EDT - The complete prescribed campaign failed retention.
6. 2026-10-14 - Twelve fixed-work profiles completed; existing exact production
   source was restored after candidate source and tests were archived.
7. 2026-10-14 - Owner requested restoration of the archived v6 candidate for
   compilation and long-running cluster profiling. The two benchmark fixture
   files already matched the archive; the median implementation and its
   candidate-specific test file were restored without redesign.

## Hypotheses and Tests
1. Sparse shortcuts and a sufficient float64 certificate preserve exact p50.
   - Evidence: full validation before selection; direct one/two-bin comparison;
     certificate `24 + D + ceil(log2(n)) <= 53`, with positive-bin count `n`
     and binary32 quantum-shift span `D`; otherwise unchanged integer arithmetic.
   - Outcome: Supported by proof and rational-oracle testing. Exactness refers
     to stored float32 bins; earlier materialization rounding remains unchanged.
2. This candidate meets the declared speed and regression requirements.
   - Evidence: frozen process pairs, lossless `elapsed_ns/N` ratios, all required
     workloads, both launch orders, no dropped outliers or optional extension.
   - Outcome: Rejected. Dense and two primary criteria pass, but four direct
     paired median regressions exceed 10%.
3. Local timing/profile results explain the reported production CPU increase.
   - Evidence: controlled synthetic inputs, fixed operations and query clocks;
     no matched production captures or measured production histogram mix.
   - Outcome: Inconclusive. Profile shares support attribution in their tested
     workload only; they do not establish production savings or causality.

## Root cause or best current explanation
The exact repair necessarily adds arithmetic work. The speed candidate reduces
that work for dense certified histograms, but dispatch/certificate bookkeeping
precedes exact fallback. The measured direct regressions are real outcomes of
this trial. Their precise instruction-level causes and field prevalence were
not established. Overall production CPU causality remains open.

## Findings
The host was native Windows, Go 1.27.1, Intel i9-10900 (10 cores/20 logical CPUs),
Balanced power plan, OS default affinity/priority and benchmark CPU setting 4.
The twelve-case prediction corpus uses normal ingestion APIs. Its final
materialized bins and expected outputs were checked; both 792-row manifests
were identical. Primary timed calls use 25% each one-bin, two-bin, certified
three-or-more-bin and aged failed-certificate cases. These frequencies are
synthetic, with no claim about production frequencies.

The 47 workloads comprise six primaries (40 pairs each), 24 prediction mixes
with 0/10/50/100% failed-certificate calls (20 pairs each), six serial/parallel
runtime workloads (20 pairs each), ten direct cases (20 pairs each), and the
existing dense benchmark (20 pairs). Prediction runs use 600,000 operations,
runtime runs 120,000, and direct/dense runs 6,000,000. Seed 61014 fixes balanced
AB/BA launch order and interleaved rounds. Binaries, fixtures, warm-up and query
clocks remain fixed; no measurement is dropped or extended after the result.

Forty pairs per primary use rank 28 of sorted candidate/baseline ratios as the
upper bound. Six-way Bonferroni coverage is at least 95.0233%, conditional on
independent, comparable matched pairs. The declared improvement bound is 2%.
All percentages below come from unrounded paired ratios, not ratios of two
separately calculated median times.

| Primary prediction | Paired median gain | Lower improvement bound | Meets 2% bound |
|---|---:|---:|---|
| normal/off | 3.6869% | 2.0778% | yes |
| normal/shadow | 2.2787% | 0.6172% | no |
| normal/enforce | 3.4456% | 1.9530% | no |
| beacon/off | 5.5846% | 3.6323% | yes |
| beacon/shadow | 4.2682% | 0.6638% | no |
| beacon/enforce | 3.0889% | 1.9171% | no |

The shipped YAML selects `enforce`; its results do not establish the declared
2% gain. The v6 any-primary gate passes through the two `off` cases. The
existing dense benchmark improves 57.2760%, exceeding its 20% point target.
Twenty pairs per direct case expose these blocking time increases:

| Direct case | Paired median time increase |
|---|---:|
| failed certificate, strict crossing | 26.7747% |
| failed certificate, near tie | 21.9877% |
| failed certificate, true tie | 15.2704% |
| wide two-bin weights | 14.6566% |

Certified three-bin cost rises 7.7064%, below its 10% guard. No tested connected
workload exceeds its 5% regression guard. Those facts do not waive the four
direct failures. The initial campaign retained no candidate under a relaxed
threshold. The later owner-directed restoration enables a cluster-level
evaluation; it does not change the recorded v6 gate result.

Profiles use the same two binaries, all modes, serial/parallel execution,
6,000,000 operations and a 183-second synthetic horizon. Median inclusive CPU
shares in `enforce` decline from 9.99% to 5.99% serial and 4.24% to 2.72%
parallel. Profiles include startup and untimed rational guards; inclusive totals
overlap. These are attribution figures, not production CPU savings.

All profile processes preserve 16,384 buckets, 448 logical bytes per bucket and
0 receiver sidecars in `off`, or 8,192 in `shadow`/`enforce`. Allocation profiles
are dominated by test-only `math/big` oracle work. Sampled live heap varies;
it cannot support byte-for-byte retained-heap comparisons. No median allocation
or lock site was introduced; parallel locks remain existing Store operations.

## Decision Linkage
- ADR created/updated: none. The initial automatic retention gate rejected the
  algorithm; the owner later requested the same candidate for profiling.
  [ADR-0301](../decisions/ADR-0301-exact-path-median-and-snapshot-recovery.md)
  continues to govern exact stored-mass behavior and snapshot recovery.
- Decision delta summary: exactness policy is unchanged; the profiling
  candidate has not demonstrated whole-cluster CPU savings.
- Contract/behavior changes: the private median dispatch is restored. Config,
  API, timestamp, admission, materialization, reader, glyph and retained-state
  contracts remain unchanged.

## Verification and Monitoring
- Candidate full native ordinary/race lanes each passed 9,401 cases across
  53 tested and 17 build-only packages, with the same 77 existing skips.
  Vet, Staticcheck and golangci-lint passed. Guided fuzz passed 555,835 general
  and 440,470 certificate-boundary executions, adding 79/180 coverage inputs.
- A later test-only carry refinement passed 83 targeted cases without skips
  under ordinary and race execution. Integer add/compare coverage is 100%;
  all intended median dispatches were exercised. Per-branch/full-corpus
  `AllocsPerRun` and all 2,120 timing processes report zero steady allocations.
- The retained carry sequence fills the integer words before adding its final
  quantum. Its standalone ordinary execution passes with 100% integer add and
  compare coverage; production arithmetic remains identical to the baseline.
- Restored-state full native ordinary/race lanes each pass 9,383 cases across
  53 tested and 17 build-only packages, with the same 77 skips. The added carry
  sequence also passes its standalone ordinary check and the full race lane.
  Final vet, Staticcheck and golangci-lint pass. Troubleshooting/support checks
  and final generated-map freshness checks pass.
- The subsequent owner-directed v6 restoration matches all four archived Go
  files byte for byte. Current path-reliability ordinary/race runs each pass
  475 cases with two existing skips; the full ordinary lane passes 9,402 cases
  across 53 tested and 17 build-only packages with 77 existing skips. Current
  vet, Staticcheck and lint pass, and the Windows cluster executable builds.
  Allocation and 448-byte bucket guards pass. These restoration checks are
  lead-owned; long-running whole-cluster profiling remains the owner's next
  measurement. Integer arithmetic and the retained carry regression are unchanged.
- Source review, independent mathematical/statistical review and lead raw-ratio
  verification actually occurred. Benchmark/profile evidence was coordinated
  and must not be described as independent production verification.
- Signals to monitor: matched build identity, actual histogram mix, median CPU,
  total CPU, latency, allocations, owner counts and receiver mode.
- Rollback triggers: the declared direct regressions triggered the initial
  restoration. The owner's later request explicitly restores the same tested
  candidate for profiling; a new algorithm remains outside that request.

## References
- Baseline commit: `9f34b88033889737dcdd916d4a8cd7654522c82b`.
- Local evidence: `.tmp/path-median-speed-v6/` contains the frozen manifest,
  complete raw records, analysis, binaries, final candidate source and lead
  validation. `.tmp/path-timestamp-v2/path-median-speed-v6-profiles/` contains
  all twelve profiles and identity/operation metadata. These ignored artifacts
  are local evidence; the tables above preserve the durable findings.
- Related record: [TSR-0093](TSR-0093-path-median-and-snapshot-recovery.md).
- Related docs: [path README](../../pathreliability/README.md),
  [support card](../../customgpt/support-cards/path-reliability.md).
