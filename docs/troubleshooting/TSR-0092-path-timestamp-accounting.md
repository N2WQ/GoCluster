# TSR-0092 - Path Timestamp Accounting

Status: Resolved
Date Opened: 2026-10-13
Date Resolved: 2026-10-13
Owner: Repository owner and Codex
Technical Area: pathreliability, cluster ingestion, path diagnostics
Trigger Source: Chat request
Led To ADR(s): ADR-0300
Tags: observation time, accounting reference, histogram precision, late reports

## RCA Summary
- What happened: source review found that one bucket timestamp served decay,
  freshness, and expiry, and could be rewound by a late observation.
- Why: incoming reports were added without aging to a monotonic accounting
  reference; capped incoming count remained one despite observation age.
  Float32 histogram conversions also had underflow/overflow boundaries absent
  from admission and temporary-distribution checks.
- What fixed it: owner-approved v2 specifies separate clocks, aged fractional
  admission, binary32 usability, common-time sampling, and honest diagnostics.
  Slices 1 and 2 implement input admission, caller/metric wiring, coherent
  accounting and sampling. Slice 3 implements fallback provenance and dated,
  availability-aware overview/log/report consumers; controlled profiling passed.
  Complete ordinary/race and analyzer gates passed, with fixed-slot/logical-size
  and collected-heap/churn evidence.
- How we know: current source at a4cbe65 established the defects; a non-steered
  independent mathematical reviewer derived the invariants and boundary vectors.
  Timestamp/ingestion and independent accounting/precision vectors passed.
  All 70 root packages passed complete ordinary and race gates for both slices.
  Connected consumer checks, instrumented fuzzing, controlled process profiles
  and retained-state/churn measurements passed. All 70 packages passed both
  complete lanes (9,268 cases and 77 existing skips per lane), and analyzers
  passed after a private test-only lint cleanup.
- Operator/support answer: do not interpret raw report count as usable mass or
  an RF accuracy estimate. Check admission counters before changing thresholds;
  no intermediate deployment is authorized by this repair.

## Triggering Request
- Request date: 2026-10-13
- Request summary: evaluate scientific completeness and wiring, then repair
  timestamp accounting in small sequential slices with full testing between them.
- Request reference: owner chat, exact authorization `Approved v2`.

## Symptoms and Impact
- A late report could make mass depend on arrival ordering and incorrectly
  determine freshness/expiry from its timestamp.
- Float64-positive incoming evidence could vanish in float32 but still displace
  a receiver; finite bin inputs could overflow temporary histogram calculations.
- Independently clamped lookups could combine evidence represented at different
  times when a query lagged one or more bucket references.
- No production accuracy, prevalence, or QSO impact was measured by this review.

## Timeline
1. 2026-10-13 - Source/caller/configuration review and independent arithmetic
   established the accounting requirements.
2. 2026-10-13 - Owner selected the 120-second clamp policy, retained raw
   diagnostics for unusable reports, common sampling time, and admissibility of
   representable increments absorbed by rounded addition; approved v2.
3. 2026-10-13 - Slice-1 targeted timestamp and ingestion checks passed;
   complete per-slice gates and subsequent implementation remain pending.
4. 2026-10-13 - The initial full run exposed a pre-existing shipped-config
   expectation of six stabilizer checks versus the owner's configured twelve.
   Untouched source reproduced the failure. The owner confirmed twelve; the
   test-only expectation was aligned, and the full actual-worktree gate restarted.
5. 2026-10-13 - The owner clarified that usable raw mass survives cap-only
   underflow, while the unusable capped increment adds no capped trust or slot.
6. 2026-10-13 - Owner also selected a 0.95 spotter reliability floor. The final
   slice-1 ordinary/race lane used both values; outdated shipped budget oracles
   now assert twelve checks / sixty seconds, retaining the independent low-P
   three-check / fifteen-second rule.
7. 2026-10-13 - Slice-2 accounting, precision, sampling and bounded reporting
   completed its ordinary/race/analyzer gate before slice 3 began.
8. 2026-10-13 - Connected consumer tests and review found a histogram label
   parsing error and missing variance interpreted as zero; both were corrected
   within the approved availability/reporting scope.
9. 2026-10-13 - Slice-3 complete ordinary/race gates passed 9,268 cases each,
   with unchanged 77 helper/opt-in skips; full analyzers passed after removing
   a constant test-helper argument. The 30 affected cases passed both reruns.

## Hypotheses and Tests
1. Observation-order changes raw additive mass.
   - Evidence/commands: store update/sample source; independent exponential sum
     for H=240 s and observations 0, -200, +10 at query +10.
   - Outcome: Supported by source; required corrected mass is
     2.516785807486235. Slice-2 regression passes all six arrival permutations
     in off, shadow and enforce modes with nonbinding caps.
2. Float64 positivity is sufficient for histogram admission.
   - Evidence/commands: independent binary32 vectors; 2^-160 rounds to zero,
     while 2^100 multiplied by 2^-160 yields representable 2^-60.
   - Outcome: Rejected; usability belongs to the weighted final contribution,
     with checks after cap scaling and safe histogram accumulation.
3. Independent nonamplifying lookup clocks guarantee coherent merging.
   - Evidence/commands: independent references 0/20, half-lives 10/20, query 10.
   - Outcome: Rejected; common time 20 gives mass 1.25, whereas independent
     clamping gives mixed-time mass 1.5.

## Root cause or best current explanation

At the inspected revision, `Store.updateBucket` used the source timestamp as
`lastUpdate`, decayed existing state only for positive elapsed time, and added
the incoming full weight. A late report therefore rewound the reference without
aging its own contribution. Sampling also used that same reference for report
freshness. Receiver admission treated incoming effective count as one, while
histogram operations rounded decay factors before multiplication and accumulated
median totals in float32. The approved repair separates these responsibilities;
the first two slices now implement its input and accounting contracts.

## Findings
- Raw valid-input freshness is distinct from newest cap-admitted evidence.
- Common-time evaluation does not make multi-bucket event inclusion atomic.
- Capped admission deliberately retains current-state arrival-order dependence.
- Hourly report-metric window rewinding is separate and deferred.

## Decision Linkage
- ADR created: [ADR-0300](../decisions/ADR-0300-path-observation-time-accounting.md).
- Decision delta: explicit source/arrival/accounting/newest-observation clocks,
  binary32 usability, coherent query time, and bounded reporting failure.
- Existing fine/coarse shape, glyph thresholds, source modes, receiver-diversity
  floors, and fallback policies are unchanged.

## Verification and Monitoring
- Validation steps run: `TestObservationTimes*` and `TestPathIngest*` through
  the native Windows runner, with required cases; source-aware post-approval
  falsifiability review. All 70 root packages passed ordinary/race validation;
  the ordinary lane resumed at the failed package after a localized test-oracle
  update. Full race: 9,027 passed cases, 77 skips for helper entrypoints and
  opt-in external qualification/assets; required slice-1 cases had no skips.
  Vet, Staticcheck, lint and YAML/comment/support checks passed. Matched eight
  original and six delayed-report benchmark medians stayed within the 10%
  review threshold; hot updates/predictions retained zero allocations, while
  reporting retained its prior one 16-byte allocation. Slice 2 additionally
  passed 372 targeted cases and all 70 packages in both complete lanes:
  9,151 passing cases and 77 existing skips per lane. Logical buckets measure
  448 bytes. Six final interleaved benchmark pairs retain zero hot allocations;
  partial shadow admission (+21.6%, about 87 ns), elapsed-second aging (+14.6%,
  about 64 ns), and populated beacon sampling (+10.3%, about 109 ns) exceed the
  10% review threshold. Necessary numerical/coherence checks are retained;
  noisy enforce-partial/report timings do not establish a confident change.
  Slice-3 targeted checks passed 252 cases without skips. Instrumented bounded
  fuzzing passed 112,902 executions with 74 new coverage inputs. Final matched
  benchmark increases are +19.85%/+21.28% for partial shadow/enforce admission
  and +11.13% for elapsed-second aging; correctness checks are retained above
  the review threshold. CPU/mutex/live-heap/allocation profiles from controlled
  serial and four-worker processes map to exact executables. Resource guards
  passed with 16,384 buckets / 8,192 populated coarse sidecars, and measured
  20,000/40,000-bucket live heap stayed comparable to baseline (about 575 B per
  bucket including overhead). Purge, compact, disjoint refill and refreshed
  zero ownership passed. Both complete root lanes passed all 70 packages:
  9,268 passing cases and the same 77 existing skips per lane. Required cases
  did not skip. Vet, Staticcheck and lint passed; the private test-helper
  cleanup received 30 ordinary and 30 race case reruns. No production code
  changed after the complete lanes or profiling.
- Signals to monitor: admission counters, timestamp-drop T, sampling/report
  availability and as-of metadata, allocations, CPU, and retained bucket size.
- Rollback triggers: violated golden vectors, nonfinite evidence, receiver
  displacement without usable admission, races, or failed resource/performance
  gates. No deployment or restart is authorized here.

## References
- Commit inspected: a4cbe65acc8ccf57ef44c6ec35aa99a28ae6f735.
- Related ADRs: ADR-0065, ADR-0130, ADR-0134, ADR-0139, ADR-0170, ADR-0300.
- Related docs: [path README](../../pathreliability/README.md),
  [path operator guide](../../data/config/PATH_PREDICTIONS.md).
