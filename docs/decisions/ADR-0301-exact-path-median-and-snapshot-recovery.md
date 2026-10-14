# ADR-0301: Exact Path Median and Snapshot Recovery

- Status: Accepted
- Date: 2026-10-13
- Decision Origin: Troubleshooting chat

## Context
Follow-up review at 684d145 found that float64 histogram accumulation can still
manufacture or lose a half-mass tie. Repeated modern report snapshots also
replace whole samples when only some bands are complete, and accept contradictory
degeneracy flags. The owner selected exact median behavior and authorized v3.
Separate non-steered mathematical review and a neutral design comparison
established the normative contract and bounded implementation choice.

## Decision
Compute p50 exactly for the supplied fixed 50-bin finite nonnegative float32
histogram. Every mass is an integer multiple of 2^-149. Decode and accumulate
in five private stack-local 64-bit words: one bin needs at most 277 bits, the
total 283, and twice a cumulative prefix 284. Compare twice the cumulative mass
with the total, without integer division or floating reconversion. Validate the
entire histogram before selection. Only true equality uses the next occupied
representative midpoint; invalid or all-zero histograms remain unavailable.

For modern logs, retain the last usable complete count/weight pair per band and
AsOf after the existing captured logger-prefix pairing. Replacement follows
existing input encounter order, not emission timestamp sorting. Missing,
malformed or unavailable later bands preserve earlier valid bands; a complete
known zero replaces them. Retain variance similarly per band and AsOf and require
ordered nonnegative bounds with deg=1 if and only if max=0. Without valid variance,
availability remains false. Preserve modern metadata/UTC and legacy behavior.

Implement sequentially: exact median and complete gate, then reader recovery
and complete gate. Preserve the 448-byte bucket and zero steady allocations.
Compare benchmarks and controlled profiles against 684d145; a warm slowdown
over 10% requires review before progression. No intermediate deployment.

## Alternatives considered
1. Abstain on rounded ambiguity: mathematically coherent product policy, but
   owner selected exact medians for every valid positive stored histogram.
2. Certified float64 filter with exact fallback or floating expansions: viable,
   but add proof and checker obligations; direct bounded integers fit this
   fixed array without duplicate arithmetic paths.
3. Whole-sample replacement: loses unrelated valid bands on partial corruption.
4. Independently merge counts and weights: can fabricate a pair from separate
   captured prefixes; retain complete pairs instead.

## Consequences
### Benefits
- Retained subnormal or very small bins can correctly determine the median.
- True ties retain ADR-0131's occupied-midpoint convention.
- Partial malformed repeats preserve unrelated quantitative band evidence.
- Contradictory flags cannot manufacture a degeneracy conclusion.

### Risks
- Exactness stops at the supplied float32 histogram. Earlier decay, shift and
  merge materialization remain approximate.
- Glyphs may change where previous floating accumulation selected the wrong bin.
- CPU cost and escape behavior require measurements; prevalence, QSO accuracy
  and production p99 are not established by local arithmetic checks.

### Operational impact
- No YAML, source-parser, threshold, cap, timestamp, fallback or schema changes.
- Support-agent docs impact: Required; path support guidance describes exact
  stored-mass ties and malformed modern snapshot recovery.
- Slice 1 passes 38 native targeted cases and both complete 70-package
  Windows ordinary/race lanes: 9,306 passing cases and 77 existing skips each.
  Vet, Staticcheck and lint pass; guided fuzzing passes 225,007 executions with
  22 new coverage inputs. Required cases do not skip. Direct median, update and
  prediction benchmarks retain zero allocations and buckets remain 448 bytes.
- Six matched benchmark pairs show populated normal/beacon medians about 2-3%
  higher without a confident difference, and normal shadow/enforce about 3-5%
  higher. Direct dense median rises from 55.74 to 410.75 ns; a single controlled
  serial workload capture rises 11.59%. These costs exceed the review threshold
  and are retained for bounded exact arithmetic. Controlled serial/parallel
  profiles preserve owner heap and show no new allocator or lock owner. They
  do not establish production p99. Slice 2 is not yet implemented.

## Links
- Related tests: snr_histogram_exact_test.go, snr_histogram_mass_test.go,
  snr_histogram_exact_bench_test.go, internal/propreport/snapshot_logs_test.go.
- Related docs: [path README](../../pathreliability/README.md),
  [operator guide](../../data/config/PATH_PREDICTIONS.md),
  [support card](../../customgpt/support-cards/path-reliability.md).
- Related TSRs: [TSR-0093](../troubleshooting/TSR-0093-path-median-and-snapshot-recovery.md).
- Supersedes / superseded by: Refines numeric conformance to ADR-0131 and
  modern reader recovery in ADR-0300; retains their other accepted contracts.
