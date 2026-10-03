# PC18/PC92 approved v16 performance amendment

The user explicitly selected **Approved v16** on 2026-10-02. This is a focused
addition to [approved v15](pc18-pc92-scope-ledger-v15.md), not a replacement.
All inherited protocol, ownership, resource, deadline and qualification
requirements remain binding. No acceptance result is implied by approval.

## Objective and evidence

Remove two measured spot-history maintenance costs while preserving existing
behavior. The completed v15 warm baseline's 660-780 second profile attributes
15.92 of 125.47 sampled CPU seconds to WHOSPOTSME bucket scrub and 9.55 to
harmonic-history cleanup. This establishes work to investigate, not causality
for all tail latency. [V15 evidence](pc92-v15-validation.md) retains exact
source/run identity and failed delivery/latency results.

## Agreed scope

| Item | Approved change | Required evidence |
| --- | --- | --- |
| V16-01 | Preserve current spot acceptance, harmonic decisions, query results, normalization, thresholds, expiry boundaries and cleanup-worker lifecycle. | Frozen pre-change behavior plus explicit boundary cases. |
| V16-02 | Keep WHOSPOTSME victim selection. Remove that victim's bucket records using its existing country totals; for sparse buckets choose the traversal examining fewer entries. No added retained index. | Warm full-capacity eviction and many-country benchmarks; multi-second victim, eviction/reinsertion/old expiry, backward time and churn. |
| V16-03 | Replace harmonic cleanup's repeated full-map scan with a typed indexed minimum heap, exactly one item per live callsign. Update every existing recency assignment in either time direction; couple removal to every corresponding deletion. | Strict global versus entry expiry, refresh on suppressed harmonics, backward time, stable per-call entry order, and all four returned values. |
| V16-04 | Bound heap cardinality by its live owner population, clear removed references, compact excess backing and expose cardinality/capacity. | Account for map-value growth, heap capacity and overlapping allocations; no stale historical heap entries under repeated refresh/expiry. Ordinary ingestion storage remains separately reported under the existing ownership boundary. |
| V16-05 | Capture comparable warm baselines before the changes and measure each change. Target substantially less maintenance CPU without added hot-path allocations or regressions in supported sparse/many-country cases. | CPU/allocation/lock benchmarks and matched 15-minute load plus 11-minute drain profiles. |
| V16-06 | Retain every original completion gate and update documentation/traceability. | Broken controls fail; affected normal/race checks and complete final lane; complete delivery, each-client/each-minute 5 ms enqueue and 25 ms first-byte targets, allocation proof and final-source qualification. |

## Boundaries and risks

Touched owners are the two spot-history components, their tests, bounded
diagnostic observations and affected documentation. No new settings, workers,
traffic-drop policies or PC18/PC92 recovery changes are authorized. Existing
WHOSPOTSME equal-age victim behavior stays unchanged; no new deterministic
tie-order claim is made. The harmonic model, thresholds and input policy are
preserved, not scientifically recalibrated or certified.

Material risks are stale expiry indexes, incorrect recency updates, a global
versus per-entry boundary mismatch, and old bucket records subtracting from a
newly admitted entry. The old detector has no hard callsign count cap; this
amendment does not invent one or claim an absolute ordinary-ingestion byte
ceiling. New index ownership must converge with the existing live owner.

Pre-approval hot-path, retained-state, design and falsifiability reviews compared
the existing scans with victim-local deletion and indexed expiry. Lazy global
cleanup can change behavior after a forward then backward time jump; a cached
earliest time alone does not eliminate repeated scans under steady expiration.
Both are excluded. Reviews were design-aware; no independent scientific
certification is claimed. The worker completes the detailed contract-to-test
matrix before its first production edit.

The lead retains authority, integration, final review and acceptance. Disjoint
worker ownership covers the two spot implementations/tests and their slice
validation record. Actual Linux companion/SQLite validation continues as part
of v15; it is not replaced by this amendment.

## Execution and closeout

Retain matched source snapshots and benchmark/profile evidence for each change.
Keep microbenchmark results, diagnostic runtime results, audit-correction
completion and overall acceptance separate. No shortened, failed, missing or
diagnostic evidence can qualify the original workload. Record actual results
in `pc92-v16-performance-validation.md` and the consolidated v15 record.
