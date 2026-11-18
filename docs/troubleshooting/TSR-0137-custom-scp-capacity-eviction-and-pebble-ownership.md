# TSR-0137 - Custom SCP Capacity Eviction and Pebble Ownership

Status: Monitoring
Date Opened: 2026-11-18
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: Custom SCP storage, delivery and retained state
Trigger Source: Operator report
Led To ADR(s): ADR-0358
Tags: custom_scp, pebble, eviction, retained state, delivery latency

## RCA Summary
- What happened: a new observation key at capacity performed a full in-memory
  scan before selecting a victim and deleting its persisted observations.
- Root cause: the expiry heap tracks the oldest receiver, while the capacity
  policy compares the newest retained receiver per key.
- What fixed it: ADR-0358 records a separate bounded eviction heap. The same
  change balances the store opener's Pebble cache reference and corrects the
  inactive max_db_bytes description.
- How we know: the baseline was reproduced on disposable checkpoint copies.
- Operator/support answer: check observation keys, eviction index items and
  slots, and the live eviction counter together. The legacy max_db_bytes
  setting does not limit disk usage.

## Triggering Request
- Request date: 2026-11-18
- Request summary: verify and repair the highest-priority findings from a
  review of the Pebble stores.
- Request reference: operator chat, Approved v1.

## Symptoms and Impact
- Live accepted observations perform the selection under the evidence lock
  before final delivery. Reduced-cap startup also uses this selection.
- On the local system, checkpoint inventories rose from 237,334 to 246,984 to
  250,000 recent keys. The newest snapshot had 2,722,600 observation rows and
  62,925 static membership rows.
- These observations establish capacity and local cost. Live event frequency,
  delivery p99, and memory attributable to the cache leak were unmeasured.

## Timeline
1. 2026-11-18 08:00 Eastern - The newest inspected local checkpoint held the
   full 250,000-key population.
2. 2026-11-18 - Baseline timings and profiling on disposable copies isolated
   the cost of selecting and deleting a victim.
3. 2026-11-18 - The cache invariant probe reproduced one outstanding reference
   after open and close. The balanced control passed.

## Hypotheses and Tests
1. Pebble writes or victim selection could explain the insertion slowdown.
   - Evidence/commands: paired runtime insertion on the checkpoint copies,
     30 new keys per trial, and go tool pprof on a separate 100-insertion run.
   - Outcome: 21–22 ms per insertion at capacity versus 5–17 µs with 30 spare
     slots. Eviction accounted for 98% of sampled CPU including callees, with
     roughly 90% in the function and map iteration.
2. The 4 GiB setting could trigger storage-pressure cleanup.
   - Evidence/commands: trace the loader field through construction and
     cleanup using git grep and current runtime source.
   - Outcome: the key is accepted and defaulted, with no runtime consumer.

## Findings
- Eviction scans the primary map under the evidence lock. The 601-second
  disk cleanup scan is a different workload, with a distinct lock boundary.
- The first-party Custom SCP constructor released its reference on open error
  and omitted it on success before the patch.
- A bounded capacity index needs an explicit owner and backing-size contract.

## Decision Linkage
- ADR created/updated: ADR-0358.
- Decision delta summary: maintain a bounded in-memory ranking for capacity.
- Contract/behavior changes: selection policy and stored evidence semantics
  are preserved. The diagnostic snapshot adds capacity index and live eviction
  fields.

## Verification and Monitoring
- On five paired checkpoint-copy trials, the candidate reduced median
  full-cap insertion from 21.18 ms to 3.28 ms (6.5x). The owner accepted
  this measured improvement for the patch after reviewing the original 10x
  target. A one-receiver synthetic fixture at 250,000 keys measured 19.97 ms
  versus 0.305 ms.
- Paired checkpoint medians opened in 3.85 s (baseline) and 3.91 s
  (candidate). The candidate retained about 15.5 MiB more Go heap after GC.
  Paired below-cap write and support benchmarks stayed within their 10%
  guard, with zero reported allocations per operation on unchanged writes
  and support reads.
- The candidate CPU profile showed Pebble prefix lookup as the main remaining
  cost. The patch preserves that deletion path and its persistence behavior.
- The candidate passed the same cache invariant probe that detected one
  outstanding caller reference on the baseline.
- After deployment, observe the live eviction counter with CPU and mutex
  profiles over periods that include capacity churn. Compare index items with
  recent observation keys and check backing slots against the effective cap.
- Investigate ownership divergence, unexpected memory growth, or a repeatable
  regression in ordinary writes, support lookups, or reopen.

## References
- Baseline: edeb4cfb.
- Related ADR: [ADR-0358](../decisions/ADR-0358-custom-scp-capacity-index.md).
- Related docs: [Custom SCP regression](../runbooks/custom-scp-regression.md).
