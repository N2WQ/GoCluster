# TSR-0138 - Neighborhood Read Amplification and Recovery Coverage

Status: Monitoring
Date Opened: 2026-11-17
Date Resolved: n/a
Owner: Repository maintainer / Codex
Technical Area: canonical neighborhood derivation, paced resource qualification
Trigger Source: Chat request
Led To ADR(s): ADR-0358
Tags: canonical evidence, allocation churn, sorted merge, recovery oracle

## RCA Summary

- What happened: the first call-correction-v2 matched capture refused 13,214
  dense inputs and failed natural recovery. A later faster capture failed only
  because its named recovery case was already empty at input stop.
- Why: scan-heavy reads rebuilt receiver maps and canonical unions from raw
  rows despite having immutable sorted publications. The checker separately
  required backlog in a particular case instead of actual recovery coverage
  across the unchanged invocation.
- What fixed it: reuse canonical source streams, capture nomination headers
  separately, derive totals/overlap without receiver maps, and require actual
  input-stop backlog plus natural completed recovery in at least one fixed case.
- How we know: independent map/set oracles, bounded source reviews, exact four
  tuning-stream parity checks, consumer race checks and a new identical-fixture
  original/candidate paced pair. All inputs and downstream counts reconcile.
- Operator/support answer: a resource failure cannot be waived by serial replay
  success or microbenchmarks. Distinguish allocation churn from retained heap,
  and distinguish an already empty case from recovery of observed backlog.

## Triggering Request

The owner requested a qualified Thursday call-correction release under
[approved v3](../call-correction-v2-scope-ledger-v3.md), including fixed paced
asynchronous capacity, profiles and no added loss or unresolved backlog.

## Symptoms and Impact

The first matched run at `669dccfd` filled its 20,000-entry output queue during
the 800-input/s dense case. Typical CPU/input also exceeded the allowed ratio
against the fresh original control. All evidence was local; no production
installation, restart or deployment occurred.

Whole-invocation pprof attributed 57.83% of allocation bytes to canonical
construction and 36.74% to preliminary neighborhood receiver maps. The capture
sampled 74,858 MB of cumulative allocation churn. Inclusive CPU totals overlap:
the merge included 89.71 seconds, including canonical construction work.

## Hypotheses and Tests

1. A larger queue or a lower input rate fixes the implementation. Rejected:
   those changes would alter the fixed qualification. The bottleneck was repeated
   derivation, not a demonstrated need for different admission policy.
2. Reusing sorted source rows can preserve semantics with less temporary work.
   Supported: a separate source reviewer identified discarded preliminary
   tallies, and map-derived complete rows/headers/witnesses match the new merge.
   Raw provenance, global-newest display support, equal-time SNR joins, maximum
   weights, nomination and source-local frequency witnesses remain.
3. One head scan is efficient for every capped shape. Rejected: 80 disjoint
   identities measured about 4 ms. A simple heap improved disjoint input but
   penalized complete overlap. The final local cursor heap batches equal keys;
   small cohorts retain the cheap scan. Ragged partial-overlap/conflict vectors
   and both cardinality shapes are covered.
4. No backlog in the named recovery case proves a runtime defect. Rejected by
   separate non-steered oracle review: the dense case had 5,976 actual pending
   items and naturally drained. The approved requirement is actual recovery
   coverage, not a case name. Sampled high water alone still supplies no coverage.

## Root cause or best current explanation

Neighborhood selection built receiver sets before an exact-only return and
discarded preliminary tallies after constructing the union. The union builder
then rebuilt canonical receiver/variant maps from raw entries on each read.
Repeated fresh selection, delayed release and family arbitration amplified that
work. The profiles identify this repeated allocation, rather than establishing
a retained-state leak or a need to reduce the configured input rate.

The local repair adds no retained cache, receiver registry, mutation owner or
lock. Temporary rows/headers follow the admitted source bounds; compact temporal
state retains no receiver generations. Source checks are not a process-memory
ceiling or production p99 measurement.

The coverage refinement preserves all three workloads, rates, assets, caps,
warm/steady windows and every-case completion/loss check. Positive and negative
coverage vectors passed 13 ordinary and 13 race cases. The old failed captures
remain failed; the replacement fixture required a fresh matching control and
candidate, with every old CPU denominator retained.

## Decision Linkage

[ADR-0358](../decisions/ADR-0358-call-correction-v2-evidence-and-bounded-bonuses.md)
owns evidence identity, lifetime and bounded derivation. No production semantic
contract changed during this performance repair. The checker-only refinement
implements the approved observed-backlog recovery obligation.

## Verification and Monitoring

`matched-coverage-v2` accepts/processes 90,000 inputs, delivers 90,000 to each
TCP client and reopens 90,003 archive rows including barriers. Typical CPU is
203,125 ns/input and passes <=1.10 times every retained control. Dense CPU is
990,234.375 versus the newest original's 578,776.042 ns/input: the 71.1% cost
increase is disclosed, with zero refusal/loss and natural drain in 15.37 seconds.
Allocation churn also exceeds the original, despite being much smaller than the
failed draft. These facts must not be summarized as improvement in every metric.

Use the [qualification record](../call-correction-v2-qualification.md) for final
policy capture and complete lanes. Preserve comparable executable/config/asset/
fixture fingerprints and inspect actual offered/processed/downstream/drain
counts before interpreting a profile. Reassess any added loss, unresolved
backlog or failed typical CPU gate; do not select a slower control afterward.

## References

- Commits: `cf217f2f` sorted derivation; `4a1a81cd` recovery coverage checker.
- [Execution evidence](../call-correction-v2-execution.md).
- [Native validation](../dev-runbook.md#native-windows-test-execution).
- [Model/input fixture learning](TSR-0137-study-geography-and-replay-clock-fixtures.md).
