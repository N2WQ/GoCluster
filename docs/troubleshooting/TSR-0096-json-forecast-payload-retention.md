# TSR-0096 - JSON Forecast Payload Retention

Status: Resolved
Date Opened: 2026-10-14
Date Resolved: 2026-10-14
Owner: GoCluster maintainers
Technical Area: pathreliability persisted VOACAP hydration
Trigger Source: Other
Led To ADR(s): ADR-0304
Tags: retained-heap, json, slice-capacity, profiling

## RCA Summary

- What happened: the first S2 candidate eliminated completion scans but retained
  excess capacity in hydrated 24-hour forecast payloads. A one-row index-only
  heap fixture passed while a realistic worker heap exposed the regression.
- Why: decoder extraction replaced the prior compact copy with direct ownership
  of the JSON decoder's growing slice. The observed 24-row slice had capacity 39.
- What fixed it: copy accepted decoded rows into an owned length-sized slice
  before returning a forecast that can live in the memory cache.
- How we know: a matched 50,000-forecast, 24-row retained-cache oracle measured
  80,292,328 extra bytes for a deliberate alias overlay, exceeding 20 MiB; the
  corrected candidate added 9,919,888 bytes and passed. The alias also failed
  the ordinary compactness/value regression case. Corrected worker capture
  completed 256 jobs and verified raw persisted output after Wait.
- Operator/support answer: this was a local candidate regression caught before
  acceptance or deployment. No cache schema migration or operator change is
  needed. Low allocation churn does not by itself establish low retained heap.

## Triggering Request

- Request date: 2026-10-14
- Request summary: implement the approved first-wave path-reliability patches
  serially with testing between slices and approved Windows executable paths.
- Request reference: owner `Approved v2`; first-wave execution record.

## Symptoms and Impact

The controlled initial S2 worker capture was much faster, but its sampled heap
was 226.73 MiB versus 155.31 MiB for S1. Most additional payload retention appeared
under `reflect.growslice`. Index-only testing discarded decoded payloads before
GC and therefore could not expose the long-lived memory-cache ownership issue.
Only local candidate builds were affected; no production impact was measured.

## Timeline

Captured times are UTC on 2026-10-15 (owner's local date 2026-10-14).

1. 01:02:05-01:15:34 - S1 warm worker workload: 256 completions, 808.303s.
2. 01:15:51 - initial S2 workload: 256 completions, 0.239s; heap review identifies
   excess retained decoder capacity despite passing index-only measurements.
3. 01:20:59 - corrected S2 workload: 256 completions, 0.248s, with compact copies.
4. Follow-up matched live-cache retention measurement proves the alias variant
   exceeds the limit and the corrected variant stays within it.

## Hypotheses and Tests

1. New index metadata explains the entire increase.
   - Evidence: exact one-row index delta 10,403,952 bytes; initial whole-worker
     sampled heap increase was much larger and concentrated in decoded arrays.
   - Outcome: Rejected as the complete explanation.
2. Retained JSON growth capacity explains the payload increase.
   - Evidence: source comparison shows prior copy versus new alias. Deliberate
     alias overlay reports len 24/cap 39 and adds 80,292,328 bytes over S1 with
     the same 50,000 full-day forecasts. Length-sized copies add 9,919,888 bytes.
   - Outcome: Supported.
3. A small logical capacity alone proves physical compaction.
   - Evidence: a full slice expression can narrow visible capacity while keeping
     its backing allocation alive. The paired retained-cache oracle measures
     actual HeapAlloc after GC, with the fallback kept alive.
   - Outcome: Rejected as a sufficient oracle.

## Findings

- Root cause: decoded arrays became server-lifetime payloads without the previous
  compact copy. The cache had the right number of forecasts and index nodes,
  but its memory shape was wrong.
- Contributing factors: metadata-only bounds and allocation-rate wins did not
  cover the payload ownership transition. Whole-day rows and actual live-cache
  retention must accompany the index oracle.
- Durable decision: preserve compact payload copies in ADR-0304.

## Decision Linkage

- ADR created/updated: ADR-0304.
- Decision delta summary: make compact hydrated-payload ownership explicit in
  the bounded incremental-persistence design.
- Contract/behavior changes: no forecast validity, key, schema or glyph change.

## Verification and Monitoring

- Validation steps run: matched native baseline/alias/candidate daily retained
  heap tests; ordinary compactness/value checks; actual worker CPU/allocation/
  heap/mutex/block captures and raw disk verification. Both complete native S2
  ordinary/race lanes passed 9,691 cases across 70 packages, with all 89 required
  identities verified; analyzers and code-map checks passed.
- Commands/evidence: `.tmp/path-reliability-first-wave/s2/verify-daily-retention.ps1`,
  `daily-retention-summary.json`, `alias-daily-unit.log`, and `profiles/`.
- Signals to monitor: retained heap with realistic row counts, map/heap
  cardinalities, and payload allocation stacks. Sampled whole-process heap
  totals have sampling noise and unrelated allocations; use the exact paired
  retained-cache oracle for its numerical budget.
- Rollback triggers: retained-cache delta above 20 MiB at 50,000 normal forecasts,
  unbounded cardinality or any failed required correctness/performance gate.

## References

- Issue(s): none
- PR(s): none
- Commit(s): S2 local checkpoint; see first-wave execution record
- Related ADR(s): ADR-0304
- Related docs: `docs/path-reliability-first-wave-execution.md`
- Related tests: `pathreliability/voacap_daily_retention_heap_test.go`,
  `internal/cluster/voacap_persistence_profile_test.go`
