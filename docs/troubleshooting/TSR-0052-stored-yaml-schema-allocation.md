# TSR-0052 - Stored YAML Schema Allocation

Status: Monitoring
Date Opened: 2026-10-09
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: filter stored YAML preflight and materialization
Trigger Source: Chat request
Led To ADR(s): ADR-0270

## RCA Summary

A synthetic 100,000-entry preset collection exposed high allocation churn
despite passing the selected byte/work guards. Distinct-key refusal allocated
about 1.22 GB cumulatively per timed decode; repeated-key admission allocated
about 2.79 GB. These are allocated-byte totals, not simultaneously retained heap.
The guarded benchmark process peaked at about 228 MB.

An allocation profile of the 1,000 repeated-key case attributed 82.32% of
cumulative allocation space to `addStoredFields`. Owner validation repeatedly
reflected and rebuilt the same fixed YAML schema table. The selected correction
reuses the existing operation-owned immutable field metadata, while preserving
every validation visit and error boundary. A benchmark PASS alone does not
resolve the operational concern. Revised
controls retained their expected outcomes and reduced the 100,000-entry
refused case to 42.7 MB cumulative allocation / 81.4 ms and the admitted
case to 430.7 MB / 458.4 ms. Peak guarded memory stayed about 228 MB; parsing
and per-value normalization remain.

Operator/support answer: file size, decoding-work units, cumulative allocations
and retained memory are different measurements. Preserve refused files; do not
increase limits or flatten YAML automatically in response to a size/work error.

## Root cause or best current explanation

Each SavedPreset preflight calls schema validation, including empty legacy
mapping entries. Marked owners also validate current values against a newly
constructed schema; materialization repeats these checks. The decoding state
already holds field metadata for the same exact Go types, but these validators
rebuild it independently. Reflection/tag splitting therefore scales with owner
occurrences rather than the small fixed set of owner types.

The old assumption that scalar decoder construction was the dominant cost in
this shape was rejected: the profile attributed 3.44% flat allocation space to
stock decoder construction. That does not contradict an earlier profile of a
different direct-map shape. Normalizing every repeated selected value remains
required; this correction cannot skip losing values or cache their acceptance.

## Fix or mitigation

Use operation-owned schema metadata only, keyed by exact reflect.Type. Route
production owner prechecks and recursive struct-value validation through it.
Keep fresh free-validator wrappers and the legacy table builder for the frozen
compatibility oracle. No global cache, new scalar conversion, changed work
charge, changed limit or winner-only materialization is selected.

Compare cached field name/type tables against the legacy builder; exercise
valid-invalid-valid owners and nested references. Retain literal multifault,
null, merge/shadow, duplicate, version and hand-counted work controls. Repeat
both refused and admitted 21/1,000/100,000-entry scale cases, not only the
over-count case. Unacceptable residual cost blocks closeout.

## Verification and Monitoring

Initial qualification artifacts are under `.tmp/command-fixes`:
`checkpoint-04d-scale.log`, `checkpoint-04d-profile.log`, the corresponding
`-final.log` files and `04d-collection-final-alloc.pprof`. The full ordinary suite and initial semantic
checkout cases passed before the cost correction. These results do not certify
the corrected implementation. Revised scale/profile evidence resolves the
repeated schema-construction blocker; final regression evidence belongs in the
execution record.

The benchmark's 1 GiB tool-job guard is local test containment, not a production
concurrency or heap ceiling. Local synthetic timings do not establish node
capacity. Monitor allocation/GC cost and preset-command latency if deploying
this change; unexpected decoder refusals or costly stored collections warrant
preserving the file and inspecting the exact admitted representation.

## References

- [ADR-0270](../decisions/ADR-0270-complete-storage-admission.md)
- [Scope Ledger v3](../command-fix-scope-ledger-v3.md), checkpoint 04d
- [Execution record](../command-fix-execution.md#checkpoint-04d-complete-storage-admission-and-save-ordering)
- `filter/configuration_storage.go`, `filter/storage_decode.go`
- `filter/storage_collection_admission_test.go`
