# ADR-0267: Compatible Stored YAML Decoding

- Status: Accepted
- Date: 2026-10-09
- Decision Origin: Design

## Context

Stored records and presets accept legacy YAML aliases and merges. Their typed
decoding also owns exact versioned values, migration and applied baselines.
The pinned yaml.v3 decoder checks duplicate mapping keys quadratically and has
an alias guard whose acceptance depends on original traversal order and nested
decoder boundaries. New storage work admission needs a document-wide count
without changing those accepted values or weakening the existing guard.

## Decision

Use a storage-specific, operation-local traversal of the original AST. Preserve
typed interpretation, ignored-value behavior, raw duplicate identity, interpreted
key identity, merge precedence and stock alias-guard prefixes. Use linear raw
duplicate checks. Keep stock Node.Decode scalar conversions on alias-free leaves;
do not fork or replace the dependency or turn stored data into a generic runtime
configuration loader.

Account physical Content nodes and scalar bytes once. Account logical original
typed-decoding visits and scalar bytes once, including merge bookkeeping and
custom dispatch. Nested records, references and baselines share that operation
count, while their old plain Node.Decode guard contexts reset separately.
Compatibility accounting precedes the main bulk materialization. The single
applied reference is temporarily materialized after its own complete accounted
walk so its existing name/snapshot failure remains before later parent fields.
This exception is not an allocation-free claim, and its final snapshot cap does
not bound intermediate legacy allocations. Preserve existing
owner prechecks, migrations and postvalidation at their authoritative boundaries.
Do not retain ASTs, budgets or type inventories beyond the decoding operation.

Compatibility means agreement on accepted complete values and refusal classes.
It does not require emitting every pairwise duplicate diagnostic, which could
itself require quadratic output. Keep the characterized excessive-alias refusal
exact. Malformed composite generic merge keys are refused rather than reproducing
the dependency's unhashable-key panic. A null-tagged root with an inline field is
also refused instead of reproducing the dependency's invalid-reflection panic;
other null-tagged mappings and sequences retain their typed stock behavior.

Checkpoint 04a introduces accounting with new thresholds inactive. Checkpoint
04d activates the approved raw, canonical and work admission together, after
04b exact preparation/commit and 04c detached human publication. Existing preset
limits, schemas 1-4, full-SSID record ownership and baseline station preset
ownership are unchanged by this decoder checkpoint.

## Alternatives considered

1. Keep stock typed bulk decoding and count afterward. Rejected because expensive
   expansion and quadratic duplicate work would occur before work admission.
2. Filter unknown values, expand aliases into a projected AST and run stock
   decoding. Viable only with the same original traversal checks; reconstruction
   adds obligations and leftover aliases can introduce a second, stricter guard.
3. Replace/fork yaml.v3. Outside the authorized scope and unnecessarily expands
   compatibility ownership beyond stored record and preset types.

## Consequences

### Benefits

Existing accepted storage keeps its exact typed values, while later admission can
bound decoding work before expansion. Operation-owned bookkeeping introduces no
server-lifetime cache or new synchronization.

### Risks

The narrow decoder must track the pinned dependency's relevant semantics.
Dependency upgrades or changes to shared prevalidation/migration helpers require
review of the frozen oracle. Differential fuzzing, hand-counted work fixtures and
scale/allocation evidence qualify this boundary; source shape alone is insufficient.

The measured two-pass decoder allocates more than the frozen decoder, and small
loads can be slower. Increasing-map measurements remove the observed quadratic
duplicate-check behavior but do not establish a universal performance win. Stock
scalar conversion is the largest allocation contributor in the sampled profile.
These are storage-load measurements, not production latency or capacity evidence.

### Operational impact

This checkpoint adds no new quota refusal. Existing malformed/future/alias-refused
records retain their protected temporary-settings behavior. No production-data
migration or automatic record rewrite is introduced.

## Links

- Related scope: [Scope Ledger v3](../command-fix-scope-ledger-v3.md)
- Related contracts: [ADR-0238](ADR-0238-named-presets.md), [ADR-0244](ADR-0244-exact-configuration-persistence.md)
- Related tests: `filter/storage_decode_compat_test.go`, `filter/storage_decode_accounting_test.go`, `filter/storage_decode_fuzz_test.go`, `filter/storage_decode_benchmark_test.go`
- Related evidence: [execution record](../command-fix-execution.md#checkpoint-04a-compatible-storage-decoding-and-work-accounting)
- Related TSR: [TSR-0051](../troubleshooting/TSR-0051-stored-yaml-alias-compatibility.md)
- Supersedes / superseded by: none; retains the existing persistence contracts.
