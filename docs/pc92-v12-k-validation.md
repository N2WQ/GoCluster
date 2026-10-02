# V12 K subject metadata correction and evidence

This slice implements V12-02 and its graph/projection accounting checks from
V12-05 under [approved v12](pc18-pc92-scope-ledger-v12.md). The baseline is
`2c0607986f3d3d9dc1921eb5b7c5ae00595143d2`. These are focused working-source
results; the lead owns final-source validation and overall acceptance.

## Behavior and implementation boundary

The pinned DXSpider receiver is
`3e9b3621d94dd45c68702e4a0f896aac33f2a91d`. Its
`DXProtHandle.pm:2296-2312` writes zero for omitted or zero K numeric values,
whereas the shared first-slot handler at lines 2173-2174 preserves numeric
values omitted or supplied as zero in A/C/D.

Each reference and Go case independently seeds version/build `5457/633`:

| Subsequent K subject suffix | Expected version/build |
| --- | --- |
| `:5457` | `5457/0` |
| omitted | `0/0` |
| `:0:0` | `0/0` |
| `::634` | `0/634` |

A later explicit K restores nonzero values. K counts of zero do not remove
existing members, change completeness, or prevent ordinary liveness renewal.
Absent IP continues to preserve the known address.

`effectiveNodeSubject` in [pc92_graph_plan.go](../peer/pc92_graph_plan.go)
returns a value copy. Only K maps the decoder's empty numeric strings to
literal `"0"`. New subject preparation, subject commitment, projected charge,
and metadata-overlap accounting use that view. The other implementation files
are [pc92_graph.go](../peer/pc92_graph.go) and
[pc92_graph_allocation.go](../peer/pc92_graph_allocation.go).

The decoded record and transit payload remain unchanged. Distinct-origin and
external relationship-edge metadata retain their existing semantics. A/C/D
omissions, implicit subjects, member defaults and IP handling are unchanged.
There is no schema or driver change. Production projection writes the corrected
node strings into the existing SQLite TEXT columns.

## Contract-to-check mapping

All checker names below are in `peer`; the post-approval design-aware review
was dispositioned before implementation. This was not independent normative
certification.

| Contract | Checkers |
| --- | --- |
| Independently seeded K replacement, populated membership, liveness and nonzero restoration | `TestPC92GraphKSubjectNumericReplacement`, `TestDXSpiderReferenceKSubjectNumericReplacement` |
| A/C/D explicit omission, explicit zero and implicit subject preservation | `TestPC92GraphSubjectNumericActionIsolation`, `TestDXSpiderReferenceSubjectNumericActionIsolation` |
| Decoded record, external edge, distinct origin and relayed payload stay unchanged | `TestPC92GraphKEffectiveSubjectIsolation` |
| New origin/external subject charge and exact/one-byte-over admission | `TestPC92GraphKSubjectNewNodeCharge` |
| Replacement peak, final reduction, atomic refusal and repeated-zero reuse | `TestPC92GraphKSubjectReplacementCharge` |
| Real receive, production project/write and exact SQLite TEXT values | `TestTopologyKSubjectNumericProjection` |
| Old active snapshot survives live clearing; combined generation charges retire independently | `TestPC92ProjectionRetainsNumericGenerationThroughKClear` |

The new files are `peer/pc92_k_metadata_test.go`,
`peer/pc92_k_metadata_interop_test.go`,
`peer/pc92_k_metadata_allocation_test.go`, and
`peer/pc92_k_metadata_projection_test.go`; the helper allocation benchmark is
`peer/pc92_k_metadata_benchmark_test.go`. Existing graph/member/IP and
projection regressions remain part of the focused regression command below.

## Accounting claims and limits

No new retained collection, owner, scratch generation or resource partition is
introduced. The helper changes numeric values in an existing entry view.
The graph retains its 96 MiB partition, including the existing 5 MiB mutation
scratch allowance; diagnostic generations retain their combined 36 MiB limit.

Allocation expectations are derived independently from the qualified size
classes: 8,193-byte numeric backing occupies 9,472 bytes; 4,097 bytes occupies
4,864; each cloned one-byte zero string is charged 8 bytes. Clearing both
numeric fields therefore requires 16 bytes of replacement overlap before old
node backing is released, even though the final graph charge decreases.
Already-owned identical zero strings do not require another replacement charge.

The accounting boundary fixtures explicitly use synthetic remaining headroom;
they are not measurements of aggregate process memory. The retained-generation
test separately verifies that an old projection still owns its original
numeric strings and charge after the live graph replaces them. Database checks
compare exact strings and SQLite `typeof` results, without numeric casts that
would hide an incorrect empty string.

These checks do not close the inherited enabled-SQLite, context-backing or
outer-retirement allocation-proof gaps, or establish overall 480 MiB acceptance.

## Executed focused checks

Every command used the process-local environment from
`D:\codex-gocluster-v12-20261001\env.ps1`. It sets the D: build/tmp locations and
the configured pinned receiver/dependency paths. Logs are in
`D:\codex-gocluster-v12-20261001`.

```powershell
go test ./peer -run '^Test(PC92Graph(KSubject|SubjectNumericAction|KEffectiveSubject)|TopologyKSubjectNumericProjection|PC92ProjectionRetainsNumericGenerationThroughKClear)' -count=1 -v -timeout=60s
```

PASS, 0.228 seconds; `k-focused-normal.log`.

```powershell
go test ./peer -run '^TestDXSpiderReference(KSubjectNumericReplacement|SubjectNumericActionIsolation)$' -count=1 -v -timeout=90s
```

PASS, 15.036 seconds; `k-receiver.log`. Both receiver tests ran, with fourteen
independently initialized cases and no skips. This executes the actual pinned
receiver component through the existing harness, not a deployed daemon.

```powershell
go test ./peer -run '^Test(PC92Graph|PC92Projection|Topology)' -count=1 -timeout=90s
```

PASS, 1.050 seconds; `k-graph-regression.log`.

The negative control uses an external Go overlay replacing the three graph
production files with their baseline versions. It also suppresses the new
helper-only benchmark, whose symbol does not exist in that baseline; every
selected regression assertion remains present. Working-source files are
unchanged:

```powershell
go test -overlay D:\codex-gocluster-v12-20261001\k-baseline-overlay\overlay.json ./peer -run '^Test(PC92GraphKSubject(NewNodeCharge|ReplacementCharge|NumericReplacement)|TopologyKSubjectNumericProjection|PC92ProjectionRetainsNumericGenerationThroughKClear)$' -count=1 -v -timeout=60s
```

Expected FAIL, 0.243 seconds; `k-baseline-negative.log`. Actual assertions expose
the old graph's retained `5457/633`, stale SQLite TEXT values, missing zero
allocation charge and stale live-graph metadata charge. This establishes that
the new regressions reject the previous implementation; it is not a failure
of the corrected source.

The initial focused compilation attempt was blocked by concurrent recovery
implementation symbols. It produced no test result; the completed executions
above occurred after that integration became buildable.

```powershell
go test -race ./peer -run '^Test(PC92Graph|PC92Projection|Topology|PC92SlowStorage)' -count=1 -timeout=90s
```

PASS, 9.180 seconds; `k-graph-race.log`. The selected checks include the existing
projection and slow-storage concurrency checks as well as the graph cases.

```powershell
go test ./peer -run '^$' -bench '^BenchmarkPC92EffectiveNodeSubject$' -benchmem -benchtime=100ms -count=1
```

PASS, 0.548 seconds; `k-helper-benchmark.log`, Windows/amd64 on an Intel Core
i9-10900. All four helper cases reported `0 B/op` and `0 allocs/op`: omitted K
5.756 ns/op, partial K 5.354 ns/op, explicit K 3.003 ns/op, and omitted C
2.770 ns/op. This is a local helper allocation check, not a comparative speed
claim or whole-controller service-cost qualification.

Full final lanes, complete service-cost qualification and overall acceptance
remain lead closeout obligations.
