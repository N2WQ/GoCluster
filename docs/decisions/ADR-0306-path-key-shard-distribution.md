# ADR-0306: Path Key Shard Distribution

- Status: Accepted
- Date: 2026-10-15
- Decision Origin: Design and troubleshooting chat, Approved v2/v3/v4

## Context

Fine path keys have zero low bits. Direct modulo placement sends all fine
evidence to shard zero with the existing 64-shard store. Updates and prediction
captures can therefore contend on one lock despite distinct geographic paths.
ADR-0305 preserves compaction after contraction distributed across small maps.

## Decision

Use one private deterministic full-key avalanche selector for both bucket
updates and captures. Keep packed-key identity, H3 encoding, bucket contents,
prediction arithmetic, persisted formats, shard count and maintenance cadence
unchanged. Fixed unsigned mixing also avoids XOR-only cancellation for
same-cell paths. No retained index, cache, worker or setting is introduced.

The owner accepts S4 for development continuation with an explicit performance
exception under Approved v4. Original Serial/off normal-writer p99.9 ratio
1.1666666667 and repaired Serial/enforce1.2361111111 exceeded1.10; separate
same-binary enforce diagnostics also exceeded repeatability bounds. These
observations remain failed, and remaining comparisons remain unrun. Defer
S4's remaining timing campaign and final matched profiles until assessment
after S5-S7. This does not establish either production benefit or regression.

The exception applies only to S4 performance acceptance. Retain source/binary/
input identity, exact routing/read-after-write and arithmetic contracts, zero
warmed allocations, full ordinary/race checks, resource bounds, real hourly
maintenance soak and fresh final review before S4's local checkpoint. The
v4 soak driver replaces both deferred prerequisites without synthesizing
timing passes or profile-completion markers. S5-S7 retain their own gates.

## Alternatives considered

1. Keep direct modulo placement: preserves the concentrated fine lock.
2. Fold key fields with XOR only: same-cell pairs can cancel into poor placement.
3. Block all further work until synthetic tails repeat reliably: the owner
   instead selected acceptance of the unresolved performance risk and later
   full-cluster assessment.

## Consequences

### Benefits

- Distinct fine paths can use independent shard locks and maps.
- One selector keeps write and capture routing consistent.
- Distributed contraction retains ADR-0305's memory-release mechanism.

### Risks

- Mixing adds work and changes cache/map placement; net runtime benefit remains
  unverified. The accepted performance exception does not erase failed results.
- A combined post-S7 profile cannot isolate S4 without controlling the other
  patches. CPU profiles alone do not measure end-to-end spot delay or tail latency.

### Operational impact

- No configuration, glyph meaning, forecast precedence or firewall change.
- S4's retained ordinary/race, resource and real hourly-maintenance validation
  passed, as tracked in the first-wave execution record. The completed soak's
  JSON postprocessor was corrected without changing its saved Go evidence.
- Future full-cluster capture requires a reviewed launch route and identified
  build/configuration/workload; this decision does not deploy or restart production.

## Links

- Related code: `pathreliability/keys.go`, `pathreliability/store.go`
- Related tests: `pathreliability/sharding_contract_test.go`,
  `pathreliability/sharding_concurrency_test.go`,
  `pathreliability/compaction_connected_test.go`,
  `internal/testsupport/fixed_benchmark.go`,
  `internal/cluster/path_sharding_soak_test.go`,
  `telnet/path_sharding_wire_test.go`
- Related docs: `docs/path-reliability-first-wave-execution.md`,
  `pathreliability/README.md`
- Related TSRs: TSR-0097
- Supersedes / superseded by: none; preserves ADR-0305 compaction and
  ADR-0300/0301 timestamp/numerical contracts.
