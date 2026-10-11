# ADR-0285: Individual Historical Receiver Freshness

- Status: Accepted
- Date: 2026-10-11
- Decision Origin: Design

## Context

Custom SCP historical lookup previously scored a key's newest observation and
counted the key's retained receivers and cells together. One fresh report could
therefore make older retained observations satisfy a fresh-corroboration gate.
Future timestamps also acquired the youngest age tier. Retention and historical
qualification are different contracts: evidence can remain stored without being
eligible for a particular query.

The owner selected individual observation qualification using the existing age
scores, stored-second boundaries and exclusion of future observations. This is
approved scope v2, B1.1; later consumer, family, capacity and static-membership
policy changes have separate patch boundaries.

## Decision

For each call/band/mode-bucket query, count a distinct retained receiver only if
its own newest observation is within the observation retention horizon, is not
future relative to the query clock and has an age score at least the applicable
minimum. Count distinct nonzero geographic cells only from those qualifying
observations. A qualifying refresh cannot lend its age score to other receivers.

Preserve the existing inclusive age tiers: 10 through 12 hours, 7 through 48
hours, 5 through 7 days, 3 through 30 days, 2 through 90 days, 1 through 180 days,
and 0 beyond 180 days. Observation and query timestamps are compared at stored
Unix-second precision; equality with the query second is eligible rather than
future. Preserve configured score and cell gates.

Qualification filtering alone does not delete within-horizon observations that
fail a score gate or are future. Queries preserve their existing horizon
pruning, empty-entry deletion and expiry maintenance. Keep existing periodic
cleanup, newest-observation mutation, record encoding, admission and
retained-state bounds. An already retained future observation remains stored but
supplies no receiver or cell support until eligible. No older per-receiver
receipt history is invented.

Retained-state statistics and maintained active-call counts remain separate
from qualifying receiver/cell counts. Static membership already has its own
`static_horizon_days`; this decision does not change its eligibility. Current
resolver consensus remains a different evidence population from history.

The qualifying population is a conditional set of observations; distinct
receiver/cell counts are cardinalities of that set. This contract does not
recalibrate the existing age tiers or establish RF identity, receiver
independence, shared-transmission association or correctness probabilities.

## Alternatives considered

1. Use the newest key timestamp for all receivers: manufactures corroboration
   from observations that fail the query's own age requirement.
2. Delete observations that fail a query: makes stricter consumers destroy
   retained history that may still qualify elsewhere and changes persistence.
3. Sum observation scores: changes existing score semantics and confuses
   freshness with distinct corroboration.
4. Retain full receipt histories or retune age tiers: adds state or scientific
   policy outside this bounded accounting repair.

## Consequences

### Benefits

- Fresh corroboration uses independently age-eligible observations.
- Receiver and cell denominators describe the same admitted population.
- Qualification preserves within-horizon history and existing cleanup ownership.

### Risks

- Some corrections or support floors previously enabled by stale observations
  no longer qualify; that visible reduction is the intended accounting repair.
- A future newest observation can temporarily remove that receiver's support;
  lookup cannot recover an older report that the store does not retain.
- Existing key and age windows do not establish a shared transmission episode.

### Operational impact

- No schema, record-byte, config/default, admission or cleanup-policy change.
- Ordinary stored counts need not equal qualifying support counts.
- Validation must separately check query results, retained values, raw persisted
  bytes before decoding, reopen recovery and actual delivered output.

## Links

- Related issues/PRs/commits: approved evidence scope v2, B1.1.
- Related tests: [freshness regressions](../../spot/custom_scp_freshness_test.go)
  and [connected delivered-output regression](../../internal/cluster/custom_scp_freshness_delivery_test.go).
- Related docs: [spot history contract](../../spot/README.md#historical-receiver-freshness),
  [execution record](../call-correction-evidence-execution.md).
- Related TSRs: none; this records the selected qualification contract.
- Supersedes / superseded by: none; refines historical qualification under
  [ADR-0039](ADR-0039-custom-scp-runtime-evidence-and-shared-pebble-resilience.md),
  preserving admission under [ADR-0043](ADR-0043-custom-scp-v-only-admission.md)
  and cleanup/database ownership under
  [ADR-0279](ADR-0279-custom-scp-cleanup-and-database-ownership.md).
