# ADR-0286: Consumer-Specific Historical Requirements

- Status: Accepted
- Date: 2026-10-11
- Decision Origin: Design

## Context

Runtime and replay construction combined resolver and stabilizer score/cell
requirements using their maxima. Configuration normalization also raised a
shared receiver minimum from the stabilizer minimum. These paths let a change
to one consumer silently tighten another, including the independent FT floor.
Replay did not pass the configured static-membership horizon to the store.

## Decision

Keep one canonical Custom SCP store for recording, persistence, statistics,
cleanup and shutdown. Pass consumer-specific score/cell requirements explicitly
to raw and normalized queries. Do not mutate store options during lookup.
Preserve the normalized path's avoidance of synthetic-call cache pollution.

Resolver and stabilizer receiver qualification uses the maximum of its own
custom minimum and the unraised common recent-support minimum. Plus1 and
edit-neighbor gates additionally apply their own minima. Preserve partial
eligible counts below a receiver gate for Bayesian prior accounting and
diagnostics; qualification thresholds do not erase evidence. Keep resolver-local
plus1 normalization and its existing default ordering, and remove the loader's
stabilizer-to-common rewrite.

Dedicated custom S-floor minima remain separate. FT retains its own evidence
store and uses the unraised common recent-support minimum. Runtime and replay
pass both configured retention horizons. No historical replay clock redesign,
family selection, capacity policy or future static-membership change is included.
In particular, existing boolean-query capacity coercion is preserved for B1.4;
this patch does not claim to enforce an above-cap requirement literally.

## Alternatives considered

1. Mutating canonical options between consumers would make query order and
   concurrency affect qualification; rejected.
2. Independent stores would duplicate retained state and lifecycle ownership;
   rejected.
3. Immutable interface views could preserve one backing store but require
   lifecycle/stat forwarding and careful recording type assertions. Explicit
   scalar queries keep those ownership contracts unchanged.

## Consequences

Consumers can differ without manufactured coupling. The same retained population
can produce different eligible counts under different age-score/cell gates.
This repairs policy wiring, not field accuracy, receiver independence or
calibrated probability. Wider transmission-episode association remains deferred.

## Links

- Refines [ADR-0039](ADR-0039-custom-scp-runtime-evidence-and-shared-pebble-resilience.md)
  and [ADR-0285](ADR-0285-individual-historical-receiver-freshness.md).
- Preserves [ADR-0143](ADR-0143-custom-scp-support-lookup-duplicate-work-removal.md)
  normalized-query ownership and
  [ADR-0279](ADR-0279-custom-scp-cleanup-and-database-ownership.md) lifecycle.
- Tests: `spot/custom_scp_freshness_test.go`,
  `internal/correctionflow/custom_scp_consumer_test.go`,
  `config/custom_scp_consumer_test.go`,
  `internal/cluster/custom_scp_consumer_delivery_test.go`,
  `cmd/rbn_replay/custom_scp_consumer_test.go`.
- Scope: `docs/call-correction-evidence-scope-ledger-v2.md`, B1.2.
