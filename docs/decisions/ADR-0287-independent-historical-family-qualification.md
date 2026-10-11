# ADR-0287: Independent Historical Family Qualification

- Status: Accepted
- Date: 2026-10-11
- Decision Origin: Design

## Context

Custom SCP family-floor queries selected the member with the greatest age score,
then receiver count, before checking geographic and receiver requirements. A
higher-ranked failure could conceal another member that independently passed
every family gate. Equal-score/count populations could also depend on query order.

## Decision

Use the user's selected policy: any relevant member may qualify by independently
passing every existing family age-score, receiver and geographic gate. Do not
pool observations or combine one member's successful gate with another's.

Evaluate all member queries previously visited, preserving query-driven horizon
pruning even after a member qualifies. Preserve the slice-free key-pair API and
its duplicate-key handling. Exact-call qualification remains first at the caller,
and static recognition remains an independent alternative.

## Alternatives considered

- Ranking before qualification rejects a legitimate independent success and can
  make ties order-dependent; rejected.
- Pooling family populations could manufacture qualifying support and violates
  the selected member-level policy; rejected.
- Short-circuiting queries would change the existing horizon-pruning side
  effects; unnecessary for this bounded repair.

## Consequences

Family-floor decisions follow complete member qualification instead of a proxy
rank. No threshold, capacity, retained-state ownership, admission, persisted
encoding or delivery-suppression policy changes. Existing capacity normalization
and future static eligibility are addressed separately in approved v2.

This establishes historical qualification integrity. It does not establish a
common transmission episode, receiver independence or calibrated correctness.

## Links

- Refines [ADR-0285](ADR-0285-individual-historical-receiver-freshness.md) and
  [ADR-0286](ADR-0286-consumer-specific-historical-requirements.md).
- Implementation: `spot/custom_scp_store.go`.
- Regressions: `spot/custom_scp_freshness_test.go` and
  `internal/cluster/custom_scp_family_delivery_test.go`.
- Approved scope: `docs/call-correction-evidence-scope-ledger-v2.md`, B1.3.
