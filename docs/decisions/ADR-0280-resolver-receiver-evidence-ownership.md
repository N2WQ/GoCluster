# ADR-0280: Resolver Receiver Evidence Ownership

- Status: Accepted
- Date: 2026-10-10
- Decision Origin: Troubleshooting chat

## Context

Neighborhood correction added bucket winners' counts and discarded dissent.
Two repeated A receivers and one B receiver became four A supporters at 100%,
authorizing an identity change visible on TCP, peer, buffer and archive output.
Correct union accounting requires receiver identities and immutable observations.

## Decision

The existing resolver owner retains the newest timestamped observation per
receiver/candidate/bucket. Equal-time frequency disagreement stays conflicted
until a newer observation supersedes it or it expires. Candidate/key timestamps
cannot roll back because an older report arrives.

Publish opaque immutable receiver generations with the key's recency window.
Reuse unchanged generations; rebuild on metadata, membership or window changes.
The existing key/candidate/receiver caps and deletion paths own their bounds.
Expose latest published incidence cardinality in resolver metrics. Callback
values are copies; no public mutable observation slice is supplied.

Union every admitted bucket's distinct receivers for the denominator and each
candidate's distinct receivers for its numerator, retaining dissent even when
it is ineligible as a replacement. Preserve whole-bucket admission rails.
Synthesized neighborhood tallies and temporal retention do not carry full
receiver generations; temporal ranks preserve their existing read-only contract.

## Alternatives considered

1. Sum bucket counts: cannot conserve distinct support or dissent.
2. Independent receiver history registry: duplicates ownership and cleanup.
3. Copy full evidence on every sweep or temporal request: unnecessary allocation
   and retention; use changed generations and compact scoring instead.

## Consequences

### Benefits

- Correct weighted receiver union and preserved competing support.
- Lock-free, zero-allocation lookup remains the read contract.
- Evidence lifetime follows existing bounded owners.

### Risks

- Dirty publication costs additional allocation; capped rows must be measured.
- Receiver incidence can overlap, so candidate shares need not sum to 100%.
- Key/window association does not establish a shared RF transmission episode.

### Operational impact

No configuration value, confidence threshold, wire field or persisted schema
changes. Production capacity and calibrated call accuracy remain unmeasured.

## Links

- Related tests: `spot/resolver_evidence_test.go`,
  `internal/correctionflow/neighborhood_evidence_test.go`,
  `internal/cluster/correction_evidence_delivery_test.go`,
  `cmd/rbn_replay/temporal_evidence_test.go`
- Related docs: [execution record](../call-correction-method-execution.md),
  [spot confidence](../../spot/README.md)
- Related TSRs: [TSR-0079](../troubleshooting/TSR-0079-resolver-receiver-accounting.md)
- Supersedes / superseded by: none
