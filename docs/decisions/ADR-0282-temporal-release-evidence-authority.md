# ADR-0282: Temporal Release Evidence Authority

- Status: Accepted
- Date: 2026-10-10
- Decision Origin: Troubleshooting chat

## Context

A decoder proposal previously carried saved counts into correction gates and
updated confirmed history before those gates accepted an identity mutation.
Expired support and newer splits could therefore be bypassed in delivered output.

## Decision

At runtime/replay release, filter the latest immutable receiver publication from
the key's inclusive lower recency cutoff through the release clock; exclude future rows.
Recompute weighted receiver union, candidate incidence, confidence and ambiguity.
Do not resurrect older observations, wait for ingestion, or advance owner
hysteresis. A reader cannot settle a new winner from removal alone.

The decoded identity is only a proposal. Current eligible settled evidence must
contain its support and satisfy existing replacement comparability; ordinary
correction gates and CTY validation still run once. Unsupported proposals may
recover through a newer settled ordinary selection. Configured abstention uses
current evidence for confidence without mutating identity. Existing scoring,
hold scope, waits and ordinary immediate selection remain unchanged.

Only accepted normalized identity mutations update committed counters and
confirmed path-switch history. History has the configured temporal key cap and
deterministically evicts the oldest confirmation. Cardinality/evictions are read
from that owner. Request-owned result context follows bounded stabilization;
an atomic marker counts the first actual consumer call once, including refusal.
This handoff attempt does not prove receipt. Separate real accepted mutations
on one context count separately; unchanged retries do not count acceptance.

Forced replay retirement selects all pending holds at the existing EOF clock,
including future deadlines. It does not move that clock forward to revive support.

## Alternatives considered

- Saved support authority: cannot establish current eligibility or ambiguity.
- Synchronous resolver mutation/RPC: unnecessary owner coupling and hysteresis risk.
- A lifetime request-ID history: unbounded and unnecessary for once-only handoff.

## Consequences

All-fresh projection reuses the immutable generation without allocation. Partial
projection uses bounded temporary rows/maps; its local cost requires measurement.
No values/defaults, protocol fields or persisted schema change. Legacy
`commit_latency_ms` remains decision latency, including rejection/fallback.
Fresh key/window evidence does not prove a shared RF episode. Wider association,
field accuracy, probability calibration and production capacity remain unverified.

## Links

- Related ADRs: [ADR-0038](ADR-0038-fixed-lag-temporal-decoder-main-replay-parity.md),
  [ADR-0280](ADR-0280-resolver-receiver-evidence-ownership.md)
- Related TSR: [TSR-0081](../troubleshooting/TSR-0081-temporal-saved-authority.md)
- Related docs: [execution record](../call-correction-method-execution.md)
- Related tests: `spot/resolver_fresh_projection_test.go`,
  `internal/correctionflow/temporal_confirmation_test.go`,
  `internal/cluster/temporal_fresh_delivery_test.go`,
  `cmd/rbn_replay/temporal_fresh_replay_test.go`
