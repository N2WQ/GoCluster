# ADR-0293: Correction Decision Diagnostics

- Status: Accepted
- Date: 2026-10-11
- Decision Origin: Design

## Context

The pipeline call stream previously showed accepted corrections under the title
`Corrected`. Rejected alternatives and cap-loss retention were absent, and a
rare loss reason could disappear from a summary's two most frequent reasons.
Cap pressure does not explain whether evidence is protected, publication has
recovered, or a generation has physically retired. Peer and archive formats do
not carry local decision provenance or support percentages.

## Decision

Use the existing bounded call stream and log fallback for evaluation-phase
events: accepted identity changes are `corrected`, rejected alternate proposals
are `rejected`, and known eligible cap-loss protection is `retained`. Healthy
unchanged leaders and missing snapshots remain quiet. Rename the visible pane
and help to `Call decisions`; keep its existing API and ownership.

Each event names its phase, outcome, fixed reason, input/current/proposed
identities, current identity's support share, and support basis (`weighted`,
`receivers`, `unavailable_loss`, or `unavailable`). Recognition is explicitly
separate, and suppression is explicit. Loss uses `resolver_evidence_loss`, an
unavailable share, and no authoritative proposed replacement. These events
precede later recognition and delivery: they do not claim a final glyph or a
client receipt. Retry evaluations may produce repeated events.

Add three cumulative counters and one gauge to existing resolver diagnostics:

- `CapLossDiscards`: discarded candidate/receiver observation incidences. Evicting
  a candidate with N retained receiver rows adds N, even if those receivers also
  support another candidate. A receiver eviction or capacity-refused receipt
  adds one. This is not a count of distinct receivers or eviction operations.
- `LossPublicationRecoveries`: owner publication transitions from protected to
  unprotected. Query-only expiry and deletion do not count. Reactivation can be
  followed by another published recovery.
- `LossIdleRetirements`: physical owner retirements at the process-idle deadline,
  once per generation. Logical read-side denial can precede this counter.
- `PublishedLossKeys`: readable owner publications marked protected. Logically
  idle or terminal generations are excluded. This is not query-age projection;
  physical `ActiveKeys` can temporarily include a logically denied generation.

Reads do not increment these counters. Keep existing pressure counter units.
Show the dedicated loss rejection count outside the top-two reason summary.
Append fixed counter columns to existing replay CSV artifacts and cumulative
counter/final-gauge fields to existing JSON diagnostics. Replay remains
resolver-only; no recognition floor, per-row percentage, or new artifact is
introduced.

## Alternatives Considered

Calling all evaluations corrections would misstate accepted mutations. Emitting
healthy traffic would fill the bounded pane with ordinary activity. Moving these
events after recognition would require additional held decision state. Counting
query recovery would make totals depend on sampling frequency. Adding spot,
peer, or archive fields would change formats and introduce unwanted provenance.

## Consequences

Accepted-only correction totals, temporal confirmation/history, and retry
acceptance deduplication remain unchanged. Independent static/history support
can still supply S without a current support percentage. Normal and DIAG TCP
describe local output; PC11/PC61 preserve original input fields; the ring retains
existing confidence availability. Reopened archives verify local identity,
glyph and existing source fields without inventing percentage or provenance.
No registry, queue, worker, model, persisted schema or network format is added.

## References

- [Approved scope, B3.2](../call-correction-evidence-scope-ledger-v2.md#b32---diagnostic-meaning-and-output-assertions)
- [Execution evidence](../call-correction-evidence-execution.md)
- [ADR-0292: Resolver Cap-Loss Authority](ADR-0292-resolver-cap-loss-authority.md)
- [ADR-0040: Overview V2 Observability Contract Refresh](ADR-0040-overview-v2-observability-contract-refresh.md)
