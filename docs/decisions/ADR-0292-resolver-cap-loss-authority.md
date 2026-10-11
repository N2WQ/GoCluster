# ADR-0292: Resolver Cap-Loss Authority

- Status: Accepted
- Date: 2026-10-11
- Decision Origin: Design

## Context

Candidate and receiver caps can discard still-eligible observations. Retained
counts then describe an incomplete population. Ordinary evaluation throttling
can delay publication, and delayed sweeping can leave a retired population
readable. Observation timestamps cannot bound idle lifetime when future reports
are retained.

## Decision

Record actual cap victims using constant oldest/newest timestamp bounds in the
existing key owner. While that interval intersects the eligible window, retain
the entering identity, block correction and current-consensus promotion, and
withhold its percentage. Static and historical recognition remain independent.
Bounds conservatively include gaps between lost observations. Shrinking a
window can deactivate protection; expansion can reactivate it during the same
generation. Keep metadata in rowless and compact publications.

Publish cap-loss controls promptly, even when ordinary evaluation is throttled.
Rebuild immutable rows and complete retained tallies. Controls do not score a
new model winner or advance hysteresis; a constant last-ordinary authority hint
can preserve only the same still-leading settled identity. Clear that hint when
the owner invalidates its winner.

Give each key lifecycle a scalar generation identifier. This differs from the
receiver-evidence publication generation described in ADR-0280. A temporal
proposal carries five fixed neighborhood witnesses, merged across its entire
decoded window. Conflicting, absent or retired witnesses cannot authorize an
override; fresh ordinary evidence may qualify independently. Loss is checked
before neighborhood comparability filters. Saved loss flags do not replace
current eligible evidence.

A loss-affected generation loses authority at process idle >=10 minutes,
independently of query and observation timestamps. Successful owner admission,
including duplicate or stale receipts, refreshes activity after refusal checks.
Enqueue or rejected reports do not. Admission at or after the deadline retires
the old generation before creating a new one. Captured snapshot cells check
their own terminal and activity state; physical retirement marks them terminal
and removes owner/publication references. Reads overlapping incomplete admission
may conservatively decline; this is not an ingress barrier.

Default owners use monotonic process elapsed time. The deterministic driver can
select a nonnegative, nondecreasing manual clock before its first owner-admitted
receipt. Historical evidence eligibility still uses the query/event clock;
paused offline execution can reach the process-idle retirement boundary.

Active loss prevents premature empty-key cleanup, bounded by the existing key
capacity. Continuous admitted traffic may keep a generation live. Preserve
whole-key refusal at capacity rather than silently evicting quarantined keys.
Retirement and restart declare a new retained population. An already accepted B
is not reversed: subsequent existing evaluations retain B, withdraw its current
consensus confidence, and block further correction while loss remains eligible.

## Alternatives considered

- Tracking every discarded identity adds retained state and a second population.
- Publishing only at ordinary evaluation leaves known loss temporarily authoritative.
- Latching uncertainty forever prevents legitimate recovery behind a future
  evaluation timestamp.
- Observation-time idle cleanup permits future timestamps to pin generations.
- Reversing an accepted correction changes accepted history and requires a new
  post-acceptance contract.

## Consequences

Some corrections and support shares are withheld conservatively. Fixed metadata
and scalar witnesses do not add a registry, worker or timer. This repairs known
candidate/receiver loss only: queue-full, whole-key admission, FT loss,
cross-restart protection and broader RF episode association remain deferred.
Percentages remain support shares, not calibrated correctness probabilities.

## Links

- Approved scope: `docs/call-correction-evidence-scope-ledger-v2.md`, B3.1.
- Execution evidence: `docs/call-correction-evidence-execution.md`.
- Refines [ADR-0280](ADR-0280-resolver-receiver-evidence-ownership.md) and
  [ADR-0282](ADR-0282-temporal-release-evidence-authority.md).
