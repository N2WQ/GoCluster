# ADR-0027 - Resolver Reliability-Weighted Voting With Fixed-Point Determinism

Status: Accepted
Date: 2026-02-25
Decision Origin: Design

## Context

Resolver-primary correction previously used unweighted unique-spotter counts for winner ranking and confidence state. Spotter reliability tables and `min_spotter_reliability` were applied in legacy correction, but resolver mode did not apply equivalent reliability semantics. This created behavioral drift between primary and shadow paths and allowed low-quality reporters to influence resolver outcomes disproportionately.

## Decision

Adopt reliability-aware resolver voting with bounded deterministic arithmetic:

1. Apply the existing reliability floor in resolver ingest:
- Compute reporter reliability from mode-specific tables (`CW`/`RTTY`) with global fallback.
- Drop resolver evidence when reporter reliability is below `min_spotter_reliability`.

2. Use fixed-point weighted support for resolver ranking/confidence:
- Convert reliability to milliweight (`0..1000`) using deterministic rounding.
- Candidate weighted support = sum of reporter milliweights.
- Total weighted support = sum of unique reporter milliweights per key.
- Resolver state classification (`confident/probable/uncertain`) uses weighted confidence.

3. Preserve sample-size safety and boundedness:
- Keep unweighted unique reporter counts in snapshots for reportability and existing rails.
- Keep all resolver bounds/caps unchanged (queue size, keys, candidates, reporters per candidate).
- Keep deterministic tie-breaking (`weighted support`, then `support`, then `lastSeen`, then callsign).

4. Surface weighted observability:
- Extend resolver snapshots with weighted support totals.
- Add `DropReliability` metric for floor-based ingest drops.

## Alternatives considered

1. Keep binary threshold-only filtering (no weighted voting)
- Pros: minimal change.
- Cons: treats near-threshold and high-reliability reporters equally; lower fidelity.

2. Use float64 weighted voting
- Pros: simpler math at first glance.
- Cons: non-deterministic rounding/ordering risk under tie conditions.

3. Reuse unweighted resolver and post-filter at apply time only
- Pros: less core resolver impact.
- Cons: winner selection remains reliability-insensitive.

## Consequences

Benefits:
- Resolver-primary now respects reliability semantics similar to legacy correction.
- High-reliability reporters carry proportionally more influence.
- Deterministic ordering is preserved with fixed-point arithmetic.

Risks:
- Behavior change in resolver winner selection and confidence under mixed-reliability evidence.
- Weighted confidence may diverge from historical unweighted percentages in logs.

Operational impact:
- No new config keys.
- Existing reliability files and threshold now affect resolver mode.
- New resolver metric (`DropReliability`) available for monitoring reliability-floor pressure.

## Links

- Code:
  - `spot/signal_resolver.go`
  - `main.go`
- Tests:
  - `spot/signal_resolver_test.go`
  - `spot/signal_resolver_bench_test.go`
  - `main_test.go`
- Related ADRs:
  - `docs/decisions/ADR-0005-mode-specific-spotter-reliability.md`
  - `docs/decisions/ADR-0026-resolver-primary-switchover-mode.md`
