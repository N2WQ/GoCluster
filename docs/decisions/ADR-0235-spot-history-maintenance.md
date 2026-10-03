# ADR-0235: Spot History Maintenance

- Status: Accepted
- Date: 2026-10-02
- Decision Origin: Troubleshooting chat

## Context

Warm CPU profiles identified repeated WHOSPOTSME bucket scrubbing and harmonic
expiry scans as material costs. The owner approved v16 to change those two
algorithms while preserving every result, ordering rule, timestamp boundary,
threshold and cleanup lifecycle. Benchmark improvement alone does not establish
the original delivery and latency acceptance criteria.

## Decision

Keep WHOSPOTSME's existing victim selection and tie rules. When removing that
victim, use its existing country totals to delete exact bucket keys, or scan a
bucket when that is the smaller traversal. Add no second ownership index.

Maintain exactly one indexed minimum-heap entry for each harmonic recency owner.
Update that same entry whenever recency changes, including backward timestamps;
couple deletion with the existing callsign state. Heap ordering changes only
whole-call expiry traversal. Preserve the order of a call's fundamentals and
all four ShouldDrop results. Current production callers supply UTC timestamps.

Clear removed string references and refresh equal-call string headers so the
index does not retain an obsolete input allocation. Empty heap storage is nil.
After removal its capacity is at most max(8,4 times live entries); shrink to
max(8,2 times live entries) when necessary. Report live count, capacity and
element size, and include overlapping old/new storage in the ownership account.
This index belongs to the existing shared ingestion history, outside the peer
protocol partitions. It introduces no new callsign admission cap.

## Alternatives considered

1. Keep the scans: simplest ownership, but retains the measured maintenance cost.
2. Add another WHOSPOTSME reverse index: rejected because existing country
   totals already provide the required keys without another consistency owner.
3. Append lazy harmonic expiry records: rejected because repeated refreshes
   could retain multiple records per live callsign and complicate the bound.
4. Change windows, thresholds or population caps: outside the selected
   behavior-preserving repair and unnecessary for these algorithms.

## Consequences

### Benefits

- Victim cleanup traverses the smaller relevant population.
- Harmonic expiry examines the earliest recency owners instead of all calls.
- Expiry storage remains coupled to live ownership with explicit spare bounds.

### Risks

- Heap positions must remain consistent across refresh, expiry and pruning.
- Backward timestamps require repair in either direction.
- Growth and shrink allocate; their transient overlap must remain visible.
- Microbenchmarks cannot establish network tail latency or complete delivery.

### Operational impact

No configuration, threshold, output or worker-lifecycle change. The new heap
statistics describe shared history ownership. The original qualification rates,
latency limits and protocol memory ceiling remain binding.

## Links

- [Approved v16](../pc18-pc92-scope-ledger-v16.md)
- [Implementation and validation](../pc92-v16-performance-validation.md)
- [TSR-0036](../troubleshooting/TSR-0036-spot-collisions-and-peer-ownership.md)
- Extends the measured performance repair in
  [ADR-0234](ADR-0234-peer-owned-resources-and-exact-spot-keys.md); supersedes no
  prior behavior or protocol requirement.
