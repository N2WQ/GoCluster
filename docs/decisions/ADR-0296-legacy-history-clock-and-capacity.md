# ADR-0296: Legacy History Clock and Capacity

- Status: Accepted
- Date: 2026-10-11
- Decision Origin: Troubleshooting chat

## Context

Legacy recent-band history also owns separate FT support. Clamping impossible
Boolean minima, retaining future admission and using wall maintenance during
historical replay could supply or remove support contrary to accepted evidence.
The owner selected future rejection and clear startup failure for impossible
consumed requirements in the approved follow-up ledger.

## Decision

Legacy admission uses explicit owner time: wall UTC live, evaluation time offline.
A zero source timestamp resolves to that admission time. Refuse a later source
timestamp before normalization, pruning, overwrite, eviction or retained-state
changes. Preserve existing lower pruning and bounded valid-admission behavior.

Queries count receiver observations in inclusive [queryNow-window, queryNow],
using time.Time precision. Backward queries exclude later rows without deleting
them solely for being later. Lower pruning still applies; there is no historical
reconstruction guarantee. Nonpositive Boolean minima still mean two. Above-cap
minima return false; count queries preserve eligible partial evidence.

Config-owned pure checks run at actual owner startup boundaries before relevant
resolver/history creation or replay scratch deletion. Live/replay general legacy
holds 64 receivers; reveng's actual legacy backend also holds 64 even when its
configuration selects CustomSCP; separate live FT holds 16. Check actual enabled
consumers, including FT's floor with correction globally disabled. Replay lacks
the live support floor; reveng runs resolver gates without a live stabilizer or
support floor. Keep existing raw defaults and Custom receiver-threshold folding.
Do not change YAML schema/defaults or the generic loader's CustomSCP check.

Legacy replay starts no wall cleanup worker. Its existing evaluation owner
performs synchronous maintenance at the existing interval using one scalar next
deadline. First evaluation initializes cadence; a due/gap evaluation sweeps once
at current time without catch-up ticks. Ordinary rows, fixed sampling and forced
terminal evaluation advance maintenance. Preserve live and CustomSCP maintenance,
primary/reference deletion coupling and all existing caps.

## Alternatives considered

- Query-only future filtering permits destructive future admission; rejected.
- Clamping or silently leaving an impossible gate running weakens policy or
  hides an operational failure; reject before creating relevant owners.
- A replay clock callback in a worker adds asynchronous clock publication and
  synchronization without a needed benefit; existing synchronous ownership fits.
- Validating all fields regardless of consumers rejects unused settings and
  confuses separate store capacities; use actual startup ownership.

## Consequences and operational impact

Previously accepted impossible enabled configurations fail with setting,
effective requirement, owner and capacity. Operators must choose attainable
requirements; the dedicated FT cap is fixed. Future legacy/FT reports cannot
replace valid history. Replay maintenance agrees with evaluation eligibility.
No new worker, registry, config switch, persisted encoding or skew tolerance is
added. Historical recognition remains separate from current support percentage
and is not calibrated call correctness. Local checks do not prove field accuracy.

## Links and supersession

- Extends the actual-owner principle of
  [ADR-0288](ADR-0288-historical-requirements-respect-storage-capacity.md) to
  legacy/FT; CustomSCP timestamp/storage policy remains separate.
- Refines [ADR-0019](ADR-0019-spot-distance-core-and-cleanup-runner.md)
  only for replay legacy maintenance ownership; live cleanup is unchanged.
- Troubleshooting: [TSR-0089](../troubleshooting/TSR-0089-legacy-history-clock-and-capacity.md).
- Authority and validation: [ledger](../call-correction-followup-scope-ledger-v2.md)
  and [execution record](../call-correction-followup-execution.md).
