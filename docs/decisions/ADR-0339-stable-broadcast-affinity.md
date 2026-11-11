# ADR-0339: Stable Ordinary Broadcast Session Affinity

- Status: Accepted
- Date: 2026-11-11
- Decision Origin: Troubleshooting chat

## Context

Rebuilding client shards by current map iteration could move a surviving session
to another worker while older jobs still retained the previous immutable snapshot.
An idle new worker could deliver a later ordinary broadcast ahead of an older job.
The owner approved stable session affinity in overnight Scope Ledger v1.

## Decision

- Give each Client one one-based worker index; zero means not yet assigned.
  Read/write it only under the existing membership lock during shard rebuilding.
- First retain every surviving session on its assigned worker. Then place new
  sessions on the shortest current shard, resolving ties by lowest worker index.
  The worker count remains fixed for the server lifetime as before.
- Publish fresh immutable snapshots. Existing queued jobs retain their previous
  recipient pointers, including the previous session after a same-call reconnect.
- Preserve ordinary broadcast order per surviving session through the same FIFO
  worker. Existing filtering, queue drops, pause, and client writer admission still
  apply; this is not a guarantee of every spot reaching the network.
- Keep direct SELF delivery separate and immediate. It can overtake ordinary
  broadcasts; this repair introduces no ordering promise across delivery lanes.

## Alternatives considered

1. Rebalance survivors: requires a cross-worker migration fence and broader
   lifecycle changes to preserve older queued ownership.
2. Hash callsigns: avoids a field but loses the existing balanced-new-session
   behavior and couples identity normalization to scheduling.
3. A per-client side map or historical counters: unnecessary retained state.
4. Serialize all workers: removes intended concurrency and changes overload costs.

## Consequences

### Benefits
- Membership churn cannot reroute a surviving session around its queued jobs.
- No per-spot lock, queue, side map, counter or goroutine is added.

### Risks
- Departures can leave worker load skew. Only arriving sessions fill shorter
  shards; survivors never migrate merely to improve balance.
- One integer is retained with each Client object. Existing membership, snapshots
  and queued jobs own its lifetime; affinity adds no independently retained key.

### Operational impact
- No worker count, batching, buffer, protocol or preference default changes.
- Local rebuild/cached-path measurements appear in the execution ledger; they
  establish neither production p99 nor a whole-process memory ceiling.

## Links
- Scope/evidence: [overnight ledger](../codebase-correctness-overnight-2026-11-10.md).
- Tests: `telnet/broadcast_affinity_test.go`, `server_resilience_v4_test.go`.
- Related TSR: [TSR-0125](../troubleshooting/TSR-0125-broadcast-order-churn.md).
- Supersedes / superseded by: none.
