# TSR-0125 - Broadcast Order During Membership Churn

Status: Monitoring
Date Opened: 2026-11-11
Date Resolved: n/a
Owner: Cluster maintainers
Technical Area: telnet worker shard membership
Trigger Source: Chat request
Led To ADR(s): ADR-0339
Tags: broadcast, ordering, churn, sessions, worker affinity

## RCA Summary
- What happened: A later ordinary broadcast could reach a surviving client before
  an older queued broadcast when another client disconnected.
- Why: Each dirty shard rebuild reassigned all clients by current map iteration;
  queued jobs kept old snapshots and could execute on different workers.
- What fixed it: Session-owned stable worker affinity; only new sessions are
  balanced onto the shortest shard. Published snapshots remain immutable.
- How we know: A controlled two-worker departure delivered literal newer before
  older on the original code in both immediate and batched modes. The same
  actual-worker cases and churn/concurrency selections pass after repair.
- Operator/support answer: Surviving sessions retain ordinary order through
  membership churn. Direct SELF is a separate immediate lane; overload drops,
  filters and pause still apply. Departures can leave worker load skew.

## Triggering Request
- Request date: 2026-11-10.
- Request summary: Repair reviewed correctness defects in validated slices.
- Request reference: Exact `Approved v1`, overnight execution ledger.

## Symptoms and Impact
- Two individually FIFO worker queues do not imply one client's FIFO delivery
  when that client's assignment changes between broadcasts.
- Reconnect creates a new Client; it must not inherit an old queued job's pointer.

## Timeline
1. 2026-11-10 - Design review compared affinity, hashing and migration fencing.
2. 2026-11-11 - A deterministic two-worker red case reproduced both modes.
3. 2026-11-11 - Stable assignment passed owner ordinary/race selections.

## Hypotheses and Tests
1. Per-worker FIFO is enough without stable recipient placement.
   - Evidence: Complete worker 0 through a sentinel before starting worker 1.
   - Outcome: Rejected. Original survivor output is newer then older.
2. A timer delay is needed to distinguish batched ordering.
   - Evidence: Exact batch-size fillers and sentinel completion force dispatch.
   - Outcome: Rejected. The test does not wait for an arbitrary batch tick.
3. New same-call sessions should retain the old Client's queued ownership.
   - Evidence: Immutable snapshots retain old pointers; 128 replacements leave
     only the current pointer in each new snapshot while a survivor stays put.
   - Outcome: Rejected. Assignment belongs to each session object.

## Findings
- Root cause: Membership reconstruction changed the queue ordering authority.
- Retention: One integer per existing Client object, no side table and no extended
  lifetime. Existing client count covers the primary active population; old
  snapshots/jobs retain their existing bounded queue-owned pointers.
- Durable decision: ADR-0339 records affinity and the skew/cross-lane boundaries.

## Decision Linkage
- ADR created/updated: ADR-0339.
- Decision delta summary: Stable ordinary broadcast session placement.
- Contract/behavior changes: Surviving sessions no longer migrate on rebuild.

## Verification and Monitoring
- Validation steps run: Targeted ordinary/race selections and matched local
  cached/rebuild benchmarks; exact commands/results are in the execution ledger.
- Signals to inspect: Worker queue pressure/skew, worker/client drops, session
  counts and ordinary spot order. No new independent retained structure exists.
- Rollback triggers: Migration, mutable old snapshots, changed SELF/dedupe/drop
  behavior, missing replacement recipients or an allocation on cached lookup.

## References
- Source/tests: `telnet/server.go`, `telnet/broadcast_affinity_test.go`.
- Decision: `docs/decisions/ADR-0339-stable-broadcast-affinity.md`.
- Execution: `docs/codebase-correctness-overnight-2026-11-10.md`.
