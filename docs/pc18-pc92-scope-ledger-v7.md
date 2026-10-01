# PC18/PC92 approved v7 execution record

The user authorized **Approved v7** on 2026-10-01. Work remains on `p92`.
V7 inherits every D1-D8 decision and S01-S14 item in
[v6](pc18-pc92-scope-ledger-v6.md), including the 10,000 distinct new
PC11/PC61/PC26 forwarding keys/minute target. It changes only the qualification
contract described below. Implementation and acceptance are still in progress.
The approved [v8 amendment](pc18-pc92-scope-ledger-v8.md) adds bounded,
behavior-preserving shared spot-comment parsing and its required validation.

## Approved qualification amendments

Q1-Q3 use an isolated configuration with `telnet.broadcast_batch_interval_ms=0`,
`call_correction.stabilizer_enabled=false`, and
`call_correction.temporal_decoder.enabled=false`. Shipped defaults and ordinary
correction/admission policies remain unchanged. All other qualified runtime,
queue, CPU, GC, memory and correction settings remain effective. Each healthy
client must meet p99 <=5 ms ingestion to successful enqueue and <=25 ms
ingestion to first received byte, for every fixed input-minute cohort and the
entire run. An immutable comment token tracks delivery even when correction
changes the DX callsign. Every required delivery must be accounted for; an
unknown, missing, duplicate, or observer overflow cannot count as success.

Repeat Q1's 45-minute load and 11-minute drain with shipped intentional holds
enabled. Report its timings and delivery outcomes separately without applying
the two low-latency thresholds to that profile.

Q4 has two 30-minute fill/release/refill phases. Phase A has 1,000 local sessions,
64 established peers and 128 prelogin candidates, with maximum reachable
graph/cache/queue/reader/active occupancy. Phase B has 1,000 local sessions,
63 established peers and 128 authenticated candidates competing for the last
configured identity. Exercise per-candidate/global staging, phase deadlines,
cleanup and establishment races. Admission and ownership rules remain intact.
The 480 MiB total and each partition are unchanged. Establish actual concurrent
peaks and a conservative proof that includes backing allocations, map capacity,
allocator rounding, transient growth and active generations. Separate heap
samples cannot establish a simultaneous allocation bound.

Narrow, bounded observation and fault seams may support these tests. They must
be dormant in normal builds and must trigger ordinary behavior; they cannot
insert topology, grant authority, reset caches, erase gates or bypass admission.

## Detailed test review dispositions

The post-approval test-strategy review occurred before implementing the revised
harness. It was a separate worker with inherited context, not an independent
non-steered review. The lead accepted the following checker refinements:

- Expected recipient sets are declared before delivery; surviving clients do
  not define the denominator. Histograms include exact 5/25 ms boundaries and
  fixed input-minute cohorts. Drain late output and handle prompts/split reads.
- Comment tokens do not themselves create distinct forwarding keys. Fixtures
  must vary the actual protocol key fields, preserving correction behavior.
- Collect graph state on its actor and transport reservations under their
  ownership locks. Label sampled counters and conservative reservations
  separately; neither alone proves actual simultaneous allocation peaks.
- Full freshness occupancy requires prior ordinary lifecycle history. During
  pre-load setup only, a tagged authority clock advances graph/freshness/local
  publication UTC consistently. Payload TTL, I/O and handshake deadlines, gate
  stability and qualification durations remain real elapsed time.
- Populate 4,096 origins through C; renew their watermarks through PC93 without
  resetting observations; let three hourly rounds remove their nodes. Repeat
  for a second cohort while renewing the detached first cohort, then populate
  4,096 live origins and 4,096 PC93-only origins. Assert the resulting 8,192
  detached topology watermarks, 4,096 live-node watermarks, and 4,096 message-only
  watermarks. Do not assume initial direct-peer authority is absent: verify it.
- Renew each live cohort at two hours and before its third expiry round to
  avoid the reference's 8,220-second midnight ordering boundary. Refresh older
  detached watermarks in steps below 1,800 seconds. Reconcile every real-time
  PC93 cache admission against its count/byte limits; refusal invalidates setup.
- Use private renewal messages to a nonexistent fixture recipient, avoiding
  incidental local announcement load. Prove unchanged membership after renewals,
  real node expiry and retained topology classification. Freeze the offset before
  load and report natural tombstone expiry during the real 30-minute run.

These refinements resolve reachability and measurement issues without changing
protocol policy, limits, required durations or acceptance thresholds. Long
qualification, the complete final Go validation lane, documentation, final diff
review and S01-S14 traceability remain required. No release/deployment, commit
or push is authorized by this record.
