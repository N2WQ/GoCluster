# ADR-0042 - Telnet Reject Workers, Immutable Shard Cache, and Writer Micro-Batching

Status: Accepted
Date: 2026-02-27
Decision Origin: Design

## Title
Telnet Reject Workers, Immutable Shard Cache, and Writer Micro-Batching

## Context
- Tier-A admission controls (ADR-0041) bounded prelogin sessions, but reject banner writes still executed inline on the accept loop.
- During heavy reject storms, inline reject I/O can stall `Accept()` handling and increase connection churn latency.
- Broadcast shard cache updates were written under a read lock, which risks races and non-deterministic behavior under concurrent fan-out and client churn.
- Client writer loop flushed per message, increasing syscall/flush pressure during bursty control+spot mixes.
- `Server.Stop()` was not idempotent and could panic on repeated calls.

## Decision
- Move reject-banner I/O off the accept path:
  - add bounded reject queue + fixed reject worker pool,
  - if reject queue is full, close immediately without banner (deterministic shed),
  - enforce reject write deadline (`reject_write_deadline_ms`).
- Replace mutable shard cache writes with immutable snapshot publication via atomic pointer:
  - rebuild under `clientsMutex` write lock,
  - publish snapshot atomically,
  - preserve `shardsDirty` invalidation on register/unregister.
- Add per-connection writer micro-batching:
  - keep control-before-spot ordering,
  - batch up to `writer_batch_max_bytes` or `writer_batch_wait_ms`,
  - flush once per batch and preserve deterministic close-after-control behavior.
- Make `Server.Stop()` idempotent via `sync.Once`.
- Expose new YAML knobs:
  - `telnet.reject_workers`
  - `telnet.reject_queue_size`
  - `telnet.reject_write_deadline_ms`
  - `telnet.writer_batch_max_bytes`
  - `telnet.writer_batch_wait_ms`

## Alternatives considered
1. Keep inline reject writes in accept loop.
   - Pros: minimal code change.
   - Cons: accept-loop stalls remain under reject floods.
2. Disable reject banners entirely during overload.
   - Pros: strongest anti-stall behavior.
   - Cons: loses operator/client feedback in moderate overload.
3. Keep per-message writer flushes.
   - Pros: minimum buffering latency.
   - Cons: higher flush/syscall churn and weaker throughput under burst.

## Consequences
- Benefits:
  - accept loop remains non-blocking under reject pressure,
  - shard cache publication becomes race-safe and deterministic,
  - writer path reduces flush churn while keeping control priority,
  - repeated stop calls become safe.
- Risks:
  - when reject queue saturates, some rejected clients receive no banner,
  - writer batching introduces small configurable buffering delay.
- Operational impact:
  - operators gain five new telnet YAML knobs for reject/write behavior.

## Links
- Supersedes: none
- Related: `docs/decisions/ADR-0041-telnet-tier-a-prelogin-admission-gate.md`
- Code:
  - `telnet/server.go`
  - `config/config.go`
  - `main.go`
- Tests:
  - `telnet/server_resilience_v4_test.go`
  - `config/telnet_prelogin_test.go`
- Docs:
  - `data/config/runtime.yaml`
  - `README.md`
  - `docs/ENVIRONMENT.md`
  - `docs/decision-log.md`
