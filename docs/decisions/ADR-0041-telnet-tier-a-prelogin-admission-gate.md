# ADR-0041 - Tier-A Prelogin Admission Gate for Telnet DoS Resilience

Status: Accepted
Date: 2026-02-27
Decision Makers: Cluster maintainers
Technical Area: telnet/session admission, config, main network observability
Decision Origin: Design
Troubleshooting Record(s): none
Tags: telnet, dos, admission-control, bounded-resources

## Context
- Telnet had strong post-login backpressure controls, but unauthenticated sockets were only bounded indirectly by login timeout and OS limits.
- `max_connections` applied to logged-in sessions only, which left a prelogin flood path that could consume file descriptors and goroutines.
- The contest-weekend requirement is immediate abuse resistance with minimal new operator knobs and deterministic behavior.

## Decision
- Introduce Tier-A prelogin admission controls with five knobs:
  - `telnet.max_prelogin_sessions`
  - `telnet.prelogin_timeout_seconds`
  - `telnet.accept_rate_per_ip`
  - `telnet.accept_burst_per_ip`
  - `telnet.prelogin_concurrency_per_ip`
- Enforce admission before spawning per-connection session work:
  - global prelogin cap,
  - per-IP concurrency cap,
  - per-IP token-bucket rate cap.
- Apply a short prelogin timeout budget from accept to successful callsign login.
- Keep resources bounded by capping tracked per-IP admission state and evicting oldest idle entries when necessary.
- Add Tier-A observability counters/gauge in telnet server snapshots and network summary output.

## Alternatives considered
1. Keep only `max_connections` + long prelogin timeout
   - Pros:
     - No code change.
   - Cons:
     - Leaves prelogin flood path effectively unbounded in app-layer policy.
2. Add per-/24 and staged sub-timeout knobs immediately
   - Pros:
     - Stronger and more granular control surface.
   - Cons:
     - Higher operational complexity under urgent rollout conditions.
3. Edge-only mitigation (proxy rate limits) with no app changes
   - Pros:
     - Can be fast where edge infra exists.
   - Cons:
     - Weak defense-in-depth and not guaranteed in every deployment.

## Consequences
- Positive outcomes:
  - Deterministic, bounded prelogin admission under flood.
  - Faster cleanup of silent unauthenticated sessions.
  - Explicit operator controls for contest and abuse scenarios.
- Negative outcomes / risks:
  - NAT-heavy legitimate users can hit per-IP caps/rate under burst reconnects.
  - Additional admission-state complexity in telnet server.
- Operational impact:
  - New telnet YAML knobs and default values.
  - New prelogin metrics in network status output.
- Follow-up work required:
  - Evaluate optional per-prefix controls if per-IP policy is insufficient in production.

## Validation
- Added config tests for defaulting and prelogin-concurrency clamping.
- Added telnet tests for:
  - global prelogin cap,
  - per-IP concurrency cap,
  - per-IP token bucket behavior,
  - bounded tracked-state eviction,
  - prelogin timeout lifecycle and ticket release.
- Evidence that would invalidate this decision:
  - persistent legitimate rejection under normal contest load that cannot be tuned safely with existing knobs.

## Rollout and Reversal
- Rollout plan:
  - Use defaults: `max_prelogin_sessions=256`, `prelogin_timeout_seconds=15`, `accept_rate_per_ip=3`, `accept_burst_per_ip=6`, `prelogin_concurrency_per_ip=3`.
  - Observe prelogin reject/timeout counters during live traffic.
- Backward compatibility impact:
  - User-visible prelogin behavior changes (faster timeout, rate/concurrency rejects).
  - Existing `login_timeout_seconds` kept as legacy fallback.
- Reversal plan:
  - Set permissive values (higher caps/rates) or revert this ADR with a superseding decision.

## References
- Related ADR(s):
  - `docs/decisions/ADR-0013-call-correction-telnet-stabilizer.md`
  - `docs/decisions/ADR-0020-stabilizer-max-checks-retries.md`
- Code:
  - `telnet/server.go`
  - `config/config.go`
  - `main.go`
- Tests:
  - `telnet/prelogin_gate_test.go`
  - `config/telnet_prelogin_test.go`
- Docs:
  - `README.md`
  - `docs/ENVIRONMENT.md`
  - `docs/decision-log.md`
