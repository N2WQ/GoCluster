# ADR-0048 - Multi-Dimensional Prelogin Admission and Guardrailed Reject Logging

Status: Accepted
Date: 2026-03-03
Decision Makers: Cluster maintainers
Technical Area: telnet/session admission, reputation integration, config, observability
Decision Origin: Design
Troubleshooting Record(s): none
Tags: telnet, dos, ddos, admission-control, ipinfo, observability

## Context
- ADR-0041 added Tier-A prelogin controls (global cap, per-IP rate/concurrency) and bounded state.
- Contest traffic and reconnect churn require stronger fairness across NAT-heavy and distributed sources without adding contest/normal mode branching.
- We already maintain an IPinfo-backed reputation database for logged-in flow decisions; prelogin admission should leverage the same local metadata path to avoid extra network dependencies.
- Reject logging must remain bounded during floods to avoid turning mitigation into log I/O pressure.

## Decision
- Keep a single always-on admission mode and extend Tier-A prelogin gating to five limiter dimensions using Go's `golang.org/x/time/rate` primitive:
  - per-IP
  - per-subnet (`/24` IPv4, `/48` IPv6)
  - global
  - per-ASN (IPinfo)
  - per-country (IPinfo)
- Enforce limiters in deterministic order: global cap -> global rate -> per-IP concurrency/rate -> subnet rate -> ASN rate -> country rate. [ADR-0380](ADR-0380-prelogin-shared-allowance-accounting.md) refines the shared charge and tracker maintenance order.
- Replace custom token refill math with `x/time/rate` for all prelogin admission token buckets.
- Use a store-only IPinfo lookup path for prelogin (`LookupIPForAdmission`) so admission remains bounded and deterministic under load (no live API/Cymru dependency).
- Add reject-log guardrails:
  - per-interval aggregate counts by reject reason,
  - sampled per-event logs (`admission_log_sample_rate`),
  - per-interval cap on sampled lines (`admission_log_max_reason_lines_per_interval`).
- Expose all knobs in YAML with contest-safe defaults.

## Alternatives considered
1. Keep only per-IP limiter from ADR-0041
   - Pros:
     - Minimal complexity.
   - Cons:
     - Weak fairness under large NAT pools and distributed attacks.
2. Add edge-only L4 limits and keep app limiter unchanged
   - Pros:
     - Offloads work from app tier.
   - Cons:
     - Not portable across deployments; weak defense-in-depth.
3. Build custom limiter implementations for each keyspace
   - Pros:
     - Full control over behavior.
   - Cons:
     - More bug surface than using well-tested Go primitives.

## Consequences
- Positive outcomes:
  - Stronger flood resistance and fairness across IP/subnet/ASN/country scopes.
  - Lower implementation risk using standard `x/time/rate` behavior.
  - Bounded rejection logging during attack conditions.
- Negative outcomes / risks:
  - More limiter state maps increase memory pressure if tracking caps are mis-sized.
  - Aggressive ASN/country defaults may affect very large shared networks.
- Operational impact:
  - New telnet YAML knobs for subnet/global/asn/country rate/burst and admission logging.
  - Prelogin admission metrics include dimension-specific reject counters.
- Follow-up work required:
  - Tune defaults from production telemetry after contest traffic.

## Validation
- Added/updated tests for:
  - global/subnet/asn/country limiter rejects in telnet prelogin admission,
  - admission log sampling and interval caps,
  - config defaults/clamping for new YAML knobs,
  - reputation admission lookup using store+cache path.
- Evidence that would invalidate this decision:
  - sustained legitimate rejects under normal load with safe tuning exhausted,
  - admission latency regressions due to lookup or limiter contention.

## Rollout and Reversal
- Rollout plan:
  - Deploy with defaults in `data/config/runtime.yaml` and monitor reject-reason distributions.
- Backward compatibility impact:
  - User-visible behavior may include earlier prelogin rejects for subnet/global/asn/country pressure.
  - No contest-vs-normal mode split; one always-on policy.
- Reversal plan:
  - Relax or zero specific rate limits; if needed, supersede with a new ADR.

## References
- Related ADR(s):
  - `docs/decisions/ADR-0041-telnet-tier-a-prelogin-admission-gate.md`
- Troubleshooting Record(s): none
- Code:
  - `telnet/server.go`
  - `reputation/gate.go`
  - `config/config.go`
  - `main.go`
- Tests:
  - `telnet/prelogin_gate_test.go`
  - `reputation/gate_test.go`
  - `config/telnet_prelogin_test.go`
- Docs:
  - `README.md`
  - `docs/ENVIRONMENT.md`
  - `data/config/runtime.yaml`
