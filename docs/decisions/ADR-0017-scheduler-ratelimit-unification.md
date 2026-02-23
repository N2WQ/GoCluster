# ADR-0017 - Shared Daily Scheduler Parsing and Rate-Counter Semantics

Status: Accepted
Date: 2026-02-23
Decision Makers: Cluster maintainers
Technical Area: internal/schedule, internal/ratelimit, main, uls, reputation, pskreporter, ingest
Decision Origin: Design
Troubleshooting Record(s): none
Tags: scheduling, ratelimit, config-compatibility, observability

## Context
- Daily refresh scheduling logic was duplicated across multiple components with drift risk in:
  - fallback UTC defaults per subsystem
  - accepted config syntax (strict `HH:MM` vs permissive `daily@HH:MMZ`)
  - next-run delay math.
- Rate-limited log counters were duplicated in `internal/ratelimit`, `pskreporter`, and ingest validation with a subtle semantic split:
  - one-shot CAS (can skip interval log under contention)
  - CAS retry loop (ensures one winner once interval elapsed).
- We need consolidation without changing externally visible scheduling/config behavior or silently changing contention semantics.

## Decision
1. Introduce `internal/schedule` as the single helper for:
   - parsing UTC refresh strings with explicit options (`AllowDailyPrefix`, `AllowTrailingZ`)
   - computing next daily UTC delay.
2. Keep existing per-component config contracts by selecting parse options per caller:
   - strict `HH:MM` for main schedulers and ULS
   - permissive `daily@HH:MM` + trailing `Z` for reputation.
3. Keep existing fallback times per subsystem:
   - skew `00:30`, known calls `01:00`, CTY `00:45`, ULS `02:15`, reputation `03:00`.
4. Extend `internal/ratelimit.Counter` to support both CAS modes explicitly:
   - `NewCounter` retains one-shot CAS behavior
   - `NewCounterWithRetry` preserves retry-CAS behavior for ingest validation.
5. Move `pskreporter` and ingest validation to `internal/ratelimit.Counter` and remove local duplicate implementations.

## Alternatives considered
1. Standardize all schedulers on permissive parsing everywhere.
   - Pros:
     - smallest API surface for callers.
   - Cons:
     - silently broadens accepted config in strict components.
2. Standardize all rate counters on one CAS strategy.
   - Pros:
     - simpler counter implementation.
   - Cons:
     - either changes ingest contention behavior (if one-shot) or may add extra CAS spin (if retry) where not needed.
3. Keep duplicated implementations and align via comments only.
   - Pros:
     - zero refactor risk.
   - Cons:
     - continued drift risk and repeated bug-fix effort.

## Consequences
- Positive outcomes:
  - Single source of truth for daily delay math and refresh parsing.
  - Shared rate-counter implementation with explicit, documented contention behavior.
  - Less duplication and lower semantic drift risk.
- Risks:
  - Helper misuse could accidentally widen parser behavior for strict callers.
  - Retry-CAS mode could increase CPU under extreme contention if applied broadly.
- Operational impact:
  - No intended user-visible behavior change to schedule defaults or accepted syntax per subsystem.
  - Counter totals and throttled log cadence remain component-compatible with prior behavior.
- Mitigations:
  - Parse options are explicit at each callsite.
  - Retry-CAS is only used where prior code already used that behavior.

## Links
- Related ADR(s): none
- Code:
  - `internal/schedule/daily.go`
  - `internal/ratelimit/counter.go`
  - `main.go`
  - `uls/downloader.go`
  - `reputation/downloader.go`
  - `ingest_validation.go`
  - `pskreporter/client.go`
- Tests:
  - `internal/schedule/daily_test.go`
  - `internal/ratelimit/counter_test.go`
- Docs:
  - `docs/decision-log.md`
