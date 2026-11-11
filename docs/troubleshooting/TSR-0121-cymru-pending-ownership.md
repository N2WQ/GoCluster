# TSR-0121 - Cymru Pending Ownership

Status: Monitoring
Date Opened: 2026-11-10
Date Resolved: n/a
Owner: Cluster maintainers
Technical Area: reputation Cymru DNS queue and pending identities
Trigger Source: Chat request
Led To ADR(s): none
Tags: reputation, DNS, queue overflow, pending, ownership

## RCA Summary
- What happened: Rejected Cymru lookups retained pending IP identities and
  suppressed retries even though no request had been admitted.
- Why: The producer published pending before its nonblocking queue offer; a
  full-queue drop never retired that entry. Completion also deleted by key
  without establishing which accepted request owned the entry.
- What fixed it: Admission and ownership publication share one short mutex.
  Failed offers create no owner. Requests carry a unique, request-held pending
  object; completion removes only that same object, independently of wall time.
- How we know: The original queue held 16 requests while red tests observed
  3,016 pending IPs and refused immediate retry of a rejected IP. Separate red
  completion tests exposed removal of newer owners. Repaired ordinary/race
  owner suites passed, including repeated and backwards timestamps.
- Operator/support answer: DNS fallback order, timeouts, caches, worker count
  and the admitted-request five-minute retry rule are unchanged. Overflow no
  longer creates a false pending lookup or an independent historical IP set.

## Triggering Request
- Request date: 2026-11-10.
- Request summary: Repair the queue-overflow retained-state finding.
- Request reference: Exact `Approved v1` overnight scope ledger.

## Symptoms and Impact
- A unique-IP burst while the queue was full could retain identities for the
  component lifetime. The pending TTL was a retry check, not a cleanup timer.
- An expired retry can coexist with an old executing lookup. An old completion
  must not remove the newer request's duplicate-suppression ownership.
- Repeated/backwards wall time makes a timestamp alone unsuitable as identity.

## Timeline
1. 2026-11-10 - Queue/drop/completion ownership was inspected before approval.
2. 2026-11-10 - Red regressions reproduced overflow retention and lost ownership.
3. 2026-11-10 - Admission coupling and identity comparison passed owner checks.

## Hypotheses and Tests
1. The five-minute TTL removes abandoned pending IPs.
   - Evidence: Rejected keys accumulated with no queued or executing owner.
   - Outcome: Rejected. Time is checked only when the key is requested again.
2. Removing the key unconditionally on completion is safe.
   - Evidence: Controlled old/new completions removed a newer active owner.
   - Outcome: Rejected. Completion must compare the request's owner identity.
3. Timestamp equality uniquely identifies the owning generation.
   - Evidence: After the middle generation completes, a third admission can
     reuse or precede the oldest still-live request's timestamp.
   - Outcome: Rejected. Nonzero request-held objects provide distinct identity
     while older requests remain alive.

## Findings
- Root cause: Pending ownership was independent of successful admission.
- Resource bound: Each mapped identity belongs to queued or executing work;
  pending identity count is at most queue capacity plus actual live workers.
  With one ordinary Start, the queue is eight times configured workers, giving
  at most nine times that worker count. Old replaced ownership objects remain
  owned by their corresponding requests until those requests finish.
- Limits: This is not a process-memory ceiling or a claim that arbitrary IPv6
  zone strings have a fixed size. Existing Start idempotence, Gate.Close and
  cancellation/drain behavior are outside this repair. Cancellation can leave
  queued requests; tests do not relabel that as a complete queue drain.

## Decision Linkage
- ADR created/updated: none; this restores the existing bounded-resource duty.
- Decision delta summary: No DNS, cache-cap, TTL, queue or worker policy change.
- Contract/behavior changes: Rejected work owns no pending identity; admitted
  completion cannot delete another request's pending reservation.

## Verification and Monitoring
- Validation steps run: Native reputation ordinary and race suites each passed
  42 cases, zero skips. The cancellation cleanup refinement also passed its
  targeted race selection. Evidence is retained in the overnight execution log.
- Coverage includes three rounds of occupied workers, full queue, 1,000 extra
  unique refusals, complete admitted-work drain/join, and zero historical growth.
  Full-queue refusal adds zero allocations in the owner allocation check.
- Signals to inspect: Queue pressure, DNS timeout/cache outcomes and pending
  identity cardinality when using local diagnostics; no new production metric
  or field qualification is claimed.
- Rollback triggers: Duplicate admitted ownership, stranded completed entries,
  retry-boundary changes, altered positive/negative caching, or worker races.

## References
- Execution: `docs/codebase-correctness-overnight-2026-11-10.md`.
- Source/tests: `reputation/cymru.go`, `reputation/cymru_test.go`.
- Policy context: `data/config/reputation.yaml`.
