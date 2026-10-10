# ADR-0274: Terminal Classifier Retirement

- Status: Accepted
- Date: 2026-10-10
- Decision Origin: Troubleshooting chat

## Context

ADR-0114 bounds human-comment classification and fails open on unavailable AI.
Worker joins alone do not define complete retirement: pending jobs, completed
results and active HTTP requests have separate owners. Start and admission must
also be ordered against Stop so worker bounds and pending accounting hold.

The production output loop receives Results without checking channel closure.
Its bounded grace and downstream handoff remain a separate runtime responsibility.

## Decision

- Start creates at most one configured worker group. Pre-Start admission remains
  available, but Stop before Start retires that work and permanently prevents
  worker startup.
- One short lifecycle mutex orders startup, admission, terminal publication and
  outcome/cache commitment. It never spans HTTP, blocking publication or joins.
- Stop is terminal and idempotent. Concurrent callers wait for one complete
  operation: fence admission, cancel active HTTP work, join all workers, retire
  queued jobs and discard buffered results.
- Request timeouts derive from classifier-owned cancellation, independent of
  runtime cancellation so the runtime can first allow its bounded grace.
  The real client must honor cancellation during request and body reads.
- Eligible UNKNOWN human submissions after terminal publication synchronously
  become UNAVAILABLE before local-gate/cache checks. They do not enqueue, restart
  workers or increment AI, queue-full, local-bypass or cache-hit counters.
  Non-human and already-classified bypasses remain.
- Each accepted job retires its own pending count exactly once. Pending covers
  queued, active and blocked-publication work; buffered results have their own
  backlog count. Both are zero after Stop returns.
- Terminally discarded work does not mutate spots, populate the cache or count
  as timeout, malformed or unavailable classification. A decision committed
  before terminal publication can retain its normal counters/cache entry even
  if its unreceived result is subsequently discarded.
- Results keeps its channel identity and remains open. A result already received
  belongs to the consumer and can still be applied; Stop cannot revoke it.
- The existing memory-only cache retains its hard cap, lazy TTL, collision
  protection and detached decision categories. It does not retain spot pointers.

## Alternatives considered

1. Rely on request timeout and worker joins. This leaves slow HTTP retirement and
   queued references, and does not prevent restart or late admission.
2. Use independent Start/Stop once guards. This does not order worker Add against
   Wait or admission against terminal publication.
3. Add a lifecycle controller goroutine and acknowledgments. This can work but
   adds another owner and pre-Start protocol without a required responsibility.
4. Close Results at retirement. Existing one-valued receivers could interpret
   channel closure as an endless stream of zero results.
5. Abandon a client call in a helper goroutine. This hides rather than joins
   retained work; clients instead must honor context cancellation.

## Consequences

### Benefits
- Worker count, admission and retirement share a directly testable boundary.
- Stop releases queued references and promptly cancels real HTTP stalls.
- Counters distinguish ordinary failure from terminal cleanup.

### Risks
- Prompt retirement relies on the internal client's cancellation contract.
- Human admission gains a short mutex; non-human bypass remains outside it.
- An already received result cannot be withdrawn; consumers own its handoff.

### Operational impact
- No configuration, schema, model, privacy or normal fail-open rule changes.
- Production must retain one result consumer through grace, then invoke Stop.
  Correct runtime ordering is checkpoint 20b, not established by this decision's
  component tests alone.
- Local allocation benchmarks qualify ordinary admission overhead; they do not
  establish production latency or a process-wide leak verdict.

## Links
- Related issues/PRs/commits: approved v3 scope ledger, checkpoint 20a.
- Related tests: `internal/toxicity/classifier_lifecycle_test.go`,
  `internal/toxicity/classifier_http_retirement_test.go`,
  `internal/toxicity/cache_decision_ownership_test.go`.
- Related docs: `docs/command-fix-execution.md`, `data/config/README.md`,
  `customgpt/support-cards/toxicity-filter.md`.
- Related TSRs: [TSR-0068](../troubleshooting/TSR-0068-classifier-terminal-retirement.md).
- Supersedes / superseded by: none; refines the lifecycle part of
  [ADR-0114](ADR-0114-human-comment-toxicity-classifier.md).
