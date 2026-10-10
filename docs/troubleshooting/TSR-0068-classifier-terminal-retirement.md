# TSR-0068 - Classifier Terminal Retirement

Status: Monitoring
Date Opened: 2026-10-10
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: internal/toxicity lifecycle, queues and HTTP cancellation
Trigger Source: Chat request
Led To ADR(s): ADR-0274
Tags: shutdown, classifier, cancellation, bounded-queues

## RCA Summary
- What happened: classifier Stop did not cancel active HTTP work, retire queued
  spot references or discard buffered results. Repeated Start could add workers.
- Why: startup, admission and retirement had no shared lifecycle boundary;
  request timeouts were derived from Background rather than an owned cancellable
  context. Joining workers alone did not retire the queues.
- What fixed it: one terminal operation fences admission, cancels requests,
  joins the single worker group and retires queued jobs and buffered results.
- How we know: focused ordinary/race controls and complete 70-package ordinary
  and race suites passed. Real header/body stalls cancel well before the
  30-second deadline, workers join, pending/backlog retire and exact counters
  separate ordinary errors from terminal cleanup.
- Operator/support answer: terminal cancellation is cleanup, not an AI timeout
  or classification. Later eligible submissions fail open without new AI work.

## Triggering Request
- Request date: 2026-10-10
- Request summary: implement approved v3 checkpoint 20a in the command fixes.
- Request reference (chat/issue/link): `docs/command-fix-scope-ledger-v3.md`.

## Symptoms and Impact
- Stop could wait for the ordinary HTTP timeout instead of canceling a stall.
- Queued or buffered spot references could remain retained after worker exit.
- Restart/admission after Stop had no terminal contract; repeated Start could
  exceed the configured worker count.
- This record covers the classifier component. Runtime grace and downstream
  shutdown ordering require the separate approved checkpoint 20b.

## Timeline
1. 2026-10-10 - Inspected classifier, client, cache, output callers and ADR-0114.
2. 2026-10-10 - Dispositioned lifecycle ownership and falsifiability findings.
3. 2026-10-10 - Implemented and qualified the terminal component boundary;
   production runtime ordering remains checkpoint 20b.

## Hypotheses and Tests
1. Joining workers is sufficient retirement.
   - Evidence/commands: worker and Stop source inspection shows queued jobs and
     completed results remain separate retained owners.
   - Outcome: Rejected.
2. Request timeouts alone provide prompt shutdown.
   - Evidence/commands: existing requests used a Background-derived timeout;
     Stop could not interrupt response-header or response-body stalls.
   - Outcome: Rejected.
3. Start and Stop can use independent once guards without an admission fence.
   - Evidence/commands: semantic caller inspection and design review found an
     Add/Wait ordering gap and submissions after terminal publication.
   - Outcome: Rejected.

## Findings
- Root cause: component lifecycle and queue reference ownership were incomplete.
- Contributing factors: completed results and pending work are distinct; a
  one-valued result receive also precludes closing the existing result channel.
- A durable decision is needed because terminal admission, cancellation,
  accounting and result ownership are shared lifecycle contracts.

## Decision Linkage
- ADR created/updated: [ADR-0274](../decisions/ADR-0274-terminal-classifier-retirement.md).
- Decision delta summary: one worker group; terminal admission; canceled and
  joined HTTP work; exact retirement of queued jobs and unreceived results.
- Contract/behavior changes: received results stay consumer-owned; post-Stop
  UNKNOWN human submissions become UNAVAILABLE; Stop does not close Results.

## Verification and Monitoring
- Validation steps run: source/caller/cache inspection; focused lifecycle,
  HTTP and cache ordinary/race controls; complete 70-package ordinary and race
  suites; vet, Staticcheck and lint; guarded before/after admission benchmarks.
  The benchmark allocation counts remained 0/3/6/4 for non-human/local/cache/
  full-queue paths. This does not establish production latency or leak freedom.
- Signals to monitor: Pending and ResultBacklog must be zero after Stop;
  cancellation must not inflate Timeout or Malformed outcomes.
- Rollback triggers: stranded workers/references, Add/Wait races, incorrect
  outcome counters or changed ordinary classification behavior.

## References
- Issue(s): approved v3 scope ledger, checkpoint 20a.
- PR(s): none.
- Commit(s): pending.
- Related ADR(s): [ADR-0114](../decisions/ADR-0114-human-comment-toxicity-classifier.md),
  [ADR-0274](../decisions/ADR-0274-terminal-classifier-retirement.md).
- Related docs: `docs/command-fix-execution.md`, `data/config/README.md`.
