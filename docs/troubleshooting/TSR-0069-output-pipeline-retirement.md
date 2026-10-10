# TSR-0069 - Output Pipeline Retirement

Status: Monitoring
Date Opened: 2026-10-10
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: runtime output, classifier grace, temporal/FT and stabilizer lifecycle
Trigger Source: Chat request
Led To ADR(s): ADR-0275
Tags: shutdown, output, grace, temporal, stabilizer

## RCA Summary
- What happened: runtime shutdown did not request or join its output loop;
  primary dedup Stop left output open. Downstream services could stop while
  classifier results or stabilizer releases still owned processing.
- Why: goroutine launch had no runtime completion handle. Scheduler completion
  was mistaken for completion of its separate release consumer. Hold flushing
  preceded late classifier results and used a synthetic future clock.
- What fixed it: one runtime retirement owner requests and joins the pipeline;
  its first request starts bounded classifier grace independently of transferred
  processing. Late holds and the stabilizer consumer retire before dependencies.
- How we know: permanent production-owner controls, real local HTTP header/body
  faults, complete 70-package ordinary/race baselines and analyzers passed.
  Both actual checkout builds passed 136 cases with qualified retirement receipts.
  Evidence and claim boundaries are in `docs/command-fix-execution.md`, 20b.
- Operator/support answer: grace bounds new classifier-result processing, not
  the whole shutdown. Already admitted processing retains its downstream owners.

## Triggering Request
- Request date: 2026-10-10
- Request summary: implement approved v3 checkpoint 20b after terminal
  classifier component retirement.
- Request reference (chat/issue/link): `docs/command-fix-scope-ledger-v3.md`.

## Symptoms and Impact
- Dedup output remaining open could keep the output consumer alive indefinitely.
- Stopping peer/telnet/stores before processing completed lost its handoff owner.
- A blocked callback could postpone HTTP cancellation beyond classifier grace.
- Flushing before classifier drain stranded holds created by late results.
- Adding 24 hours inflated temporal age and missed accepted longer lag values.
- Joining only the stabilizer scheduler left released-envelope processing
  unowned; terminal retries could retain input without a live scheduler.

## Timeline
1. 2026-10-10 - Inspected runtime startup/shutdown, semantic callers, classifiers,
   holds, scheduler, released fanout and durable contracts.
2. 2026-10-10 - Dispositioned design/falsifiability findings and obtained a
   separate non-steered timing oracle without proposed design/test expectations.
3. 2026-10-10 - Implemented explicit retirement and added local production-owner
   controls. Ordinary/race, analyzer, source-identity and checkout qualification
   passed after correcting the raw-status and Windows listener fixtures.

## Hypotheses and Tests
1. Dedup Stop or input closure establishes production output completion.
   - Evidence/commands: current dedup Stop only signals its private stop;
     semantic callers show runtime launches output without a join.
   - Outcome: Rejected; the consumer must not close a producer-owned channel.
2. Sequential grace/process/Stop bounds active HTTP retirement.
   - Evidence/commands: synchronous grid/fanout callback can block while other
     HTTP work still uses a long ordinary timeout.
   - Outcome: Rejected; independent terminal expiry retains admitted ownership.
3. Advancing time 24 hours reliably retires all holds.
   - Evidence/commands: independent arithmetic gives 86,400,500ms rather than
     500ms; positive 25-hour lag is accepted and still beyond the shortcut.
   - Outcome: Rejected; use force selection with actual evaluation time.
4. Joining the stabilizer scheduler proves downstream completion.
   - Evidence/commands: released envelopes run in a separate consumer and can
     block in a real downstream callback or attempt a retry.
   - Outcome: Rejected; terminal admission and consumer join are separate.

## Findings
- Root cause: output processing, bounded grace, held work and downstream closure
  lacked a shared completion contract.
- A preselect deadline check was insufficient: select may choose ready data
  after expiry. Processing admission is checked again before applying a result.
- Missing lower-bound cancellation evidence could falsely accept an early
  timer. The HTTP control brackets the first deadline and rejects early cancel.
- Scientific review supports timing/force semantics, not correction accuracy,
  calibrated probability or optimal heuristic thresholds.

## Decision Linkage
- ADR created/updated: [ADR-0275](../decisions/ADR-0275-runtime-output-retirement.md).
- Decision delta summary: runtime-owned stop/completion, first-request grace,
  processing admission, actual-clock holds and joined released fanout.
- Contract/behavior changes: terminal HTTP cancellation is independent of
  admitted processing; unreceived/expired work is discarded; downstream closure
  follows pipeline completion. Existing gates and bounded drops remain.

## Verification and Monitoring
- Validation steps run: source/caller/contract and design/test-strategy review;
  separate non-steered timing derivation; focused and complete ordinary/race
  tests; vet, Staticcheck and zero-issue lint; 84 runner and 29 collector controls;
  136 actual checkout cases per build and 24 source-matched stop receipts.
- Signals to monitor: classifier pending/backlog and stabilizer pending zero
  after retirement; holds/timers empty; no downstream close before completion.
- Rollback triggers: early/late cancellation, stranded held work, new lifecycle
  races, abandoned processing or altered normal delivery gates.
- Limits: queue handoff is not socket receipt or durable archive confirmation;
  local lifecycle controls do not prove process-wide ingress or production leaks.

## References
- Issue(s): approved v3 scope ledger, checkpoint 20b.
- PR(s): none.
- Commit(s): checkpoint 20b on `fix_commands`; see the execution record.
- Related ADR(s): [ADR-0274](../decisions/ADR-0274-terminal-classifier-retirement.md),
  [ADR-0038](../decisions/ADR-0038-fixed-lag-temporal-decoder-main-replay-parity.md),
  [ADR-0148](../decisions/ADR-0148-post-stabilizer-shared-secondary-fanout.md).
- Related docs: `docs/command-fix-execution.md`, `data/config/README.md`.
