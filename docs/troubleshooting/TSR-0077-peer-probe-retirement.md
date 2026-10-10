# TSR-0077 - Peer Probe Retirement

Status: Monitoring
Date Opened: 2026-10-10
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: diagnostic peer client, session ownership and unsuccessful outcomes
Trigger Source: Chat request
Led To ADR(s): ADR-0278
Tags: peerpoc, cancellation, deadlines, keepalive, retirement

## RCA Summary
- What happened: callback errors could disappear, repeated PC22 could add
  keepers and cancellation did not own interruption or join session workers.
- Why: the process context and mutable keeper channel replaced explicit
  connection-level ownership and retirement.
- What fixed it: one session owner, first-cause cancellation, guarded deadlines,
  serialized writers and joined retirement before BYE and reconnect.
- How we know: checkpoint34a in the execution record owns qualification and
  records any failed or incomplete checks separately.
- Operator/support answer: inspect the first session failure; BYE is bounded
  best effort. One-session EOF is unsuccessful, while reconnect mode retries.

## Triggering Request
- Request date: 2026-10-10.
- Request summary: implement approved v3 peer-probe ownership checkpoint.
- Request reference: `docs/command-fix-scope-ledger-v3.md`, checkpoint34a.

## Symptoms and Impact
- A failed keeper could leave its reader apparently healthy.
- A repeated establishment marker could create duplicate workers.
- Stalled I/O could survive cancellation and obstruct retirement or reconnect.
- Automation could treat an unsuccessful single attempt as success.

## Hypotheses and Tests
1. Canceling a context or closing a socket proves all workers ended.
   - Evidence required: actual worker completion barriers before BYE/next dial.
   - Outcome: joins are part of retirement, not inferred from cancellation.
2. An expired deadline proves canceled I/O stays interrupted.
   - Evidence required: both setter orders and pre-canceled startup.
   - Outcome: ordinary future read deadlines cannot revive a canceled session.
3. One tick yielding one frame proves one keeper exists.
   - Evidence required: ticker creation/stop and completion ownership witnesses.
   - Outcome: duplicate workers can compete for one channel and appear correct.
4. A BYE helper call proves a BYE was delivered.
   - Evidence required: real wire receipt, failed-writer and withheld-read controls.
   - Outcome: sticky buffered-writer failure cannot be credited as delivery.

## Findings
- Root cause: connection-local cancellation, writer failures and worker lifetime
  lacked one complete retirement owner.
- Phase deadline and cross-reconnect timestamp defects remain separate approved
  checkpoints; ownership qualification does not certify them.

## Decision Linkage
- ADR created/updated: ADR-0278.
- Contract changes: bounded joined retirement, surfaced causal failures,
  one-session unsuccessful status and preserved deliberate local cancellation.

## Verification and Monitoring
- Validation steps: checkpoint34a in `docs/command-fix-execution.md`.
- Signals: causal failure, actual wire frames, ticker/worker retirement and
  next-dial ordering.
- Rollback triggers: revived I/O, swallowed errors, duplicate keepers or an
  attempt admitted before the old session retires.
- Limits: local fixtures do not establish external-peer interoperability,
  firewall acceptance, production load or long-running leak freedom.

## References
- Issue(s): approved v3 checkpoint34a.
- PR(s): none.
- Commit(s): checkpoint34a on `fix_commands`; see execution history.
- Related ADR(s): [ADR-0278](../decisions/ADR-0278-peer-probe-session-ownership.md).
- Related docs: `cmd/peerpoc/README.md`, `peer/reader.go`, `peer/timestamp.go`.
