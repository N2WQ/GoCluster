# TSR-0077 - Peer Probe Retirement

Status: Monitoring
Date Opened: 2026-10-10
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: diagnostic peer client, phase budgets, timestamp publication and session outcomes
Trigger Source: Chat request
Led To ADR(s): ADR-0278
Tags: peerpoc, cancellation, deadlines, keepalive, retirement, timestamps

## RCA Summary
- What happened: callback errors could disappear, repeated PC22 could add
  keepers and cancellation did not own interruption or join session workers.
- Why: the process context and mutable keeper channel replaced explicit
  connection-level ownership and retirement; per-session timestamp state and
  allocation before the writer gate did not preserve origin publication order.
- What fixed it: one session owner, first-cause cancellation, guarded deadlines,
  serialized writers and joined retirement before BYE and reconnect; absolute
  phase deadlines cover credential/init output and every input category;
  command-owned timestamps are allocated with actual publication under the
  common writer gate and retained across retries.
- How we know: checkpoints34a/b/c in the execution record own qualification and
  record any failed or incomplete checks separately.
- Operator/support answer: inspect the first session failure; BYE is bounded
  best effort. One-session EOF is unsuccessful, while reconnect mode retries.

## Triggering Request
- Request date: 2026-10-10.
- Request summary: implement approved v3 peer-probe ownership, phase budgets and timestamp publication.
- Request reference: `docs/command-fix-scope-ledger-v3.md`, checkpoints34a/b/c.

## Symptoms and Impact
- A failed keeper could leave its reader apparently healthy.
- A repeated establishment marker could create duplicate workers.
- Stalled I/O could survive cancellation and obstruct retirement or reconnect.
- Automation could treat an unsuccessful single attempt as success.
- Credential/init writes could stall before timing began; repeated markers or
  recognized traffic could evade the handshake budget.
- Reconnect could reuse an origin's timestamps, and a competing producer could
  allocate a newer timestamp before an older record reached the wire.

## Hypotheses and Tests
1. Canceling a context or closing a socket proves all workers ended.
   - Evidence required: actual worker completion barriers before BYE/next dial.
   - Outcome: joins are part of retirement, not inferred from cancellation.
2. An expired deadline proves canceled I/O stays interrupted.
   - Evidence required: both setter orders and pre-canceled startup.
   - Outcome: ordinary future read/write deadlines or phase clearing cannot
     revive a canceled session.
3. One tick yielding one frame proves one keeper exists.
   - Evidence required: ticker creation/stop and completion ownership witnesses.
   - Outcome: duplicate workers can compete for one channel and appear correct.
4. A BYE helper call proves a BYE was delivered.
   - Evidence required: real wire receipt, failed-writer and withheld-read controls.
   - Outcome: sticky buffered-writer failure cannot be credited as delivery.
5. Socket deadlines alone prevent an expired marker from being dispatched.
   - Evidence required: actual bytes acquired before a delayed Read return,
     then release after the observed phase deadline.
   - Outcome: explicit post-successful-read gating is required for buffered input.
6. Traffic and repeated init responses demonstrate handshake progress.
   - Evidence required: fixed deadline identity under bounded prompts, WWV,
     raw negotiation and repeated markers, plus naturally stalled phase writes.
   - Outcome: phase budgets start before output and cannot be renewed by traffic.
7. A mutex inside the generator proves emitted records remain ordered.
   - Evidence required: blocked actual Flush with competing keeper/init output,
     plus allocation/failure publication ownership witnesses.
   - Outcome: allocation and actual publication need the common writer gate.
8. Sharing a generator between manually constructed sessions proves retry retention.
   - Evidence required: actual command runner, uncertain sent prefix and repeated
     rate/clock refusals across connections.
   - Outcome: the command owns retained state; failed sends may leave unreused gaps.

## Findings
- Root cause: connection-local cancellation, writer failures and worker lifetime
  lacked one complete retirement owner.
- Phase deadlines and cross-reconnect timestamp publication are separately
  qualified by34b/c; ownership qualification alone does not certify them.

## Decision Linkage
- ADR created/updated: ADR-0278.
- Contract changes: bounded joined retirement, surfaced causal failures,
  one-session unsuccessful status, absolute phase budgets, invocation-owned
  timestamp publication and preserved deliberate local cancellation.

## Verification and Monitoring
- Validation steps: checkpoints34a/b/c in `docs/command-fix-execution.md`.
- Signals: causal failure, actual wire frames, ticker/worker retirement and
  next-dial ordering.
- Rollback triggers: revived I/O, swallowed errors, duplicate keepers or an
  attempt admitted before the old session retires.
- Limits: local fixtures do not establish external-peer interoperability,
  firewall acceptance, production load or long-running leak freedom.

## References
- Issue(s): approved v3 checkpoints34a/b/c.
- PR(s): none.
- Commit(s): checkpoints34a/b/c on `fix_commands`; see execution history.
- Related ADR(s): [ADR-0278](../decisions/ADR-0278-peer-probe-session-ownership.md).
- Related docs: `cmd/peerpoc/README.md`, `peer/reader.go`, `peer/timestamp.go`.
