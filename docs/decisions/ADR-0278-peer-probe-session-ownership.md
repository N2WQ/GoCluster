# ADR-0278: Peer Probe Session Ownership

- Status: Accepted
- Date: 2026-10-10
- Decision Origin: Troubleshooting chat

## Context

Approved v3 separates the diagnostic peer client's ownership, phase deadlines
and timestamp publication into checkpoints34a/b/c. The old tool ignored Telnet
reply errors, could start multiple keepers and did not join them before the next
connection. Process cancellation could leave socket I/O blocked. One-session
EOF and other failures could return a successful command status.

## Decision

- Keep one synchronous protocol/read owner per connection. Each attempt owns
  its child cancellation context, socket, buffered writer, one interruptor and
  at most one keeper/ticker. A writer mutex serializes full lines and raw Telnet
  replies; no output queue or shared peer API is introduced.
- Record the first causal failure before cancellation. Callback failure cannot
  dispatch input returned by the same read. Short writes and keeper failures
  reach the session owner.
- Guard ordinary read-deadline installation against cancellation. The short
  deadline gate never covers blocking I/O or worker joins. Cancellation expires
  socket deadlines without acquiring the writer mutex; setter failure closes
  the owned connection as a fallback.
- Keep absolute phase state with the synchronous protocol owner. Start the
  existing loginTimeout+initTimeout budget before credential writes and the
  first unestablished initTimeout budget before initialization output. Install
  each fixed write deadline through the cancellation gate, also covering raw
  Telnet replies. Preserve monotonic time values for elapsed budgets.
- Gate every read against the earlier active phase/per-ReadLine idle deadline,
  and reject successfully acquired or buffered input after phase expiry before
  dispatch. Preserve actual read failures. Repeated markers cannot renew the
  init budget. Either establishment path clears the phase write deadline through
  the same gate before keeper acquisition; later PC18 cannot reopen a phase.
- Keep the existing established per-ReadLine idle contract. Outgoing keepalive
  does not refresh inbound liveness, and established writes gain no new timeout
  domain. A zero ordinary write deadline cannot undo cancellation.
- Join the keeper and interruptor before best-effort BYE, socket retirement and
  reconnect. Only post-join BYE may restore a future write deadline; retain its
  two-second bound. Preserve the first cause and never replace a failed buffered
  writer to replay an uncertain partial frame.
- First PC22 starts at most one keeper with a captured mode. Preserve later
  PC18 compatibility responses, spot-only establishment and spot-to-first-PC22
  ownership. Preserve existing flags, defaults, frame formats and backoff.
- One-session failures, including EOF, exit nonzero. Deliberate local command
  cancellation exits zero; internal session cancellation is not sufficient.
- Keep test controls private and tool-local. The execution record separately
  qualifies34a ownership and34b phase deadlines. Checkpoint34c owns
  cross-reconnect timestamp publication; this decision does not certify tests.

## Alternatives considered

1. Close before joining: interrupts I/O but prevents healthy best-effort BYE.
2. Add a writer queue: adds publication acknowledgements, drain semantics and
   another owner without solving a necessary tool-local requirement.
3. Join while holding the writer mutex: can deadlock the blocked producer that
   cancellation must release first.
4. Add a phase timer/watch owner: introduces replacement, disarming and
   expiry-versus-establishment ownership, while buffered ingress still needs
   explicit protocol checks. Fixed connection deadlines bound phase writes
   using the existing interrupt/retirement owners.

## Consequences

### Benefits

- Failure and retirement ownership are explicit and locally falsifiable.
- A reconnect cannot inherit an old keeper or writer.

### Risks

- Best-effort BYE is not a delivery guarantee.
- Local source and fixture evidence cannot prove long-running runtime behavior.

### Operational impact

- Single-attempt automation can detect unsuccessful sessions.
- Periodic keeper errors retire the session and reconnect when enabled.

## Links

- Related issues/PRs/commits: approved v3 checkpoints34a/b on `fix_commands`.
- Related tests: `cmd/peerpoc` ownership, wire and actual-main fixtures.
- Related docs: [tool guide](../../cmd/peerpoc/README.md),
  [execution record](../command-fix-execution.md).
- Related TSRs: [TSR-0077](../troubleshooting/TSR-0077-peer-probe-retirement.md),
  [TSR-0037](../troubleshooting/TSR-0037-peer-session-cancellation-publication.md).
- Supersedes / superseded by: none; production peer-manager contracts remain
  owned by their existing decisions.
