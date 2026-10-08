# ADR-0231: PC92 Audit Corrections and Deadline Ownership

- Status: Accepted
- Date: 2026-10-01
- Decision Origin: Troubleshooting chat

## Context

The audit of `0d8a728` falsified several implementation and qualification
assumptions behind ADR-0230. The owner approved v11 after explicitly selecting
one-second membership queue admission even during recovery. The existing
resource partitions, reference revision and product policies remain controlling.

## Decision

- Normalize wire identities with pinned DXSpider receiver rules and require
  stable valid canonical identities in both loader and direct construction.
  Literal inbound authentication and local-only ambiguous users remain intact.
- Replace ADR-0050's generic trailing-token stripping for PC92/PC93 with
  grammar-aware hop extraction. Preserve positional payload, empty fields and
  optional K revision; dedupe hashes the complete payload, excluding transport
  hop only. Collapse only unambiguous hop stacks.
- Key graph membership by call and node/user kind. Prepare complete C before
  mutation, then traverse removals without retaining an unbounded removal list.
  Store receiver-compatible metadata and keep original transit payload.
- Add `peer_pc92_typed_edges` atomically and leave old edge rows historical.
  Replace current nodes and typed edges together; never restore live authority.
- Keep alternate-ingress observations atomic and tied to the original payload
  admission age. A duplicate never refreshes topology, watermark or liveness.
- Carry fixed monotonic handshake deadlines through controller queues and
  startup retries. Establishment has one expiry/commit winner. Bounded staged
  replay keeps the live reader parked and retains the batch reservation until
  retirement. Local recovery is scheduled at commitment.
- Service lifecycle, input and staged replay in bounded rotating turns on the
  existing owner. Reserve legal timestamp capacity for ordinary D/A and local
  recovery C/A; group identical fanout. An admitted C keeps its immutable A
  baseline before subsequent catch-up. Every healthy established recipient,
  including one recovering, remains subject to one-second queue admission from
  the eligible change. Five-second recovery never extends that obligation.
- Diagnose unsafe clock within the maximum five-second external deadline;
  require continuously advancing safe UTC for one second before reopening.
  Keep local users and established legacy links available while PC9x is gated.
- Expose PC93 mailbox refusals separately from cache refusals. Restore named
  announcements while preventing invalid private destinations from broadcasting.
- Qualification uses independently applied faults, externally timed outcomes,
  retained binaries and before/build/after source manifests. Only the wrapper's
  final report can qualify a run; missing evidence or changed inputs fails it.

## Alternatives considered

1. Restart C on every membership revision: can indefinitely postpone the
   matching metadata A under churn and violate the one-second deadline.
2. Defer the membership obligation for a recovering peer: explicitly rejected
   by the owner; queue admission and receiver processing are distinct promises.
3. Keep one key per callsign or rebuild old untyped rows: loses relationships
   accepted by the receiver and obscures rollback diagnostics.
4. Replace persistence or increase budgets: outside v11; the complete SQLite,
   context backing and retirement proofs remain open.

## Consequences

### Benefits

Receiver state tests distinguish wire compliance from parser self-consistency.
Lifecycle and publication milestones have explicit deadlines and ownership.
Qualification artifacts cannot become authoritative before all wrapper checks.

### Risks

Conservative pacing may defer startup requests within their fixed allowance.
Individual graph transactions and consistent snapshots remain indivisible;
combined-load evidence is required. Changed-owner arithmetic does not establish
whole-system capacity or enabled SQLite memory compliance.

### Operational impact

Peer identities may normalize differently from human login strings. Invalid
active constructor configuration fails before storage opens. Named groups can
reach local announcement delivery. Old `peer_pc92_edges` rows are historical;
current diagnostic membership is `peer_pc92_typed_edges`. Reconnect backoff
returns to its normalized nonzero base after successful establishment.

## Links

- Related issues/PRs/commits: approved v11 on `p92`, baseline `0d8a728`; no deployment.
- Related tests: `peer/pc92_*test.go`, `peer/session_*test.go`, wrapper contract fixtures.
- Related docs: [v11 ledger](../pc18-pc92-scope-ledger-v11.md),
  [allocation proof](../pc92-allocation-proof.md), [qualification](../pc92-qualification.md).
- Related TSRs: [TSR-0035](../troubleshooting/TSR-0035-pc92-qualification-accounting.md).
- Supersedes / superseded by: supersedes ADR-0050 generic hop stripping for
  PC92/PC93; refines ADR-0230 identity, typed storage, scheduling and evidence.
  Other ADR-0050/0230 requirements remain effective.
  [ADR-0232](ADR-0232-pc92-wire-and-recovery-evidence.md) refines the raw wire
  identity and admission recovery/evidence clauses after the2c06079 re-audit.

Admission-recovery clauses are superseded by [ADR-0233](ADR-0233-pc92-controlled-retries-and-peer-cap.md) under Approved v14. Other clauses and historical evidence remain effective.

[ADR-0260](ADR-0260-cccluster-outbound-handshake.md) refines outbound CCCluster
startup and bounded handshake publication retry progress. The authority, fixed
deadline, resource and mandatory C/A recovery contracts remain effective.
