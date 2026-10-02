# ADR-0230: PC18/PC92 Authority, Recovery and Bounds

- Status: Accepted
- Date: 2026-10-01
- Decision Origin: Design

## Context

GoCluster's PC18/PC92 behavior needed a complete authority, ordering, membership,
failure and resource contract against a pinned DXSpider implementation. A shared
payload cache could let spot forwarding consume topology/message headroom.
Session-owned sequencing, incomplete publication and unbounded retained backing
could undermine interoperability even when individual records parsed correctly.
The user approved D1-D8/S01-S14, the v5-v7 capacity/qualification amendments,
and the v8 behavior-preserving shared-parser amendment.
Accepted design authority does not assert that implementation qualification has
finished; current evidence remains in the linked qualification record.

## Decision

V11 refinements are recorded in [ADR-0231](ADR-0231-pc92-audit-corrections.md).
They preserve this authority/resource contract while correcting wire identity,
typed membership, scheduling, deadline ownership and qualification evidence.

- Support PC92 A/C/D/K against DXSpider revision
  `3e9b3621d94dd45c68702e4a0f896aac33f2a91d`. Drop unsupported actions without
  topology/freshness effects. Preserve selected legacy and CCCluster startup;
  exclude CCCluster from transit PC92. No blanket protocol-family parity claim.
- Use one manager-owned controller for local timestamp allocation/publication,
  remote topology, shared PC92/PC93 freshness, failure gates and recovery.
  Authenticated candidate records remain bounded staging until establishment
  wins; first established ownership wins duplicate-session races.
- Derive local membership from actual current telnet owners, publish available
  IPs, keep ambiguous/unrepresentable identities local, and require one current
  owner for private PC93 delivery. IP preference commands remain out of scope.
- Publish complete C and then metadata A for establishment/recovery regardless
  of periodic C/K settings. Coalesce safe rate-limited changes; never fabricate
  `.100` timestamps, truncate complete membership, or silently drop required
  control output.
- Close/gate PC9x on publication overflow or unsafe clock. Close/gate affected
  authoritative-admission failures until their required resource recovers.
  Payload-class saturation refuses new untrackable work in that class while
  preserving unexpired keys and healthy sessions. Retain clock/freshness
  protection through failure; stability, observation and reconnect deadlines
  are explicit in the qualification contract.
- Separate spot, PC92, PC93 and bulletin caches. Payload expiry uses real
  elapsed time strictly beyond600 seconds; duplicates do not refresh it.
  Shared origin freshness is independently retained while live and at least
  1,800 seconds after acceptance, with safe-clock expiry.
- Enforce complete owned allocation budgets, including rounded strings, map
  capacity, queue backing, active staging/writes and overlapping projections.
  Keep the total480 MiB subsystem contract distinct from process RSS and the
  unchanged1536 MiB process GOMEMLIMIT. Configuration counts are upper limits;
  independent byte budgets can admit fewer records.
- Keep SQLite an asynchronous, bounded diagnostic projection. Failed or
  oversized generations do not mutate authority. Cancel/join workers before
  closing storage and release terminal staging/transport/projection ownership.
- PC18 truthfully describes the runtime GoCluster build independently of numeric
  protocol compatibility metadata. Retain5457/633 defaults and support explicit
  empty-build omission. Enabled invalid identity fails startup.
- Qualify declared Q1-Q6 profiles with complete delivery accounting, actual
  reference state, allocation proof and real-duration load/fault runs. V7
  isolates intentional holds for the tight-latency profile and separately
  measures shipped behavior. Tagged seams cannot grant protocol authority.

## Alternatives considered

1. Retain the shared cache and silently evict live entries: conflicts with the
   selected loop-suppression and independent-class contract.
2. Publish partial populations or keep clock-unsafe PC9x links open: rejected
   by the user's complete-publication and close/gate policy selections.
3. Persist routing authority or perform SQLite work on the protocol owner:
   complicates freshness and makes slow storage part of the receive path.
4. Change shipped stabilization/batching to satisfy a benchmark: outside this
   protocol scope; user selected isolated profiles and a shipped-policy repeat.

## Consequences

### Benefits

Ordering, membership, lifecycle and failure ownership become testable contracts.
Separate budgets contain overload by traffic class. Receiver state and complete
delivery accounting expose failures that parser round trips can miss.

### Risks

Conservative complete-allocation admission can refuse work before logical
cardinality limits. Complete-publication and clock failures intentionally
disconnect PC9x links. Optional database diagnostics may lag or remain stale.
Maximum workload and whole-allocation claims require the pending qualification;
source inspection and short tests cannot replace it.

Enabled SQLite persistence still needs a complete owned-allocation proof.
The approved isolated v9 feasibility experiment rejected its proposed driver
candidate under the permitted repair boundary. Production modernc SQLite and
this diagnostic-projection decision remain unchanged; v9 does not establish
the 480 MiB aggregate bound.

### Operational impact

Operators should inspect the specific capacity/clock/projection diagnostic and
the effective private config. Reconnection cannot cure the underlying gate.
Available IPs are published, recovery works with disabled periodic timers, and
SQLite contents never make a restarted node's routing knowledge authoritative.

## Links

- Related issues/PRs/commits: approved work on `p92`; no deployment authorization.
- Related tests: `peer/pc92_*test.go`, `peer/pc18_identity_test.go`,
  `peer/session_lifecycle_test.go`, `telnet/peer_membership_test.go`, runtime qualification.
- Related docs: [approved v6](../pc18-pc92-scope-ledger-v6.md),
  [approved v7](../pc18-pc92-scope-ledger-v7.md),
  [approved v8](../pc18-pc92-scope-ledger-v8.md),
  [v9 persistence evidence](../pc92-persistence-feasibility-v9.md),
  [qualification](../pc92-qualification.md),
  [operator behavior](../../peer/README.md).
- Related TSRs: [TSR-0035](../troubleshooting/TSR-0035-pc92-qualification-accounting.md).
- Supersedes / superseded by: Refines ADR-0050's PC92 application ordering and
  ADR-0054's queue/resource contract. Their hop-insensitive keys, overlong
  diagnostics, priority and local-ingest relay rules remain effective.
