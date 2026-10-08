# ADR-0260: CCCluster Outbound Handshake

- Status: Accepted
- Date: 2026-10-08
- Decision Origin: Troubleshooting chat

## Context

An authorized live CCCluster wire session exposed remote PC20 and startup PC51
requests before normal traffic. GoCluster's outbound handler required PC22 and
only serviced pings after establishment. The pinned DXSpider PC20 handler sends
fresh local configuration, sends PC22 and enters normal operation. A corrected
wire simulation subsequently received remote PC22 and live spots. Source and
simulation evidence are distinct from a deployed cluster result.

The owner approved this bounded correction as Approved v1. Existing authority,
resource, retry and deadline contracts remain controlling.

## Decision

- After local initialization, configured outbound `ccluster` sessions answer
  remote PC20 with fresh negotiated configuration then PC22, and complete the
  handshake. PC9x configuration is A then K; legacy configuration is PC19.
  Premature PC20 cannot complete startup. Existing PC22 completion remains.
- Preserve outbound DXSpider's PC22 requirement, existing inbound family paths
  and bannerless startup eligibility. Local `initSent` is progress, not proof
  of remote identity. Add no diagnostic outbound PC18.
- Service PC51 requests during either handshake direction with existing
  destination rules and bounded priority output. Propagate output failure.
  Ping replies do not establish authority or extend fixed phase deadlines.
- Keep separate bounded candidate progress for initial A/K and CC response A/K
  on the existing controller owner. If timestamp capacity is exhausted after A,
  resume at K without duplicating A. No new owner or unbounded retry queue.
- Preserve staged remote topology and the single establishment winner. Repeated
  completion markers must not cause double registration or startup loops.
  After establishment, complete C then metadata A remains mandatory; startup
  A/K is not recovery or confirmation of remote processing.

## Alternatives considered

1. Continue requiring PC22 from CCCluster: conflicts with the observed exchange
   and the pinned reference's response to PC20.
2. Complete on every PC20 regardless of family or startup state: weakens
   DXSpider sequencing and accepts premature completion.
3. Restart A/K whenever K lacks timestamp capacity: duplicates A and consumes
   additional sequence capacity rather than preserving bounded progress.
4. Extend handshake time on ping traffic: lets a responsive but unfinished peer
   retain a candidate beyond its original deadline.

## Consequences

### Benefits

CCCluster receives its configuration response and startup keepalive service.
The family boundary, exact response order and partial-publication retry progress
are directly testable without changing runtime configuration.

### Risks

One bounded live session cannot establish complete CCCluster interoperability or
sustained resource qualification. Existing admission, clock, snapshot and queue
gates may still close a peer even when its protocol exchange is correct.

### Operational impact

Operators must distinguish remote duplicate-stream login rejection from
configuration exchange failure. Inspect the configured peer family and deployed
version; a reachable socket or banner alone does not prove establishment.
No deployment or runtime configuration change is authorized by this record.

## Links

- Related issues/PRs/commits: owner troubleshooting chat, Approved v1.
- Related tests: `peer/cc_handshake_test.go`, `peer/cc_response_retry_test.go`,
  `peer/cc_live_test.go`, peer handshake and pinned DXSpider interoperability
  regression tests.
- Related docs: [Peer behavior](../../peer/README.md#control-plane),
  [domain contract](../domain-contract.md#pc18pc92-authority-and-recovery).
- Related TSRs: [TSR-0045](../troubleshooting/TSR-0045-cccluster-outbound-handshake.md).
- Reference: [DXSpider DXProtHandle.pm](https://github.com/EA3CV/dxspider/blob/3e9b3621d94dd45c68702e4a0f896aac33f2a91d/perl/DXProtHandle.pm),
  PC20 and PC51 handlers, revision `3e9b3621d94dd45c68702e4a0f896aac33f2a91d`.
- Supersedes / superseded by: refines outbound CC startup in
  [ADR-0230](ADR-0230-pc18-pc92-authority-and-bounds.md) and handshake
  retry progress in [ADR-0231](ADR-0231-pc92-audit-corrections.md). Their authority,
  resource, fixed-deadline and C/A recovery clauses remain effective.
