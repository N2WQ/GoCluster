# ADR-0054 - Peering Control-Plane Priority and Local-Acceptance Relay Gate

Status: Accepted
Date: 2026-03-07
Decision Makers: Core maintainers
Technical Area: peer
Decision Origin: Design
Troubleshooting Record(s): none
Tags: peering, overload contract, reconnect semantics

## Context
- ADR-0053 defined receive-only versus forwarding behavior, but it did not define what happens when forwarding is enabled and the node is overloaded.
- Peer keepalive/config traffic (`PC51`, `PC92 K`, `PC92 C`) previously used the same normal write queue as spot traffic. Under a large outbound spot backlog, those control frames could be dropped while the session otherwise remained up, causing remote idle expiry or topology drift without a deterministic local signal.
- Inbound peer relay previously continued even when the local ingest queue had already dropped the same spot. That hid local loss and let an overloaded node behave as a transit hop for traffic it was no longer processing itself.
- The change must keep bounded-resource behavior intact: no new queues, no new knobs, and no blocking on peer socket read paths.

## Decision
- Later refinement: [ADR-0230](ADR-0230-pc18-pc92-authority-and-bounds.md)
  specifies physical queue budgets, ownership and clock/publication/admission
  gates. Control priority and the local-ingest relay condition remain effective.
- Later refinement: [ADR-0239](ADR-0239-peer-original-validation-and-relay.md)
  requires original PC11/PC61/PC26 validation before local handoff and dedupe,
  then separates locally corrected spots from original-field relay. The local
  queue acceptance gate and control-priority decision remain effective.
- Treat peer control-plane traffic as health-critical:
  - enqueue periodic `PC51`, `PC92 K`, and `PC92 C` on the existing priority lane instead of the normal write queue
  - if the priority lane is full, close the session immediately and let the existing reconnect/backoff path re-establish a healthy channel
- Treat inbound relay as conditional on local acceptance:
  - relay inbound `PC11`, `PC61`, and `PC26` only after the local ingest queue accepted the parsed spot
  - if local ingest sheds the spot, do not propagate it onward
- Keep current resource bounds and configuration surface unchanged.

## Alternatives Considered
1. Leave keepalive/config traffic on the normal queue
   - Pros:
     - No behavior change under overload.
   - Cons:
     - Silent keepalive loss remains possible, so sessions can drift into remote expiry without a deterministic local failure.
2. Drop priority-lane control frames silently when the lane is full
   - Pros:
     - Avoids reconnect churn.
   - Cons:
     - Preserves ambiguous session health and undermines the point of prioritizing liveness traffic.
3. Continue relaying inbound frames even when local ingest drops them
   - Pros:
     - Maximum downstream propagation during local overload.
   - Cons:
     - Hides local loss and turns an overloaded node into an implicit transit hop.

## Consequences
- Positive outcomes:
  - Peer sessions either deliver liveness/control traffic on time or fail fast and reconnect.
  - Overload behavior becomes deterministic and operator-visible instead of silent.
  - Nodes no longer relay peer spot traffic they have already dropped locally.
- Negative outcomes / risks:
  - Heavy sustained outbound pressure can cause more reconnects than the previous silent-drop behavior.
  - Downstream propagation may reduce during local ingest overload because transit relay now stops with local shed.
- Operational impact:
  - Operators should expect reconnect/backoff behavior rather than latent idle expiry when peer control priority saturates.
  - Inbound peer spots still ingest locally when capacity exists; receive-only mode from ADR-0053 is unchanged.
- Follow-up work required:
  - Monitor whether reconnect frequency under legitimate peak load suggests a future need for more peer write observability.

## Validation
- Added/updated tests:
  - `peer/session_keepalive_test.go`
  - `peer/session_ping_test.go`
  - `peer/manager_test.go`
- Validation commands:
  - `go test ./peer -run "TestKeepalive|TestPriorityLaneSaturation|TestHandlePingUsesPriorityQueue|TestInboundSpot"`
  - `go test ./...`
  - `go vet ./...`
  - `staticcheck ./...`
  - `go test -race ./...`

## Rollout and Reversal
- Rollout plan:
  - Deploy normally; no config migration is required.
- Backward compatibility impact:
  - With `peering.forward_spots=true`, transit relay now stops when local ingest drops the spot.
  - Peer sessions under control-lane saturation now disconnect and reconnect instead of silently missing keepalives/config refreshes.
- Reversal plan:
  - Restore normal-queue keepalives/config refresh and unconditional relay-after-parse behavior, then mark this ADR superseded.

## References
- Issue(s): none
- PR(s): none
- Commit(s): pending
- Related ADR(s): ADR-0049, ADR-0050, ADR-0053
- Troubleshooting Record(s): none
- Docs: `README.md`, `data/config/peering.yaml`, `docs/domain-contract.md`
