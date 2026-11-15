# ADR-0354: Peer Flood Verdict And Retirement

- Status: Accepted
- Date: 2026-11-15
- Decision Origin: Design

## Context

Original peer relay followed initial local enqueue, before shared validation
and flood evaluation. The owner selected one PEER charge/verdict for both
outputs, independent maintenance and unchanged duplicate charging.

## Decision

A peer-owned FIFO is consumed by the existing single validation owner. Reserve
at most 64 outstanding builders/queued/active jobs together and 8 MiB conservative
backing before cloning/parsing. Include fixed descriptors, immutable originals,
pre-primary Spot backing, identity tokens and the active relay's two maximum
output variants. Parser/destination budgets remain separate; this is not RSS.

Eligible jobs invoke synchronized flood evaluation and nonblocking primary
admission once, outside the ordinary flood queue. Release flood locks before
admission/relay. Suppress/drop and ingress/primary pressure stop both outputs.
Allow/observe retains original hop/age/forwarding/dedupe/destination/size gates.
Completed CTY/license rejection retains existing transit without a flood charge;
cancellation/closure is distinct and never permits relay. Copies reaching flood
retain their current charges.

Capture originals before mutable ownership transfer. Jobs retain immutable
nonzero session identity tokens, not sessions/sockets. Replacement has a fresh
token; disconnect does not revoke admitted work. Nil source excludes nobody.

Close refuses reservations, drains ready items, cancels active validation and
joins builders/active work outside owner locks. Manager begins closure before
withdrawal and joins after canceling readers. Retire peer admission before
primary/output. Join cancellable validation before metadata/license teardown;
cold license lookups retain their existing five-second timeout.

## Alternatives considered

1. Synchronous reader validation/flood: changes charging order or blocks control.
2. New envelopes for every source: unnecessarily broad shared API migration.
3. Generic Spot callbacks or pointer/ID side maps: avoidable retention coupling.

## Consequences

### Benefits

- One verdict controls local admission and eligible original relay.
- Maintenance does not wait for spot validation or flood completion.
- Pending work has explicit count/backing and retirement ownership.

### Risks

- The internal guard may shed in observe mode while a cold dependency blocks.
- Coordinating relay adds a handoff and queue delay. Local component benchmarks
  establish neither production throughput nor p99.
- Reported origin is distinct from transport; no transport rate limiter is added.

### Operational impact

`SpotIngress.Usage()` exposes count/conservative backing; `Refusals()` counts
refused reservations. Bounded `peer_ingress_refused` diagnostics name the guard.
Shipped numbers/actions remain unchanged; observe does not disable pressure caps.

## Links

- Tests: `peer/spot_ingress_test.go`,
  `internal/cluster/peer_flood_delivery_test.go`, `floodcontrol/direct_admission_test.go`.
- Docs: `peer/README.md`, `docs/flood-control-v2-execution.md`.
- Supersedes ADR-0239's initial-enqueue-only relay boundary; preserves its original
  content/encoding contracts. Refines ADR-0062 peer delivery scope.
