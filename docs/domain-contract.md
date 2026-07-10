# docs/domain-contract.md

This document defines the domain and operational contract for the telnet/packet
DX cluster.

## System posture

This is a long-lived, line-oriented TCP service with high fan-out broadcast and
mixed control/data traffic. The system must stay bounded, predictable, and
operable under normal load, reconnect churn, malformed input, and slow-client
pressure.

## Protocol scope and telnet byte policy

- The protocol is line-oriented over TCP.
- Accept `\n` and `\r\n`.
- Telnet negotiation is not implied by the use of TCP.
- Default policy:
  - treat the service as a raw line protocol
  - if telnet negotiation bytes (`IAC`, `0xFF`) are observed, close the
    connection with a deterministic reason indicating telnet negotiation is
    unsupported
- If minimal telnet compatibility is intentionally added later:
  - strip or ignore IAC sequences deterministically
  - never allow telnet bytes to reach business handlers
  - do not implement partial/ad hoc telnet behavior

## Input parsing

- Use streaming parse with bounded per-connection buffers.
- Default bounds:
  - max line: 1024 bytes
  - max token: 64 bytes
- Handle partial reads correctly.
- Reject unexpected control characters per policy.
- Close on repeated abusive input.
- Do not retain subslices of shared read buffers when data must outlive the read
  cycle.

## Source identity normalization

- Peer PC11, PC61, and PC26 DE/spotter calls strip only a terminal `-#`
  skimmer marker before local ingest.
- DXSummit DE/spotter calls strip only a terminal `-#` skimmer marker before
  local ingest.
- Numeric SSIDs before the marker are preserved, so `N2WQ-1-#` becomes
  `N2WQ-1`.
- DXSummit `-@` provenance remains a separate accepted display/archive marker
  and must not be stripped by the `-#` rule.
- This normalization does not change DX call normalization, RBN/local skimmer
  ingest, peer hop suffix handling, source stats, queues, forwarding policy, or
  YAML/config behavior.

## Human/upstream telnet registry

- `human_telnet` is an ordered registry of zero to 64 complete entries; the
  historical single mapping is a one-entry compatibility form.
- Enabled entries connect and retry independently through the shared RBN
  telnet client lifecycle. One failed upstream must not delay or stop another.
- Each client keeps its own bounded spot queue. Individual `slot_buffer` values
  are `1..64000`; enabled entries have a combined maximum of `64000`.
- Config names are safe, case-preserving identifiers and case-insensitively
  unique. They become the spot `SourceNode` and console label
  `HUMAN/<name>`.
- Every enabled entry contributes one to the dashboard enabled count and only
  contributes to connected while its TCP generation is connected. Rows retain
  YAML order, are never collapsed into an aggregate Human row, and remain
  scroll-discoverable behind a ten-row visible pane bound.
- Human feeds preserve the existing minimal parser, `UPSTREAM` classification,
  shared dedupe/flood pipeline, and nonblocking raw announcement behavior.
- Runtime shutdown cancels every client, joins all spot producers, closes the
  shared raw channel once, and then joins the raw consumer.

## Output buffering

- Use bounded per-connection queues.
- Default shape:
  - control queue: 32 slots
  - spots queue: 256 slots or 256 KB effective bound
- One writer goroutine owns socket writes per connection.
- Writer owns queue drain semantics and flush strategy.
- Coalesce writes when practical.
- Apply explicit write deadlines and a stall timeout.
- Disconnect on sustained inability to write.

## Fan-out and backpressure

- Ingest must never block on per-client I/O.
- Fan-out must not create one goroutine per message.
- Broadcast should enqueue into per-connection queues and return promptly.
- Overload must first shed at slow connections before considering broader
  system-level shedding.
- Preserve per-connection ordering within a stream.
- Control traffic is prioritized over spot traffic.

## Drop and disconnect semantics

These rules must be explicit, deterministic, and testable.

### Spots queue full

- Drop the incoming spot.
- Do not evict older queued spots to make room.

### Control queue full

- Disconnect immediately.

### Duplicate bulletins

- Suppress identical WWV, WCY, and `TO ALL` announcement lines before they
  enter per-client control queues when the configured telnet bulletin dedupe
  window is enabled.
- The bulletin dedupe state must have both a time window and a hard cardinality
  cap.

### Prioritization

- Control messages drain before spots.
- Control traffic must not be starved by high-volume spot traffic.
- Peer liveness/config traffic must bypass the normal peer spot backlog.
- If the peer control-priority lane is full, close the peer session and rely on
  reconnect/backoff rather than silently dropping keepalives.

### Relay under overload

- Do not relay inbound peer spot data after the local ingest queue already
  dropped it.
- A node that is shedding inbound peer spots locally must not continue acting as
  a transit hop for those same frames.

### Sustained slow consumer policy

If the spot drop rate exceeds 5% over a rolling 30-second window, with a
minimum sample threshold to avoid noise:

- strict mode: disconnect the slow consumer
- lenient mode: keep connection alive and continue aggressive dropping

The chosen mode must be explicit in config, docs, and tests.

## p99 targets under nominal load

- ingest to enqueue: <= 5 ms p99
- ingest to first byte out: <= 25 ms p99 for healthy clients

Under overload:

- memory remains bounded
- shedding increases predictably
- no GC thrash spiral
- ingest still does not block on client I/O

## Operational readiness targets

- Per-connection buffers and queues are bounded.
- Global caches and worker counts are bounded.
- Goroutine count is bounded by O(connections) plus fixed workers.
- No known leak classes:
  - blocked goroutines
  - orphan timers/tickers
  - unbounded retries
  - unbounded logs
  - unbounded per-client state
- Observability is sufficient for production operation.

## Required observability

At minimum, the system should expose enough metrics/logging to answer:

- how many connections are active
- queue depths by class
- spot drops by reason
- disconnects by reason
- write stalls and stall durations
- ingress and egress rates
- latency histograms for enqueue and first-byte-out
- alloc rate and RSS trends
- slow-consumer incidents
- reconnect storms or churn indicators

## Security and robustness

- Enforce strict input bounds.
- Rate-limit commands per connection with bounded state.
- Truncate and rate-limit untrusted input in logs.
- Make abuse handling deterministic.
- Avoid logging raw untrusted payloads unless redacted/bounded.

## Graceful shutdown contract

The default shutdown sequence is:

1. stop accepting new connections
2. signal cancellation
3. stop or quiesce ingress producers as required
4. stop writers with bounded drain policy
5. drain or drop control traffic per explicit policy
6. close connections
7. verify goroutine/timer cleanup

Shutdown behavior must be explicit, bounded, and testable.

## Determinism requirement

Slow-client, overload, reconnect, parser-error, and shutdown behavior must be
deterministic from the operator's perspective. Differences between strict and
lenient modes must be explicit, documented, and test-covered.
