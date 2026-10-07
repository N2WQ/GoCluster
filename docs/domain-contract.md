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

- Validated peer PC11, PC61, and PC26 DE/spotter calls strip only a terminal
  `-#` skimmer marker for local ingest. Their onward peer payload retains the
  original spotter spelling.
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

### PC18/PC92 authority and recovery

- PC18 uses truthful runtime product/build identity independently of numeric
  compatibility metadata. Invalid enabled identity fails startup.
- PC92 supports A/C/D/K. Complete C is atomic; unsupported F/R and unknown
  actions grant neither topology nor freshness. CCCluster receives direct local
  publication but is excluded from transit PC92 broadcasts.
- One manager owner orders local timestamps/publication and remote authority.
  Startup PC92 remains staged until establishment wins. Actual current telnet
  owners define local membership; available IP addresses are published.
- Publication overflow and unsafe clock close/gate PC9x while local users and
  established legacy links continue. Authoritative admission failure closes and
  fences replacement until controller ingress invalidation, then permits controlled
  retries with shared configured backoff and fair one-second startup pacing.
- Recovery requires complete C followed by metadata A, including when periodic
  C and K are disabled. Local recovery does not make incomplete remote state
  complete; that requires an authoritative remote C. The C/A pair uses one
  immutable baseline. Stable eligible membership changes require control-queue
  admission within one second for every healthy established peer, including
  recovery; the five-second recovery allowance does not extend this deadline.
- Node/user relationships use separate typed identities even with the same
  callsign. Local wire identity follows the pinned receiver normalization;
  ambiguous or unrepresentable human logins stay local.
- Received PC92 calls pass raw grammar before stable canonicalization: no
  origin repair; only entry-call trailing ASCII spaces are tolerated. Malformed
  records reject atomically. K subject numeric omission/zero replaces old
  version/build with literal zero, preserving separate A/C/D and IP rules.
- Admission retries have one candidate per configured identity, authentication
  before ownership, unchanged absolute deadlines, and one terminal outcome per
  attempt. History resets after matching C/A local Flush and establishment/replay
  plus60 uninterrupted healthy seconds. Global closure interrupts this interval
  without advancing cooldown. Refused records are never retained/replayed.
- Required integer `peering.max_peers` is1–64 (shipped64), with no fallback.
  Active enabled identities must fit N; direct construction validates before
  resources. Pending128 is unchanged; transport ownership is N+128. Complete
  publication reserves actual enabled identities, not phantom population N.
- Separate spot/PC92/PC93/bulletin pools never evict unexpired payload keys.
  Exactly 600 elapsed seconds is unexpired. PC92 and PC93 share origin freshness;
  unsafe UTC does not erase retained ordering protection.
- The full resource/workload contract is in [PC92 qualification](pc92-qualification.md).
  Include backing capacity, rounding and active generations in allocation
  accounting. Optional SQLite projection is never live authority.

### Relay under overload

- Validate original PC11, PC61 and PC26 sentences and fields before local
  normalization/fallback, queue handoff or either primary/peer dedupe admission.
  Malformed originals must not be displayed, archived as accepted spots or
  relayed, including when forwarding is disabled.
- Keep local corrections separate from transit content. Relay original
  callsigns, comments, mode-bearing text, frequency, timestamp and origin;
  only hop decrement, required framing and legacy PC61-to-PC11 conversion may
  change the transmitted sentence. PC11 remains PC11 for modern peers; PC26
  retains its destination restrictions and legitimate no-hop local form.
- Check original spot calls using the agreed broader GoCluster call syntax,
  not PC92's narrower wire-identity rule. Original comments reject bytes
  `0x00-0x08`, `0x0A-0x1F`, `0x80-0x9F` and literal `0xFF`. Tabs and nonempty
  whitespace-only comments remain valid. The rule applies to individual bytes
  even inside valid UTF-8; native Telnet negotiation does not imply support
  for literal IAC in spot content.
- Accept exactly 11-byte original UTC dates with either a two-digit day or
  DXSpider's one-ASCII-space-padded day 1-9 (`04-Oct-2026` / ` 4-Oct-2026`).
  Retain real-calendar validation, English month-case tolerance, four-digit
  years and strict `HHMMZ`; reject unpadded days and arbitrary padding.
  Assign the validated instant to the local `Spot.Time` before dedupe-key or
  ingest-age checks. Preserve original date bytes in onward fields.
- Own compact field copies before normalization-cache access; capture the peer
  key and original timestamp before local handoff. Relay must never read the
  handed-off mutable `Spot`.
- Keep the existing `forward_spots`, hop, age, duplicate, source-exclusion and
  queue gates. Valid H0/H1 inputs remain locally eligible. PC11/PC61 retain
  their second original-timestamp age check; PC26's age-check behavior remains.
- Emit `^Hn^~` plus writer CRLF and refuse each oversized output variant without
  truncation. Retain the 65,536-byte writer sentence maximum, configured reader
  limits, parse/queue/cache budgets, existing dedupe identities and expiry.
  Detailed field and framing grammar is in [peer behavior](../peer/README.md#original-spot-admission-and-relay)
  and [ADR-0239](decisions/ADR-0239-peer-original-validation-and-relay.md),
  refined for original dates by
  [ADR-0240](decisions/ADR-0240-peer-dxspider-date-admission.md) and for payload
  encoding/comment framing by
  [ADR-0241](decisions/ADR-0241-peer-frame-payload-and-comment-framing.md).
- Treat `~` within PC11/PC61/PC26's comment field as payload in the reader,
  parser and original validator. Recognize terminal markers outside that field
  and retain the same distinction while discarding overflowing input, including
  fragmented headers. CR/LF and other PC families retain their existing ending
  behavior. No frame, queue or scratch limit is raised.
- Accept plain RFC 4291 IPv6 addresses under the existing `netip.ParseAddr`
  rule, including compressed and embedded-IPv4 forms, without zones, prefixes,
  ports or surrounding padding. Preserve incoming IP text during relay.
  Selected broader callsign/IP policies remain different from the pinned
  DXSpider receiver; admission and byte fidelity do not imply its acceptance.
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

## Registered State And Province Metadata

- FCC and ISED registered address codes are factual reference metadata,
  independent of admission enforcement. Disabling either source's enforcement
  keeps refreshes and enrichment active.
- DE State belongs to central ingest; DX State belongs to the final corrected
  callsign, including delayed delivery. Base-call CTY jurisdiction selects the
  source. Canadian coverage is ADIF 1, 211 and 252; US login remains ADIF 291.
- ISED uses club province when club information exists, otherwise personal
  province. Exact active special calls use listed ISED trustee evidence; active
  prefix substitutions use assigned ordinary base calls. Blank, invalid or
  conflicting evidence stays unknown without removing a valid assignment.
- Event dates are inclusive UTC days. Prefix admission means callsign
  plausibility, without proving residency, membership or event eligibility.
  Unsupported or ambiguous event evidence fails open for affected candidates.
- Each registry owns its database, refresh state and generation. Both ISED
  archives publish as one completed projection with their exact hash pair.
  Failed refreshes retain last-good data. The aggregate cache is capped at
  200,000 entries with the existing FCC TTL; Canadian facts also revalidate at
  UTC midnight. Old generation/date queries cannot publish stale facts.
  Unavailable facts are never cached.
- State accepts 60 FCC and 13 Canadian codes. Named PASS lists exclude unknown
  State; named REJECT lists admit it. NEARBY suspends/restores these rules.
- Archive version 6 records both observed States. Versions 2-5 remain unknown;
  history never consults today's registry to hydrate old records.
- Machine YAML schema 1 preserves its shape and hidden State rules; explicit
  schema 2 exposes the same State fields. Disk version 2 keeps its layout with
  73-key State bounds. Older binaries can reject Canadian codes; downgrade uses
  matching backups.

See [ADR-0253](decisions/ADR-0253-fcc-state-enrichment-and-filtering.md) and
[ADR-0254](decisions/ADR-0254-canadian-ised-license-and-state-reuse.md).
