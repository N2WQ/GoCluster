---
title: Domain And Operational Contract
description: Runtime, protocol and operational invariants for GoCluster.
---

This document defines the domain and operational contract for the telnet/packet
DX cluster.

## System posture

This is a long-lived, line-oriented TCP service with high fan-out broadcast and
mixed control/data traffic. The system must stay bounded, predictable, and
operable under normal load, reconnect churn, malformed input, and slow-client
pressure.

## Protocol scope and telnet byte policy

- The protocol is line-oriented over TCP.
- Human lines accept LF, CRLF, CR-NUL and CR followed by the next line's data.
- The configured default `ziutek` transport decodes Telnet negotiation. The
  `native` transport strips supported IAC sequences in the input reader.
- Negotiation and escaped IAC bytes never reach business handlers. Newlines
  inside Telnet subnegotiation do not end application lines.

## Input parsing

- After existing login callsign normalization, local user admission permits at
  most one final hyphen and one or two ASCII SSID digits (`0` through `99`).
  Rejection occurs before CTY/license lookup and profile restoration; accepted
  full normalized spellings, including leading zeroes, retain separate records.
- Existing stored profiles for rejected logins are not migrated or deleted.
  Spot, peer and shared callsign validation contracts are unchanged.
- Use streaming parse with bounded per-connection buffers.
- Default human bounds are 32 bytes for login and 128 bytes for commands;
  configured line limits apply before dispatch.
- Handle partial reads correctly.
- Reject unexpected control characters per policy.
- Reject an invalid or overlong human physical line with zero command effects.
  Discard its tail without editing or echo, through a safe data line boundary.
- Recovery spends at most 64 KiB of wire data and five seconds, respecting any
  earlier installed read deadline. Count buffered data and codec negotiation;
  conservative codec-buffer reservation may disconnect earlier near the limit.
- Disconnect if recovery fails or a partial human read is interrupted. Editing
  cannot erase physical progress. An empty logged-in idle timeout is retryable.
- Machine headers/uploads retain their separate framing and terminal rules.
- Do not retain subslices of shared read buffers when data must outlive the read
  cycle.

## Telnet command pacing

- Pace post-login command attempts per session with an initially full token
  bucket. The shipped rate is 5 per second with burst 30. Runtime YAML requires
  integer `telnet.command_rate_per_second` in 0..1,000,000,000 and integer
  `telnet.command_burst` in 1..1,000,000,000. A zero rate disables pacing.
- Count each completed line, including blank keepalives, unknown commands and
  machine headers, and each safely recovered invalid human line. Do not count
  login input, Telnet negotiation, idle timeouts or framed YAML body lines.
  Process attempts in order after a token is available. The limiter delays
  excess attempts; it does not reject them or disconnect for their rate.
- Preserve immediate terminal handling for reader-level unsafe failures. A
  complete malformed machine header is paced before its existing dispatch and
  terminal rules. Preserve whole-line recovery, machine framing, and no
  dispatch of a rejected line or machine body as another command.
- At most one attempt is read ahead, in addition to existing bounded transport
  buffers. Reader echo, validation, recovery and their existing logs precede
  pacing; pacing is not byte-flood protection. Shutdown and session replacement
  cancel waits. A retired reader leaves its pending attempt unprocessed and
  waits for the existing duplicate-notice writer to close or for shutdown.
  Replacement is not atomic with a command already past its final check. A peer
  EOF may not be observed until the next read or writer
  failure. A fresh connection starts a fresh burst; sessions do not share rates.
- `Command pacing waits` is the cumulative count of attempts that enter a
  pacing wait, including waits later cancelled, reset on server restart. It is
  not a count of users, rejected commands or total wait time. The shipped values
  have not been calibrated against production startup or contest workloads.

## Telnet session capacity

- An admitted connection has one pre-login time budget from listener accept
  through its registration decision. After admission, timeout or shutdown
  interrupts socket I/O, subject to scheduling and prior admission delay;
  the timeout worker is retired before registration and cannot close the new
  owner later. Each worker and timer is bounded by a pre-login ticket and joins
  before its ticket is released. Admission rejection notices keep their separate
  bounded send period.
- Synchronous admission, lock and profile operations cannot be forcibly canceled.
  An operation already underway may complete after expiry, including profile
  writes. An expired login cannot register or replace the previous owner;
  reservations and tickets remain held until safe cleanup. The timeout counter
  counts an expired pre-login attempt once, including expiry before input or
  during handoff; shutdown and unrelated disconnects are not timeouts.
- The final registration ownership change is ordered with the shutdown signal.
  If shutdown is signaled first, a waiting reconnect cannot replace its owner.
  If replacement wins first, both the current and displaced session can be
  interrupted on shutdown even when a replacement notification callback is
  blocked. A writer already in socket I/O can first reach its existing write
  deadline; scheduling and synchronous work can delay cleanup. Duplicate notice
  delivery remains best effort and is not guaranteed during shutdown.
- A positive `telnet.max_connections` is a strict ceiling on registered telnet
  sessions, including concurrent logins. A same-identity replacement does not
  add a session: identity is the full normalized callsign and SSID. Different
  SSID spellings, including leading zeroes, remain separate identities.
- A new identity is rejected when registered sessions fill the ceiling, before
  its profile or shared preset library is changed. The full-server notice is
  best effort; the connection is closed even if the notice cannot be delivered.
- A positive `telnet.max_sessions_per_base_call` also limits registered sessions
  for each base callsign. Zero disables this limit. Base identity follows
  `spot.NormalizeOwnCallsign`: numeric SSIDs are removed, while portable
  location forms remain distinct. Full normalized callsigns and SSIDs remain
  separate profile and replacement identities. Both ceilings are decided and
  reserved atomically before profile or shared preset writes; a matching
  registered identity can replace its owner without consuming another slot.
- A new identity rejected by the station ceiling receives a best-effort
  station-specific notice and is closed. If both ceilings reject it, the global
  full-server notice takes precedence. Capacity rejection does not change the
  profile, upgrade backups, or shared preset library.
- A positive `telnet.max_sessions_per_ip` limits registered sessions for each
  observed connection IP, including admissions in progress. Zero disables this
  limit. The peer IP is canonicalized, IPv4-mapped IPv6 is grouped with IPv4,
  and an IPv6 scope zone remains part of the identity. Ports do not identify
  separate IPs. The connection peer is authoritative; forwarded addresses and
  subnet groupings are not used. When enabled, a missing or invalid peer IP is
  rejected before profile writes. When disabled, non-nil peer address handling
  stays the same; a nil peer is handled defensively rather than panicking.
- Exact-identity replacement at the same IP reuses its existing IP slot.
  Replacement from a different IP needs destination capacity. If registered
  sessions fill that IP's limit, reject without changing the old owner or saved
  profile, backups, or shared presets. Reserve IP, station and global capacity
  atomically as applicable. For a new identity rejected by multiple registered
  ceilings, use global, station, then IP notice precedence.
- When in-progress logins occupy the remaining capacity, another new identity
  or cross-IP replacement waits for their outcome and rechecks all ceilings,
  subject to its existing login deadline, cancellation and shutdown. No fairness
  or arrival order is promised.
  Separate pre-login limits and the existing per-identity profile handoff remain
  in force. Other login failures retain their existing persistence behavior.
- Runtime YAML requires a positive value for `telnet.max_connections`. Direct
  server callers may set `MaxConnections` to zero or less to disable this
  registered-session ceiling; pre-login limits remain separate.
- Runtime YAML requires an explicit nonnegative `telnet.max_sessions_per_base_call`.
  The shipped value 10 is operator policy, not a calibrated safety threshold.
  Station accounting is coupled to active sessions and reservations; historical
  station identities do not accumulate. Pre-login limits remain separate, and
  callsign validation does not authenticate the user.
- Runtime YAML also requires an explicit nonnegative `telnet.max_sessions_per_ip`;
  the shipped value 10 is operator policy, not a calibrated threshold. IP
  accounting is coupled to registered owners and active reservations, and
  historical address entries do not accumulate. Pre-login gates remain separate.

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
- WWV and WCY history is independent of recipient eligibility and the optional
  spot archive. Each type retains at most 48 records from the inclusive
  24-hour receipt-time window. Archived text is capped at 256 Unicode code
  points and marked when truncated; live text is unchanged. Deduplication uses
  the original text before truncation. Login and explicit SHOW requests bypass
  the live WWV/WCY switches; later live delivery still honors them. Both
  dialects accept SHOW/SH with a space or slash before WWV/WCY, returning the
  latest five by default and the whole retained window with a separate FULL.
- Detected history loss is reported for the affected window. A storage failure
  or a restart can make completeness unknown; history is not proof of messages
  that never reached the cluster. See [ADR-0323](decisions/ADR-0323-wwv-wcy-bulletin-history.md).

### Prioritization

- Control messages drain before spots.
- Control traffic must not be starved by high-volume spot traffic.
- Peer liveness/config traffic must bypass the normal peer spot backlog.
- If the peer control-priority lane is full, close the peer session and rely on
  reconnect/backoff rather than silently dropping keepalives.

### PC18/PC92 authority and recovery

- Outbound `ccluster` startup accepts remote `PC20` only after local
  initialization. It replies with fresh negotiated configuration (PC9x A/K,
  legacy PC19), then PC22, and completes establishment. Outbound `dxspider`
  still requires PC22; inbound family-specific startup remains unchanged.
  Existing bannerless initialization eligibility is not remote identity proof.
- Handshake PC51 requests use existing destination rules and bounded priority
  output. Ping replies do not grant authority or extend fixed phase deadlines.
  The initial and CC response A/K exchanges have separate bounded candidate
  progress; timestamp retries after A resume K without duplicating A. Neither
  handshake exchange substitutes for post-establishment C/A recovery. See
  [ADR-0260](decisions/ADR-0260-cccluster-outbound-handshake.md).
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
- Required integer `peering.max_peers` is1�64 (shipped64), with no fallback.
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

### Original-content local peer publication

- Peer publication of locally accepted human/manual spots uses their accepted
  source fields, not the final locally corrected spot. Capture after primary
  normalization and completion of toxicity classification, before output
  preparation, call correction or frequency averaging. This is the accepted
  `Spot` boundary, not a promise to preserve literal DX command-line spelling,
  whitespace or frequency precision already consumed by the command parser.
- Own a compact immutable publication record. Carry it with the existing
  temporal, FT and stabilizer pending owners through release, retry, overflow
  and shutdown; never reconstruct it from the mutated local spot. No provenance
  map, worker, queue or persisted field is added.
- Preserve representable comment bytes exactly. Do not infer a mode for a blank
  comment, trim spaces, replace delimiters or sanitize content. Empty comments,
  carets and bytes forbidden by the existing peer grammar refuse that output
  variant; local delivery can still proceed. Nonempty whitespace and permitted
  tabs remain representable.
- Encode captured finite frequency without additional one-decimal rounding;
  preserve valid captured IP text. Use existing UTC date/minute, origin and hop
  framing. Validate each existing destination variant independently, including
  its size limit. A refused modern PC61 does not gain a new PC11 fallback.
- Preserve source eligibility, `forward_spots`, shared post-stabilizer MED
  admission, actual hold/timeout suppression, queue and delivery timing.
  Family display hiding is telnet-only: it does not clear shared ring/archive/peer
  permissions or skip eligible history admission after hold ownership resolves.
  Hiding uses one fresh eligible receiver population and the existing whole
  support percentages. Higher support wins; usable slash/base ties favor slash,
  other ties show both. Missing, split, uncertain or incomplete evidence has no
  authority. Only successful ordinary broadcast admission records separate
  FAST/MED/SLOW literal/time/frequency witnesses; failed attempts never refresh
  or delete rivals. Queue admission is an offer, not client receipt. See
  [ADR-0295](decisions/ADR-0295-fresh-admitted-family-display.md).
  Weighted percentages retain their basis: known or rounded zero is available
  as 0%, all-zero weighted totals are unavailable, and raw coverage requires an
  explicitly unweighted population. Zero display alone does not authorize
  family hiding; positive usable support remains required. An all-zero weighted
  observation contributes no decoder support. See
  [ADR-0298](decisions/ADR-0298-consistent-resolver-support-basis.md).
  Local display, ring history
  and archive may contain corrected B while peer publication contains original
  A. PC11/PC61 carry no local confidence percentage or rejection reason.
- Native received-frame relay remains the separate original-field path above;
  locally republishing a received peer spot must not duplicate that relay.
  See [peer publishing rules](../peer/README.md#publishing-rules) and
  [ADR-0284](decisions/ADR-0284-original-content-peer-publication.md).

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
- State accepts 60 FCC and 13 Canadian codes as registered-address metadata,
  not proof of operating location. `DXSTATE`/`DESTATE` Boolean atoms compare
  canonical observed codes; missing, invalid or noncanonical State is unknown.
  Every comparison, including `<>`, is false for unknown State; NOT negates
  that result. NEARBY combines with these comparisons through ordinary Boolean
  logic and does not suppress geography.
- Archive version 6 records both observed States. Versions 2-5 remain unknown;
  history never consults today's registry to hydrate old records.
- Native configuration version seven carries separate GO numbered rules and
  native CC geography. The retained schema-six GO interface carries numbered
  rules, not hidden old-schema selections. Recognized old profiles
  have the reset-only login upgrade below; future or malformed documents remain
  protected. Downgrade uses matching
  binary/data backups.

See [ADR-0253](decisions/ADR-0253-fcc-state-enrichment-and-filtering.md) and
[ADR-0254](decisions/ADR-0254-canadian-ised-license-and-state-reuse.md) for
metadata history; their pre-v5 category/schema mechanics are historical.

Call correction checks a replacement's home-base license eligibility before
identity, confidence, metadata or accepted-mutation accounting changes. A
confirmed ineligible replacement leaves the original for ordinary final
validation, without searching another candidate. Later authority changes can
still reject an accepted replacement at the final gate; accepted mutation counts
are not rolled back and do not imply delivery. Replay uses frozen input bytes
and simulated UTC eligibility, with process-time cache lifetime and explicit
per-run cleanup. See [ADR-0302](decisions/ADR-0302-correction-evidence-admission-and-license-authority.md).

## Boolean Personal Rules

This section governs the GO bank. The current independent native CC, version-seven
storage and schema-seven contract is [CC dialect](cc-dialect.md), approved in
[ADR-0330](decisions/ADR-0330-native-cc-banks-and-generation-seven.md).

This is the approved normative Boolean MVP contract; see
[ADR-0329](decisions/ADR-0329-nearby-boolean-predicate-and-generation-six.md),
which replaces the affected NEARBY/schema clauses of ADR-0317.
It does not authorize production deployment or establish private-cluster
acceptance or representative performance qualification. There is one prepared
Boolean personal-rule engine; the old category model has been removed.

### Slots, Combination, Defaults And Reset

- Slots 0-9 each hold one PASS or REJECT action and one complete expression.
  `PASS RULE <0..9> <expression>` and `REJECT RULE <0..9> <expression>` use the
  GO grammar. CC refuses Boolean rule mutations. Setting a slot replaces its action and expression.
- Any matching active REJECT vetoes delivery. Otherwise active PASS rules are
  ORed. With no active PASS, personal selection is unrestricted except for
  REJECTs; with no rules there is no personal restriction. Slots are not
  priorities. Adding a PASS broadens selection; narrow an existing PASS by
  editing it rather than adding another.
- New records and recognized old-profile resets receive the same validated
  configured MODE and SOURCE selections as an ordinary editable PASS in slot 0.
  Selections combine with AND; ALL, both sources or an empty source selection
  writes SOURCE=HUMAN,SKIMMER explicitly. Startup validates defaults after
  taxonomy loading and before clients.
  Existing/restored empty records remain empty, including after reconnect.
- Omitted SOURCE imposes no source restriction on PASS or REJECT.
  Explicit SOURCE comparisons follow ordinary Boolean evaluation, including OR
  and NOT. COMMENT text is not a field occurrence. Readbacks add no synthetic
  source guard. NEARBY adds no implicit source or geography policy.
  Apply this meaning to restored profiles and presets without rewriting them.
  Explicit SOURCE=HUMAN stays human-only, including old generated defaults.
  Shipped default_sources includes HUMAN and SKIMMER; operator selections remain
  authoritative. New accounts and old-profile upgrades use those configured
  defaults. Current-generation records and user-issued resets do not receive new defaults.
- `RESET FILTER RULE <0..9|ALL>` clears only the selected slots.
  `RESET FILTER` clears all personal rules, including their NEARBY terms.
  Neither reinstalls slot 0 nor resets separate controls or profile settings.
- Retired category, MODE-delta, MINSNR, COMMENT-list, DXBM and NOFILTER mutations
  and old category aliases are refused, not translated. There is no `RESET RULE`
  or `SET DX FILTER` alias. Existing separate-control commands and shortcuts
  keep their own grammar.

BEACON, WWV, WCY, ANNOUNCE/PC93, SELF and TOXIC remain separate
controls. NEARBY is an ordinary operand-free expression predicate. The six stored control choices preserve
DEFAULT versus explicit false/true. Bulletin delivery remains independently
controlled. Live qualifying self-spots require SELF enabled and must pass TOXIC;
they bypass personal rules. Command/history saved self exceptions
remain independent of SELF and still honor TOXIC. Explicit history selector,
BAND, MODE and COMMENT selections remain mandatory even for self-spots and
apply before counting.

### Expressions And Field Semantics

An atom is a field comparison. Expressions use parentheses and NOT, AND and OR,
with precedence NOT > AND > OR and short-circuit evaluation. `=` against a
comma list, bare or bracketed, matches any value. `<>` means none of the listed
values only when the spot operand is known. Every comparison against unknown
data is false, including `<>`; NOT negates that result. For example,
`DXSTATE<>CA` is false for unknown State, while `NOT DXSTATE=CA` is true.
A reject-only set can admit unknown State without making its `<>` atom true.
There are no generic ALL/ON/OFF wildcard operands.

The fields are BAND, MODE, SOURCE, EVENT, DXCALL, DECALL, DXCONT, DECONT,
DXZONE, DEZONE, DXDXCC, DEDXCC, DXSTATE, DESTATE, DXGRID2, DEGRID2, FREQ,
SNR, CONF, PATH and COMMENT. Equality/inequality apply to all; ordered
`<`, `<=`, `>` and `>=` apply only to FREQ, SNR, DXZONE and DEZONE and
take one value.

- BAND uses supported normalized bands; MODE uses configured taxonomy/aliases.
  Blank spot MODE is known UNKNOWN, not missing. SOURCE is HUMAN for
  `Spot.IsHuman`, SKIMMER otherwise.
- EVENT compares the taxonomy event bitset; NONE is known when no event is
  present. Eventless spots have no exemption from expressions.
- DXCALL reads `DXCall`; DECALL prefers `DECallStripped`, otherwise `DECall`.
  Patterns contain letters/digits, slash or hyphen and at most one `*` at an
  end, including `*` alone. No internal/multiple stars or `?` wildcard.
  A lone star matches only a nonempty known call.
- DXCONT/DECONT use supported continent codes; DXZONE/DEZONE use integral
  CQ zones 1-40. Human and machine DXDXCC/DEDXCC operands accept positive ADIF
  integers or exact canonical CTY Prefix labels. One request-owned lazy index
  resolves all labels, across fields and slots, before full-rule preparation
  completes; NEARBY does not remove geography. Unknown/conflicting labels
  refuse the whole proposal. Numeric inputs require no CTY; integer overflow
  cannot fall through to label resolution. All prepared definitions, readbacks
  and stored expressions are numeric; native loaders and matching remain
  CTY-independent. Repetitions still consume current operand budgets.
  A positive spot ADIF is known without CTY-membership validation.
  `DXDXCC=IT9` normalizes to entity 248 with that CTY association;
  `DXCALL=IT9*` selects a literal prefix. SHOW DXCC and history CTY selector
  precedence remain separate. See [ADR-0321](decisions/ADR-0321-boolean-canonical-dxcc-input.md).
- DXSTATE/DESTATE use the observed canonical 60 FCC plus 13 Canadian codes
  described above. Rule operands normalize case; missing, lowercase or invalid
  spot codes are unknown. The home/base-call license jurisdiction used to
  obtain State is distinct from positive ADIF geography metadata or a portable
  operating prefix. DXGRID2/DEGRID2 compare two-letter fields AA-RR, not exact
  user GRID settings.
- FREQ operands are finite nonnegative kHz numbers. `=`/`<>` also accept
  inclusive low-high ranges. A spot frequency is known only when finite and
  positive. Comparisons use the actual floating value, with no epsilon or
  additional rounding.
- SNR is a strictly integral report, known exactly when `Spot.HasReport` is
  true, including human, zero and negative reports. No implicit human
  exemption applies; missing reports follow the ordinary unknown rule.
- CONF is the field name, not CONFIDENCE. It is unknown for confidence-exempt
  modes, with no exemption bypass inside an expression. For other modes blank
  confidence normalizes to the known `?` glyph.
- PATH classes match exactly: UNLIKELY excludes CLOSED. To select both use
  `PATH=UNLIKELY,CLOSED`. Missing/invalid prediction becomes the known
  INSUFFICIENT class.
- COMMENT is an ASCII case-insensitive literal substring of the stored cleaned
  comment, not regex/glob/word matching. Empty comments are unknown. Each
  phrase is 1-64 printable ASCII bytes with a non-space byte. Preserve literal
  case, punctuation and spaces; double a literal quote inside a double-quoted
  value, and treat backslashes literally.

The parser admits printable ASCII with ASCII-space whitespace, rejects trailing
tokens and reports 1-based byte columns. Quote values containing spaces or
punctuation. Non-COMMENT operands normalize under their field's rules, not by
uppercasing all expression bytes. This example preserves two literal spaces:

```text
PASS RULE 0 COMMENT="CQ ""DX""  !;=()[]"
REJECT RULE 9 SOURCE=SKIMMER AND MODE=CW,RTTY AND SNR<10
```

Trial guardrails are ten slots, 128 total atoms and 512 total values. Each
expression has independent 1,024-byte raw and canonical limits, 32 atoms,
128 nodes and nesting depth 8; a comparison atom has at most 32 values. Depth counts
parentheses and NOT, not flat AND/OR tree height. These do not enlarge human,
machine or stored-document limits. Any parse, domain, slot or budget refusal
leaves the previous complete rule set intact.

### NEARBY Predicate And Recovery

Support exactly the case-insensitive bare predicate NEARBY, including NOT and
ordinary AND/OR/parentheses. It has zero operand values and consumes one atom
and tree node. Comparisons such as NEARBY=TRUE, ON/OFF operands, radius parameters
and unknown bare fields are invalid. COMMENT="NEARBY" is literal text. Preserve
short-circuit evaluation and cache true or false once per reached spot/user
evaluation across all PASS and REJECT rules. No user-specific result is retained
in a shared program. Nil spots and invalid ordinary matching owners fail before
negation; valid empty rule sets retain unrestricted personal selection. Preserve
the existing caller-specific self exceptions.

Preserve the actual geometry helper exactly. Either endpoint can match the
user's H3 cell; a missing opposite endpoint cannot veto it. 160m, 80m and 60m
use coarse resolution; other supported bands use fine resolution. Preserve
normalized-band/raw-band/frequency fallback and fine endpoint-cache precedence;
coarse matching converts grids rather than reusing fine cells. Only empty or
??? after fallback forces false for the band; unsupported nonempty labels retain
current fine-cell behavior. A missing required user cell or no matching endpoint
makes NEARBY false. NOT negates that result and is not positive evidence of
distance. Other OR branches can qualify without usable proximity.

GRID, provenance and its fine/coarse caches have one client-configuration owner.
Capture them with rules and controls under path-before-filter locks; release
locks before mapping, evaluation or prediction. Resolve missing cells independently
from that captured GRID, preserve each valid captured cell, and never reread
mutable client state or publish caches from the pure resolver. NEARBY does not
depend on PATH or prediction/display enablement. Captures needing neither NEARBY
nor PATH add no mapping work. Existing conditional PATH publication must still
reject stale GRID ownership. Unrelated edits preserve location and do not repeat
callsign-derived GRID lookup. This replaces TSR-0109's independent-pair contract.

Rule creation/editing needs no usable GRID. One predicate governs YAML PUT,
PATCH and VALIDATE recovery: a rules list is supplied and every other saved
setting and separate control is unchanged. Only unchanged GRID mapping
availability is waived. Locator syntax, disabled dedupe choices and all other
settings checks remain mandatory; empty GRID retains lookup meaning. Keep ordinary
human GRID admission, removing only the extra both-resolutions toggle requirement.
No mapping-table reload or new location service is introduced.

NEARBY never changes saved or effective dedupe. Broadcast admission, diagnostics,
readback and greeting substitutions use the ordinary resolved policy, including
normal DEFAULT/disabled-policy fallback. An explicit settings or preset dedupe
change is still permitted. MED/SLOW users may see fewer local repeats than with
the retired automatic FAST override.

### Prepared Matching And Readback

Matching captures immutable prepared programs and scalar controls, cells and
path inputs coherently under path-before-filter locks. Rule edits replace
bounded sets and share only unchanged immutable programs. Live, command and
history matching use this engine without an extra category gate; rendering
reads retained programs rather than recompiling expressions. History captures
per-page matching state under its configuration transaction; propagation
observations remain current per candidate. Rule identity and runtime matching
geometry participate in existing continuation/generation fences. Matching and
fingerprinting use the same snapshot-local effective pair. Lazy equivalent
cache filling is stable; true changes, including change then restoration,
invalidate pending pages and continuation. Propagation observations stay outside
that fingerprint.

`SHOW FILTER` and `SHOW FILTER RULE` show current settings, canonical numbered
rules and individual controls. `SHOW FILTER FULL` adds one-program preparation
counts. All views preserve actions and complete Boolean structure; there are no
projected expressions, suspended rules or global NEARBY fields.
`SHOW/FILTER` and `SH/FILTER` remain readback aliases; category views do not.
Human output uses at most 78 printable ASCII columns plus CRLF. Short rules
use directly reusable command syntax. Long exact values use Go-style
quoted/escaped complete `+` chunks concatenate losslessly; that display encoding
is not expression quoting or necessarily copyable command syntax. Human input
still has its configured limit, 128 bytes by default; use schema 6 YAML for
longer expressions. Preserve the unconditional human reading hold through
preparation, delivery and successful write/flush plus its reading interval,
RESUME supersession, bounded queues and 65,536-byte complete-response limit.
Machine operations remain pause-neutral.

Source owners in the checkout are `filter/rule_set.go`, `filter/expr/`,
`filter/rule_compile.go`, `filter/rule_evaluate.go`, `filter/rule_view.go`,
`filter/match_snapshot.go`,
`telnet/matching_snapshot.go` and `telnet/history_filter.go`.
`commands.BooleanRuleHelpLines` in `commands/boolean_rule_help.go` owns
`HELP RULE`; see the [telnet contract](../telnet/README.md#filters).

## Per-Mode Minimum SNR

Use Boolean SNR comparisons with explicit MODE and SOURCE guards, for example
`REJECT RULE 9 SOURCE=SKIMMER AND MODE=CW,RTTY AND SNR<10`.
This rejects present skimmer reports below 10, not reports equal to 10, and
does not implicitly exempt human reports in other SNR expressions. Missing
reports follow unknown/NOT semantics. There is no saved `min_snr` map, dormant
mode-threshold state or MINSNR mutation compatibility.
Source parsing, report units, aggregation, archive layouts and model semantics
are unchanged. [ADR-0258](decisions/ADR-0258-per-mode-minimum-snr-filter.md)
describes historical pre-v5 category behavior, not the active contract.


## Station Reputation

Numeric SSIDs share baseline station reputation and active posting allowance;
configuration ownership remains full-SSID. Benign reconnects preserve spent
counts and the current window except normal elapsed advancement. Risk penalties
may tighten restrictions or extend deadlines without clearing spent counts or
shortening an applicable deadline. Baseline ASN/country history is reused; old
SSID files are preserved and unmerged. Counters are runtime-only and may be lost
on restart or state eviction. See
[ADR-0271](decisions/ADR-0271-shared-station-reputation.md).

## Stored Configuration Admission

Current native profiles and libraries use version seven and independent GO/CC
banks. The complete ownership, reset, preset, resource and recovery contract is
[CC dialect](cc-dialect.md), with exact approved limits in
[Scope Ledger v4](cc-dialect-v4-scope-ledger.md) and durable rationale in
[ADR-0330](decisions/ADR-0330-native-cc-banks-and-generation-seven.md).

Each bank owns six exact DEFAULT/false/true controls and its own immutable applied
preset baseline. Configured shared preferences preserve empty/zero/default
selections; runtime fallback does not rewrite them on ordinary login restore. Login/IP and fixed
notice metadata are separate from writable preferences. Full schema-seven CONFIG
restores both banks and shared settings; ordinary edits cannot write inactive
state. Named presets carry exactly one tagged bank plus shared preferences.

Explicit preset LOAD retains ADR-0244's applied-setting contract: report an
unavailable dedupe/PATHSAMPLES adjustment, persist its applied value and establish
that value as the selected bank's baseline. Keep the shared named snapshot and
the other bank/reference unchanged. Empty GRID remains configured empty;
lookup-derived location is runtime state. Machine writes still reject unavailable
choices, and ordinary login/readback preserves exact configured selections.

Pre-seven upgrade wipes all old filters, switches and applied references; both
banks start from current operator mode/source defaults. Selected dialect and
unrelated settings remain. Old named presets are cleared once per shared library.
Reset generation and library replacement commit together, so later SSID/profile/
notice retries preserve newly created presets. A completed reset is never inferred
from successful notice delivery, and failed notice acknowledgement never repeats it.

Each eligible upgrade replacement is admitted and its original backup verified
before replacing that file. When both files are eligible both backups precede
library then profile writes. Separate commits retain independent failure outcomes;
an independently safe profile reset may finish while preset reset remains pending.
Only initial authoritative profile absence permits new-user creation. Malformed,
unsafe, future and over-budget originals remain protected, including at logout.

The full-SSID lease spans capture, admission, persistence and publication. The
base-call library stripe is acquired after it. Publication uses path then filter
locks and stable Filter identity. Machine and LOAD commit before publishing;
human late-save failures keep both admitted banks in memory with an unsaved
warning. SAVE's library-success/profile-failure outcome keeps both prior references.
Metadata-only notice acknowledgement reloads strict current storage and rechecks
ownership without publishing, resetting, saving unsaved preferences or advancing
configuration/history revisions.

Physical profile 1 MiB, preset 256 KiB, library 8 MiB/20 names and shared decode-work
2 MiB/16 MiB bounds remain. CC canonical state is capped at 128 KiB, complete
CONFIG/tagged preset at 192 KiB and final schema-seven frame at 256 KiB. Full encoding,
metadata, both baselines and promised human/wire inspection must all fit. Payload
and work charges are not exact heap bounds. Process-wide input/output reservations
remain held through decoder ownership and writer flush/discard respectively.

Stop writers before backup/repair/restore; use one writer process per user directory.
Rollback needs matching executable, profiles and library and loses later changes;
preserve current data first. Deployment/live data operations remain separately
authorized. Historical version-six DTO codecs remain frozen for legacy GO protocol
handling and cannot represent a native CC bank.

### Machine Schema 6

This is the retained legacy interface, not the current complete backup schema.
FILTER/CONFIG require effective GO; CONFIG proposals must remain in GO.
SETTINGS/CAPABILITIES work in CC without exposing empty/inactive GO rules and
include compatibility guidance. SETTINGS can switch the active bank. All
changes preserve the other bank and use the whole-preference revision. Complete
both-bank restore and native CC editing use [schema seven](cc-dialect.md#yaml-and-backups).

Every `GET YAML FILTER|SETTINGS|CONFIG|CAPABILITIES SCHEMA 6 [ID <id>]` requires
explicit schema 6, including SETTINGS/CAPABILITIES. PUT/PATCH YAML
FILTER/SETTINGS/CONFIG have no schema header suffix; their bodies require
`schema_version: 6`. `VALIDATE YAML CONFIG` alone validates a complete CONFIG
without applying or saving. Missing/old schemas and category fields are refused
without translation or hidden-field preservation.

FILTER's `configuration` directly contains its seven fields; SETTINGS directly
contains its six fields; CONFIG nests `filters` and `settings`. PUT/VALIDATE
require complete resources. PATCH preserves omitted fields, but a supplied
`rules` list replaces all slots; `rules: []` clears it. It is not a per-slot
merge. Duplicate slots and mapping keys are errors.

The strict single-document writable envelope contains `schema_version`,
`request_id`, `if_revision` and `configuration`, not read-only status or preset
baselines. IDs are 1-32 ASCII letters/digits/hyphens; PUT/PATCH require the
current connection's matching revision, while VALIDATE permits an optional
revision. Keep session/epoch/revision fences and refetch after conflicts.
Reject unknown/read-only fields, nulls, anchors, aliases, merges, custom tags
and extra documents. Preserve 65,536-byte upload and complete-response limits,
framing, deadlines and terminal-error distinctions. Admit the resulting complete
CONFIG readback including reserved metadata before writing; a valid large
preset can LOAD even when that readback would exceed the response limit.
No partial publication or truncation is a success.

CAPABILITIES advertises `supported_schema_versions: [6]`, actual rule
fields/operators/vocabularies, bounds and collection replacement, not the old
per-server settings-choice matrix. Runtime settings admission remains
authoritative. NEARBY appears in rule_predicates with zero operands and an
empty operator list, distinct from comparison fields. Status contains canonical
{slot,action,expr} entries, configured_count and one program count domain.
Retired nearby_enabled, nearby_active, projected and suspended fields are absent;
any retired field in a schema-6 proposal is refused, even false.
All machine operations, including errors, leave pause state alone.
See the [client protocol](../telnet/README.md#client-yaml-configuration) and
checkout owners `telnet/machine_v6_input.go`, `telnet/machine_v6_schema.go`,
`telnet/machine_v6_schema_rules.go`, `telnet/machine_v6_commands.go` and
`telnet/machine_v6_readback.go`.

## Human Preference Publication

Human filter and settings commands prepare writable preferences on detached
state under the existing full-SSID transaction lease. Refused commands publish
and persist nothing. Read-only commands retain their live readback paths.

Accepted commands publish before attempting persistence; a save failure keeps
the edit live and reports the existing warning. Repeated numbered-rule sets
retain save intent; removing an absent slot is an accepted no-op. Unchanged
DIALECT remains unchanged without a save. GRID-owned cells remain runtime state;
canonical rules remain authoritative.

Human SET SOLAR deliberately reapplies scheduling at publication, including an
unchanged cadence. Other human edits preserve broadcaster progress. Unchanged
machine cadence preserves its clock; LOAD starts a fresh schedule. Publication
retains stable Filter identity and does not replace diagnostics, read-pause or
connection lifecycle ownership. See
[ADR-0269](decisions/ADR-0269-detached-human-preference-publication.md).

## Literal Comment Filtering And Archive Selection

Saved COMMENT conditions are atoms in numbered Boolean rules, after ingestion
cleanup and before display diagnostics or synthetic fallbacks. They follow the
literal bytes, unknown/NOT semantics and rule budgets above, not separate
PASS/REJECT phrase lists. A slot edit replaces the whole expression/action;
there is no phrase move, individual-list removal or COMMENT reset command.
Human expression quoting uses doubled quotes and literal backslashes; display
chunk encoding is separate. Failed admission leaves the whole rule set
unchanged. Accepted human edits retain late-save warnings; machine writes
remain commit-before-publication.

An explicit SHOW DX/MYDX COMMENT phrase is mandatory even for self-spots, applies
before row counts, and is retained by NEXT without changing preferences. Saved
comment edits invalidate continuation and pending publication. Existing retention,
page work bounds, warnings, queues and archive storage ownership remain unchanged.

DX/MYDX history also accepts BAND and MODE lists after the optional selector/count
and before COMMENT, in either order, once each. Commas are required between
values; spaces around commas and empty comma fields are allowed.
OR applies within each list and AND across supplied selections. Existing band and
filter-mode normalization applies, including UNKNOWN for blank modes. Missing or
unsupported values, missing commas, repeated categories and BAND/MODE ALL/NONE
reject the whole request without replacing its cursor. Explicit lists narrow
saved filters and remain mandatory even for self-spots; saved self exceptions
remain intact.
Deduplicated detached canonical names are retained only by the existing bounded
connection query and carried through NEXT, with existing replacement/invalidation
and close ownership. COMMENT keeps its entire literal remainder. No saved rule,
archive encoding, index, schema or configuration setting changes. See
[ADR-0262](decisions/ADR-0262-history-band-mode-selections.md) and
[ADR-0263](decisions/ADR-0263-history-list-comma-boundaries.md). Configured BAND/MODE
mode aliases are values first or after a comma; otherwise standalone keywords
start clauses. A keyword unsupported in the current category starts a clause,
including after empty comma fields. Finish a list without a comma before starting
another clause to avoid interpreting a configured keyword alias as a value.

The GO bank in native version seven and retained machine schema six carry COMMENT
expressions in `rules`, not `comments`/`block_comments` maps or lists. PATCH
replaces a supplied complete rule list; omission preserves it. Old schema/disk
ordinary codec documents are refused; login separately recognizes safe retired
profiles for the bounded reset without translating rules. Malformed
or oversized stored rules fail without truncation or protected-record rewrite.
[ADR-0261](decisions/ADR-0261-literal-comment-filter-and-history.md) records
historical pre-v5 saved-list behavior; the explicit archive phrase grammar and
its ownership remain unchanged. See
[telnet comments](../telnet/README.md#comment-filters-and-searches).
