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
  that result. NEARBY projects these atoms out of effective rules, retaining
  the latest full saved expressions.
- Archive version 6 records both observed States. Versions 2-5 remain unknown;
  history never consults today's registry to hydrate old records.
- Native configuration version 5 and explicit machine schema 5 carry numbered
  rules, not State maps or hidden old-schema selections. Recognized old profiles
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

This is the approved normative Boolean MVP contract; see
[accepted test-only ADR-0317](decisions/ADR-0317-boolean-filter-replacement-mvp.md).
It does not authorize production deployment or establish private-cluster
acceptance or representative performance qualification. There is one prepared
Boolean personal-rule engine; the old category model has been removed.

### Slots, Combination, Defaults And Reset

- Slots 0-9 each hold one PASS or REJECT action and one complete expression.
  `PASS RULE <0..9> <expression>` and `REJECT RULE <0..9> <expression>` use the
  same grammar in GO and CC. Setting a slot replaces its action and expression.
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
  source guard. A geography-only suspended PASS stays absent.
  Apply this meaning to restored profiles and presets without rewriting them.
  Explicit SOURCE=HUMAN stays human-only, including old generated defaults.
  Shipped default_sources includes HUMAN and SKIMMER; operator selections remain
  authoritative. New accounts and old-profile upgrades use those configured
  defaults. Existing Boolean records and resets do not receive new defaults.
- `RESET FILTER RULE <0..9|ALL>` clears only the selected slots, preserving
  NEARBY. `RESET FILTER` clears all personal rules and turns NEARBY OFF.
  Neither reinstalls slot 0 nor resets separate controls or profile settings.
- Retired category, MODE-delta, MINSNR, COMMENT-list, DXBM and NOFILTER mutations
  and old category aliases are refused, not translated. There is no `RESET RULE`
  or `SET DX FILTER` alias. Existing separate-control commands and shortcuts
  keep their own grammar.

NEARBY and BEACON, WWV, WCY, ANNOUNCE/PC93, SELF and TOXIC remain separate
controls, not expression fields. The six stored control choices preserve
DEFAULT versus explicit false/true. Bulletin delivery remains independently
controlled. Live qualifying self-spots require SELF enabled and must pass TOXIC;
they bypass personal rules and NEARBY. Command/history saved self exceptions
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
  completes or NEARBY projection removes geography. Unknown/conflicting labels
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
128 nodes and nesting depth 8; an atom has at most 32 values. Depth counts
parentheses and NOT, not flat AND/OR tree height. These do not enlarge human,
machine or stored-document limits. Any parse, domain, slot or budget refusal
leaves the previous complete rule set intact.

### NEARBY Projection And Recovery

NEARBY ON selects a prepared effective expression by structurally removing only
DX/DE continent, zone, DXCC, State and grid leaves, not substituting true/false.
A NOT whose subtree disappears also disappears; an AND/OR with one surviving
child collapses to that child. An entirely emptied rule is ignored, contributing
neither PASS nor REJECT. Calls, BAND, MODE, SOURCE, EVENT, COMMENT, FREQ, SNR,
CONF and PATH remain. Full saved expressions stay intact and editable.
OFF evaluates the latest full rules, never a pre-ON map or restoration snapshot.

Either the DX or DE endpoint may satisfy H3 geometry; an unavailable opposite
endpoint cannot veto a match. 160m, 80m and 60m use the coarse cell; other
supported bands use the fine cell. Enabling NEARBY or changing GRID while ON
requires usable geometry. The saved GRID and dedupe choice remain saved;
temporary NEARBY effective dedupe does not rewrite the user's selection.

Already-ON restored NEARBY with unavailable
GRID/cells must permit rule-only edits through human commands and YAML when
every other saved selection is unchanged. Retain GRID and ON; geometry must
admit no ordinary spots when both user cells are unavailable. Preserve the live SELF/TOXIC and
command/history self exceptions above. Do not require clearing geography,
changing unrelated selections or restoring an old map to edit the full rules.

Rule-only YAML recovery waives only mapping availability for the unchanged saved
GRID. Exact locator syntax and every other settings check remain mandatory.
Existing human control/settings admission is unchanged. If only one user cell
is available, the bands using that cell may work; the other bands reject.
Readback reports this as partly usable geometry. The joined fresh-child
`TestGridMappingAvailability` fixture covers actual PATCH, PUT and VALIDATE with
an explicit saved GRID and genuinely unavailable mappings.

### Prepared Matching And Readback

Matching captures immutable prepared programs and scalar controls, cells and
path inputs coherently under path-before-filter locks. Rule edits replace
bounded sets and share only unchanged immutable programs. Live, command and
history matching use this engine without an extra category gate; rendering
reads retained programs rather than recompiling expressions. History captures
per-page matching state under its configuration transaction; propagation
observations remain current per candidate. Rule identity and runtime matching
geometry participate in existing continuation/generation fences.

`SHOW FILTER` and `SHOW FILTER RULE` show current settings, exact saved/effective
numbered rules and individual controls. `SHOW FILTER FULL` adds geometry detail
and preparation counts. All views preserve actions, canonical expressions and
NEARBY projection/suspension without flattening Boolean relationships.
`SHOW/FILTER` and `SH/FILTER` remain readback aliases; category views do not.
Human output uses at most 78 printable ASCII columns plus CRLF. Short rules
use directly reusable command syntax. Long exact values use Go-style
quoted/escaped complete `+` chunks concatenate losslessly; that display encoding
is not expression quoting or necessarily copyable command syntax. Human input
still has its configured limit, 128 bytes by default; use schema 5 YAML for
longer expressions. Preserve the unconditional human reading hold through
preparation, delivery and successful write/flush plus its reading interval,
RESUME supersession, bounded queues and 65,536-byte complete-response limit.
Machine operations remain pause-neutral.

Source owners in the checkout are `filter/rule_set.go`, `filter/expr/`,
`filter/rule_compile.go`, `filter/rule_evaluate.go`, `filter/rule_view.go`,
`filter/rule_set_projection.go`, `filter/match_snapshot.go`,
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

Native user records require explicit `configuration_version: 5` and nested
`configuration.filters/settings`. FILTER owns `rules`, `nearby_enabled` and
the six controls `include_beacons`, `allow_wwv`, `allow_wcy`, `allow_announce`,
`allow_self` and `allow_toxic`. SETTINGS retains `dialect`, `grid`,
`noise_class`, `dedupe_policy`, `path_min_observation_count` and
`solar_summary_minutes`. Rules contain `slot`, lowercase `action: pass|reject`
and `expr`; expressions canonicalize without changing COMMENT literal bytes.
Retain DEFAULT versus explicit control false/true, empty default strings and
numeric zero selections, subject to runtime settings admission.

User metadata `recent_ips` and `last_login_utc` stays outside configuration.
An optional `preset` contains `name` and an independent `baseline` with its
own `configuration_version: 5` and `configuration`. A named library contains
only a `presets` mapping of normalized names to individual versioned v5
configurations: no invented top-level library version or machine envelope.
All current entries and baselines require v5. Strict v5 codecs do not interpret
older schemas or accept explicit disk zero. Encoder DTO version zero is only
an in-memory new-object convention, stamped to 5.

An explicit login-owned upgrade handles recognized unversioned and version
1–4 profiles. It discards category filters, NEARBY and the applied association,
preserves six settings, separate controls and login metadata, and installs
the same configured mode/source defaults as a new account in editable PASS
slot 0. This is a one-time reset, not translation of old rules.
Existing v5 profiles and new Boolean presets must not be reset again.
Shared-library cleanup removes old entries under the existing base-call stripe
and retains valid v5 entries. Back up originals before replacement; each file
commits atomically, with explicit partial-failure/retry behavior rather than a
cross-file atomicity claim. Future, malformed and unsafe/over-budget inputs
remain protected. No production data reset is authorized merely by implementing
or documenting this upgrade.

Full callsign/SSID owns its user record and applied baseline; numeric SSIDs
share the base-call named library, not login metadata or live settings.
Ordinary saves retain the applied reference. Library overwrite/deletion does
not change that independent baseline; differences show `(modified)`, and
reversing them clears it. LOAD rebuilds geometry and records the applied
snapshot, including reported server adjustments; unavailable geometry retains
stored ON with a warning rather than rewriting rules. SAVE associates the same
capture committed to the library.

User-record physical reads, normalized encoding and exact prepared writes each
have a 1,048,576-byte limit. Configuration and applied baseline use separate
262,144-byte standalone SavedPreset encoding limits, also applying to each
named preset. One document-wide physical-plus-logical decoding-work budget
includes repeated alias expansion:
2,097,152 units for a record and 16,777,216 for a collection. Work units are not
heap or file bytes; nested baselines share the operation budget. Existing
20-name/8,388,608-byte (8 MiB) collection limits remain.

Admit human candidates and temporary records before publication. A refused
destination permits fitting warned temporary changes; a prospective oversized
replacement rejects without effects. Initial login protection is sticky, while
warm failures retry authoritative metadata on later saves and ordinary logout.
An unreadable/malformed/unsupported record stays untouched, including login/IP,
ordinary preference and teardown saves. Protected login uses warned temporary
defaults, not successful restoration. LOAD, machine writes and SAVE are refused
for that session; SAVE fails before library replacement. A failed login metadata
save after a successful read retains the restored preferences/reference with a
warning.

SAVE admits library and association before either write; only later I/O failure
can leave library-only success, retaining the previous live/disk association and
baseline. Machine and LOAD publish only after committing the complete record.
An unchanged machine write still persists, repairing a prior late human save
failure without resetting unrelated state.

The full-SSID configuration lease spans capture, admission, persistence and
publication, fencing session replacement. SAVE takes the base-call library
stripe after that lease and admits both immutable payloads before library-then-
record commits; callbacks must not reenter preset APIs. Publication preserves
path-before-filter lock order and stable Filter identity. Atomic replacement
requires writing, syncing and closing the temporary file first. Stop writers
before backup, repair or restore and use one writer process per data directory;
downgrade requires a matching binary/data backup. Refusal never grants authority
to replace a protected destination.

Source owners are `filter/configuration_v5.go`, `filter/storage_v5.go`,
`filter/storage_v5_decode.go`, `filter/storage_v5_io_adapter.go`,
`filter/preset_v5.go` and the existing user-record/preset coordinators.
See [operator storage](../data/config/README.md#user-configuration-storage) for
native examples and [ADR-0270](decisions/ADR-0270-complete-storage-admission.md)
for admission history, not pre-v5 migration authority.

### Machine Schema 5

Every `GET YAML FILTER|SETTINGS|CONFIG|CAPABILITIES SCHEMA 5 [ID <id>]` requires
explicit schema 5, including SETTINGS/CAPABILITIES. PUT/PATCH YAML
FILTER/SETTINGS/CONFIG have no schema header suffix; their bodies require
`schema_version: 5`. `VALIDATE YAML CONFIG` alone validates a complete CONFIG
without applying or saving. Missing/old schemas and category fields are refused
without translation or hidden-field preservation.

FILTER's `configuration` directly contains its eight fields; SETTINGS directly
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

CAPABILITIES advertises `supported_schema_versions: [5]`, actual rule
fields/operators/vocabularies, bounds and collection replacement, not the old
per-server settings-choice matrix. Runtime settings admission remains
authoritative. Status reports configured/effective/suspended rules, effective
expressions and full/nearby/retained program counts, not retired maps.
All machine operations, including errors, leave pause state alone.
See the [client protocol](../telnet/README.md#client-yaml-configuration) and
checkout owners `telnet/machine_v5_input.go`, `telnet/machine_v5_schema.go`,
`telnet/machine_v5_schema_rules.go`, `telnet/machine_v5_commands.go` and
`telnet/machine_v5_readback.go`.

## Human Preference Publication

Human filter and settings commands prepare writable preferences on detached
state under the existing full-SSID transaction lease. Refused commands publish
and persist nothing. Read-only commands retain their live readback paths.

Accepted commands publish before attempting persistence; a save failure keeps
the edit live and reports the existing warning. Repeated numbered-rule sets
retain save intent; removing an absent slot is an accepted no-op. Unchanged
DIALECT remains unchanged without a save. Effective NEARBY projections and
cells remain runtime state, not a saved restoration map. Full saved rules remain
authoritative.

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

Explicit machine schema 5 and native configuration version 5 carry COMMENT
expressions in `rules`, not `comments`/`block_comments` maps or lists. PATCH
replaces a supplied complete rule list; omission preserves it. Old schema/disk
documents are refused, not given hidden rules or migrated defaults. Malformed
or oversized stored rules fail without truncation or protected-record rewrite.
[ADR-0261](decisions/ADR-0261-literal-comment-filter-and-history.md) records
historical pre-v5 saved-list behavior; the explicit archive phrase grammar and
its ownership remain unchanged. See
[telnet comments](../telnet/README.md#comment-filters-and-searches).
