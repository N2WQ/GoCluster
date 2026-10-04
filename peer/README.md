# Peer Behavior

This directory owns DXSpider-style cluster peering.

## What The Peer Layer Does

- accepts inbound peer sessions
- optionally opens outbound peer sessions
- exchanges spot-bearing and control-plane frames
- maintains topology and keepalive traffic

## Enablement And Direction

Peering is explicit:

- each peer record in `peering.peers[]` must be individually enabled
- `direction: outbound` dials the peer
- `direction: inbound` waits for the peer to connect
- `direction: both` allows either side to establish the link
- omitted `direction` defaults to `outbound`
- omitted `family` defaults to `dxspider`
- the node can run receive-only peering when `forward_spots` is false or omitted

Inbound admission is explicit:

- the global `peering.acl.*` block is only a coarse prefilter
- a connecting peer must also match an enabled peer record by `remote_callsign`
- `allow_ips` on a peer record optionally pins that peer to specific source IPs/CIDRs
- if `direction: both` produces simultaneous inbound/outbound attempts, the first established session wins and later duplicates are rejected

In receive-only mode:

- valid inbound peer spots still ingest locally; malformed originals are rejected first
- inbound peer spotter calls ending in the skimmer marker `-#` strip only that terminal marker before local ingest; numeric SSIDs are preserved
- maintenance traffic still runs
- only local `DX` command spots are peer-published

With `forward_spots: true`:

- normal transit forwarding is re-enabled
- local acceptance still gates whether relayed traffic continues onward

## Original Spot Admission And Relay

PC11, PC61 and PC26 originals pass one admission boundary before local
normalization, comment parsing, timestamp/origin fallback or either the primary
or peer dedupe cache. Malformed originals are dropped: they do not enter the
local ingest queue, accepted-spot archive, display or peer relay. This boundary
also applies in receive-only mode. A valid spot may still be refused by an age,
queue, duplicate or forwarding gate; that is separate from malformed input.

Local processing may normalize or correct an accepted spot's callsigns,
comment, mode, frequency, timestamp and origin. Peer relay uses the original
fields, not the locally processed `Spot`. Its only content changes are the hop
decrement, canonical sentence framing and PC61-to-PC11 conversion for a legacy
destination. Original PC11 stays PC11 even for a modern destination; converting
it to PC61 would require an IP field that was not received.

### Original field rules

The caret is a field delimiter. The current native reader treats the first
`~` anywhere as a terminator, so literal comment tildes are not admitted; this
is a known DXSpider compatibility limitation, not a restriction imposed by
DXSpider's comment rule. Required payload fields, before any hop suffix, are:

| Type | Payload fields in order |
| --- | --- |
| PC11 | frequency, DX call, date, time, comment, spotter call, origin call |
| PC61 | PC11's seven fields, then spotter IP |
| PC26 | PC11's seven fields, then an optional merge/request field |

Extra fields, including extra empty fields, are malformed. The optional PC26
field may be omitted, empty, one ASCII space, `*`, or a valid original callsign.
Its original spelling and presence are retained. A legitimate no-hop PC26 merge
sentence remains locally eligible and is not transit-forwarded. The pinned
DXSpider PC26 generator emits this no-hop form; hop-bearing PC26 relay remains
a separate case with the existing modern-destination restriction.

- DX, spotter, origin and any requested call are checked as supplied, without
  trimming, uppercasing or stripping suffixes. They are 3-15 ASCII bytes matching
  `^[A-Z0-9]+(?:[/-][A-Z0-9#]+)*$`, with at least one slash/hyphen-delimited
  identity segment containing a digit followed by a letter and at most two
  letters before its first digit. This is GoCluster's broader spot-call rule:
  `W1XYZ-#` and `W1XYZ-123` can be valid originals. It is separate from the
  narrower PC92/configuration identity contract. Origin must be a nonempty
  valid original call; the authenticated sender cannot repair it.
- Frequency is an unpadded unsigned ASCII decimal, `[0-9]+(?:\.[0-9]+)?`, in
  kHz. Signs, exponents, hexadecimal notation, NaN and infinity are unsupported.
  Parsing and the existing half-up 10 Hz rounding must remain finite, with the
  rounded kHz value in `[0, 2^32)` so the unchanged primary frequency identity is
  representable. Zero and out-of-band values are not rejected merely for being
  outside an amateur band.
- Date is exactly 11 ASCII bytes with a real UTC calendar date and an English
  month abbreviation, retaining the existing Go parser's month-case tolerance
  and four-decimal-digit year domain. The day is either two decimal digits or
  one ASCII space followed by a digit from 1 through 9: `04-Oct-2026` and
  ` 4-Oct-2026` represent the same instant. The latter is DXSpider's standard
  `%2d` spelling. Unpadded days, tabs, extra spaces, trailing padding and invalid
  calendar dates are rejected. Time is exactly `HHMMZ`, with valid hours and
  minutes. The validated UTC timestamp is assigned to the local `Spot` before
  dedupe identity or ingest-age checks; relay retains the original date bytes.
  Neither field falls back to today's date.
- Comment is nonempty. Reject byte ranges `0x00-0x08`, `0x0A-0x1F`,
  `0x80-0x9F`, and literal `0xFF`, even when a prohibited byte occurs inside
  otherwise valid UTF-8. Tabs (`0x09`), whitespace-only comments and permitted
  bytes such as `0xFE` remain accepted. This is a byte rule, not a UTF-8 validity
  rule. `0xFF` is unsupported because the native Telnet writer cannot preserve
  decoded literal IAC content through the next receiver without escaping.
- PC61 IP is a nonempty plain IPv4 or IPv6 address accepted by `netip.ParseAddr`,
  without a zone, prefix length, brackets, port or surrounding whitespace.
  Accepted spelling is retained, including IPv4-mapped forms. No private-address
  or unicast-only filter is added.

### Sentence framing and gates

PC headers and H hop markers retain case-insensitive transport recognition.
Optional `~` and the reader's existing CR/LF and repeated-terminator tolerance
remain. Leading/trailing sentence whitespace, padded hop tokens and malformed
suffixes are rejected. PC11 and PC61 require a hop token; each token is `H`/`h`
plus one or two digits, from 0 through 99. Valid numeric stacks keep the
rightmost-hop interpretation, but every stacked token must be valid. A closing
caret is required; only one closing caret is framing. Extra empty slots remain
subject to the field-count rule. Payload
fields such as an origin or PC26 requested call `H1ABC` are never consumed as
hop tokens.

Onward sentences end in `^Hn^~`, followed by writer CRLF. A modern destination
receives original PC11, PC61 or eligible PC26 fields. A legacy destination
receives original PC11 fields, or original PC61 fields with only its IP removed.
PC26 retains its modern-destination restriction. Forwarding still requires
`forward_spots: true`, successful local queue handoff, hop greater than one,
peer-dedupe admission and source exclusion. Valid H0/H1 inputs remain locally
eligible. The original timestamp governs age admission and PC11/PC61's second
age check immediately before relay; PC26 keeps its existing age-check behavior.

Each output variant must fit the existing writer sentence limit, including
`~`. The maximum sentence is 65,536 bytes; writer CRLF adds two bytes. Smaller
configured reader limits continue to govern incoming sentences. Refuse an oversized variant without truncating its
fields. A fitting PC61-to-PC11 variant may still be sent when the modern PC61
variant does not fit. Existing cache admission and expiry remain in force;
refusal does not reset a key or create an automatic retry.

### Ownership and support evidence

The handler clones at most eight original payload fields before the local
parser can place derived strings in normalization caches. These compact owned
copies cannot retain the reader's entire line through a short cached callsign.
After local parsing it assigns the already validated UTC instant to `Spot.Time`,
then captures the existing peer key before local handoff. Neither dedupe nor
ingest age can observe a tolerant parser's fallback timestamp. Relay uses the
captured original timestamp and never reads the handed-off `Spot`. Temporary
copies and encodings
remain inside the existing shared 8 MiB parse-budget contract; queue/cache
owners retain their separate existing charges.

Field-validation failures may emit bounded `parse_rejected` diagnostics. Invalid
sentence framing is discarded by `ParseFrame` before the spot handler. Diagnose a
missing valid spot separately through age, ingest queue, forwarding, hop and
dedupe gates. Diagnostic overload may lose records, so absence of a record is
not proof of acceptance. The existing dedupe identities are unchanged: valid
spots differing only in a comment may still be suppressed as duplicates.

Broad GoCluster validity does not promise acceptance by every downstream
receiver. Compare the decoded original fields, emitted bytes and actual
receiver storage separately. See
[ADR-0239](../docs/decisions/ADR-0239-peer-original-validation-and-relay.md),
its date-contract refinement
[ADR-0240](../docs/decisions/ADR-0240-peer-dxspider-date-admission.md), and
[TSR-0039](../docs/troubleshooting/TSR-0039-peer-normalized-relay-and-telnet-iac.md)
for the contract and the source-grounded failure explanation.

### Required DXSpider framing correction

Literal comment `~` support is required and pending in a separate bounded
framing change. DXSpider permits this byte and its generators replace comment
carets with it. The current shared reader splits at the first tilde, so a valid
DXSpider comment such as `CQ~TEST` cannot survive incoming processing. This
reader behavior predates original-payload relay and is unchanged by the date
correction.

That framing change must cover literal comment tildes, terminal markers,
fragmented reads, consecutive frames and overflow recovery without weakening
existing limits. Full DXSpider spot compatibility remains qualified until it
passes verification. Sender-generated admission, literal outbound bytes and
actual downstream receiver storage establish different claims; the earlier
zero-padded outbound fixtures did not establish sender-date compatibility.

## Publishing Rules

The runtime is intentionally conservative about what it republishes to peers.

- local non-test human and manual spots are eligible for normal peer publishing
- skimmer-origin spots are not blindly republished as local human traffic
- local `DX` command spots remain the operator-authored exception

## Control Plane

Configured peer family drives inbound startup:

- `dxspider` peers keep the strict inbound path and still need remote `PC20` to finish startup
- `ccluster` peers can complete inbound startup from a `PC18` banner carrying `CC Cluster Version:` or from the first valid `PC92`
- outbound CC behavior is unchanged in this slice

Keepalive and topology traffic has higher priority than normal outbound spot backlog.

If the control lane saturates:

- the session closes
- reconnect backoff takes over

That is preferred over silently drifting until the remote side times out.

WWV/WCY (`PC23`/`PC73`) and announcement (`PC93` to `ALL`/`*` or a named group) frames are parsed in the peer layer and then delivered to telnet as bulletins. Peer loop suppression keys use canonical payload fields, not raw hop-bearing wire text, so the same bulletin arriving with different hop values is treated as one peer event before telnet delivery.

## PC18 and PC92 compatibility profile

The selected reference is DXSpider revision
`3e9b3621d94dd45c68702e4a0f896aac33f2a91d`. The supported PC92 actions are
**A, C, D and K**. Unsupported F/R and unknown actions do not update topology or
freshness. CCCluster retains its documented startup and direct local
publication, and is excluded from transit PC92 broadcasts. This does not claim
complete CCCluster interoperability or a full legacy topology bridge.

PC18 identifies GoCluster using the startup-resolved UTC YYMMDD version
(for example, 261003), with separate commit and build time, plus the
Go version. Release-script builds include a separate release tag (261003r2);
plain and PGO builds omit it. These fields are distinct from numeric compatibility
version/build (`5457`/`633` by default). An explicitly empty `node_build` is
omitted. Enabled peering rejects invalid identities and metadata at startup;
it does not silently continue with an invented banner.

PC92 membership comes from current telnet sessions. Available user/peer IPs
are published; there is currently no per-user IP-publication preference.
Local published identities follow the pinned receiver normalization, including portable
forms and numeric SSIDs. For example, `K1ABC/P-01` publishes as `K1ABC-1`;
normalization is not a generic slash removal rule. Canonical callsign collisions, names outside the publication envelope, and
local/configured-peer name collisions stay local. Private PC93 delivery
requires exactly one current matching telnet owner. A departed session cannot
withdraw or receive on behalf of its replacement.

Received PC92 identities must first be valid raw wire calls. Invalid spellings
such as `K1ABC/`, `/K1ABC` and `K1ABC-000` reject the entire record before any
topology, freshness, staging or remote metadata change. The origin is not
trimmed or uppercased; entry call portions allow trailing ASCII spaces. Only
then is the existing stable canonical mapping applied. These received-wire
checks do not change human login normalization or literal peer authentication.
Accepted transit retains its original payload apart from the transport hop.

The manager owns one ordered local-origin timestamp sequence, remote topology,
and shared PC92/PC93 freshness. A valid C replaces the complete subject
membership; A/D update it; only fresh C/K refresh node liveness. Startup records
are staged until the authenticated session wins establishment. Losing or failed
candidates cannot alter global authority. Recovery always publishes complete C
followed by metadata A from the same immutable snapshot, even when periodic
C/K are disabled. Later membership changes follow the matching A. Each stable
eligible join, withdrawal or IP change must reach every healthy established
peer's control queue within **one second**, including during recovery. This is
queue admission, not confirmation of DXSpider processing. The five-second
recovery allowance does not extend that one-second deadline.

A node and a user can have the same canonical callsign under one parent. Their
relationships are distinct; deleting one kind does not remove the other.
For an explicit K subject node, omitted or zero version/build replace old
numeric metadata with zero. For example, K version5457 with no build replaces
5457/633 with5457/0. Both omitted become0/0. A/C/D omission behavior, absent IP
and external relationship metadata keep their separate rules.
Handshake phase deadlines include waiting for controller admission and startup
timestamp capacity. The session wins authority once, then its reader remains
parked until staged replay completes; failed candidates never acquire authority.

Session cancellation installation and terminal closure share a session-local
mutex. Closure before installation cancels the later operation and refuses
startup; the cancellation worker uses the same one-time socket close. Run
closes before joining workers and retires the context operation before returning
transport credits. See [TSR-0037](../docs/troubleshooting/TSR-0037-peer-session-cancellation-publication.md)
for the reproduced publication race and regression evidence.

## Bounds and operator recovery

Enabled peering requires integer `peering.max_peers` (1–64, shipped64). It bounds
enabled direct identities; disabled rows do not count and `both` counts once.
There is no runtime fallback or unlimited sentinel. Add the key to existing
configuration and restart; active over-cap configurations fail startup.
Pending candidates remain128 and transport owners are bounded by N+128, with
1,000 local session records, and a 64 KiB peer frame envelope. Smaller configured
frame limits remain effective. The complete local snapshot is never truncated.
The detailed per-class and allocation limits are in
[`docs/pc92-qualification.md`](../docs/pc92-qualification.md).

| Condition | Runtime behavior | Resume condition |
| --- | --- | --- |
| Complete local snapshot does not fit | Close/gate PC9x sessions; local users and established legacy links continue | Complete snapshot fits, including capacity reserved for configured peers that can reconnect; stable for one second |
| Unsafe or stalled UTC clock | Close/gate PC9x within five seconds; retain freshness and issuance protection | UTC advances beyond retained issuance and remains healthy for one second |
| Authoritative PC92 admission fails | Close the affected link, mark its knowledge incomplete and gate retry | Configured identity cooldown expires, controller invalidation is acknowledged, and a fair global startup grant is available |
| Spot, PC93 or bulletin dedupe pool fills | Refuse new untrackable work in that class; keep links open | Payload TTL expiry frees space; refused work is not replay queued |
| PC93 input mailbox fills | Refuse that message; increment `PC93InputRefused`, separately from cache `PC93Refused`; keep links open | Mailbox drain frees space; refused messages are not replayed |
| Control queue overload/age or stalled write | Close the affected transport | Ordinary reconnect/backoff and full membership/metadata recovery |

Global clock/publication recovery is observed within the next second after its
one-second healthy interval. After handshake, complete C then required A must
be delivered within five seconds, even when periodic C/K are disabled.

Authoritative refusal starts controlled retries with shared inbound/outbound
configured exponential backoff (normal loaded defaults2,4,8…300 seconds).
Only one recovery candidate per identity can proceed. Authenticate inbound
candidates before retry ownership; a failed outbound TCP dial consumes no
startup grant. Global startup grants are at least one second apart with circular
fairness; absent or expired candidates reserve no grant. Original handshake
phase deadlines never extend, so a64-way wave may require candidate retries.
Refused payloads are not retained or automatically replayed; every new record
must pass ordinary authority and capacity admission.

Sampled `Peering: PC92 retries` diagnostics report cooldown, waiting, active,
healthy-reset, eligible-without-candidate and global-gated identity counts.
These counts describe local attempt state, not remote membership completeness.

Retry history resets only after matching post-establishment C/A has locally
flushed and establishment/replay has completed, followed by60 healthy seconds.
Local Flush is not confirmation of remote processing. Quiet PC9x peers qualify;
legacy fallback does not. A global clock/publication closure interrupts the
healthy interval without adding a failure or restarting cooldown. Remote
knowledge remains incomplete until a valid authoritative remote C arrives.

Spot, PC92, PC93 and WWV/WCY payload caches have separate budgets. Unexpired keys
are never evicted; duplicates do not extend their age. Payloads expire strictly
after 600 elapsed seconds. Shared origin watermarks live while their node is
live and for at least 1,800 seconds after acceptance; unsafe UTC pauses their
expiry. Telnet bulletin dedupe remains a separate configurable final fanout
filter. `forward_spots: false` leaves the peer transit spot cache unused while
local ingest and the local DX-command exception continue.

## Diagnostics And Persistence

Optional SQLite tables are diagnostic projections, never restored live routing
authority. Slow or failed storage does not block the protocol owner. Oversized
diagnostic generations are refused whole; a prior database snapshot can remain
stale. Current edges are in `peer_pc92_typed_edges`, keyed by parent, call and
kind (`0` user, `1` node). The former `peer_pc92_edges` table is historical and
must not be combined with current nodes as a current graph. Check the bounded `Peering:` capacity/clock/projection diagnostic before
assuming a remote failure.

Acceptance evidence and outstanding qualification are tracked in
[`docs/pc92-qualification.md`](../docs/pc92-qualification.md). A component
reference test or diagnostic smoke alone is not a sustained-load qualification.


Detailed peer events and `logs/peering_overlong.log` are owned by the sibling
`peerdiag.exe` (Windows) or `peerdiag` (Linux). The cluster does not search PATH.
Keep the companion beside the cluster executable, built from the same source.
The console Ingest Sources panel retains connectivity and peer callsigns but
does not display peer-log health or loss counters. The manager's
`DiagnosticStats()` API retains logging state and known `dropped` and
`unconfirmed` write counts. A write without a successful
acknowledgement is unconfirmed, since the file may already contain part or all
of it. Diagnostic overload can discard records; it cannot discard protocol
traffic. A failed process termination retains its resource charge and blocks
replacement. The general logger continues serving nonpeer components.

The optional topology store uses one serialized connection in the pinned local
SQLite fork; other application databases keep their existing drivers. One
process-wide reservation covers opening, active work and failed retirement, so
a second topology-enabled manager cannot overlap it. Database deadlines include
waiting for that owner. A configured database that cannot fit its memory budget
refuses startup with an error and preserves the file and committed data. A
runtime persistence failure leaves live protocol authority intact. Disabling
persistence remains the explicit empty `topology.db_path` setting.
If a Windows native file close fails after consuming its Go handle state,
persistence retains its reservation and refuses replacement until process
restart; it cannot safely retry that consumed handle.

Current qualification has demonstrated a DSN compatibility failure:
`_pragma=data_store_directory(...)` no longer redirects later relative opens
by the application's other SQLite driver. This remains an unresolved defect
against the selected compatibility contract, not an accepted configuration
policy. See the [SQLite evidence](../docs/pc92-v15-sqlite-validation.md).

The resource ceiling and final qualification remain open until the complete
source ownership proof and original workloads pass. See
[ADR-0234](../docs/decisions/ADR-0234-peer-owned-resources-and-exact-spot-keys.md)
and [v15 evidence](../docs/pc92-v15-validation.md).

## Operator View

The main landing page now keeps only the high-level peering summary. The detailed forwarding and receive-only behavior belongs here because it is implementation-heavy and changes more often than the operator quickstart.

For the high-level overview, see [`../README.md`](../README.md).
