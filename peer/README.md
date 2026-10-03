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

- inbound peer spots still ingest locally
- inbound peer spotter calls ending in the skimmer marker `-#` strip only that terminal marker before local ingest; numeric SSIDs are preserved
- maintenance traffic still runs
- only local `DX` command spots are peer-published

With `forward_spots: true`:

- normal transit forwarding is re-enabled
- local acceptance still gates whether relayed traffic continues onward

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
Go version. Release-script builds include a separate release tag (261003-r2);
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
Peer status reports `ready`, `disabled`, `degraded` or `cleanup-failed`, with
known `dropped` and `unconfirmed` write counts. A write without a successful
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
