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

WWV/WCY (`PC23`/`PC73`) and announcement (`PC93` to `ALL`/`*`) frames are parsed in the peer layer and then delivered to telnet as bulletins. Peer loop suppression keys use canonical payload fields, not raw hop-bearing wire text, so the same bulletin arriving with different hop values is treated as one peer event before telnet delivery.

## PC18 and PC92 compatibility profile

The selected reference is DXSpider revision
`3e9b3621d94dd45c68702e4a0f896aac33f2a91d`. The supported PC92 actions are
**A, C, D and K**. Unsupported F/R and unknown actions do not update topology or
freshness. CCCluster retains its documented startup and direct local
publication, and is excluded from transit PC92 broadcasts. This does not claim
complete CCCluster interoperability or a full legacy topology bridge.

PC18 identifies GoCluster using the actual runtime version, commit, build time
and Go version. Those fields are distinct from the numeric compatibility
version/build (`5457`/`633` by default). An explicitly empty `node_build` is
omitted. Enabled peering rejects invalid identities and metadata at startup;
it does not silently continue with an invented banner.

PC92 membership comes from current telnet sessions. Available user/peer IPs
are published; there is currently no per-user IP-publication preference.
Canonical callsign collisions, names outside the publication envelope, and
local/configured-peer name collisions stay local. Private PC93 delivery
requires exactly one current matching telnet owner. A departed session cannot
withdraw or receive on behalf of its replacement.

The manager owns one ordered local-origin timestamp sequence, remote topology,
and shared PC92/PC93 freshness. A valid C replaces the complete subject
membership; A/D update it; only fresh C/K refresh node liveness. Startup records
are staged until the authenticated session wins establishment. Losing or failed
candidates cannot alter global authority. Recovery always publishes complete C
followed by metadata A, even when periodic C/K are disabled.

## Bounds and operator recovery

Enabled peering supports at most 64 configured peers, 128 pending candidates,
1,000 local session records, and a 64 KiB peer frame envelope. Smaller configured
frame limits remain effective. The complete local snapshot is never truncated.
The detailed per-class and allocation limits are in
[`docs/pc92-qualification.md`](../docs/pc92-qualification.md).

| Condition | Runtime behavior | Resume condition |
| --- | --- | --- |
| Complete local snapshot does not fit | Close/gate PC9x sessions; local users and established legacy links continue | Complete snapshot fits, including capacity reserved for configured peers that can reconnect; stable for one second |
| Unsafe or stalled UTC clock | Close/gate PC9x within five seconds; retain freshness and issuance protection | UTC advances beyond retained issuance and remains healthy for one second |
| Authoritative PC92 admission fails | Close the affected link, mark its knowledge incomplete and gate retry | The resource required by the refused record has headroom for one second |
| Spot, PC93 or bulletin dedupe pool fills | Refuse new untrackable work in that class; keep links open | Payload TTL expiry frees space; refused work is not replay queued |
| Control queue overload/age or stalled write | Close the affected transport | Ordinary reconnect/backoff and full membership/metadata recovery |

Gate recovery is observed within the next second. Configured reconnect backoff
(maximum 300 seconds) and phase deadlines still apply; after handshake completes,
complete C then required A must be delivered within five seconds. Reconnecting
alone cannot clear a still-failing publication, clock or admission condition.

Spot, PC92, PC93 and WWV/WCY payload caches have separate budgets. Unexpired keys
are never evicted; duplicates do not extend their age. Payloads expire strictly
after 600 elapsed seconds. Shared origin watermarks live while their node is
live and for at least 1,800 seconds after acceptance; unsafe UTC pauses their
expiry. Telnet bulletin dedupe remains a separate configurable final fanout
filter. `forward_spots: false` leaves the peer transit spot cache unused while
local ingest and the local DX-command exception continue.

Optional SQLite tables are diagnostic projections, never restored live routing
authority. Slow or failed storage does not block the protocol owner. Oversized
diagnostic generations are refused whole; a prior database snapshot can remain
stale. Check the bounded `Peering:` capacity/clock/projection diagnostic before
assuming a remote failure.

Acceptance evidence and outstanding qualification are tracked in
[`docs/pc92-qualification.md`](../docs/pc92-qualification.md). A component
reference test or diagnostic smoke alone is not a sustained-load qualification.

## Operator View

The main landing page now keeps only the high-level peering summary. The detailed forwarding and receive-only behavior belongs here because it is implementation-heavy and changes more often than the operator quickstart.

For the high-level overview, see [`../README.md`](../README.md).
