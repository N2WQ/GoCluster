# ADR-0218: Multi Human Upstream Telnet Registry

- Status: Accepted
- Date: 2026-07-10
- Decision Origin: Troubleshooting chat

## Context

The runtime configuration exposed `human_telnet` as a single `RBNConfig` and
the cluster runtime owned one `humanTelnetClient`. An operator reasonably
configured more than one upstream server, but the typed YAML shape and
singleton startup, health, dashboard, and shutdown paths could only represent
one. This contradicted the intended deployment model: a node may consume any
practical number of independent human/upstream DX-cluster telnet feeds.

Multiple long-lived upstreams also require explicit bounds and ownership. A
failed server must not hold cluster startup or another server hostage; stale
connection generations must not affect replacements; aggregate queues must
remain bounded; shutdown must not close a shared raw channel while a producer
can still send; and operators need one truthful state per configured server.

ADR-0181 established bounded startup retry and per-feed liveness for the two
production RBN feeds. This decision extends the same client lifecycle
foundation to an ordered human/upstream registry without adopting peer
protocol, password, topology, or manager semantics.

## Decision

Make `human_telnet` an ordered registry with these config rules:

- accept the canonical YAML sequence and the historical single mapping as a
  one-entry compatibility form
- allow zero to 64 entries and require every field even when an entry is
  disabled
- preserve configured name case and YAML order; require names matching
  `[A-Za-z0-9][A-Za-z0-9._-]{0,31}` and case-insensitive uniqueness
- reject duplicate identities after lowercasing and trimming the host, keeping
  the exact port, and upper-normalizing the trimmed callsign
- require each `slot_buffer` to be `1..64000` and cap the sum across enabled
  entries at `64000`

Reuse one `rbn.Client` per enabled entry with the existing minimal parser,
transport selection, per-client bounded spot channel, `UPSTREAM` provenance,
bad-call reporting, dedupe/flood pipeline, and nonblocking raw passthrough.
Each client starts independently through context-aware `Start(ctx)` and owns a
single serialized dial/login/read/retry supervisor with generation-local
sockets and workers. Retry is capped and jittered; cancellation and `Stop`
join all client work.

The cluster runtime owns one shared raw announcement channel. Shutdown cancels
and stops every client, joins the spot forwarders, closes the shared raw channel
exactly once, and joins the raw consumer. Clients never close that caller-owned
channel.

Expose every enabled entry independently in ingest health and the console as
`HUMAN/<name>`, in YAML order. Each contributes one enabled source and one
connected source only while its current TCP generation is connected. Do not
add an aggregate Human row. Retain every row in a scrollable console pane with
a maximum visible height of ten rows.

## Alternatives considered

1. Add numbered singleton keys such as `human_telnet_2`.
   - Rejected because it creates a fixed, repetitive schema and duplicates
     loader/runtime wiring instead of using the existing peer-style registry
     pattern.
2. Reuse `peer.Manager` and peer connection configuration directly.
   - Rejected because human feeds are line-oriented ingest sources, not peer
     protocol sessions; peer passwords, topology, PC92, direction, and manager
     shutdown semantics do not apply.
3. Start one goroutine per feed around the old mutable client connection fields.
   - Rejected because stale readers, keepalives, reconnects, and Stop could race
     replacement sockets or shared-channel closure.
4. Display only an aggregate Human health row.
   - Rejected because it hides which named upstream is failed and cannot
     truthfully count independently connected servers.

## Consequences

### Benefits

- Operators can define up to 64 independent upstream telnet servers in the
  same ordered style used for peer registries.
- One unavailable server remains red and retrying without blocking healthy
  upstreams or cluster startup.
- Parser, provenance, dedupe, flood, and downstream fan-in behavior remain on
  established code paths.
- Per-entry bounds plus an enabled aggregate cap keep retained spot queues
  explicit and finite.
- Console and ingest health state identify every enabled upstream separately.

### Risks

- A bad entry retries for process lifetime. Capped jittered backoff prevents a
  tight loop, while the red `HUMAN/<name>` row and ingest event log expose it.
- Up to 64 feeds increase sockets and goroutines linearly. The hard registry
  and queue bounds constrain that growth.
- The compatibility mapping creates two accepted YAML shapes. The sequence is
  canonical, and tests keep both forms explicit.
- Human feed names become operational identities. Renaming an entry changes its
  `SourceNode`, health identity, and dashboard label after restart.

### Operational impact

- Existing single-map `human_telnet` configurations continue to load as one
  entry; the checked-in example now demonstrates two disabled list entries.
- Config errors identify indexed entries. Unknown fields remain warnings.
- A console operator may need to focus and scroll the Ingest Sources pane when
  more than ten rows are present.
- No peer protocol/config behavior, dynamic reload, parser grammar, spot
  admission, dedupe policy, flood policy, or performance target changes.

## Links

- Related issues/PRs/commits: operator report and 2026-07-10 troubleshooting chat
- Related tests: `config/human_telnet_config_test.go`,
  `rbn/client_lifecycle_test.go`,
  `internal/cluster/human_telnet_runtime_test.go`,
  `internal/cluster/main_stats_test.go`, `ui/dashboard_v2_sources_test.go`
- Related docs: `README.md`, `data/config/README.md`,
  `docs/OPERATOR_GUIDE.md`, `docs/domain-contract.md`, `rbn/README.md`
- Related TSRs: TSR-0032, TSR-0031
- Supersedes / superseded by: extends ADR-0181; supersedes none
