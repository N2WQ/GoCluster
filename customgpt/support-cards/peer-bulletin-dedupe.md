# Support Card: Peer Bulletin Dedupe

## Match

Use when a node operator or telnet user reports duplicate peer bulletins,
surprising peer fanout, peer topology confusion, or asks about peer passwords.

## First Safe Check

Classify whether the issue is peer connection state, spot forwarding, bulletin
dedupe, topology cache, or private peer configuration.

## Must Include

- Bulletin dedupe is separate from ordinary spot dedupe.
- Duplicate bulletin behavior may involve peer fanout and canonical bulletin
  payload keys.
- Peer hostnames, passwords, and private topology details must stay redacted.
- Peer spot, PC92, PC93 and WWV/WCY pools have independent limits and do not
  evict unexpired keys. Telnet bulletin dedupe is a separate final fanout filter.
- Distinguish publication overflow/clock gates (PC9x-wide) from authoritative
  admission failure (affected peer) and payload-pool refusal (affected class).
  Reconnection alone does not resolve a still-failing gate.
- PC92 supports A/C/D/K against the pinned reference profile. Current-session
  membership and available IPs are published; no IP preference command exists.
  Recovery C/A is mandatory even with periodic C/K disabled.
- PC18 identifies GoCluster truthfully; numeric compatibility values are not
  its product version. Optional topology SQLite rows can be stale diagnostics.
- V9 rejected an isolated unmodified replacement-driver candidate. V15 now
  integrates a narrowly repaired, pinned SQLite fork only for peer topology;
  other database owners keep their existing drivers. The historical v9 failures
  do not describe the repaired candidate's current results. Complete allocation
  proof and original final-source qualification remain open; see the
  [v15 record](../../docs/pc92-v15-validation.md).
- Keep the sibling peerdiag companion with the cluster executable. Detailed
  peer logs moved to that dedicated owner. The console Ingest Sources panel
  shows connectivity and peer callsigns, without peer-log health or loss
  counters. The manager's `DiagnosticStats()` API retains known dropped records
  separately from unconfirmed file writes. Logging failure does not itself
  close peering. Failed native/process cleanup retains its owner and prevents
  an overlapping replacement.
- A configured topology database that exceeds its resource budget refuses
  startup while preserving committed data. Runtime persistence failure does
  not replace or reset live protocol authority. Empty topology.db_path remains
  the explicit disable setting.
- Ordinary ingestion spot caches now compare their complete existing encoded
  keys. The former32-bit collision could lose a local spot while peer relay
  succeeded; this is distinct from PC92 payload-cache admission or topology
  loss. Check complete token evidence before attributing a missing spot.

## Must Avoid

- Do not expose private peer hosts or passwords in examples.
- Do not treat all peer behavior as ordinary spot dedupe.
- Do not infer full DXSpider daemon or CCCluster parity from component tests,
  or sustained qualification from a short smoke. Check the qualification record.

## Sources

- `customgpt/troubleshooting-index.md`
- `peer/README.md`
- `telnet/README.md`
- `data/config/README.md`
- `docs/troubleshooting/TSR-0018-peer-bulletin-duplicate-fanout.md`
- `docs/pc92-qualification.md`


V11 troubleshooting refinements:
- One second is successful membership control-queue admission for healthy
  established peers, including during recovery. It is not a receiver-processing
  acknowledgement; the five-second recovery limit does not extend it.
- Inspect `PC93InputRefused` for controller mailbox pressure and `PC93Refused`
  for message-cache saturation. Neither means PC92 authority exhausted.
- Callsign normalization can merge portable/SSID aliases. Check unique current
  ownership before assuming a missing publication or private message is a fault.
- Current SQLite edges use `peer_pc92_typed_edges` with node/user kind. Old
  `peer_pc92_edges` rows are historical; never combine them with current nodes.
- Go qualification observations are provisional. Use the wrapper's final
  verdict, source manifests and retained binary; open allocation proof or
  missing final-source profiles still prevents overall acceptance.
- A Q4 queue at its record limit can still miss the required byte population.
  Inspect per-peer pressure evidence and the fixture admission barrier before
  attributing a failed capacity diagnostic to production transport behavior.
- Evidence: [v11 ledger](../../docs/pc18-pc92-scope-ledger-v11.md),
  [ADR-0231](../../docs/decisions/ADR-0231-pc92-audit-corrections.md).

V12 re-audit refinements:

- Distinguish raw PC92 wire identity from local login normalization. Malformed
  trailing/leading slash and oversized SSID forms reject the complete record;
  a successful human login does not establish raw wire validity.
- K numeric omission/zero clears prior subject version/build to zero. An
  observed zero is not evidence that the remote node disconnected; K preserves
  membership and has separate liveness semantics.
- V14 admission refusal closes the affected link, invalidates its ingress, then
  permits controlled retries. Diagnose cooldown, waiting for a fair startup
  grant, active attempt,60-second healthy-reset interval and global gating using
  sampled `Peering: PC92 retries` counts. `eligible` means no owned candidate is
  currently waiting; it does not promise a remote has connected.
- Backoff is shared across inbound/outbound overload attempts; grants are at
  least one second apart. Authentication and original handshake deadlines still
  apply. Waiting/denied arrivals and pure global closure do not advance history.
- Matching post-establishment C/A local Flush and successful establishment/replay
  start the60-second interval; enqueue/initial A/K cannot substitute. Local
  recovery does not prove remote topology complete. Refused frames are neither
  retained nor automatically replayed.
- Add required integer `peering.max_peers` (1–64, shipped64) to existing YAML.
  Restart to change it; active enabled rows above the cap fail. Pending remains
 128 and transport owners N+128. Do not silently truncate a configured registry.
- V12's sustained63-blocked/one-live experiment failed while its zero-blocker
  control passed. V14 supersedes that recovery mechanism; its short service
  gate is not full qualification. Use [v14 evidence](../../docs/pc92-v14-validation.md)
  and keep correction completion separate from the open aggregate allocation
  proof and required final-source profiles.
- The retained CTY refresh has separately recorded provenance and exact hashes;
  do not silently compare new-asset qualification to old-asset runtime results.
- Sources: [v12 validation](../../docs/pc92-v12-validation.md),
  [ADR-0232](../../docs/decisions/ADR-0232-pc92-wire-and-recovery-evidence.md),
  [CTY refresh](../../docs/cty-refresh-2c06079.md).


## Continued qualification (2026-10-02)

- Full sustained-cache qualification passed. Corrected Q1 fixture pings kept
  all 16 peers connected through the 45-minute load and 11-minute drain;
  all required PC92 relays and peer spot deliveries arrived.
- Q1 still failed local spot delivery and sustained latency. Every local client
  missed the same nine spot IDs. Exact pair reproductions matched one primary
  and eight SLOW-cache 32-bit collisions that suppress distinct logical keys.
  No shared-dedupe repair is included in v14. Do not equate peer forwarding with
  successful delivery through the shared local spot pipeline, or use a short
  preflight to certify sustained latency.
- The 480 MiB proof remains open for enabled SQLite, context child-map backing
  and peer terminal log buffers retained after transport-owner release. The
  tested blocked-logger path did not retain whole session objects; formatted
  log copies are a distinct heap owner and are not runtime stack overhead.
- Use the [continued v14 record](../../docs/pc92-v14-validation.md#continued-overall-qualification-on-2026-10-02)
  for final verdicts and diagnostic limits. Overall acceptance remains incomplete.
