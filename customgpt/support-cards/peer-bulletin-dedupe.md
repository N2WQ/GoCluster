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
- V9 rejected an isolated replacement-driver candidate; production modernc
  SQLite remains unchanged. Do not attribute the candidate's failures to the
  current driver. The complete 480 MiB allocation proof is still open; see the
  [v9 evidence report](../../docs/pc92-persistence-feasibility-v9.md).

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
