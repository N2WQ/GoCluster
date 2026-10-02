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
