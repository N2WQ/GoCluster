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
