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

## Must Avoid

- Do not expose private peer hosts or passwords in examples.
- Do not treat all peer behavior as ordinary spot dedupe.

## Sources

- `customgpt/troubleshooting-index.md`
- `peer/README.md`
- `telnet/README.md`
- `data/config/README.md`
- `docs/troubleshooting/TSR-0018-peer-bulletin-duplicate-fanout.md`
