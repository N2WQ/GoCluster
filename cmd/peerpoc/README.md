# Peer diagnostic client

`peerpoc` connects to a configured peer, prints received and sent protocol
frames, and sends initialization and periodic keepalive frames. Use it only
against a peer you operate or have permission to test. It is a diagnostic
client, not the cluster's production peer manager.

Run `go run ./cmd/peerpoc -help` for every flag and its default. Choose `-host`,
`-port` and `-call` for your peer; supply `-password` only when required.
Automatic credential sending and reconnect are enabled by default.
`-reconnect=false` makes a single attempt. Dial, EOF, read, write and protocol
failures in that mode exit unsuccessfully; deliberate local cancellation exits
zero. In reconnect mode, failed attempts retain the configured bounded backoff.

Each connection owns one reader, one cancellation interruptor and at most one
keeper. The first PC22 starts the keeper using the mode selected at that time.
Repeated PC22 does not add workers. A later PC18 can receive a compatibility
initialization response without changing the keeper's captured mode.
Spot-based establishment alone starts no keeper or BYE ownership; a later first
PC22 does. `-keepalive=0` disables the periodic keeper.

Cancellation interrupts blocked socket I/O and joins the keeper and interruptor
before a best-effort BYE and socket retirement. BYE retains a two-second bound;
an unusable connection or failed buffered writer can prevent its delivery.
Its failure does not replace the original session failure. A replacement
connection starts only after the previous attempt has retired.

The [execution record](../../docs/command-fix-execution.md) distinguishes session
ownership qualification from the separate absolute phase-budget and
cross-reconnect timestamp checkpoints. Local fixtures do not establish
external-peer compatibility, firewall acceptance or long-running leak freedom.
