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

The initial absolute handshake budget is `-login-timeout + -init-timeout`
(75 seconds with the defaults), starting before credential writes. The first
initialization while unestablished starts one `-init-timeout` budget (60 seconds
by default) before its output. Repeated markers and compatibility responses do
not restart it. Both incoming traffic and stalled outgoing phase writes are
subject to these budgets, including Telnet negotiation replies.

PC22 or a spot received after initialization ends the phase budget. Later PC18
responses do not reopen it. `-idle-timeout` retains its existing budget for each
line read; outgoing keepalive traffic does not extend that read deadline.
Established writes gain no separate idle timeout. Cancellation still interrupts
them through the owned retirement path.

PC9x timestamp state belongs to the whole command invocation and continues
across reconnects. Each record's allocation, formatting, write and flush share
the same writer lock as ordinary lines and Telnet replies. A partial or failed
send may consume a timestamp; that gap is retained and the frame is not replayed.

Each full UTC second permits 100 distinct values: its second-of-day integer,
then `.01` through `.99`. Exhaustion or an unsafe clock retires the attempt;
reconnect retains the existing backoff and timestamp state. After a backwards
clock change, reaching the previously retained second is still refused; UTC
must advance beyond it. The normal UTC midnight wrap is permitted. These UTC
values are independent of the monotonic clocks used for elapsed phase budgets.

The [execution record](../../docs/command-fix-execution.md) distinguishes session
ownership, phase-budget and timestamp-publication qualification. Local fixtures
do not establish external-peer compatibility, firewall acceptance or long-running
leak freedom.
