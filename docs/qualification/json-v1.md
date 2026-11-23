# GoCluster JSON v1 qualification

Scope Ledger v5 covers the authenticated Telnet output pilot. The reference build is `73bdaabd3991594ac776338c2fde11cca51558ad`. The fixtures use temporary profiles, local connections and offline archives. The protocol and every outbound record type are documented in the [Telnet wire guide](../../telnet/README.md#json-output) and [ADR-0374](../decisions/ADR-0374-session-json-output.md).

## Acceptance evidence

| Acceptance | What the checks exercise |
| --- | --- |
| A1–A4, A12 | Literal live records, independent JSON decoding, archived v2–v7 binaries in their original layouts, event recovery, absent CW data, beacon fallback, escaped and malformed input, and the inclusive boundary for each optional field. A live PSKReporter observation retains its longer grids through archive reopen and both JSON endpoints display six characters. Recipient view checks compare the actual text glyph and class across grid changes. A warmed encoder allocation check is separate from the socket replay. [Spot records](../../telnet/json_spot_test.go) · [Limits](../../telnet/json_limits_test.go) |
| A5–A7 | The native and Ziutek clients exchange raw TCP bytes with echo on, including a negotiation, erase, and commands sent before the switch receipts. The fixture decodes every line between the acknowledgement and the final notice. It verifies refusals and a fresh text login. Gated writes track individual queued spots, and injected short writes, disconnect, shutdown, queue saturation and duplicate-login retirement exercise switch cancellation. [Transport tests](../../telnet/json_transport_test.go) |
| A8 | Real command output from HELP, SHOW FILTER, SHOW SETTINGS and machine readback is followed by an eligible spot. Manual pause suppresses the spot and resume admits the next one. A separate clocked test exercises an overlapping automatic pause, pending hold and manual deadline at negotiation. [Producer and pause tests](../../telnet/json_producers_test.go) |
| A9–A10 | Frozen text and CC11 spot and history transcripts exercise the legacy formats. The full socket replay checks the bytes of every received spot against a separate golden file for seven source types. Byte distribution, timings, CPU profile and sampled live heap are in the [writer measurements](json-v1-writer.md). |
| A11, A13 | A real TCP GET against a maximum-size saved profile recovers the complete CRLF document. A separate renderer stress test exercises the maximum raw document size through the writer. Both schemas retain their terminal-error documents. Receipt, reservation, cancellation and replacement tests exercise queue pressure and stalled sockets. The [producer inventory](../../telnet/README.md#json-output) covers trusted labels, user-supplied text and excluded private fields. [Saved-profile GET](../../telnet/json_get_boundary_test.go) · [Renderer ceiling](../../telnet/json_string_test.go) · [Routed bulletins](../../peer/bulletin_delivery_test.go) |
| A14–A16 | The archive, real writer and continuation checks cover seven selected of 50 requested, 250 interleaved rows, same-timestamp order, early NEXT, retry, invalidation, NEARBY, own spots and reconnect cleanup. The [history measurements](json-v1-history.md) distinguish scan scratch, page backing and fixed connection costs. |

## Qualification environment and replay

The reference commit and the changed Go files and fixtures identify the tested code. For the latter, the SHA-256 is `7d4f5057e47259571a912ed17d7c6639e94089b246855c08bba9b8cf486547ed`. It hashes the 59 modified or new `.go`, `.bin`, `.ndjson` and `.txt` files in sorted repository-path order, each path followed by NUL and the tested file bytes. Both Windows checkouts contained the same files. The Linux copy used the same inputs on an ext4 filesystem.

Windows 11 Pro build 26200 and Ubuntu 24.04 under WSL2 ran Go 1.27.1 on the same Intel Core i9-10900 workstation. The native acceptance selection passed 181 cases across archive, commands, peer and telnet. Focused Windows tests repeated the clock-tie and immediate-resume cases four times. Twelve-second fuzz runs with two workers passed for `FuzzJSONSpotComments` and `FuzzJSONStringChunks` on each platform. Windows executed 577,096 and 253,310 inputs respectively. Linux executed 14,132 and 800. Seed coverage and input cost affect those counts.

### Full validation

| Host and command | Result |
| --- | --- |
| Windows, `./scripts/test-windows.ps1 -Timeout 30m` | 15,701 passing cases, 100 skips, 62 tested packages and 14 build-only packages. |
| Windows, `./scripts/test-windows.ps1 -Race -Timeout 30m` | 15,701 passing cases, the same 100 skips, 62 tested packages and 14 build-only packages. |
| Ubuntu, `go test -p 4 -timeout 30m ./...` | 61 packages passed and 16 had no test files. |
| Ubuntu, `go test -p 4 -timeout 30m -race ./...` | 61 packages passed and 16 had no test files. |
| Both hosts, `go vet ./...`, `staticcheck ./...` and `golangci-lint run ./... --config=.golangci.yaml` | All three exited successfully on each host. |

Of the 100 Windows skips in each run, 56 require an external DXSpider installation. The rest include subprocess helpers, platform-specific H3 and symlink cases, and opt-in live, replay, profiling or data-fixture campaigns. All selected JSON acceptance tests ran. The Windows runner's default 10-minute package timeout expired during an earlier cluster integration run. The final runs allowed 30 minutes per package. An earlier Linux race run lost its temporary checkout. Both final Linux suites used a persistent ext4 checkout and a separate build scratch directory, with logs written outside both.

### Socket replay

| Windows socket measurement | Observed result |
| --- | --- |
| Five uncompressed 70,000-row runs | 350,000 exact rows and first-byte observations. Median admission through verified loopback delivery 2,692 ns/spot, range 2,286–3,142. Mean 276.3 wire bytes/spot. Of 5,935 writes, 5,930 were 8–16 KiB and five were 2–8 KiB. |
| Allocation and retained heap | The warmed encoder recorded 0 B and 0 allocations per spot. The full socket fixture measured 48 B and one allocation per timestamped admission envelope. Across five post-GC samples, the full 256-slot queue retained a median 279,968 bytes over negotiated idle. Closed sessions measured a median 1,536 bytes above their starting samples. |
| Separate CPU-profiled replay | 1.4 million exact rows. The writer accounted for 70.35% of cumulative sampled CPU and the live encoder 60.68%. The two include shared call stacks. |
| Text and CC11 before/after | Ten alternating pairs pinned to the same two logical CPUs, 250,000 verified rows per benchmark per run. `benchstat` found no timing difference at its 5% threshold for normal, diagnostic, nil-server or CC11 output (p = 0.247–0.529). Allocation counts matched. Other local validation ran concurrently and the timing intervals were wide. |

The generation used to resolve clock ties occupies an existing 48-byte allocator class for both the broadcast payload and per-recipient envelope. A worker job grew from 64 to 72 bytes, adding 120 KiB of queue and batch storage at the shipped sizes. The isolated one-recipient text admission and fan-out measurement stayed at 96 B and two allocations per spot. Pinned dispatch and text-admission timing samples overlapped. Production delivery latency requires observation on the target workload. See the [Linux socket measurements](json-v1-writer.md) for profile details and reproduction commands.

## Implementation review

| Area | Production files reviewed |
| --- | --- |
| Live limits, projection and delivery | `spot/taxonomy.go`, `internal/cluster/main_runtime.go`, `telnet/json_limits.go`, `json_spot.go`, `json_string.go`, `json_writer.go` |
| Connection, modes and pause | `telnet/server.go`, `input_wire.go`, `json_session.go`, `ve7cc_session.go`, `readback_pause.go`, `control_kind.go` |
| Historical selection, publication and cleanup | `archive/history.go`, `commands/history.go`, `telnet/history.go`, `json_history_rows.go`, `json_history_stream.go`, `ve7cc_history.go`, `bulletin_history.go` |
| Machine and human replies | `telnet/machine_commands.go`, `machine_v6_commands.go`, `machine_v6_banked_commands.go`, `machine_v7_commands.go`, `configuration_transfer.go`, `configuration_readback.go`, `configuration_render.go`, `human_reserved_readback.go`, `native_cc_readback.go`, `dedupe_readback.go`, `reset_notice_v7.go` |
| Bulletin metadata and TALK routing | `peer/manager.go`, `pc92_controller.go`, `pc92_receive.go`, `telnet/peer_membership.go` |
| Command help and support | `commands/configuration_help.go`, `cc_filter_help.go`, `help_filters.go`, the two reviewed FILTER EXAMPLES screens, `telnet/README.md`, `customgpt/source-map.md`, the domain contract, decision index and generated code maps |

Test changes include the linked suites above, `telnet/configuration_readback_test.go`, `commands/configuration_help_test.go`, the six native archive fixtures and the frozen seven-record TCP replay fixture. Review traced every tagged control to its writer, the archive iterator and projected-row lifetime, and the configuration reservation through flush or discard. The slow-client review found two issues. Every registered session now signals retirement even with pacing disabled. A live spot spanning multiple writes now records latency when the first batch containing its bytes flushes.

A TCP test exercised the normal saved-profile and GET paths on both operating systems. A 196,608-byte configuration passed admission and the same configuration with one extra byte was rejected. The accepted profile returned 261,778 raw YAML bytes with its actual login status and revision. The JSON wire record was 262,210 bytes and decoded byte for byte to that same session’s text-mode response. Separately, the renderer test supplied the widest status and revision used by the capacity calculation. It produced 262,144 raw bytes and recovered them from a 262,708-byte JSON line delivered in ordinary socket batches. [Saved-profile GET](../../telnet/json_get_boundary_test.go) · [Renderer ceiling](../../telnet/json_string_test.go)

## Resource limits

The hello advertises a per-node wire bound including LF. The calculation takes the greater of the enabled live-input envelope and the readable archive envelope, then adds fixed grammar, configured event names and optional fields. It allows for escaping before comparing sizes and rounds very large configurations upward to a number representable by JSON clients. The node and the two country labels each allow 256 original bytes. Each independently becomes null above its limit. The wire bound does not allocate a buffer of that size. [Limit derivation](../../telnet/json_limits.go)

While JSON is active the writer borrows its configured ordinary byte batch and at most 64 enqueue timestamps. It uses one 6,146-byte scratch array for quoting and spot encoding. On entry the writer releases any oversized legacy batch backing. A raw schema 7 document is at most 262,144 bytes. For the renderer's UTF-8 YAML, the maximum escaped envelope is 524,300 bytes including LF. Large rows and documents span bounded writes. Each write has the ordinary socket deadline. [String encoder](../../telnet/json_string.go) · [Connection writer](../../telnet/json_writer.go)

A positive history page charges its owned projected strings, row and speed-vector allocations and page overhead. The largest tested 250-row projection used 150,656,896 bytes of the 167,772,160-byte process history budget. Up to two collecting scans can run concurrently. The 128 MiB per-scan estimate covers application candidate work. The estimates and post-GC samples measure different resource owners. Whole-process peak RSS and production latency require a workload on the target host.

On Windows, a broadcast admitted just after a successful RESUME can share the command’s wall-clock and monotonic timestamp. The server stamps a broadcast once at admission with the current pause generation. On an exact clock tie, the JSON writer can distinguish a new arrival from a spot already in the pipeline when the pause ended. Text and CC11 keep the existing time-based cutoff.
