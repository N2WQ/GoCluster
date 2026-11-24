# GoCluster shared SSID qualification

Phase 3 implements Scope Ledger v8 and unified scope Rev 5 against `main`
at `e996e0002bda7139a4687ff63124fab13cf731f9`. It covers D27–D40 and A24–A38.
The Rev 5 Markdown export has SHA-256
`cc7f0853646a63c652552a090dbff74bbaf35b6f490798f54fc9d422c511bbe2`.
The final change hashes to
`11656018270474963735ce0d8c170f63b957805b2b088c5642806dbdb8a0c58c` across
105 modified or new `.go` and `.py` paths. The digest uses sorted repository
paths, each followed by NUL and the checkout's file bytes.

The scope is the server. Public launch also requires the separately built
console to pass login, change notice, reconnect and filter-edit journeys.
The [operator guide](../../telnet/README.md#browser-and-telnet-on-one-callsign)
covers GO, failed saves, independent delivery and private TALK behavior.

## Implementation and acceptance evidence

| Criteria | Implementation and verification |
| --- | --- |
| A24–A25, D27–D28, D35–D36 | Real Telnet and WebSocket sessions exercise both login orders, an independent SSID, same-kind replacement and global, base-call and per-IP refusal. A cross-IP WebSocket replacement at the destination IP limit leaves both incumbent sockets usable. `configuration_session.go` owns the charges and current-slot markers. |
| A26, D29, D37, D40 | Both sessions share the live profile. Real reconnect tests check the complete saved record separately from the live readback, including an active failed edit, IP order and the two orders of login metadata versus a successful preference write. Existing schema 7 tests cover `if_revision` and commit-before-publish. `configuration_session_v7.go` keeps the authoritative disk read and metadata commit inside the lease. |
| A27–A28, D30–D31 | The browser selects GO on registration. `DIALECT CC`, preset load, and schema 6 and 7 settings updates check for a current browser before a CC selection. Slash-style history queries are accepted by the GO dispatch and help. The recovery tests check the live transition, persistence retry and the logger's command guidance. |
| A29, A31, D32, D34, D39 | The two login orders deliver browser presence to the logger. The blocked-reply test checks the logger's notice for a browser edit. A different SSID saves and deletes a preset while the original pair receives separate text and JSON refresh notices. The browser's profile revision tolerates that preset change and rejects a stale edit after the logger changes preferences. Browser login transcripts retain the Phase 2 address exclusion. |
| A30 | The logger runs CC11 after reconnect and the browser runs JSON. Both receive the same live spot through the ordinary broadcast path. Existing per-connection tests cover pauses, queues, the matching projection, and CC11 history serialization. |
| A32–A33, D33, D35 | A pair contributes one entry and two slots. The representative snapshot and notification count stay unchanged during a browser replacement. The logger's pre-flap recipient still delivers a private message. A logger departure changes the advertised IP, rejects the stale recipient and makes the browser eligible. The final departure publishes removal. Existing peer tests cover canonical collisions and PC92 framing. |
| A34–A35, D38, D40 | The departure fixture checks both cleanup orders, one retry per dirty departure, both final save outcomes against disk, and a later clean login. A rejoin while the last old writer is blocked inherits the unsaved profile and leaves the saved record intact. The blocked edit-reply fixture checks disk and live readback after loss after commit. The partial upload fixture and existing schema 7 admission tests cover loss before commit. |
| A36–A37, D37–D39 | A logger held in a write survives 100 browser flaps and receives one summary. A sibling edit succeeds during browser upload and readback stalls. Dual replacements alternate browser IPs at the base-call limit, followed by shutdown with empty capacity accounting and returned configuration reservations. Rejoins retry only pending GO persistence and keep login metadata updates. The native CC11 transcript checks the live spot, the separate browser notice line, and the history row after re-enabling VE7CC under GO. |
| A38 | See the silent TCP failure measurements below. |

The new socket checks are in [shared_ssid_test.go](../../telnet/shared_ssid_test.go).
The reader and writer fixtures use temporary user storage and explicit gates
for pending reads, disk commits and blocked writes. The peer checks observe
the local provider and routing fence. Live peer-network storm measurements
remain open.

## Configuration lease audit

The baseline had 20 ordinary `acquireConfiguration` callers and one direct
`acquireConfigurationWithRecheck` caller. Phase 3 adds the registered `DX`
command fence. Test hooks aside, the current paths release the lease before
socket I/O or a writer receipt.

| Call sites | Boundary |
| --- | --- |
| `acquireLoginCapacity`, `unregisterClient`, `Client.saveFilter` | Admission uses the existing prelogin callback, capacity reservation and durable record transaction. The greeting and duplicate close run after the lease is released. Departure waits for writer retirement before taking the lease and rechecks the slot afterward. |
| `runPreferenceCommand`, filter engine `Handle`, `handlePresetCommand`, `processRegisteredCommand` | Stage, validate, save and publish preferences or admit a spot. The command loop sends the response after the call returns. Shared notices are nonblocking offers to the existing writer. |
| `renderBooleanLoginBlock`, `renderV7CCLoginBlock`, `prepareV7ResetNotice`, `acknowledgeV7ResetNotice` | Render snapshots or persist a completed delivery receipt. Greeting and reset notice output are outside the lease. |
| `handleHumanReadback`, `prepareBankedV6Readback`, `prepareV6MachineReadback`, `prepareV7MachineReadback` | Prepare a detached response under the lease. The writer receives it after release. |
| `applyBankedV6MachineRequestOwned`, `applyV6MachineRequest`, `applyV7MachineRequestOwned` | Read and validate the upload envelope before entering these commit paths. They return a response without waiting on the socket. |
| `beginHistory`, `publishHistory` | Capture a connection's cursor and filter under the lease. Archive scanning and writer completion are separate. The enqueue under the lease is nonblocking. |
| `handleJSONCommand`, `handleVE7CCCommand` | The format request uses a nonblocking control enqueue. Replies, close handling and any switch completion wait follow release. |

## Resource and compatibility checks

The primary map holds one representative per SSID. The companion map contains
only pairs and is bounded by admitted connections. Disconnect and replacement
invalidate the cached fan-out projection. The advertised IP prefers Telnet
while the logger is present. Both transports use the existing prelogin and
reputation accounting. The existing peer wakeup coalesces changes and the
publisher samples membership at its fixed 100 ms service interval. An IP
change for a current SSID updates its metadata in one A record. The original
PC92 sequence budget and recovery pacing still apply.

Registration, replacement and unregister use the transport slot. Keepalives,
self-spots, bulletins, solar summaries, `BroadcastRaw`, fan-out shards and
`Stop` visit both slots. `GetClientCount`, `ListClientCallsigns` and peer
membership visit one representative per SSID. Private delivery also checks
that the selected representative and membership revision are still current.

Each live writer owns five latest advisory views, up to 16 pending presence
transitions and a one-slot wakeup. Pressure collapses pending transitions into
one count and latest presence. Retirement discards these notices. No new
writer, timer or separate goroutine is created for the profile or its notices.
Each connection retains its own writer, heartbeat, format, pause and history
resources.

Six alternating native Windows benchmark pairs at `GOMAXPROCS=1` measured the
dispatch median at 307.1 ns before and 311.6 ns after (`p = .937`). Single-
and eight-recipient COMMENT fan-out medians were 2.712 vs 2.619 ms and 21.73
vs 21.21 ms. Dispatch stayed at zero allocations. Fan-out stayed at one and
eight allocations (48 and 384 bytes). A concurrent test consumed about three
CPU seconds over the 27.55-second comparison, which limits interpretation of
the timing samples.

## Silent TCP failure

The [opt-in fixture](../../telnet/phase3_blackhole_qualification_test.go)
and its [Linux peer](../../telnet/testdata/phase3_blackhole_peer.py) install a
socket receive filter after login. The peer remains open until GoCluster
releases the slot. The packet capture checks that the server actually
retransmits and the peer sends no ACK, FIN or RST after the filter takes
effect. The timer ends at the real registry removal after kernel failure.
The fixture uses the shipped 120-second application keepalive and existing
TCP keepalive request. These times describe the tested systems and TCP path.

| Server | Baseline | Phase 3 | Phase 3 capture |
| --- | --- | --- | --- |
| Windows 11, native Go socket | 2m18.911s–2m18.931s | 2m18.943s–2m18.956s | 10 server packets, zero peer ACK/FIN/RST |
| Ubuntu 24.04 WSL2, kernel 6.6.87.2, `tcp_retries2=15` | 17m42.763s–17m42.776s | 17m43.406s–17m43.419s | 17 server packets, zero peer ACK/FIN/RST |

Windows established a direct outbound TCP connection to the WSL peer and
fed that native socket through the server's normal accept loop. Linux
accepted a local TCP connection on loopback. Host firewall and TCP policy
were left in place. Both retired to zero occupied slots after kernel failure.
Later changes to notice delivery, paired dirty saves and stale spot fan-out
do not run in the idle, single-Telnet-session fixture.

## Local validation

| Check | Observed result |
| --- | --- |
| Latest shared SSID and WebSocket profile framing tests | Passed twice under the race detector on Windows and Ubuntu WSL2. |
| Affected package lane on Ubuntu WSL2 | `commands`, `filter`, `peer` and `telnet` passed ordinarily and under the race detector. The Telnet lane used the final production code with an earlier revision of the recovery tests. |
| Full Ubuntu WSL2 lane | `go test -p 6 -timeout 30m ./...` passed all 61 test packages. The remaining 16 packages had no test files. The later fixture-only changes passed in the focused race rerun. |
| Full native Windows lane | `scripts/test-windows.ps1 -Timeout 30m` passed 15,751 cases across 62 tested packages, with 101 skips and 14 build-only packages. |
| Windows Telnet package | The full package passed under the race detector. Later capacity, revision and last-departure fixture additions passed in the focused race run. |
| Command and writer fuzzing | `FuzzHistoryCommand` and `FuzzJSONStringChunks` passed on Windows with 35,692 and 12,878 generated inputs. |
| Static checks on Windows | `go vet ./...`, `staticcheck ./...`, `golangci-lint`, generated code-map checks and `git diff --check` passed. |

An earlier Windows run returned `configuration_busy` in one command-pacing
case. It passed 30 isolated repetitions and the final full Windows lane.
The ordinary suites skip the opt-in A38 fixture. Its separate results are
recorded above.
