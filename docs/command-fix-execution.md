# Command fixes: approved v3 execution

Repository: `C:\src\gocluster`, branch `fix_commands`.
Authority: the user's exact `Approved v3` selection on 2026-10-09.
The [approved planning transcript](command-fix-scope-ledger-v3.md) retains its
original review baseline and historical statements. Execution starts at
`03fd055b41952f1643433c467caa6b9d6aa29c0c`, preserving the intervening HELP and
stable Windows runner changes. No scope or policy change is inferred from drift.

Checkpoints execute sequentially with their tests and direct diff review. A
dependent change rolls back with its dependents or in reverse dependency order.
Routine testing requires no user action. External cluster tests, deployment,
pushes, firewall changes and workflow changes are outside this scope.

Current progress: checkpoints 00, 01 and 02 complete; checkpoint 03 is next;
43 checkpoints remain.

## Checkpoint 00: unattended checkout-server fixture

Status: complete. The matrix records the
obligations established before implementation; observed evidence follows.

The fixture uses real `config.Load`, taxonomy/IARU/CTY/H3 loaders,
`commands.Processor`, telnet startup/dispatch, per-user storage and the Pebble
archive. A private held listener binds `127.0.0.1:0`; production `Start` shares
the same startup sequence and retains its existing binding policy. Only tracked
configuration is staged. Optional tool/secret configuration is omitted, network
URLs become loopback placeholders, and feed/refresh workers are never started.

The fixture's bounded manual-spot bridge snapshots into archive and telnet
broadcast. It does not prove production correction, dedupe, reputation,
classifier or runtime retirement. Checkpoint 20b owns production pipeline proof.

### Contract-to-test matrix, completed before implementation

The post-approval inherited-context test-strategy review was read-only. It was
not an independent scientific review. All identified false-green paths are
dispositioned below; none needs a new policy decision.

| Contract / failure | Stimulus and required observation | Checker |
|---|---|---|
| Held listener reaches actual checkout | Login and correlated GET through the listener's actual endpoint | Real child/socket transcript |
| Stale binary or wrong endpoint | Fresh build; compare source before/after, executable/config hashes, PID and nonce; SHOW BUILD must expose child nonce | Driver manifest and wire identity |
| Loaded inputs reach consumers | Fixture greeting, CTY Canada lookup and FN31 mapping/GRID control | Child H3 checks and wire controls |
| Tolerant frame decoding hides defects | Require exact resource/schema/request ID and aggregate every framing failure | Imported strict decoder plus explicit failure qualification |
| Python removes assertions | `-O` and `PYTHONOPTIMIZE` fail before imports/build/connection | Offline negative launches |
| Disconnect repairs failed save | Read saved GRID/noise/dialect bytes while first session remains connected | Parent on-disk assertion |
| Live cache hides missing durability | Replace child, reopen identical owned stores, restore settings and exact nonce archive row | Two real processes per transport |
| Queue acknowledgement hides lost spot | Reject invalid frequency; archive exactly one valid labeled spot; restart and query again | Actual archive-backed history |
| Skip/no-tests becomes green | Require fixed case list, child execution marker, PASS and complete retirement receipt | Qualification negative controls |
| Concurrent runners replace active binary | Same native exclusive lock as stable PowerShell runner; verify exclusion both directions | Native Windows lock controls |
| Shutdown races store ownership | Close clients; Stop server; observe registry/prelogin retirement; join bridge; Stop archive; wait child | Receipt plus actual process exit |
| Child failure/timeout becomes graceful | Bounded readiness, logs and shutdown; forced kill remains failure; retain stores for diagnosis | Driver failure paths |

The final code-quality review found three P2 checker/ownership defects, all
repaired within checkpoint 00: compiler descendants now belong to a Windows
Job Object before tool launch and retire before lock release; each DIALECT save
is inspected immediately and nondefault CC survives restart; failed child launch
closes its log. Offline controls start a real descendant, force timeout/output
overflow and require no delayed write after lock reacquisition. The job wrapper
uses documented Windows APIs and fails closed on unsupported assignment.
The driver qualifies native Windows only. It refuses other hosts explicitly;
there is no unqualified POSIX process-tree fallback.
Job cleanup is protected by an enclosing finally even when direct-process
kill/wait fails; an offline fault control checks retirement and handle closure.

The parent's fresh build and source stability check establish source provenance;
the child's source digest is a correlation field, not independent proof of its
compiler inputs. Binary and staged-config hashes are calculated by the child.
The endpoint's injected Version nonce uses existing SHOW BUILD behavior.

Private generated configuration is test input, not deployment configuration.
No package-level fixture result alone qualifies real-server execution. Offline
collector fixtures remain separate evidence. Stores and transcripts stay in the
owned ignored run directory; child exit precedes any later cleanup.

Support-agent docs impact: Not required for checkpoint 00. Production binding,
startup sequence, log wording and user command behavior remain unchanged. The
private fixture and contributor testing instructions are documented here and in
the telnet validation guide. ADR-0265 already owns the stable Windows executable
policy; checkpoint 00 adds no workflow policy or separate architecture decision.

### Observed checkpoint 00 qualification (2026-10-09)

| Command / scope | Observed result |
|---|---|
| `python -m unittest discover -s scripts -p '*commands*test.py'` in pinned venv | 29 existing collector fixtures passed; no live-server claim |
| `python -m unittest discover -s scripts -p test_command_fixes_test.py` | 17 runner failure controls passed, including real descendant cancellation and both lock directions |
| `python scripts/test-command-fixes.py` | 10 required server cases passed; zero wire/cleanup errors; artifact `.tmp/command-fixes/20261009-090813-8aef5259` |
| Same driver with `--race` | All 10 cases passed; zero wire/cleanup errors; artifact `.tmp/command-fixes/20261009-091001-fa6ca8c8` |
| `pwsh -NoProfile -File scripts/test-windows.ps1` | Complete 70-package run passed; log `.tmp/command-fixes/checkpoint-00-baseline.log` |
| Same runner with `-Race` | Complete 70-package run passed; no race report; log `.tmp/command-fixes/checkpoint-00-race.log` |
| `go vet ./...`; `staticcheck ./...`; `golangci-lint run ./... --config=.golangci.yaml` | All passed; lint reported zero issues |

The first full run stopped at the existing peer TCP backoff timing test
(98.5546 ms observed against 100 ms, with a 1 ms tolerance). Its isolated rerun
and the subsequent complete ordinary/race runs passed. No peer code or test was
changed. Overlapping validation launches were refused by the shared lock; those
attempts executed no tests and are excluded from passed evidence.

Fresh final verification was lead-owned. Review findings were dispositioned,
owned-process retirement was exercised locally, and the final diff was inspected.
No external cluster, deployment or production-load evidence is claimed.

## Checkpoint 01: physical-line rejection

Status: complete; checkpoint 00 is committed as `c179916`.
The post-approval lifecycle/test-strategy review was inherited-context and
read-only. Its transport findings are incorporated before implementation.

The read owner retains the installed read deadline. Recovery uses its earlier
value or five seconds, drains without editing/echo through the selected codec,
and owns a timer whose expiry interrupts the raw socket, including negotiation
writes. Acceptance and expiry are linearized; cleanup joins a started callback.
Typed terminal human errors precede timeout/validation retry in both caller loops.
Physical progress survives erasure; an empty idle timeout remains retryable.

A raw guard is installed before the codec and limits new reads only during
recovery. Native buffered bytes are counted exactly. Ziutek gets single decoded
byte exposure while reading human/header lines so it cannot hide an offending
byte behind a later negotiation write. Machine body bulk reads remain available.
Reserve the codec's hidden 256 bytes plus twice any already buffered decoded
bytes (escaped IAC can represent two wire bytes). This is a conservative at-most
64 KiB guarantee, not a promise to accept every boundary exactly at 64 KiB.
Recoverability near the limit may require disconnecting somewhat earlier.

| Contract / broken design | Required stimulus and observation | Checker |
|---|---|---|
| Rejected tail executes | Original invalid DX and overlong input containing REJECT BAND ALL | Exact unchanged configuration/revision and stored bytes; no tail acknowledgement |
| Recovery consumes next command | Bad line then read-only SHOW OWN sentinel | Exactly one sentinel reply and correlated GET; reader returns exactly the next line |
| Decoded count misses wire | Large IAC/subnegotiation streams through actual codecs | Recorded raw bytes stay within admitted recovery budget; terminal closure |
| Buffered debt bypasses limit | Native prefetch and ziutek hidden/decoded buffering | Debt reserved before any new raw read |
| Deadline slips or extends | Trickle/buffered input and earlier installed deadline | Absolute deadline governs, including with watchdog deliberately delayed |
| Callback Write stalls | Invalid byte followed by actual ziutek negotiation Write | Offending byte is exposed first; watchdog interrupts socket and reader joins |
| Editing hides partial read | Partial text followed by Ctrl-U/BS then timeout | Terminal human error; caller disconnects and never dispatches tail |
| Idle behavior changes | Empty timeout then valid command | Existing idle retry and usable session |
| Raw newline ends SB prematurely | CR/LF and escaped IAC inside subnegotiation | Actual data boundary wins in both transports |
| Timer outlives recovery | Delayed expiry/accept races and repeated recovery | Callback joined; retired timer cannot close later command |
| Shared reader breaks machine input | Existing machine/transport/editing suites | Reserved machine intent, body deadlines, case and normal editing preserved |

Validation adds required cumulative checkout cases, targeted transport/caller
tests, structured protocol fuzzing and race coverage. Allocation checks target
zero additional allocations in the raw guard's ordinary Read; no latency or
production-load improvement is claimed.

### Observed checkpoint 01 qualification (2026-10-09)

| Command / scope | Observed result |
|---|---|
| Stable Windows runner, telnet targeted `Test(Human\|InputWire\|ReadLine\|Machine\|Malformed\|YAML)\|FuzzHumanRecoveryBoundary` | Passed, including real blocked ziutek negotiation Write, split CRLF/CR-NUL idle retry, terminal login/command callers, bulk decoded debt, literal limits and both callback winners |
| Pinned Python `unittest discover -s scripts -p test_command_fixes_test.py` | 18 failure controls passed; transient receipt sharing retries remain bounded and retire the child on expiry |
| `python scripts/test-command-fixes.py` | All 14 required cases passed, zero wire/cleanup errors; artifact `.tmp/command-fixes/20261009-093545-d01aea38` |
| Same driver with `--race` | All 14 cases passed, zero wire/cleanup errors; final artifact `.tmp/command-fixes/20261009-095433-c631745b` |
| Stable fuzz-instrumented binary, `-test.fuzz=^FuzzHumanRecoveryBoundary$ -test.fuzztime=10s -test.parallel=2` | Passed 39,096 executions, 15 new coverage inputs; log `.tmp/command-fixes/checkpoint-01-fuzz-guided.log` |
| `pwsh -NoProfile -File scripts/test-windows.ps1` | Complete 70-package run passed; log `.tmp/command-fixes/checkpoint-01-baseline.log` |
| Same runner with `-Race` | Complete 70-package run passed, no race report; log `.tmp/command-fixes/checkpoint-01-race.log` |
| `go vet ./...`; `staticcheck ./...`; `golangci-lint run ./... --config=.golangci.yaml` | Passed; lint reported zero issues |

The first fixture attempt failed on a transient Windows sharing refusal while
opening an atomically published readiness receipt. The retry is restricted to
missing/sharing-refused files within the existing 45-second live-child bound;
invalid JSON and identity failures remain terminal. Its positive/expiry control
and subsequent ordinary/race server runs passed. The failed attempt remains
diagnostic evidence and is excluded from passed qualification.

The first exploratory fuzz launch lacked coverage instrumentation. Qualification
uses the subsequent fresh `go test -c -fuzz=^FuzzHumanRecoveryBoundary$` build at
the stable path, protected by the shared runner lock. Literal unit oracles
independently enforce 65,536 wire bytes and a five-second effective deadline.
The fake-clock trickle test withholds expiry and asserts reading stops before
the boundary; a registered deadline alone cannot satisfy it.

The inherited-context code review found and fixed split-terminator idle
accounting. It also prompted stronger literal-bound, bulk-prefetch and timer
winner evidence. Fresh final verification was lead-owned. Ordinary Read added
zero allocations in its focused check; this is not a latency or load claim.

Support-agent docs impact: Required and completed. The connectivity card,
troubleshooting route, telnet README and affected domain-contract sections
describe whole-line rejection and safe recovery/disconnect. Existing unrelated
legacy bytes in the domain contract were preserved. ADR-0266 records the durable
recovery policy; TSR-0048 preserves the parser/codec root cause and support
learning. Machine schemas and upload policy remain unchanged.

## Checkpoint 02: NOFILTER and NEARBY restoration

Status: complete; qualification and final review are recorded below.

Pre-change source confirmed that NOFILTER was the sole production caller of
`Filter.Reset`. Reset cleared most geography before `DisableNearby` restored the
saved geography snapshot; states were cleared afterward, so state-only checks
missed the bug. The fix disables NEARBY before resetting every filter domain. Ordinary
NEARBY OFF and RESET FILTER retain their existing restoration/default behavior.

The inherited-context read-only test-strategy challenge found an important
history oracle trap: DX own-call matches bypass ordinary filters. The cumulative
case uses a separate observer whose baseline differs from the posted DX,
proves its geographic restriction hides the exact archived row, then requires
NOFILTER to reveal it. Posting alone does not establish a DX own-call match.

| Contract / false-green risk | Required stimulus and observation | Checker |
|---|---|---|
| Old geography survives reset | Active NEARBY snapshot with allow, block and block-all geography across all ten axes | Exact permissive rules and a previously hidden non-self candidate matches |
| Stale snapshot restores later | Reset, then NEARBY OFF and another enable/disable cycle | Snapshot/cached filter cells cleared; old restrictions never return |
| OFF/default behavior accidentally changes | Independent restrictive snapshot and custom mode/source defaults | Ordinary OFF restores exact snapshot; RESET FILTER uses defaults; NOFILTER permits all |
| DX own-call bypass hides bug | Distinct observer with a different DX identity; restrictive geography hides exact labeled archive row before enabling NEARBY | Same archive selection visible after GO PASS NOFILTER / CC SET/NOFILTER |
| Save/reconnect silently repairs result | Inspect saved record while connected, then replace server process with same stores | Permissive saved geography and restored configuration/history; no autosave-only proof |

Required qualification comprises targeted filter/telnet tests, cumulative
native/ziutek fixtures, and the complete production-Go baseline. This change introduces
no parser, schema, snapshot format, new lifetime owner or filter hot-path change.

### Observed checkpoint 02 qualification (2026-10-09)

| Command / scope | Observed result |
|---|---|
| Stable Windows runner, targeted filter and telnet reset tests | Passed: all ten geography axes, retired snapshot/cells, exact ordinary OFF restoration, configured defaults, both dialects and immediate saved matching |
| Same targeted tests with `-Race` | Passed in both packages, no race report; log `.tmp/command-fixes/checkpoint-02-targeted-race.log` |
| Pinned Python cumulative checkout driver | All 18 required cases passed; artifact `.tmp/command-fixes/20261009-100346-da0fc7cc` |
| Same driver with `--race` | All 18 cases passed, zero wire/cleanup errors; artifact `.tmp/command-fixes/20261009-102421-c00296d2` |
| `pwsh -NoProfile -File scripts/test-windows.ps1` | Complete 70-package run passed; log `.tmp/command-fixes/checkpoint-02-baseline.log` |
| `go vet ./...`; `staticcheck ./...`; `golangci-lint run ./... --config=.golangci.yaml` | Passed; final lint reported zero issues |

The first archive control used REJECT NA, which cannot hide the bridge's unknown
geography. That attempt failed and was not counted as qualification. Its
replacement first proves visibility, then uses an EU-only allowlist to hide the
exact row before testing reset. Unit tests separately use known geography.

Two complete-suite attempts failed in unchanged
`peer.TestOutboundTCPBackoffAfterEstablishedDisconnect`, measuring 998.5822 ms
against one second and 98.7486 ms against 100 ms. The test timestamps an
asynchronously polled diagnostic, rather than the timer's start. Uneven observer
delay can shorten that measured interval; these failures do not establish an
early production timer. The isolated rerun passed, and the subsequent complete
run passed. Both failed logs remain under `.tmp/command-fixes`. No peer code or
tolerance was changed. Race qualification here covers the affected filter/telnet
tests and actual server cases; checkpoint 01 remains the latest complete race
suite. No new parser fuzzing was needed for this reset-order change.

The inherited-context final read-only review found no production or checker
defect. Fresh final verification was lead-owned. Support-agent docs impact:
Required and completed in the telnet README and troubleshooting routing.
TSR-0049 records the reset order and history-oracle traps. No ADR is required:
this restores the existing permissive reset contract without a new policy.
