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

## Checkpoint 00: unattended checkout-server fixture

Status: complete. Other 45 checkpoints are pending. The matrix records the
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
