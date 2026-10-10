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

Current progress: checkpoints 00 through 24 complete (04a used two commits);
32 of 46 approved checkpoints complete; 14 remain; 25 is next.

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

## Checkpoint 03: truthful human persistence warnings

Status: complete; checkpoint 02 is committed as `0ecb0a8`.

Pre-change filter, COMMENT and changed-DIALECT save failures were logged without a
client warning. Other human settings already warned while keeping the live edit.
Append an explicit warning on its own line to these three consumers, preserving
their acknowledgements and existing save-attempt rules. This checkpoint changes
feedback only; detached validation, admission and storage protection changes
belong to checkpoint 04.

The inherited-context pre-implementation test-strategy challenge found two
false-green risks: replacing the entire save function does not exercise actual
storage preparation/commit, and disconnect autosave can repair a failed save.
The native Windows driver will deny replacement with an owned file handle that
still permits reads. Hold the fault through teardown, verify old bytes while
connected, then check them in a fresh child. A separate malformed/future record
case exercises real preparation and protected login. Release every handle even
on failure; later admitted persistence must succeed without the fault.

| Contract / false-green risk | Required stimulus and observation | Checker |
|---|---|---|
| Success hides save failure | BAND, COMMENT add/remove/reset, RESET FILTER, NOFILTER and changed DIALECT in both dialects | Existing acknowledgement plus one warning on its own line; exact live change and old durable bytes |
| Callback bypasses real storage | Valid record held readable but non-replaceable; record replaced with malformed/future YAML after valid login | Actual commit/preparation error reaches response; bytes unchanged before teardown |
| No-op behavior changes | Same DIALECT and idempotent COMMENT; ordinary identical filter commands | No save/no warning for former; preserve existing attempted-save warning for latter |
| Protected login overwrites original | Login through malformed/future record, apply fitting human edits, teardown/restart | Temporary-settings warning, live edits, original refused bytes preserved |
| Fault outlives test or repairs result | Keep replacement fault through disconnect, then release it | Fresh child sees old preferences; later valid save succeeds and becomes durable |

Required checks: targeted warning/preparation/protected tests, cumulative actual
native/ziutek cases, complete production-Go baseline and static checks. No new
schema, storage API, parser, lifetime owner or rollback policy is introduced.
Checkpoint 04 must rerun these live/durable cases against its new storage seams.

### Observed checkpoint 03 qualification (2026-10-09)

| Command / scope | Observed result |
|---|---|
| Stable Windows runner, telnet `Test(HumanPersistenceWarning\|ConfigurationProtectedTemporaryDefaults)` | Passed: seven operation families in both dialects across injected, actual malformed/future preparation and protected-login failures; no-op/ordinary save-attempt distinction |
| Pinned Python checkout-runner offline controls | All 19 tests passed, including actual readable/non-replaceable file, release after normal completion and exception, then successful replacement |
| Existing telnet harness offline controls | All 29 tests passed; original harness remains unchanged |
| Cumulative checkout driver | All 24 required cases passed, zero wire/cleanup errors; artifact `.tmp/command-fixes/20261009-103655-794d4a53` |
| Same driver with `--race`, final runner state | All 24 cases passed, zero wire/cleanup errors and no race report; artifact `.tmp/command-fixes/20261009-104230-5b606068` |
| `pwsh -NoProfile -File scripts/test-windows.ps1` | Complete 70-package run passed; log `.tmp/command-fixes/checkpoint-03-baseline.log` |
| `go vet ./...`; `staticcheck ./...`; `golangci-lint run ./... --config=.golangci.yaml` | Passed; lint reported zero issues |

The first server attempt checked the first UTC prompt for the protected-login
warning. The configured greeting places that prompt before the appended warning,
so the assertion failed before consuming it. The corrected checker synchronizes
through PAUSE plus correlated GET, checks the actual remaining warning and
temporary-defaults status, then verifies live edits and preserved bytes. This
failed attempt remains diagnostic evidence and is excluded from qualification.

Final source/diff verification was lead-owned, after the inherited-context
test-strategy challenge. Each response retains its original acknowledgement and
appends exactly one separate warning on a failed save. The production patch adds
no concurrency, parsing or storage behavior; race evidence covers the cumulative
real server and protected handoffs, and no new fuzz target was needed.

Support-agent docs impact: Required and completed in the telnet README,
troubleshooting routing and TSR-0050. No ADR is required because this completes
the agreed existing human live-edit/failure contract without new policy.

## Checkpoint 04a: compatible storage decoding and work accounting

Status: complete. Checkpoint 03 is committed as `745a722`; the test-only frozen
baseline is committed as `8596005`. All new thresholds remain inactive in this
checkpoint.

Execution divides this approved checkpoint at a verification boundary:

1. Freeze the old typed record, reference, baseline and collection decoding in
   tests; observe the compatibility corpus before replacing production decoding.
2. Add storage-local compatible decoding and document-wide P+E accounting, then
   compare acceptance and complete values against that baseline. Admission limits
   become active only in checkpoint 04d after its dependent publication/storage
   contracts are implemented.

### Current-state configuration and impact audit

These are feature-owned persisted user/preset data and test fixtures, not merged
runtime YAML. Keep `filter.LoadUserRecord`, `readPresetCollection` and the public
stored-type unmarshalling boundaries as the authoritative paths. No runtime
loader, machine schema, optional tool/secret file or reference table changes.

Legacy user records ignore unknown values and support ordinary merges; marked
records reject unknown/root-merge fields. SavedPreset/reference prechecks are
strict even for legacy baselines. Collections use known-field validation and
currently accept root merges. Null, false, empty selections and absent optional
toggles retain the separate legacy migration and marked exact-value contracts.
Existing ADR-0238 and ADR-0244 ownership, normalization and persistence remain.

`gopls references` and current source identify record loads from filter loading,
login/IP updates, preference/configuration saves and telnet restoration. Preset
operations all read the same operation-local collection. `go list -test` confirms
direct production consumers in commands, telnet and internal/cluster, and tests
in those packages plus archive and peer. YAML interface dispatch also reaches
stored callbacks; direct call references alone cannot inventory those edges.

The inherited-context discovery used the pinned production loader to reproduce
the excessive-alias merge trap with 125 bytes. Removing the root merge yields
118 accepted bytes; the neighboring width-six expansion is accepted. This is a
minimized observed regression, not the unsupplied reported 325-byte file.
Ignored duplicate/cyclic values remain accepted, while reachable duplicate/cyclic
merge sources are refused. Legacy interpreted integer keys 1/01 keep last-write
behavior; marked configuration rejects them. Explicit fields and first sequence
merge sources retain precedence; shadowed malformed values remain skipped.

### Detailed falsifiability challenge, before decoder replacement

| Contract / broken approach | Required stimulus and observation | Checker / owner |
|---|---|---|
| Projection loses old alias refusal | Observed 125-byte root-merge trap, 118-byte unused anchors and accepted width-six neighbor | Frozen stock nested decoder plus literal positive/negative controls / filter tests |
| Global validation changes ignored values | Duplicate/cyclic ignored branches versus reachable merge sources | Paired accepted/refused corpus; require original excessive-alias reason for the trap / filter tests |
| Different duplicate or key conversion semantics | Raw Kind/Value duplicates, differing scalar tags, aliased keys and legacy 1/01 | Full typed-value comparison, not only success/error / filter tests |
| Wrong merge order or shared skip state | Explicit/sequence precedence, sibling maps and shadowed invalid values | Exact independent resulting map fields / filter tests |
| Nested decoder resets the new budget | Record/reference/baseline and collection fixtures | Hand-counted P and E; separate old guard contexts / accounting tests |
| Optimized decode adds another alias refusal | Unknown keys dilute original traversal; reduced projection still contains aliases | Compare the original guard behavior before any alias-free materialization / differential corpus and fuzzing |
| New thresholds activate too soon | Large unaliased/raw input and compact expensive scalar aliases currently accepted | Explicit acceptance controls; existing preset limits remain / filter tests |
| Apparently linear code retains quadratic work | Increasing bulk maps, root keys and nested baselines | Scale/allocation measurements with correct complete results / lead qualification |

Physical accounting follows Content only and charges parsed scalar Value UTF-8
bytes, including ignored fields and unused anchors. Logical accounting follows
the original typed decoding visits and scalar bytes, including merge bookkeeping
and nested custom dispatch. Charge it once regardless of optimization passes.
Nested Node.Decode guards reset separately; P+E remains operation-owned across
the complete document. No process-lifetime node/type cache is required.

Hand-counted source oracles include `{}` P=2/E=1, `bands: {20m: true}` P=18/E=17,
an ignored `legacy: {x: "abc"}` P=16/E=8, and a record containing preset TEST with
that bands baseline P=46/E=47. These are expected integers to test, not observed
candidate accounting results. Additional aliases, merges and collection vectors
are retained in the discovery report and will be asserted explicitly.

A separate-context read-only design-space challenge is complete. The lead selects
storage-specific, operation-local traversal of the original AST before typed
materialization. A projected AST remains viable but needs the same traversal and
adds reconstruction and second-guard risks. Scalar conversions retain stock
Node.Decode on alias-free leaves; bulk map duplicate checks become linear.

The challenge findings are dispositioned as follows: raw duplicate identity,
interpreted key identity and generic merge bookkeeping remain separate; visited
merge sources are checked even when their values are shadowed; original guard
prefix order and nested reset boundaries are retained. Physical census follows
Content, while logical expansion uses an active-alias path rather than a global
visited set. Custom handoffs charge the outer dispatch and fresh plain root;
record/reference/baseline share P+E while their stock guards reset. Duplicate
compatibility means refusal and error-class agreement, not reproducing quadratic
pairwise diagnostic output. The observed excessive-alias refusal remains exact.
These are engineering dispositions within the approved contract, not new policy.

The test-only 04a.1 baseline is complete. It freezes the old nested callbacks,
compares complete values and refuses altered stored bytes, and includes an
isolated work-limit control whose raw, canonical and configuration bytes fit.
The complete filter package passed on final source in
`.tmp/command-fixes/checkpoint-04a-1-filter.log`; targeted filter vet, staticcheck
and lint passed (zero lint issues). Production decoding remains unchanged.
Support-agent/operator docs and a new ADR are NotRequired for this test-only
slice. Decoder closeout still requires accounting assertions, differential tests,
parser fuzzing, scale evidence, cumulative server storage cases, the complete
selected lane and fresh final review. The inherited-context test challenge and
separate-context design review are findings-only evidence; lead authority remains.

### Compatible decoder implementation and final qualification

`storage_decode.go` owns the original typed traversal, linear raw duplicate
checking, separate stock guard contexts and checked P/E counters. The cohesive
engine is 495 lines; owner callbacks/finalization are separated into the
190-line `storage_decode_owners.go`. `LoadUserRecord`, the stored public
callbacks and `readPresetCollection` now use those operation-owned paths. No
AST, key/type inventory or counter survives the operation. All nine literal
accounting vectors match their hand-counted integers, and overflow is refused.

A separate-context read-only Go quality review found a null-tag dispatch
mismatch. The lead verified it with eight failing literal controls, corrected
the dispatch and reran those controls plus the differential targets. Null tags
skip pointer/custom preparation but do not replace mapping/sequence dispatch.
Both the dependency's composite-key panic and its null-tagged inline-root panic
are now normal refusals; no previously accepted value is removed. Direct typed
callback errors accumulate, while wrapped owner failures remain terminal; the
pinned distinction has an explicit regression and justified local lint exception.

The applied reference is temporarily finalized after its own accounted walk to
retain its old early name/snapshot failure boundary. Main bulk materialization
still follows complete preflight. This exception is not allocation-free and the
final snapshot cap is not an intermediate legacy-allocation bound. ADR-0267
records that limitation and the phased threshold activation.

Final observed evidence:

- Complete ordinary Windows lane: 70 packages passed in
  `.tmp/command-fixes/checkpoint-04a-baseline-retry.log`. The first attempt hit
  the unchanged peer retry-observer timing test (98.8274 ms against 100 ms);
  its isolated control passed before the complete rerun. The failed attempt is
  retained in `checkpoint-04a-baseline-final.log`, not counted as qualification.
- Full filter package with race detection passed in
  `.tmp/command-fixes/checkpoint-04a-filter-race.log`. Vet and Staticcheck passed
  repository-wide; final repository lint passed with zero issues.
- Corrected differential fuzzing passed 30,441 raw and 51,918 structured
  executions with two workers, including expanded null-tag seeds, in
  `.tmp/command-fixes/checkpoint-04a-fuzz-final.log`. Frozen callbacks preserve
  stock nested guard boundaries; unchanged prevalidation/migration helpers remain
  shared oracle seams. Candidate panics are failures, not tolerated refusals.
- All 32 scale measurements passed complete-value checks for direct/merged maps,
  ignored legacy roots and nested baselines at 128/512/2,048/8,192 entries in
  `.tmp/command-fixes/checkpoint-04a-scale-final.log`. The initial aliased merge
  scale fixture reached the old guard and was corrected to an accepted inline
  merge; that failed fixture attempt is diagnostic evidence only.
- Measurements show higher allocation and sometimes slower small loads for the
  two-pass decoder. For direct maps, candidate allocated bytes grow from 232,416
  at 128 entries to 11,774,056 at 8,192; the frozen baseline is 86,656 to 5,135,488.
  The sampled allocation profile identifies stock scalar conversion as the largest
  contributor. No universal speed, allocation, production latency or capacity
  improvement is claimed.
- The cumulative checkout runner passes 28 required cases across native and
  ziutek transports, with zero wire/cleanup failures, in
  `.tmp/command-fixes/20261009-115909-6a8a9cc1`. Its race counterpart also passes
  all 28 in `.tmp/command-fixes/20261009-120409-ff0aac67`. Cases exercise accepted
  legacy aliases, exact false/empty selections, detached baselines, merged
  collections, old alias refusal, preserved refused bytes and fresh-child
  restoration. The first attempt used PyYAML's space-separated datetime, which
  the pinned time decoder refuses; the fixture now retains RFC3339. That failed
  attempt is excluded from qualification.
- Nineteen runner lifecycle/qualification controls passed. The TSR checker
  passed after repairing the required cause/fix labels in this work's earlier
  TSR-0048 through TSR-0050; their behavior statements remain unchanged.

The fresh separate-context quality pass found no remaining material defect on
the final source. It did not execute suites; qualification and the final
source/diff review remain lead-owned. Support-agent documentation impact is
Required and completed in the troubleshooting index and TSR-0051. ADR-0267
records the durable decoder decision. This checkpoint does not activate new
admission limits or migrate production data.

Code maps were generated and checked fresh on this state; they produced no
content diff. Final whitespace/diff checks and the indexed TSR checker passed.

## Checkpoint 04b: exact-byte preparation and commit

Status: complete.
Checkpoint 04a.2 is committed as `f8f7fce`. New quotas remain inactive.

Current saves already marshal once before the atomic writer. Configuration and
preference saves first load authoritative SSID metadata, apply the captured
preferences and merge current/fallback IPs. Preserve those differing validation
boundaries: ordinary record preparation must not call `Configuration.Preset()`
or add that preset's size restriction. Preference saves retain the association;
configuration saves explicitly replace it, including nil to clear it.

A separate-context read-only design challenge confirms an opaque prepared user
record owned by filter: captured path, private final YAML bytes, byte length and
a byte-only atomic commit. Preparation retains no caller maps, slices, baseline,
record pointer or closures; input synchronization/detachment remains the caller's
existing responsibility during preparation. Commit neither reloads metadata nor
validates, normalizes or marshals again. The payload grants no transaction
authority: telnet still holds its full-callsign lease through preparation/commit.

The lead selects exported `PrepareUserRecord`, `PrepareConfiguration` and
`PrepareUserPreferences`, returning `PreparedUserRecord` with `ByteLen` and
`Commit`. Existing save functions retain prepare-then-commit wrappers and their
private per-call writer seams. Collection preparation stays private, inside the
existing preset stripe through commit. An unlocked exported collection snapshot
is rejected because competing SSIDs could overwrite each other's library edits.

| Broken design | Required falsifying evidence |
|---|---|
| Preparation retains mutable inputs or commit serializes again | Prepare, copy expected bytes, mutate maps/lists/toggles/IPs/metadata/baseline; commit exact original bytes and length, including captured target path |
| Metadata or association contract drifts | Literal timestamp and primary/fallback IP ordering/cap controls; missing-record creation, preference association retention and configuration nil-reference clearing |
| Refused preparation has durable effects | Invalid candidate and malformed/future authoritative record controls; no writes/directories/temp files or changed inputs |
| Extracted commit bypasses atomic failure behavior | Prepared bytes through real write/sync/close/replace stage faults; preserve target, underlying error, exact stop order and temp-file cleanup |
| Preparation enables stale unlocked library writes | Existing concurrent collection updates plus lock-scoped collection preparation; retain full-callsign reconnect ownership tests |
| Publication or SAVE ordering moves early | Human warnings retain live edits; real machine/LOAD replacement failures retain live state/revisions/runtime/association; library failure prevents association attempt and later association I/O failure retains library-only success |
| New quotas activate before dependent human/admission work | Positive prepared-record/configuration controls beyond future bounds; existing preset/collection limits remain in force |

This checkpoint preserves today's SAVE order: library write precedes association
preparation. Coordinated admission of both prepared payloads before the first
write belongs to 04d. No new product-policy blocker was found. Cross-process
concurrency and power-loss durability are outside the evidence established here.

`filter/prepared_user_record.go` now owns the immutable bytes/absolute target.
The existing record/configuration/preference saves are prepare-then-commit
wrappers. Collection serialization is separately prepared under the existing
stripe, with its old limit intact. No new quota, publication or SAVE-order change
was introduced. A relative target is captured lexically after validation/marshal;
no symlinks or filesystem objects are resolved during preparation.

Final observed evidence:

- Both complete filter and telnet packages passed targeted ordinary and race
  checks (`checkpoint-04b-targeted.log`, `checkpoint-04b-race.log`). New tests
  mutate caller maps/lists/toggles, metadata, baseline, UserDataDir and working
  directory after preparation, then verify captured bytes/path and literal
  restoration. They cover differing validation order, metadata/IP cap/full SSID,
  retained/cleared/explicit associations, no preparation effects, future limits
  remaining inactive, and real atomic stage faults with stop order/cleanup/retry.
- The complete ordinary Windows suite passed all 70 packages in
  `.tmp/command-fixes/checkpoint-04b-baseline.log`. Repository-wide vet,
  Staticcheck and lint passed; lint reported zero issues.
- All 32 required checkout cases passed across both transports in
  `.tmp/command-fixes/20261009-122008-d0d0079f`; all 32 also passed with race
  detection in `.tmp/command-fixes/20261009-122942-0eb7e0b7`. Both runs have zero
  wire/cleanup failures. Real denied replacements prove machine/LOAD failure
  retains configuration/revision/effective state/association and bytes. SAVE
  retains library-only success after association failure; library failure with
  a writable record preserves both targets. Fresh children recover those exact
  outcomes. Existing reconnect and concurrent collection tests retain ownership
  evidence; mock save callbacks alone are not the filesystem-failure proof.

A separate-context read-only Go quality review found no material defect. The
lead verified final source and the actual diff and owns all qualification claims.
No new parser behavior was introduced; 04a's differential corpus also runs in
the complete filter tests. Support-agent documentation impact is Required and
completed in the persistence troubleshooting route. ADR-0268 records prepared
ownership and its limits. Code-map/whitespace and indexed TSR checks close out
this checkpoint; the new admission behavior remains deferred to 04d.

## Checkpoint 04c: detached human preparation and publication

Status: complete. Checkpoint 04b is committed as `4471943`.

The selected design constructs a fresh, operation-owned minimal Client adapter
with detached filter maps/lists/toggles, configured settings and current effective
path/dialect/dedupe state. It never copies live mutexes, atomics, queues, cursors,
sockets or lifecycle ownership. Preserve NEARBY cells, the immutable restoration
snapshot and the legacy scalar omitted from Configuration. DisableNearby copies
the snapshot's maps, so ordinary restoration does not mutate the shared snapshot.

Stage handlers contain no persistence. Their explicit outcome distinguishes
handled/refused/read-only commands from publication/save intent and deliberate
human SOLAR rescheduling. Preserve command-specific success/warning formatting;
neither configuration equality nor response text determines save intent. Existing
ordinary repeated PASS commands still save; unchanged DIALECT/COMMENT choices do
not. Read-only summaries remain on the live readback path before candidate cloning.

The existing full-SSID lease spans staging, publication, saving and response.
Keep runPreferenceCommand's generic lease callback contract, which semantic
references also identify in history tests/fuzz fixtures. Production staged
handlers coordinate within that existing lease. Publish the staged runtime
Filter directly with stable Filter identity and path-then-filter lock order;
machine preparation must not reconstruct human NEARBY state or defaults.
Filterless lightweight settings callers remain supported.

An accepted human edit still publishes before its save attempt, and a save
failure keeps it live with the existing warning. Same-cadence human SET SOLAR
starts its deliberate fresh schedule at publication. Unrelated edits preserve
live broadcaster progress between preparation and publication. Machine unchanged
cadence and LOAD retain their different existing scheduling contracts. No new
quota, argument grammar, MODE, GRID or dedupe policy activates in this checkpoint.

A separate-context read-only detailed falsifiability pass refined the evidence
without finding a new policy/scope requirement. The lead dispositions its five
findings as checker-only refinements, covered below. The 19 runner
lifecycle/qualification controls passed in
`checkpoint-04c-runner-controls.log` after extending the required checkout cases.

| Contract / broken design | Stimulus and required observation | Evidence / checker owner |
|---|---|---|
| Construction initializes or otherwise changes live state | Snapshot raw uninitialized metadata, revisions, history/cursor and lifecycle fields before invoking any capture/revision helper; build and discard candidate; exact original values remain | Direct constructor test, telnet |
| Candidate retains writable live references | Mutate candidate maps/lists/toggles and stage accepted/refused commands; original live rules/path/settings and saved bytes remain unchanged until publication | Direct detached-stage tests, telnet |
| Partial invalid command changes live or disk | Mixed valid/invalid GO and CC selections plus settings failures; explicit refused result; no save attempt, revision/effective/rule/file changes | Unit command matrix and checkout runner |
| Read-only/no-op paths acquire publication/save intent | Current/LIST DIALECT, SHOW filters/DEDUPE, unchanged COMMENT/DIALECT, repeated ordinary PASS; assert the distinct literal save-attempt rules | Existing persistence tests plus explicit stage outcomes, telnet |
| Publication reconstructs or loses NEARBY restoration | ON/OFF, active GRID update, unrelated setting, NOFILTER, RESET and loaded active state without a snapshot; literal restoration rules/cells and stable Filter pointer | Runtime publication tests plus actual checkout/fresh-child readbacks |
| SOLAR uses preparation time or overwrites broadcaster progress | Bracket stage-to-publication with a cross-clock-boundary test and advance the live tick in between; human SET uses literal publication deadline, unrelated edit retains advanced tick | Direct clock/barrier tests; existing machine/LOAD scheduling tests, telnet |
| Failure test reaches the wrong persistence branch | After valid login, allow reads/temp-file writes and deny atomic replacement with production save hooks unset; live human edit survives and exact saved bytes do not change | Real filesystem-failure checkout cases; preparation/protected controls remain separate |
| New bounds activate prematurely | Large ordinary warm-session record and fitting ordinary human edit beyond future record/configuration limits; accepted live change and durable save, without Preset/readback admission | Positive quota-inactivity test, telnet |
| New path bypasses ownership or touches unrelated session state | Pause a save reached through the new human coordinator while reconnect attempts its occupied stripe and fan-out path/filter reads progress; successor restores the committed edit, retired teardown cannot overwrite; solar advancement, diagnostics/read-pause/history semantics are tested separately | Human-entry-point handoff test plus clock/publication and existing handoff controls, race-enabled telnet |

Final observed evidence:

- Complete filter and telnet packages passed ordinary and race checks
  (`checkpoint-04c-targeted-retry.log`, `checkpoint-04c-race.log`). The first
  ordinary run exposed an existing machine test that captured a collection before
  an intervening human command; detached human publication intentionally replaces
  collections. The test now captures at the unchanged machine PUT boundary and
  retains its collection-identity assertion. No machine production change was
  required. The failed log remains `checkpoint-04c-targeted.log`.
- The complete ordinary Windows suite passed all 70 packages in
  `checkpoint-04c-baseline.log`. Repository-wide vet, Staticcheck and lint passed;
  lint reported zero issues. The changed Python runner passed native AST syntax
  validation.
- All 36 required checkout cases passed across both transports in
  `.tmp/command-fixes/20261009-130814-75c8df3f`; all 36 also passed with race
  detection in `.tmp/command-fixes/20261009-132009-d113ef9f`. Each run has zero
  wire/cleanup failures and two joined children per transport. New cases check
  refused mixed lists/settings, real denied replacement after login, retained
  live changes and saved bytes, NEARBY restoration/GRID cells, diagnostics/pause,
  read-only saves and exact outcomes recovered by a fresh child.

A separate-context read-only Go quality review found no material production
defect. Its evidence-wording refinement is reflected in the handoff row above:
path/filter progress is observed during stalled saving, while solar advancement
is tested separately at publication. The lead verified the final source and
actual diff. Publication transfers collection ownership; the command discards
its adapter afterward.

Support-agent documentation impact is Required and completed in the persistence
route, telnet documentation and domain contract. ADR-0269 records the detached
ownership and scheduling decision. No new troubleshooting root cause or parser
algorithm was introduced. Existing parser coverage remains in the complete
packages; no new performance or production-capacity claim is made. Code-map,
indexed TSR and whitespace checks close out this checkpoint. New admission
thresholds and SAVE admission ordering remain deferred to 04d.

## Checkpoint 04d: complete storage admission and SAVE ordering

Status: complete; final qualification and review are recorded below. Checkpoint
04c is committed as `4333811`.

The selected design retains the approved byte/work limits and current owners.
Filter bounds actual record reads at limit plus one byte, checks shared P+E before
bulk materialization, and admits configuration/baseline through standalone v4
SavedPreset encoding. A pure encoding/admission helper checks normalized complete
UserRecord bytes without adding path resolution to loading. PreparedUserRecord
remains the immutable durable payload; commits do not reload or serialize.

Telnet admits a detached human candidate's configuration, baseline and bounded
session-temporary record first. Capture the session timestamp and at most five
detached recent IPs on restore; those values never authorize overwriting a
refused record. Prepare the authoritative destination before human publication.
An explicit current-record-load error preserves causality: an existing refused
file allows fitting warned temporary human settings, whereas a successfully
loaded record whose newly combined prospective bytes exceed admission rejects
the proposed edit with zero effects. Initial protected-login status stays sticky;
warm refusals remain retryable on later save-intent commands and ordinary logout.

Replace configuration/login fault seams with commit seams receiving only the
admitted PreparedUserRecord. Production commits its captured bytes. Machine and
LOAD retain persistence before publication and VALIDATE remains nonpersistent.
Human commit failure retains the published edit and the existing warning style.

SAVE's narrow callback coordinator retains the internal baseline-callsign
collection stripe while the caller holds its full-SSID lease. Prepare private
library bytes, then an admitted association payload; reject nil/zero payloads
before either writer. Commit library then record, reporting library-only success
only after a successful library replacement. Callbacks may not reenter preset
operations; no unlocked prepared collection or new release lifecycle is exposed.

Separate-context read-only design and detailed test reviews found no new product
policy. The lead accepts all six detailed-review findings as checker-only
refinements: production/direct-callback coverage, counted-reader evidence,
unusable SAVE payloads, distinct logout retries, preserved API validation order,
and login-metadata overflow after a successful load. No implementation or
qualification was performed by those reviewers.

A further source-grounded refinement enforces the existing 20 visible presets
before bulk materialization. A saturated typed-key observer stores at most 21
keys, follows the existing insertion/null/merge predicates, and leaves value
preflight and P+E unchanged. Continue preflight after count overflow to preserve
existing type/owner/alias refusals; the reader checks trailing-document framing
before returning the original count error. This changes no cardinality policy.
Hand-derived 100,000-entry and repeated-key controls establish why count and
work alone must not be described as heap-byte guarantees. No winner-only
materialization or additional limit is selected. Allocation/scale evidence must
check operational usefulness; inadequate protection or unacceptable cost blocks
closeout rather than silently changing thresholds.

| Contract | Boundary / fault | Stimulus | Required result | Level | False-green risk | Planned checker | Owner |
|---|---|---|---|---|---|---|---|
| Authoritative configuration bytes | Serializer disagreement | Standalone v4 SavedPreset at 262,144/262,145 bytes; Configuration encoding larger than admitted SavedPreset | Exact boundary accepts/rejects using SavedPreset; preserve false/default/ordered/UTF-8 values | Unit and storage | Measuring Configuration or a lower bound only | Admission boundary tests | filter |
| Raw reading | Large file consumed before refusal | Counted reader with unread sentinel beyond 1,048,577 bytes and actual file controls | At most limit+1 consumed; exact raw boundary accepts; excess refuses without effects | Unit/file | Read-all then length check | Production read-helper and Load tests | filter |
| Canonical and prospective records | Aliases or fresh metadata enlarge output | Raw-small canonical-large IP aliases; fitting loaded metadata plus fitting replacement preferences yields excessive prepared bytes | Distinct refusal boundaries; prospective refusal publishes/writes nothing | Unit/consumer | Classifying all admission errors as existing-file refusal | Canonical/prepared and human tests | filter, telnet |
| Active work budgets | Nested reset or helper bypass | Hand-counted exact/over P+E, UTF-8/ignored nodes, nested baseline/whole collection, seventy 32 KiB scalar aliases | Shared approved limits before bulk; old alias/cycle/skipping refusals remain | Unit/fuzz/storage | Unlimited differential helper substituted for production evidence | Load/all preset verbs/direct callbacks plus limited fuzz | filter, telnet |
| Existing collection count | Too many defaults materialized | 20/21 and many empty entries; aliased keys, null insertion, overrides and shadowed merges | Count visible typed entries; saturated observer; refuse before bulk; preserve prior errors and framing | Unit/storage/scale | Counting raw pairs or skipping losing values | Collection observer and refusal allocation tests | filter |
| Operational usefulness | Admitted repeated values still expensive | Increasing refused distinct-key and admitted repeated-alias-key collections | Correct outcome and measured allocation/time; no heap or production-capacity guarantee | Bench/scale | Count-only benchmark misses admitted duplicate values | Guarded collection scale controls | filter, lead |
| Fitting warm human edit | Disk becomes refused after login | Actual raw/canonical/work-refused files, then fitting human commands | Warned live edit, unchanged file, retained runtime state; no sticky warm protection | Unit/checkout | Only acknowledgement or protected-login fixture | Registered warm-session and checkout cases | telnet, runner |
| Temporary admission | Candidate, baseline or cached metadata too large/invalid | Refused file plus individually bad candidate/baseline/session-temporary record | Zero publication/write/revision/history/SOLAR/NEARBY effects | Consumer | Fallback blindly permits every human command | Temporary representation tests | telnet |
| Logout and metadata | Retry paths differ | Still-refused logout; repair then command; repair then logout only | Preserve bad bytes; successful retries use fresh authoritative timestamp/IPs | Consumer/checkout | A second command masks broken teardown | Warm/protected/reconnect exit tests | telnet, runner |
| Login metadata overflow | Load succeeds, new timestamp/IP does not fit | Admitted source record near final-record boundary | No writer; restored usable configuration and warning; protection remains false | Consumer | Oversized-load tests cover the wrong branch | Login preparation-overflow test | telnet |
| SAVE admission/order | Association cannot be admitted or is unusable | Quota/load refusal, nil/zero callback payload, real library/record replacement faults | No writers before admission; library failure stops record; later record I/O yields library-only success | Unit/checkout | Generic save hook bypasses preparation or wrong filesystem fault | Coordinator and actual SAVE failures | filter, telnet, runner |
| Ownership/exact payload | Concurrent SSIDs/reconnect or source changes | Stalled commits, same-base SAVE/DELETE, same-SSID handoff, mutation after preparation | No lost library update or retired write; exact admitted bytes committed | Race/consumer | Sequential or callback-only proof | Existing/new lease and prepared-byte tests | filter, telnet |
| Preservation | Scope leaks into later behavior | Existing API markers/order, no-ops, SOLAR clocks, NEARBY, large LOAD/readback and VALIDATE | Preserve all distinctions; replace only explicit inactive-quota expectations | Unit/checkout | New shared helper changes unrelated validation/defaults | Complete filter/telnet and cumulative checkout cases | filter, telnet, runner |

The selected lane includes complete filter/telnet ordinary and race checks,
the full ordinary Windows suite and all repository analyzers, actual checkout
cases in both transports with and without race, meaningful limited-decoder
fuzz/scale checks, runner controls, support/decision documentation, indexed TSR,
code maps and final direct diff/whitespace review. Observed evidence follows.

Initial ordinary filter/telnet and the full 70-package Windows suite passed on
the pre-cost-correction production state (`checkpoint-04d-packages.log` and
`checkpoint-04d-baseline.log`). The cumulative ordinary checkout run passed
48 required cases, 24 per transport, in
`.tmp/command-fixes/20261009-141648-127cc21e`; the runner's 22 offline controls
passed separately. These establish semantic/fixture evidence, not final closeout.
Early runs caught test build mistakes, a null-key fixture that YAML interprets
as null, an invalid preset fixture name and a VALIDATE test using an unsupported
resource. Those failed logs are retained; the fixtures were corrected against
the frozen source and existing command contract.

The active admission fuzz target passed 1,372 executions over about 16 seconds
with two workers (`checkpoint-04d-fuzz.log`). Its tool job had a 1 GiB guard;
that is a qualification harness bound, not a production heap guarantee.

All six initial collection scale controls returned the required result, but
their benchmark PASS did not satisfy operational usefulness. At 100,000 entries,
distinct-key refusal allocated 1,224,268,176 bytes cumulatively and took
1.7547554 seconds per timed decode; repeated-key admission allocated
2,793,917,992 bytes and took 3.843905 seconds. The benchmark job's peak was
228,032,512 bytes under the 1 GiB guard. Parsing and final collection canonical
checks were outside the timed decode; parsing was inside the guarded process.
These are local synthetic measurements, not
production latency, concurrent-memory or worst-case guarantees.

The lead accepted the quality review's operational-cost blocker. A guarded
allocation profile of the 1,000 repeated-key control attributed 82.32% of
cumulative allocation space to `addStoredFields`, which repeatedly rebuilds
the same fixed YAML field table; stock decoder construction accounted for
3.44% flat. Source inspection confirms production owner prechecks bypass the
already operation-owned field metadata. Logs/profiles are
`checkpoint-04d-scale.log`, `checkpoint-04d-profile.log` and
the updated profile `04d-collection-final-alloc.pprof` under
`.tmp/command-fixes`; the original allocation summary remains in its log.

The bounded correction reuses only immutable schema metadata within the existing
operation state, keyed by exact Go type, through owner and recursive value
validation. Free validators remain fresh for the frozen oracle. Every owner,
value, error boundary, work charge, alias guard and losing-value materialization
remains. No scalar decoder, limit, winner-only optimization or retained cache
is selected. Design and quality review found this correction within approved
04d implementation scope. Revised scale evidence must resolve the blocker;
otherwise any materially different mitigation requires revised scope approval.

The review also found a weak timed SAVE/DELETE ownership checker. It now asserts
the actual stripe is held at both preparation and record commit, retains the
real concurrent final-state check, and releases/joins workers with bounded
cleanup. Targeted coordinator tests passed in
`checkpoint-04d-coordinator-review.log`. Separate temporary admission tests
now cover oversized/invalid baselines and oversized cached metadata, checking
unchanged SOLAR schedule, NEARBY cells/snapshot, history cursor/generation,
revision, pause, dedupe, bytes and commit count. The lead accepted both checker
findings and verified the fixes. These reviews are technical separate-context
evidence, not an independent scientific assessment.

Revised scale controls passed on the frozen schema-reuse implementation. At
100,000 entries, distinct-key refusal fell to 42,702,512 cumulative bytes and
81.3999 milliseconds; repeated-key admission fell to 430,713,408 bytes and
458.4064 milliseconds. All six 21/1,000/100,000 controls retained their required
outcomes. Peak job memory was 228,618,240 bytes, essentially unchanged because
parsing and the owned tree remain. The follow-up profile no longer has
`addStoredFields` among the displayed allocation contributors; normalization
and stock scalar decoding remain deliberate per-value work. Results are in
`checkpoint-04d-scale-final.log` and `checkpoint-04d-profile-final.log`.

The lead dispositions the repeated schema-construction blocker as resolved by
the narrow approved correction. This does not claim a production memory,
concurrency or latency ceiling. No skipped values, new scalar conversion or
changed bounds were needed. Fresh source review confirms free legacy validators,
exact-type schema isolation and per-node validation/accounting remain intact.
Final source review found no unresolved material defect. The remaining repeated
value allocation is substantial; these serial local controls do not establish
worst-case or concurrent production capacity. Normal collection writers retain
20 canonical entries; no arbitrary reduction of approved limits was selected.

Final validation:

| Check | Observed result / artifact under `.tmp/command-fixes` |
|---|---|
| Complete filter/telnet ordinary and race suites | PASS; `checkpoint-04d-packages-final.log`, `checkpoint-04d-race.log` |
| Complete ordinary Windows suite | PASS, 70 packages; `checkpoint-04d-baseline-final.log` |
| Vet, Staticcheck and golangci-lint | PASS; `checkpoint-04d-vet-final.log`, `checkpoint-04d-staticcheck-final.log`, `checkpoint-04d-lint-final.log` |
| Actual checkout server, both transports | PASS, 48 required cases each for ordinary and race builds; `20261009-143641-16784c56/results.json`, `20261009-143710-8f8155d5/results.json` |
| Runner controls | PASS, 22 controls; `checkpoint-04d-runner-controls.log` |
| Active bounded admission fuzzing | PASS, 2,397 executions over about 16 seconds with two workers; `checkpoint-04d-fuzz-final.log` |
| Six increasing admitted/refused collection controls and follow-up profile | PASS; revised cost evidence above and `checkpoint-04d-scale-final.log`, `checkpoint-04d-profile-final.log` |
| Indexed troubleshooting and support-critical entry comments | PASS; `checkpoint-04d-tsr-final.log`, `checkpoint-04d-comments.log` |
| Code maps and final whitespace/diff review | Generated, checked and reviewed before commit |

Each server run had 24/24 passing required cases per transport, zero wire and
cleanup failures, and two joined child generations per transport. Both captured
the same compiled-Go/module source digest:
`dc12077e714e646cda751a1a0791732caf93bdbb40033ec503c2f04128504aac`.

The final analyzers caught an intentionally invalid nil callback result needing
an explicit fixture lint exemption and an unread private schema-fixture field.
The fixture now checks actual YAML exclusion of that field. An initial check
mistakenly decoded the separate inline-collision fixture and panicked; it was
corrected to use the plain field fixture. Both ordinary and race targeted checks
passed (`checkpoint-04d-analyzer-fixtures-final.log` and `-race.log`). These
test-only corrections did not invalidate the preceding complete production
suite; final checkout builds and analyzers include them. Failed logs remain.

Implementation mapping: `filter/storage_admission.go` owns exact record admission
and bounded reads; `storage_decode*.go` owns shared work and collection preflight;
`configuration*.go`, `user_record.go`, `user_preferences.go` and
`prepared_user_record.go` enforce the selected representations; `presets.go`
coordinates both admitted SAVE payloads. `telnet/configuration_admission.go`,
`human_preference_stage.go`, session restoration, machine transactions and preset
dispatch preserve publication/ownership contracts. Boundary, consumer, race,
fuzz, scale and cumulative checkout tests map to the matrix above.

Support impact is runtime/operator-visible: updated telnet and configuration
guides, domain/environment contracts, the configuration support card, source map
and support coverage ledger. ADR-0270 records the durable admission decision;
TSR-0052 retains the measured schema-allocation learning and production-monitoring
boundary. No external service, deployed support agent or production-load check
was run. All accepted reviewer findings are dispositioned above; no waiver is
used to declare this checkpoint complete. Checkpoint 04d is committed as `c0ad48b`.

## Checkpoint 05a: shared station reputation without reconnect replenishment

Status: complete. Architecture and checkers preceded production edits; final
qualification and implementation mapping follow below.

Inspected pre-change source: telnet login calls `Gate.RecordLogin` with full-SSID identity;
manual DX already passes the displayed baseline sender to `Gate.Check`.
`RecordLogin` reloads/touches history before taking the call-state lock, then
unconditionally resets the active window and counts. `Store.Touch` serializes
writes, but `Store.Load` and the earlier history observation are not serialized
with those writes. Sharing the key therefore also needs coherent history
observation/update and state publication, without holding the Check lock across
enrichment or disk I/O. Semantic `gopls references` confirms the production
login and manual-post consumers; no additional production gate consumer appeared.

Approved constraints: normalize login and posting at the Gate boundary to the
same baseline callsign. Preserve full-SSID configuration ownership and baseline
spot display. Ordinary reconnects keep active counts/window, except normal elapsed
window advancement. Configured penalties can tighten limits or extend applicable
deadlines, without clearing spent counts or shortening a deadline. Preserve old
SSID files, use existing baseline history, and apply ordinary new-station
probation when no baseline exists. Counts remain runtime-only.

Retained-state audit: keep existing 32 call-state shards, their TTL sweep and
per-shard sweep threshold, bounded history slices and existing prefix/cache
owners. No new callsign registry or durable counters are selected. The sweep
threshold is not an insertion-time hard cap. Eviction/restart can discard runtime
counts; the approved active-state guarantee does not add persistence or change
eviction policy. Tests must show aliases co-own one state, expiry deletes it,
and no secondary per-login state survives deletion.

| Contract | Boundary / fault | Stimulus | Required result | Level | False-green risk | Checker | Owner |
|---|---|---|---|---|---|---|---|
| Shared key | Different numeric SSIDs | Actual login and Check using mixed case/SSID and baseline forms | One baseline history/state; independent full-SSID preferences | Unit/checkout | Changing only one caller, or checking only display | Gate API and actual registered server/processor cases | reputation, runner |
| Benign reconnect | Spent per-band and total allowance | Post through one SSID, reconnect another before the natural boundary | Further posts refused; window/counts/deadlines preserved | Unit/checkout | Login-only test never spends quota | Explicit-time API decisions plus archive absence/presence | reputation, runner |
| Natural advancement | Just before/at exact window end | Reconnect and Check around a literal UTC boundary | No early replenishment; existing elapsed-window progression at boundary | Unit | Sleeping through a boundary hides reset defects | Controlled-time gate cases | reputation |
| Probation | Existing later wait | Reconnect while waiting, then valid posts around deadline | Reconnect never shortens wait | Unit | Only checking first login | Before/at deadline decision controls | reputation |
| Risk tightening | New ASN/country after spent allowance | Distinct known IP evidence and retained baseline history | Existing counts remain; applicable limits tighten/deadlines extend | Unit | Checking a fresh empty state | Actual RecordLogin/Check with spent allowance | reputation |
| Upgrade ordering | Previously scheduled later upgrade | Penalty after maturity, then ramp/time advancement | No premature cap upgrade or shortened later floor | Unit | Only nextAllowedAt asserted | Explicit ramp and total-upgrade timeline | reputation |
| History ownership | Old SSID files or baseline absent | Byte-fixed old records, existing/missing baseline and new gate | Old files untouched/unmerged; correct baseline history/probation | Unit/checkout | Auto-migration or new key skips all history | Exact file readback and restart controls | reputation, runner |
| Concurrent ownership | Lookup/history/publication interleave | Bounded barriers and concurrent SSID login/Check | No corrupt history, lost counts or stale final owner metadata | Race/unit | Serial calls and race flag without concurrent stimulus | Controlled ownership plus concurrent quota/history tests | reputation |
| I/O isolation | Slow history work | Hold the history owner while checking established state | Check remains usable without a network/disk-held state lock | Unit | Final-state-only test misses lock obstruction | Direct lock/barrier checks with bounded joins | reputation |
| Retention | Aliases, expiry and churn | Many SSIDs for one station, expire/sweep, insert replacements | Single active station state; existing deletion/bounds remain | Unit | Count only on first insertion | Shard cardinality and expiry/churn checks | reputation |

The lead-owned detailed falsifiability pass identifies no need for a new clock
API: controlled UTC Gate calls cover exact time rules; actual checkout cases
prove login/post wiring, durable identity and lack of queued/archive effects.
Generic client rejection feedback remains checkpoint 05b. A separate-context
read-only design challenge resolved the narrow history/publication owner before
production edits, with the lead disposition below. This is technical review,
not scientific independence.

Lead disposition: accept the challenge's fixed login-only mutex on each existing
call shard. Perform enrichment before it; retain it through history observation,
touch and state publication. Take the separate state mutex only for bounded
in-memory publication, re-fetching the map entry after possible eviction. Current
semantic Store callers are Gate-owned, so this eliminates the shared history
race without changing Store APIs, callback machinery or disk failure semantics.
There is no new per-call lock registry. Strict arrival-time ordering is not
required; completed history transactions and their publication are serialized.

Advance existing elapsed windows before applying login changes. On configured
risk, tighten current caps and take later applicable probation/ramp/upgrade
deadlines, leaving the window and counts intact. Retain the existing role of
extra delay (ramp, not added probation). Require per-band maturity before a
pending total upgrade, then retain the later old floor or configured delay.
Keep lastSeen nondecreasing. Checks must progress during stalled login history;
concurrent publication and eviction-during-preparation checkers are added to
the matrix's ownership/retention rows. These are within-scope correctness and
checker refinements; no unresolved product selection or scope change remains.

The fresh source-quality review found three checker gaps, all accepted and
corrected: directly prove the login mutex remains held after the history write
through stalled state publication; preserve a pre-existing later ramp floor;
and explicitly exercise replacement churn beyond the actual per-shard sweep
threshold. The new controls use actual files/locks with bounded worker release
and joins. The failed-history control now asserts both 50-second probation and
150-second ramp floors survive a newly derived shorter restriction. Existing
concurrent/eviction controls also check older operation timestamps cannot reduce
lastSeen. No material production defect remained in the fresh source review;
final broad and checkout qualification follows after all worker edits freeze.

Configuration impact: `data/config/reputation.yaml` changes comments only.
The YAML header and comment-stripped-value checker passed. Its two boolean
inline-comment prompts were reviewed: their effects are the configured risk
triggers, which now reapply tighter restrictions without resetting spent counts.
No key/default/sentinel changes or package-loader fallback were introduced.
The real fixture retains loaded initial-wait policy and seeds existing baseline
history for eligible positive posting controls; missing-baseline/exact-time
behavior is separately proven by Gate unit cases.

Final qualification:

| Check | Observed result / artifact under `.tmp/command-fixes` |
|---|---|
| Complete reputation/commands/telnet ordinary and race suites | PASS; `checkpoint-05a-packages-final.log`, `checkpoint-05a-race-final.log` |
| Complete ordinary Windows suite | PASS, 70 packages; `checkpoint-05a-baseline-final.log` |
| Complete race Windows suite | PASS, 70 packages; `checkpoint-05a-full-race.log` |
| Vet, Staticcheck and golangci-lint | PASS; `checkpoint-05a-vet.log`, `checkpoint-05a-staticcheck.log`, `checkpoint-05a-lint.log` |
| Actual checkout server, both transports and profiles | PASS, 60 required cases each in ordinary and race builds; `20261009-152232-d5dc6d4d/results.json`, `20261009-152306-080885f8/results.json` |
| Final runner controls | PASS, 24 controls; `checkpoint-05a-runner-controls-final.log` |
| Comment-only YAML and indexed troubleshooting | PASS; `checkpoint-05a-yaml-docs.log`, `checkpoint-05a-tsr-final.log` |
| Support-critical entry comments, code maps and direct diff/whitespace review | Passed and reviewed before commit |

Each server run has the preceding 48 required cases plus six reputation cases
per transport. All four profiles passed, with zero wire/cleanup failures and two
joined child generations each. Both builds captured compiled-Go/module digest
`43fca3866c2d9fa4c153c7abcef7d7940f56cd5ed8988f7e1722b2f67bf474f9`.
That digest does not cover Python scripts. The fixture shares the actual loaded
Gate between telnet login and Processor posting, with enrichment workers stopped.
This qualifies those consumers and owned history/state, not external services
or the production correction/classifier pipeline.

Two failed attempts remain in the evidence. The first broad run stopped at the
unchanged `TestSignalResolverResourceCaps` candidate-pressure assertion
(`checkpoint-05a-baseline.log`). Its isolated retry and both subsequent complete
ordinary/race runs passed without spot source/test changes. The test's winner
predicate does not fence processing of every queued evidence item; this is a
plausible timing explanation, not a reproduced causal proof or a flake repair.

The first checkout child rejected an invalid private `initial_wait_seconds: 0`
before startup (`20261009-152114-46ec7991`). The staging code had assumed the
later normalization fallback would admit zero, but the strict loader rejects it
first. The private fixture now retains the shipped valid 60 seconds; known
baseline history provides eligible positive controls. Final runner controls and
both actual-server builds were rerun after this script-only correction. It did
not change Go source or invalidate the completed Go lane. No failed attempt is
counted as passing server evidence.

Implementation mapping: `reputation/gate.go` owns baseline ingress, fixed shard
login/history publication, elapsed-window progression and upgrade ordering;
`login_restrictions.go` tightens restrictions without replacing counts/window.
`login_restrictions_test.go` covers literal UTC boundaries, risk, failed history,
stalled I/O, concurrent ownership, eviction and churn. The checkout fixture and
runner prove actual login/post wiring, full-SSID persistence, archive effects and
restart limits. Client rejection wording remains checkpoint 05b.

Support-agent documentation impact: Required. The developer reputation route
now links ADR-0271; telnet/configuration guides and the domain contract describe
shared allowances, preserved files and runtime-only counters. ADR-0271 records
the durable decision and ADR-0115 points to its refinement. No separate durable
troubleshooting record is needed for the fixture input error. The final lead
pass reviewed source, tests, scripts, documentation, scope and observed checks;
no material defect, waiver or new product decision remains. No production-load,
external enrichment service, deployment or external support-agent check ran.
Checkpoint 05a is committed as `f8fa835`.

## Checkpoint 05b: truthful manual DX rejection feedback

Status: complete. Inspected Processor dispatch and the actual shared
Gate/profile from checkpoint 05a. All gate drops currently return an empty
response after the existing diagnostic callback, before Spot construction and
queue admission. The approved change affects that response only: return
`Spot rejected.` without exposing internal gate details. Preserve the callback,
queue ordering, baseline display and existing accepted/full-queue responses.
No new state, interface, configuration or lifecycle owner is needed.

| Contract / fault | Stimulus | Required observation / checker |
|---|---|---|
| Silent or overly detailed rejection | Real Gate probation, band, total and prefix refusals through Processor in both dialects | Exact generic response, zero queued spots and correct internal callback reason |
| Response depends on diagnostic callback | Refusal with callback absent | Same exact response and zero queue effects |
| Accepted controls lose baseline identity | Eligible real Gate posting from numeric SSID | Existing queue acknowledgement; queued DE/source remain baseline |
| Wire acknowledgement hides queue effects | Actual loaded server, spent band/total quotas and reconnects | Generic response in both transports, accepted FIFO archive barrier and refused archive absence, repeated after fresh child |
| Oracle accepts silent denial | Offline controls with empty, accepted or internal-detail feedback | Refusal checker fails those samples; real-server case remains required |

The lead-owned test-strategy pass uses actual Gate decisions and existing
consumer/archive barriers. Unit prefix priming uses a later explicit timestamp
to prevent elapsed refill from making the cause depend on test speed. The
diagnostic reason must prove which branch refused the post. Rejection wording
and test strengthening are within approved 05b; no product ambiguity remains.

Observed qualification:

| Check | Result / artifact under `.tmp/command-fixes` |
|---|---|
| Focused real-Gate feedback cases | PASS; `checkpoint-05b-targeted.log` |
| Complete commands/telnet ordinary and race suites | PASS; `checkpoint-05b-packages.log`, `checkpoint-05b-race.log` |
| Complete ordinary Windows suite | PASS, 70 packages; `checkpoint-05b-baseline.log` |
| Vet, Staticcheck and golangci-lint | PASS, zero lint issues; `checkpoint-05b-vet.log`, `checkpoint-05b-staticcheck.log`, `checkpoint-05b-lint.log` |
| Runner controls | PASS, 25 controls; `checkpoint-05b-runner-controls.log` |
| Actual checkout server | PASS, 60 required cases each in ordinary/race builds; `20261009-153544-71d34510/results.json`, `20261009-153619-c6d3e06b/results.json` |

Each build passed all four profiles with zero wire/cleanup failures and two
joined child generations per profile. Their compiled-Go/module source digest is
`e586fb57a20de6d3d05ff3a372c261b16a95d56c48850b4192bb465dc9eb395d`.
The real-server checker requires only the generic response, preserving the
existing archive barrier and post-restart refusal-absence checks. Unit cases
prove all four gate causes still reach the internal callback and rejected spots
never reach the queue. No new production concurrency was introduced; targeted
race/actual-server evidence supplements 05a's unchanged state-owner full race
qualification. No failed attempt or waiver was needed for checkpoint 05b.

Implementation mapping: `commands/processor.go` changes the gate-drop response;
`manual_dx_reputation_test.go` proves both dialects, all causes, absent callback
and accepted baseline display. The runner and its offline controls require exact
generic wire feedback and retain independent archive outcome controls.

Support-agent documentation impact: Required. `commands/README.md` documents
queue/rejection outcomes, `telnet/README.md` points operators to configured gate
diagnostics, and `customgpt/common-questions.md` routes the rejection question to
those sources. No new architecture or troubleshooting decision is introduced.
Final source/diff review found no unresolved material defect; entry-comment and
map/whitespace checks are recorded with the checkpoint artifacts before commit.
External services, production load and the production correction/classifier
pipeline remain outside this consumer qualification.
Checkpoint 05b is committed as `a483708`.

## Checkpoint 06: canonical frequency validation before admission effects

Status: complete. Inspected pre-change Processor parsing accepts positive plain
decimal kHz, but admission uses its raw band and only the later constructor
rounds frequency. Tiny values can become zero; out-of-envelope and rounded
overflow values reach admission. The approved contract is finite canonical
frequency inside the existing inclusive Spot envelope, before admission/queue
effects, retaining interior-gap behavior.

Inspected `spot/spot.go`, `bands.go`, constructor/band tests, Processor dispatch
and both argument orders, the actual server's bounded input reader, and ADR-0045.
Current envelope is 135.70 through 2450000.00 kHz, including the upper disjoint
13cm segment. `gopls references` confirms the existing rounder's three
constructor and two FT-frequency production consumers, FrequencyBounds' Spot
validator consumer, and both private frequency-parser call sites. Targeted
`go list -deps -test -json` confirms commands -> spot/reputation and telnet ->
commands/spot/reputation consumers and test dependencies. Existing constructors
and FT callers require compilation/regression coverage, not behavior changes.

Lead-owned design-space disposition: expose the existing rounder through one
stateless public wrapper. Compute the canonical value in manual DX dispatch;
use it for envelope/finiteness checks and the admission band. Retain existing
raw inputs to comment parsing and constructor normalization. Earlier Spot
construction would move its timestamp/allocation before admission; duplicating
rounding would permit drift. The narrow wrapper adds no owner, state, goroutine,
registry or algorithm. This is a local implementation choice within approved
06; no propagation/correction/model rule or global Spot validator change is
selected, and no scientific independence claim is needed for reusing ADR-0045.

| Contract | Boundary / fault | Stimulus | Required observation | Level | False-green risk | Checker | Owner |
|---|---|---|---|---|---|---|---|
| Canonical envelope | Raw neighbors/ties at min/max | Literal decimal vectors and adjacent representable floats in both orders/dialects | Exact accepted canonical values or invalid-frequency response; queue agrees | Unit | Testing raw bounds instead of rounded bounds | Literal boundary vectors and queued Spot fields | commands |
| Finite output | NaN/Inf, parse overflow, finite value whose scaling overflows | Plain/nonplain token vectors and long decimal values | Invalid response, no queue | Unit/fuzz | Lexer rejects only Inf but finite scaling still overflows | Direct Processor boundary and active domain fuzzing | commands |
| No admission cost | Invalid values before an eligible post | Real Gate with a one-token prefix budget and one total allowance | No diagnostics/queue for invalid values; eligible control still queues | Consumer | A later archive validator or prefix refill masks prior admission/queue effects | Direct queue, callback and remaining-budget control | commands |
| Canonical admission band | Raw value just below 20m rounds into it | Accepted boundary post then ordinary 20m post | Canonical queued band; same-band refusal | Consumer | Only checking frequency, while Gate still keys raw band | Real Gate reason/queue checks | commands |
| Interior gaps | Existing envelope gaps | Representative HF and disjoint-13cm gap values | Existing acceptance and unknown-band result retained | Unit | Replacing envelope with segment membership | Literal gap controls | commands |
| Actual ingress | Invalid values before spending shared allowance | Registered server in both transports with archive barriers/restart | Exact frequency feedback; later positive quotas intact; invalid archive absence | Checkout | Reply alone or off-target server becomes green | Required loaded-gate profile case | runner |
| Public rounding access / discovery | Shared API and HELP disagreement | Existing constructor vectors plus both HELP DX topics | Same 10 Hz rounder; current inclusive bounds and gap wording | Unit/full build | Copying arithmetic or stale hard-coded help | Golden vectors, HELP width/catalog checks and complete baseline | spot, commands |

The lead-owned detailed test-strategy pass separates raw/canonical neighbors,
parse/scaling overflow, queue effects and admission effects. Fuzzing checks the
actual Processor's admitted-output domain with tokens up to 1024 bytes; this is
a test-work bound, not a new ingress limit. Long overflow values use the direct
API because the real fixture retains its smaller loaded human-line ceiling.
The existing generator/profile/receipt and independent FIFO archive barriers
remain. No unselected behavior or remaining product choice is inferred.

Observed qualification:

| Check | Result / artifact under `.tmp/command-fixes` |
|---|---|
| Focused boundaries and public rounding access | PASS; `checkpoint-06-targeted.log` |
| Complete commands/spot/telnet ordinary and race suites | PASS; `checkpoint-06-packages.log`, `checkpoint-06-race.log` |
| Complete ordinary Windows suite | PASS, 70 packages; `checkpoint-06-baseline.log` |
| Vet, Staticcheck and golangci-lint | PASS, zero lint issues; `checkpoint-06-vet.log`, `checkpoint-06-staticcheck.log`, `checkpoint-06-lint.log` |
| Runner controls | PASS, 26 controls; `checkpoint-06-runner-controls.log` |
| Active admitted-domain fuzzing | PASS, 75,665 executions, about 16 seconds, two workers; `checkpoint-06-fuzz-final.log` |
| Actual checkout server | PASS, 62 required cases each in ordinary/race builds; `20261009-160258-10023f4f/results.json`, `20261009-160334-f6fa2822/results.json` |

The server runs retained 24 preceding cases and added a required frequency case
to each reputation profile (now seven per transport). All four profiles passed,
with zero wire/cleanup failures and two joined child generations each. Both
captured compiled-Go/module digest
`70154cbde12c3c0e2df2f7692db097014c531999b267a270143fcb6ee4ef7235`.
Invalid values received exact frequency feedback, left existing quotas available,
and remained absent after the FIFO barrier and a fresh child. Long overflow
vectors belong to direct API/fuzz evidence, not a claimed wire test above the
fixture's human-line ceiling. Interior-gap and exact rounded-neighbor controls
prove queue admission at the direct Processor boundary.

The first native fuzz launch omitted the required `-test.fuzzcachedir` flag and
exited before fuzzing (`checkpoint-06-fuzz.log`). Its ignored qualification
helper now supplies an owned cache, rebuilds with fuzz instrumentation, holds
the existing native runner lock and uses the existing bounded Job Object tool
owner. The corrected run passed and retired workers before releasing the lock;
the failed launch is not active-fuzz evidence. No production/test-source change
was needed for that launch correction. The final lead falsifiability review also
tightened the budget test to one total allowance, so prefix refill cannot hide
an accidentally spent allowance; both complete targeted suites include it.

Implementation mapping: `spot.RoundFrequencyTo10Hz` exposes the unchanged
constructor rounder; Processor validates its effective value and uses its band
before Gate admission. Raw comment/constructor inputs remain unchanged. New
frequency tests cover both orders/dialects, literal and representable neighbors,
parse/scaling overflow, gaps, remaining allowance and canonical-band accounting;
the active fuzzer checks admitted outputs independently against the envelope and
10 Hz lattice. Existing constructor and FT callers remain untouched, and a
final `gopls references` pass confirms the wrapper's only production caller is
manual DX dispatch. The required checkout case and strict offline response
controls qualify actual ingress and outcomes.

Support-agent documentation impact: Required. Compiled `HELP DX` and the command
guide expose the current rounded envelope and preserved gaps; the common-question
index routes rejected/differently counted frequencies there and to ADR-0045.
Existing ADR-0045 owns rounding; no new algorithm/default or separate durable
troubleshooting decision is introduced. Final source/test/script/doc review
found no material defect or waiver. Entry-comment, generated-map and whitespace
checks accompany final commit qualification. No external service, production
load or global ingest-validation change is claimed.
Checkpoint 06 is committed as `397875c`.

## Checkpoint 07: MODE visibility-preserving deltas

Status: complete. The selected D01 contract changes only named modes;
every unlisted mode keeps its prior visibility, including custom/unseen tokens
and future taxonomy entries. Other finite-family transitions remain unchanged.

Current-source discovery inspected Filter.SetMode, Matches, UnknownModeVisible,
passesStringFilter, mode command dispatch, effective MODE snapshots, human
overview/detail predicates, marked and unmarked user/preset decoding, current
tests and ADR-0018/0052/0247/0249. Semantic SetMode references have only two
production consumers, the telnet PASS/REJECT loops. The actual matcher treats
any nonempty allow map as restrictive, including false-only maps, regardless
of AllModes; BlockAllModes dominates dormant selections. Semantic
UnknownModeVisible
references identify only the two telnet feedback consumers. Targeted package
dependency inspection confirms filter -> spot/pathreliability and telnet/commands
-> filter without new APIs or interfaces. Marked owners preserve
flags/maps exactly; only unmarked historical records invoke migration. No
loader, schema, taxonomy, global matcher or default-policy change is selected.

The Filter owns the delta. Determine the prior open fallback using the flags
and raw map length, preserve that fallback, and update the named allow/block
entry. Opening one mode from BlockAll clears dormant allows so unrelated modes
stay hidden. Rejecting the last restrictive allow must leave a restrictive
empty map. Unknown-mode feedback uses the same matcher. MODE-only snapshots
ignore false/noncanonical entries without promoting finite taxonomy coverage to
an open fallback. General finite-family helpers and human exact stored-rule views stay
unchanged. State remains client-owned under existing detached publication and
filter locks; no new owner, retained structure or lifecycle is introduced.

The bounded separate-context design/falsifiability review was read-only. It
confirmed the pointwise design through a finite Boolean-state reasoning probe;
that is design evidence, not implementation validation or scientific independent
review. Its refinements are covered below: false-only maps cannot become open
after filtering display entries; UnknownModeVisible must agree with Matches;
old tests constructing a whitelist with ResetModes + SetMode must explicitly
start restrictive; marked persistence must retain empty restrictive states.
The lead owns implementation, oracles, final review and validation.

| Contract | Boundary / fault | Stimulus | Required observation | Level | False-green risk | Checker | Owner |
|---|---|---|---|---|---|---|---|
| Named delta | Open/restrictive, false-only, empty, conflicting flags/maps | All Boolean flags and absent/false/true entries; PASS/REJECT CW/FT8 | Named token equals requested visibility; every other observed token unchanged | Unit | Testing only default curated list | Matches-backed exhaustive pointwise invariant | filter |
| Dormant state | BlockAll with positive and false latent entries | PASS FT8; REJECT CW; repeat transitions | Only selected PASS opens, no latent mode revives | Unit | Starting block-all with empty maps only | Full state-space cases and literal checkpoints | filter |
| Unknown feedback | AllModes true with a restrictive map | UnknownModeVisible and actual blank/UNKNOWN match before/after | Same answer, including false entries | Consumer | Comparing two flag-only summaries | Actual Matches oracle | filter, telnet |
| Future/custom preservation | Nonstandard map keys and later taxonomy | Apply named deltas, add a taxonomy mode | Original open/restrictive fallback remains; retained custom visibility unchanged | Unit | Enumerating supported modes as a complete universe | Literal unseen/custom probes and private taxonomy fixture | filter |
| Truthful mode snapshots | False/padded/lowercase/alias extras, all-known restrictive list, contradictory flag | MODE effective response and SHOW FILTER MODE | Enabled tokens agree with normalized Matches; no false/custom promotion | Consumer | Finite display union hides false entries or future fallback | Matcher-backed snapshot and detail assertions | telnet |
| Persistence | Last allow removed, open minus block, latent/custom records | Human commands, marked save/load and reconnect | Live, saved and fresh-session flags/maps/visibility agree | Consumer/checkout | Logout repairs persistence or migration silently reopens modes | Immediate stored bytes plus second child readback | filter, runner |
| Other families | Same generic toggle callers | Existing BAND/SOURCE/EVENT tests and full package suites | Existing transitions unchanged | Regression | Broad helper change accidentally alters unrelated contracts | Generic helper remains untouched; full baseline | filter, telnet |

Planned qualification: focused and full filter/telnet/commands tests, targeted
race checks, complete ordinary Windows lane, vet/Staticcheck/lint, strict runner
controls, both actual-server build modes/transports and final comment/map/diff
checks. The state-space oracle states pointwise behavior independently of the
mutation representation. No parser change or performance claim is made.

Observed qualification:

| Check | Result / artifact under `.tmp/command-fixes` |
|---|---|
| Focused state/feedback/persistence cases | PASS; `checkpoint-07-targeted.log` |
| Final complete filter/telnet/commands ordinary and race suites | PASS; `checkpoint-07-packages-final.log`, `checkpoint-07-race-final.log` |
| Final complete ordinary Windows lane | PASS, 70 packages; `checkpoint-07-baseline.log` |
| Vet, Staticcheck and golangci-lint | PASS, zero lint issues; `checkpoint-07-vet.log`, `checkpoint-07-staticcheck.log`, `checkpoint-07-lint.log` |
| Runner negative controls | PASS, 27 controls; `checkpoint-07-runner-controls.log` |
| Existing COMMENT benchmark rejection guard | PASS, six cases with one iteration each; `checkpoint-07-benchmark-guard.log`; correctness only, no comparative timing claim |
| Actual checkout server | PASS, 66 required cases each in ordinary/race builds; `20261009-163641-dcb0a556/results.json`, `20261009-163719-a9492e1b/results.json` |

The implementation oracle checked 11,664 transitions (both flags, absent/false/
true entries for CW/FT8/custom, both named selections and actions), comparing
five tokens against actual Matches before and after. This includes unseen
fallback and unknown feedback. A private later taxonomy adds FUTUREMODE:
an open filter still accepts it while a restrictive filter, including one that
previously covered the complete current taxonomy, does not. Marked user records,
saved presets and applied baselines retain exact rules and actual visibility,
including restrictive-empty, false-only, dormant and custom entries.

The required checkout extension adds two main-profile cases per transport
(26 each, plus the unchanged seven reputation cases). It uses accepted CW and
FT8 archive rows as independent positive controls, verifies rejection/re-enabling
through both dialects, checks durable rules while connected, and queries again
after replacing the server process. Each build passed all four profiles with
zero wire/cleanup failures and two joined children each. Their compiled-Go/module
source digest is `ff5b66b895cf41485b20834f072e59c5b9723cc16ef70dbc1400dd7b4c0e2d5d`.
These are archive-backed history consumer checks; they do not claim a separate
unpaused live-fanout delivery qualification. Direct Matches and existing telnet
consumer suites cover the shared predicate. No parser change requires new fuzz
selection, and no performance claim requires comparison profiles.

The first focused launch mistyped the timeout argument and ran no tests
(`checkpoint-07-targeted-launch.log`). The corrected focused run passed. The
initial complete filter suite then caught an old COMMENT test using a named
PASS to construct an exclusive allowlist (`checkpoint-07-packages-initial.log`).
Its setup, the paired cheap-rejection benchmark, and old unknown-mode fixtures
now explicitly start restrictive; their negative assertions remain. Complete
targeted ordinary/race suites passed after those corrections. The final source
also passed the repeated complete ordinary lane; the preceding successful lane
is retained as `checkpoint-07-baseline-prereview.log` rather than reused as final
source evidence.

The separate-context technical quality review found no transition/ownership
defect and identified two feedback corrections: padded/noncanonical true keys
must not appear as effectively enabled, and source examples must explicitly
start a whitelist. Both are fixed and the Mode-only matrix is extended with
padded, lowercase and alias keys. Final targeted ordinary/race, full ordinary,
analyzers and actual-server builds all include those fixes. This review saw the
design and implementation; it is not scientific independent review. Fresh
lead-owned source/test/script/doc review found no unresolved material defect
or waiver. Generic toggles, matching precedence, configuration schemas, defaults
and unmarked migrations remain unchanged.

Implementation mapping: Filter.SetMode owns the named delta;
UnknownModeVisible uses its real predicate; telnet's Mode-only snapshot ignores
inactive/unreachable display keys while preserving raw fallback. The two new
test files qualify pointwise mutation, feedback, future taxonomy and marked
owners. Five exclusive unit/benchmark fixtures retain their original rejection
intent. The strict runner and controls qualify actual history, saved rules and
fresh-process outcomes. The ignored benchmark helper uses the established runner
lock, bounded tool owner, stable executable and fresh-source check; no workflow
policy or permanent runner mechanism changes.

Support-agent documentation impact: Required. The telnet guide explains deltas
and deliberate whitelist construction; the common-question index routes there,
to ADR-0272 and TSR-0053. ADR-0272 records the selected MODE-only refinement of
ADR-0018; the historical generic-family decision is preserved and cross-linked.
TSR-0053 records the observed whitelist/fallback trap and checker limitations.
Final entry-comment, TSR, generated-map and whitespace checks accompany commit.
No external service, deployment or production-load certification is claimed.
Checkpoint 07 is committed as `9affd59`.

## Checkpoint 08: independent NEARBY endpoints

Status: complete. The approved DX-or-DE condition means an unavailable
opposite endpoint cannot veto a matching available endpoint. Both unavailable
endpoints, an unknown band and an unavailable user cell still reject.

Current discovery inspected Filter.matchesNearby/nearbySpotCells, their semantic
references (one Matches caller and one cell-helper caller), existing nearby tests,
H3 encoders/table loading and the Windows fallback, Spot cache ownership and
invalidation, live deliverJob and historyFilterSnapshot matching, archive grid
encoding and the held checkout fixture. Existing fine caches use nonzero IDs
before grid derivation; coarse filtering derives grids on 160m/80m/60m. Preserve
that precedence and resolution. ADR-0146 owns Spot's cell-cache lifetime; no
cache policy, H3 calculation/table contract or geographic model is changed.

The narrow design removes joint endpoint prerequisites in the existing cell
helper and matcher. Each side resolves independently using the existing rules;
the valid user cell may equal either result. No new helper, interface, config,
retained state, allocation, worker, queue or deadline owner is introduced.
This is a Boolean correctness fix rather than a performance optimization.

Lead-owned detailed falsifiability review:

| Contract | Boundary / fault | Stimulus | Required observation | Level | False-green risk | Checker | Owner |
|---|---|---|---|---|---|---|---|
| Either endpoint | One matching side, opposite blank/invalid | Swap DX/DE on fine and every coarse band | Match succeeds symmetrically | Unit | Fixing only matcher while cell helper still discards both | Actual Matches table with loaded H3 controls | filter |
| Unmappable opposite | Valid grid absent from a private mapping | Fresh child loads owned count-valid tables with only the opposite cells removed | User grid maps, opposite grid is unmapped, either available side still matches | Isolated unit | Invalid syntax does not exercise a mapping miss; global once-loader hides fixture | Required child execution marker and positive/miss guards | filter |
| Cache precedence | Cached fine ID with missing/mismatching grid | Matching cached DX or DE; coarse band with fine cache only | Fine cache remains authoritative; coarse does not use it | Unit | Deriving every side or reusing fine IDs on coarse bands | Literal cell-ID probes plus actual loaded mapping | filter |
| Negative admission | Neither match, both absent, unknown band/user cell | Explicit negative controls | No match or delivery | Unit/consumer | Only positive cases or missing metadata interpreted as a match | Exact boolean and live queue/history results | filter, telnet |
| Other filters | Nearby match with a mode/category refusal | Existing geography suspension plus independent mode block | Suspended location rules stay suspended; unrelated gate still refuses | Unit/consumer | Nearby OR bypasses the whole filter | Matches and actual live/history consumers | filter, telnet |
| Live/history agreement | Same fixed metadata across each consumer | Actual deliverJob queue and captured history predicate; archive-backed checkout queries | Same positive/negative decisions, no own-call exemption | Consumer/checkout | Replies prove configuration only; fixture lacks metadata enrichment | Direct live queue; required real archive case with fixed metadata seed controls | telnet, runner |
| Bounded cost | Existing per-spot matching | Reuse normalized fine/coarse fixtures | No new matching allocation; existing bounded state/owners remain | Unit/static | Incidental allocation or added global cache | AllocsPerRun correctness guard and final diff | filter |

The fixture's manual bridge does not enrich metadata. Add a fixed bounded set of
explicitly synthetic spots to its existing queue before readiness, with unique
child labels and distinct frequencies. Those test seeds qualify archive/history
matching, not manual admission or the production enrichment pipeline. Direct
live-consumer tests call the actual delivery owner and inspect its queue; no
separate unpaused socket-delivery claim is made. A private fresh child is needed
for the once-owned H3 mapping miss, rather than mutating live global mappings.
All test additions stay within the approved unattended runner extension.

Planned qualification: focused and complete filter/telnet/commands suites,
targeted race, complete ordinary lane, vet/Staticcheck/lint, strict runner controls,
both checkout builds/transports and final comment/map/TSR/diff checks. Mapping
changes are test-only, bounded and guarded; no model-quality or production-load
claim follows from them. No new policy choice or scope revision is required.

Implementation and observed targeted evidence: the production diff removes the
two joint-grid prerequisites and the joint-cell veto, retaining all other
matching logic. `filter/nearby_endpoint_test.go` qualifies symmetric missing,
invalid and distant endpoints on 20m/160m/80m/60m, fine-cache precedence, unknown
band/user cells, unrelated mode refusals and zero matching allocations on reused
fine/coarse controls. Its joined private-table child proves eight available-side
matches while the opposite valid grid cannot map. The test refuses a missing
execution marker or failed child. `telnet/nearby_endpoint_test.go` qualifies 40
literal live/history consumer cases and detached history after opposite live
mode edits. The source/test review corrected that edit stimulus before complete
package qualification, retaining exact captured-result assertions.

Focused tests, complete filter/telnet/commands ordinary and race suites and 29
strict driver controls passed (`checkpoint-08-targeted.log`, `-packages.log`,
`-race.log`, `-runner-controls-final.log`). The checkout fixture adds ten fixed
synthetic metadata rows to its existing queue before readiness. The driver
requires every row before activation, then the exact expected visibility through
both GO and CC commands in both fresh generations. The ordinary and race actual
checkout runs both passed all 70 required cases (28 main and seven reputation
per transport), with zero wire/cleanup failures and two joined children per
profile. Artifacts are `20261009-170149-08fb5f12/results.json` and
`20261009-170238-10d49d09/results.json`, under `.tmp/command-fixes/`. Their
compiled-Go/module digest is
`696da90bf6aedcfe821d1fad4d02fd607c0c2b0e023fad18963efcd192bbb557`.
No separate unpaused socket-stream or production enrichment claim is made.

The first two ordinary checkout attempts failed existing manual-spot controls.
Inspection of their exact wire rows and Spot's SNR regexp showed random hex
labels containing a number followed by DB were interpreted as signal reports;
the added text clipped long labels in fixed-width history output. The first
short-prefix adjustment was insufficient because DB could occur inside the
token. The final runner converts its eight hex digits bijectively to letters
for long manual controls and keeps their tags within 20 characters. A required
offline vector covers the observed `341C7DBB` token. Exact label, frequency,
identity, mode and refused-absence assertions remain; no production parser or
formatter behavior changes. Failed artifacts `20261009-165717-67013780` and
`20261009-165915-4a76026c` and their launch logs are retained. They do not qualify
the final result. The second failure also caused downstream controls to lack
their required setup; the driver correctly rejected the complete run.

Support-agent documentation impact: Required. The telnet guide and common
question route explain independent endpoints and remaining refusal conditions.
TSR-0054 records the concrete double-veto mechanism, cache/resolution ownership
and checker limits; its index is updated. ADR-0146 remains accepted unchanged;
no new durable design decision is introduced. The changed-only entry-comment
checker passes but selects zero files here because its path whitelist excludes
filter/filter.go and test files. The changed matcher comment was reviewed
directly. The TSR index/RCA consistency check passes. Full-lane and final-map
evidence follows before checkpoint commit.

Final relevant-source qualification passed: the complete ordinary lane checked
70 packages (`checkpoint-08-baseline.log`); vet, Staticcheck and golangci-lint
passed with zero lint issues (`-vet.log`, `-staticcheck.log`, `-lint.log`). The
fresh lead-owned final review found no unresolved material defect, waiver or
scope expansion. No new parser/protocol or performance claim requires a fuzz or
comparison-profile selection. Generated-map generation/check and final
whitespace/staged-diff review accompany the isolated checkpoint commit.
Checkpoint 08 is committed as `4a65a44`; its final working tree was clean.

## Checkpoint 09: exact GRID and truthful mapping availability

Status: complete. Discovery inspected the human staged GRID owner and its
detached transaction coordinator, machine settings validation and prepared
NEARBY admission, H3 fine/coarse encoders and once-owned mappings, locator parsing,
existing settings/percent-warning tests, HELP and guide wording. The locator
parser deliberately accepts a usable prefix at its general path boundary;
human SET currently relies on that prefix behavior and ignores extra arguments.
Machine input already requires exact length but conflates syntax with mapping.
The active machine transaction already refuses unavailable resulting NEARBY
cells before any commit/publication. Preserve that ordering.

The shared telnet syntax predicate will require exactly four or six characters
and reuse the existing locator parser for field/square/subsquare validity.
Human input normalizes its single token; machine input keeps its existing
uppercase/no-whitespace contract. Missing fine mapping support remains a
refusal, with an availability response instead of a syntax accusation. Do not
introduce mapping-independent geography. Human active NEARBY will also require
the resulting coarse cell before any candidate mutation or publication. No
H3 math, table loader, global cache, user schema, default or persisted migration
changes are planned. ADR-0153 and ADR-0146 remain controlling and unchanged.

Config-contract audit: the user SETTINGS grid is the persisted selection owned
by the existing per-user configuration loader/coordinator; empty machine GRID
retains its lookup sentinel. Required H3 reference tables keep their existing
startup loader. Runtime path/NEARBY cells remain derived, not durable ownership.
Only command-boundary validation/feedback and test fixtures change; no runtime
YAML loader, required-key/default/null semantics or optional tool config changes.

Lead-owned preimplementation falsifiability matrix:

| Contract | Boundary / fault | Stimulus | Required observation | Level | False-green risk | Checker | Owner |
|---|---|---|---|---|---|---|---|
| Exact syntax | Valid prefix plus junk, five characters, out-of-range fields/subsquares, extra arguments | Both dialects, case/whitespace and explicit boundary vectors | Refusal leaves live settings, derived cells, revision and saved bytes unchanged | Unit/checkout | Calling the permissive grid parser alone or ignoring the extra argument | Actual human coordinator with immediate disk comparison; public readback | telnet, runner |
| Canonical publication | Valid four/six-character human token | Lower/mixed case, active NEARBY grid change | Uppercase saved configuration validates through the machine boundary; both resulting cells change together | Unit/checkout | A success acknowledgement without valid returned configuration | Complete validator, exact configuration and durable readback | telnet, runner |
| Mapping unavailable | No process-global mapper | Fresh child that never loads tables | Valid syntax receives availability refusal, malformed syntax receives syntax refusal; zero effects | Isolated unit | Global once-loader or invalid syntax conceals the intended fault | Required child execution marker and explicit no-mapping guards | telnet |
| Active NEARBY | Fine mapping available, coarse mapping missing for proposed locator | Fresh child with only that coarse entry removed | Active session rejects without mutation/save; inactive session keeps existing fine-only admission | Isolated unit | Clearing both maps or only checking fine cells | Separate positive fine and missing coarse guards; real coordinator and saved bytes | telnet |
| Arbitrary input | Bounded direct stage input | Seeded fuzz, independent literal character-domain oracle | Every publication has exact syntax and canonical matching settings; every refusal leaves candidate state intact | Fuzz/unit | Mirroring prefix parser or testing only curated invalid examples | Actual staged handler and independent ASCII positional oracle | telnet |
| Discoverability | Existing misleading 4-6 wording | Compiled HELP and related GRID guidance | Exact four-or-six wording, no invalid five-character suggestion | Command/docs | Editing docs while executable help stays inaccurate | HELP test and synchronized existing fixtures/lint | commands, docs |

The old warning-format regression uses an invalid GRID containing a percent
sequence. Retain its literal-format assertion directly at the existing outcome
formatter, and add actual-command refusal coverage rather than preserving that
invalid acceptance. This is a checker-only refinement. The new private mapping
fixtures are joined child processes because mappings belong to sync.Once.
Fuzzing uses the established stable executable, runner lock and bounded tool
owner. The checkout driver will add immediate refusal/readback, machine VALIDATE
and fresh-process saved GRID controls. No new policy or scope revision is needed.

Implementation: `validExactGrid` gates canonical human and machine tokens before
the existing prefix parser. The staged human owner rejects extra arguments,
invalid syntax and unavailable fine mapping before mutation; an enabled NEARBY
also requires the proposed coarse cell. Machine admission retains its existing
uppercase/lookup-sentinel and prepared-NEARBY ordering, with truthful separate
mapping feedback. Other settings and the general locator parser remain.

Complete targeted ordinary and race filter/telnet/commands suites passed
(`checkpoint-09-packages.log`, `-race.log`). Both mapping-fault children are
required, guarded and joined. Permanent `FuzzHumanGridAdmission` passed 90,571
executions over approximately 16 seconds with two workers, following nine seed
controls (`-fuzz.log`). It checks bounded direct stage input rather than claiming
a separate wire-input fuzz qualification. The ignored fuzz launcher uses the
existing exclusive runner lock, stable executable, fresh-source manifest and
bounded tool owner; no permanent workflow mechanism changes.

Thirty strict driver controls passed (`checkpoint-09-runner-controls.log`). Both
actual checkout builds passed all 74 required cases, with 30 main and seven
reputation cases per transport. Every profile reports zero wire/cleanup failures
and two joined children. The new cases require malformed GRID to preserve exact
configuration, revision, effective settings, preset status and saved bytes;
time-varying pause duration is deliberately excluded. Accepted mixed-case GRID
must match canonical/effective/saved values and succeed through machine VALIDATE
without apply/persist effects. The active IO91 selection must survive a fresh
server process in both dialects. Artifacts are
`20261009-172152-e60326c7/results.json` and
`20261009-172311-83d4de3b/results.json`, under `.tmp/command-fixes/`. Their
compiled-Go/module digest is
`d4a992da81251b510b85cd3bce3f35fb6fba4a4b3ea9aed747ae73fd36ede589`.

The first focused launch failed to build because two new test references used
incorrect existing type/API names. The corrected test built and its telnet
checks passed, then command tests correctly failed two stale HELP usage
expectations. Both failures are retained as `checkpoint-09-targeted-build-failure.log`
and `checkpoint-09-targeted-help-failure.log`; they are not qualifying final
evidence. Complete ordinary/race package logs above include the corrected HELP
assertions and every new test. The percent-argument formatter regression is now
direct, while actual percent-suffixed GRID is refused with unchanged effects.

Separate-context technical quality review found no material Go defect or scope
expansion and identified two remaining active 4-6 descriptions in the operator
guide and release README template. Both are corrected to four-or-six wording;
the archived numbered README remains historical evidence. The review inspected
implementation and intended scope and is not independent scientific evidence.
No source change followed the final Go package/fuzz/checkout qualification;
the guide corrections do not invalidate that compiled-source evidence.

Support-agent documentation impact: Required. Executable HELP, command HELP
tests, root/telnet/operator guides and the release README template now agree.
The common-question route and indexed TSR-0055 explain syntax, mapping
availability, preservation and test limits. ADR-0153 and ADR-0146 are unchanged;
no new durable design decision is introduced. Changed-only entry comments pass
for four support-critical production files; the TSR index/RCA check passes.
The first full ordinary lane failed the unchanged peer TCP retry timing test:
`positive_exponential` observed 98.23 ms against a 100 ms delay with a 1 ms
tolerance (`checkpoint-09-baseline-peer-failure.log`). No peer files changed.
Its isolated rerun passed (`-peer-timing-recheck.log`), followed by a complete
70-package ordinary lane pass on the frozen final source (`-baseline.log`).
The cause of that intermittent observation is unresolved; this checkpoint does
not claim to repair it. No required check is waived.

Whole-repository vet, Staticcheck and golangci-lint passed, with zero lint issues
(`-vet.log`, `-staticcheck.log`, `-lint.log`). The fresh lead-owned final review
covered production, tests, driver negative controls and documentation, and
found no unresolved material defect or scope expansion. Generated-map generation
and check, whitespace and staged-diff review accompany the isolated commit.
Checkpoint 09 is committed as `afa8053`; its final working tree was clean.

## Checkpoint 10: callsign-pattern admission

Status: complete. Current-source discovery inspected both dialect parsers,
the DXCALL/DECALL allow/block handlers, the existing list separator, all four
Filter pattern mutators and their actual matcher, machine validation and
capabilities, detached human publication, and existing command/filter tests.
The human handlers currently publish every nonempty list without validation;
the matcher and machine contract support only an exact pattern, `*`, or one
leading/trailing star, with ASCII letters, digits, slash and hyphen otherwise.
Both CC list aliases reach the same handler. Original CC /ON and /OFF tail
validation belongs to checkpoint 11a and is not silently added here.

Architecture: share the existing private machine pattern predicate with the
human handler, naming it for its callsign domain. Validate the complete parsed
list before the first Filter mutation. Preserve existing separators, case
normalization, list order, duplicate handling, PASS/REJECT precedence and ALL
behavior. Standalone ALL remains the reset/block-all operation; ALL inside a
list remains a literal exact pattern. A standalone star still participates in
the existing allow/block lists. No matcher, exported API, schema, migration,
runtime default, queue, lock owner or storage quota changes are needed.

Config ownership: these four per-user YAML string lists remain owned by the
existing configuration loader and prepared-record coordinator; the machine
validator remains authoritative for admitted proposals. Empty lists retain
their existing meaning. Stored legacy patterns are not rewritten or pruned.
The predicate adds no default or alternate loader. ASCII character and wildcard
restrictions already appear in machine capabilities and executable HELP.

Lead-owned preimplementation falsifiability matrix:

| Contract | Boundary / fault | Stimulus | Required observation | Level | False-green risk | Checker | Owner |
|---|---|---|---|---|---|---|---|
| Complete-list admission | Valid token before/after an unsupported wildcard or character | DX/DE, PASS/REJECT, GO/CC lists | Refusal, zero commit attempts, exact live/revision/saved bytes unchanged | Unit/checkout | Testing only a lone invalid token or discarding a partially mutated candidate | Actual coordinator plus immediate disk/revision assertions; direct staged fuzz | telnet, runner |
| Supported forms | Exact, prefix, suffix, star, slash/hyphen and mixed case | All four human routes | Canonical ordered rules are saved and accepted by full machine validation | Unit/checkout | Acknowledgement alone or tightening the existing machine domain | Exact live/durable patterns and machine VALIDATE | telnet, runner |
| Matching semantics | Allow/block overlap, ALL versus star, DX versus normalized DE | Literal matching/nonmatching spot calls | Correct visibility and existing reset/block behavior | Consumer/checkout | Testing the validator without invoking the actual matcher | Filter.Matches and correlated positive/negative archive controls | telnet, runner |
| Arbitrary syntax | Bounded malformed lists, non-ASCII, multiple/interior stars | Permanent fuzz seeds and independent regular-expression domain | Publication only for supported lists; refusal preserves staged configuration | Fuzz | Deriving expected validity from the shared production predicate | Independent grammar and actual staged handler | telnet |
| Fresh durability | Close child and reconnect identical identities | Same stores in replacement process | Exact saved rules and visibility survive | Checkout | Live settings hide failed persistence | Two joined actual checkout children per transport | runner |

The shared predicate is a bounded package-local command admission change. The
matrix is adequate without a new test seam or policy decision. No scientific
or performance claim is made; no separate non-steered review is required for
this directly falsifiable grammar. Relevant HELP/guide wording and support
routing will state the actual character domain and whole-list refusal.

Checkout evidence requires one checker refinement: server input echo may
interleave the pipelined GET echo with a history row. Read the complete bounded
history response before sending its correlated GET; never reconstruct rows by
loosely stripping echo fragments. Require an exact complete history footer,
the existing deadline, a 64 KiB response bound and strict subsequent YAML
identity. Negative controls must reject unknown/interleaved lines, partial or
missing footers, EOF and oversized replies, and prove GET follows completion.
This changes the checker only, not server echo, history, protocol or accepted
command behavior. These zero/one-row queries qualify the history body and its
semantic footer, not the optional automatic-pause trailer for ten-row replies.
Receipts retain the decoded body before GET on success and failure, including
pending bytes after EOF/timeout/buffer refusal, capped at 64 KiB with an explicit
truncation marker. They are Telnet-decoded receipts, not literal IAC wire captures.

The fixture does not run the main runtime output-layout initializer: its archive
rows use Spot's existing 78-column default, not the loaded runtime YAML's 76.
The checker requires the actual fixed columns, complete UTC tail and exact
callsign-control comment; a valid row prefix alone cannot hide inserted GET text.
The first public invalid-character vector used an underscore rejected by upstream
ingress; the corrected wire-admitted dot reaches this handler. Underscore and
non-ASCII remain covered by direct-stage tests.

The existing hex nonce can accidentally enter the production SNR grammar. The
primary control now uses the existing letter-only nonce helper, with distinct
CW and F8 suffixes: neither history comment query is a substring of the other.
The first empty CW suffix caused both rows to match when FT8 was visible; this
was a checker-label collision, not a production MODE change.

Retained failed actual runs: `20261009-175048-f145a405` (underscore stopped at
ingress), `20261009-175220-0d981a7f` (pipelined echo interleaving),
`20261009-180503-c2d8cdfa` (incorrect 76-column oracle), and
`20261009-180658-1ddf61c3` (CW/F8 prefix collision). Preliminary passing ordinary
and race runs `20261009-175826-9dbe93c1` and `20261009-175927-3b543a11` qualified
78 required cases per build but preceded final history receipt/layout controls;
they are not the final checker qualification.

The read-only separate-context technical review found the missing pending-byte
receipt and wrong fixture layout, both corrected with negative controls. It also
prompted precise PASS-star wording: PASS retains other block patterns and removes
an identical block pattern. No product selection, independent scientific review
or new runtime ownership is claimed. Earlier test failures from nil-versus-empty
slice equality, missing explicitly enriched DE baseline and a stale HELP assertion
are retained in targeted logs; corrected complete suites passed.

Observed final validation:

| Check | Result |
|---|---|
| Complete filter, telnet and commands suites | Ordinary and race runs passed; final identical-block-star consumer regression also passed under race |
| Direct staged callsign fuzz | 93,686 executions passed in approximately 16 seconds with two workers; 11 permanent seeds |
| Complete Windows ordinary baseline | All 70 packages passed on final Go source |
| Runner failure controls | All 36 passed, including row corruption, disjoint labels and bounded pending-response receipts |
| Final actual checkout runs | Ordinary `20261009-181214-8f22254c` and race `20261009-181321-8ea8a6f0`: all 78 required cases passed per build; 32 main and 7 reputation cases per transport |
| Child ownership and transport | Both configured transports passed strict checks; zero wire/cleanup failures; all eight owned child retirement receipts per build confirm client retirement, consumer join and archive stop |
| Whole-repository analyzers | vet, Staticcheck and lint passed; zero lint issues |
| Support and records | Both touched support-critical Go files passed comment checks; troubleshooting-record checks passed |

Both final actual builds and all their children report Go/module source digest
`3a86418e5ac33ec4ed6f716992bc5b30367a11982308f2de6dd9b98084f0f5fe`.
This digest does not cover Python, documentation or configuration. The final
checker was frozen for both builds. Fresh lead diff/receipt review and the
separate-context technical follow-up found no unresolved material defect.
Generated-map and whitespace checks complete the commit closeout. No waiver,
production-load qualification, external cluster run or unrelated migration is
claimed. The generated Python bytecode remains outside the staged patch.

## Checkpoint 11a: original filter arguments

Status: complete. Discovery inspected actual dialect dispatch/fallback and
hinting, named CC shortcuts, dynamic mode shortcuts, inline flags, feature and
NOFILTER handlers, coordinator/publication, executable HELP and existing tests.
Named shortcuts currently discard tails; inline /ON and /OFF replace them with
ALL before validation; feature switches and NOFILTER ignore their supplied values.

Contract disposition: preserve argument-free shortcuts and existing supported
inline flags. A named shortcut takes no arguments. A flagged category takes no
extra arguments before normalization. Feature switches and PASS NOFILTER retain
their no-value form and a single ALL, consistent with the existing type-scoped
ALL reference; arbitrary values or additional tokens must refuse with no effects.
This preserves the useful existing ALL operation rather than silently changing
it. Named shortcut ALL is still an extra argument, not that category operation.
The approved raw-argument validation and executable HELP supply authority; no
new option, mode alias, category or product-policy selection is introduced.

Architecture: validate named/dynamic shortcut arity in their private parser
before accepting the normalized command. Validate inline-flag tails before
substituting ALL. Bound feature/NOFILTER arguments at the shared handler before
mutation. Retain dialect fallback/hints, existing summaries/deprecation notes,
the detached coordinator, mode delta semantics and valid category normalization.
No exported interface, storage format, default, lifecycle or retained-state owner
changes. SHOW DXCC/logout arity remains 11b; fixed settings remain 12; comprehensive
category/HELP repairs remain 17/18.

Lead-owned preimplementation falsifiability matrix:

| Contract | Fault / stimulus | Required evidence | False-green risk | Checker / owner |
|---|---|---|---|---|
| Named shortcuts | Every fixed alias and dynamic mode shortcut with ON/OFF/ALL/additional tokens | Recognized refusal; exact live, revision and disk bytes unchanged; no commit attempt | Testing normalized handlers misses discarded tails | Actual engine/coordinator plus real checkout dispatch / telnet, runner |
| Inline flags | /ON and /OFF on list, callsign, feature and NOFILTER categories with tails | Refuse before ALL substitution; positive argument-free controls retain semantics | Checking final parser shape alone | Literal state/control expectations, immediate disk and history / telnet, runner |
| Shared switches | GO, CC and GO fallback; each feature and PC93 alias | Zero value/single ALL work; malformed values/extra tokens do not publish | An already-enabled switch hides opposite mutation | Start from opposite state; exact snapshot, commit and disk assertions / telnet |
| Fresh durability | Saved valid MODE/BAND/callsign/switch selections in second owned child | Exact restored configuration and known CW history visibility | Acknowledgement hides save failure | Cumulative isolated server runner / runner |
| Arbitrary tails | Bounded direct staged parser fuzz | No publication or detached mutation for nonempty shortcut/flag tails | Production parser supplies expected outcome | Literal prefix/tail grammar and permanent seeds / telnet |

The raw-arity guard is package-local and directly falsifiable. There is no
scientific or performance claim, architecture fork or new test seam requiring
another independent context. Final review will inspect the parser/handler diff
and positive compatibility controls directly.

Review disposition: the separate-context technical pass found no production
defect or scope expansion, but identified a missing unsupported NOFILTER/OFF
regression. Add exact refusal, zero-commit and unchanged-state/disk controls
through both the coordinator and real checkout dispatch. A direct GO-dialect
control also preserves the existing CC syntax hint for a malformed mode shortcut.
This review is technical evidence, not independent scientific evidence or approval.

Retained setup failures: the first focused run compared absent files before two
tests had performed any save; valid setup commands now create those records.
The first fuzz launcher used the unpinned system Python, which lacks PyYAML;
no fuzzing ran in that attempt. The pinned repository environment subsequently
ran the fuzz check successfully without an installation or user action.

Actual attempt `20261009-182700-7f42e508` caught another pipelined echo interleave:
GET text interrupted the exact callsign refusal on the ziutek transport. The
retained transcript shows the expected two-line response interrupted by GET echo;
this attempt is excluded from final qualification. Receive each
known complete callsign/argument refusal before its correlated GET, using exact
lines, the existing deadline, a 64 KiB receipt cap, strict CRLF/ASCII, bounded
pending-byte failure receipts and mandatory existing GET framing. No loose echo
reconstruction, server protocol change or permissive feedback match is added.
Offline controls challenge interleaved text, wrong/partial lines, EOF/timeout and
oversized replies and require receipt publication before GET. The first control
rerun retained an obsolete one-line fake reply; it was corrected to the full
literal shortcut refusal before final qualification.

Observed final validation:

| Check | Result |
|---|---|
| Complete filter/telnet/commands packages | Ordinary and race suites passed; final NOFILTER/OFF and GO-hint additions passed targeted race checks |
| Direct staged shortcut fuzz | 79,379 executions passed in approximately 16 seconds with two workers; seven permanent seeds |
| Complete ordinary baseline | All 70 packages completed successfully, with no-test packages build-checked |
| Runner controls | All 38 passed, including exact refusal ordering, corruption/partial-byte receipts and each live/durable observation owner |
| Final actual checkout runs | Ordinary `20261009-183126-87089317` and race `20261009-183250-48da12f5`: all 82 required cases passed per build; 34 main and 7 reputation cases per transport |
| Transport and child ownership | Zero strict wire or cleanup failures; all eight owned retirement receipts per build confirm clients retired, consumer joined and archive stopped |
| Whole-repository analyzers | vet, Staticcheck and lint passed; zero lint issues |
| Support and records | The one touched support-critical Go candidate passed the comment check; troubleshooting-record checks passed |

Both actual builds and all children report Go/module source digest
`a8176565c662382e80d905d54895d50d74aaf8cc1096f09abf3a8f7981f6e4fa`.
Python was frozen for both final builds; the digest does not hash Python, docs or
configuration. Fresh lead source/diff/receipt review and the technical follow-up
found no unresolved material defect. HELP reference and telnet/support wording
state the admitted arguments; TSR-0057 records the normalization failure and
checker limits without inventing a new ADR. Generated-map and whitespace checks
complete closeout. No required check was waived; no production-load, external
cluster or unrelated lifecycle qualification is claimed.

## Checkpoint 11b: DXCC lookup and logout arity

Status: complete. Discovery inspected Processor dispatch, SHOW/SH routing,
portable CTY lookup and its existing tests, HELP usage, the actual telnet caller,
SendAndClose and writer close-after-flush handling. SHOW DXCC currently ignores
arguments after the first query. BYE/QUIT/EXIT return the close sentinel even
with extra tokens. Existing HELP unambiguously specifies one DXCC query and
argument-free logout aliases; the approved 11b contract requires strict arity
and a still-usable connection after invalid logout. No policy selection remains.

Architecture: refuse wrong arity in the existing processor before CTY lookup or
the BYE sentinel. Preserve all valid portable lookups, SHOW/SH aliases, dialects,
logged-in requirement and valid logout behavior. No server, socket, deadline,
schema, preset or persistence-owner change is needed.

The connection-lifecycle audit identifies a session-owned inbound command loop
and its existing writer. Only the exact BYE sentinel queues SendAndClose; the
writer flushes the farewell then closes. Invalid logout must return ordinary
usage and continue reading. Listener/login, writer/queue ownership, idle/recovery
deadlines and shutdown/reconnect mechanics remain outside this arity change;
there is no new goroutine, timer or socket owner. Tests must establish usable
subsequent commands and server-originated EOF for valid logout, not just an
acknowledgement followed by a client-initiated close.

Lead-owned preimplementation falsifiability matrix:

| Contract | Stimulus / fault | Required evidence | False-green risk | Checker / owner |
|---|---|---|---|---|
| DXCC query arity | SHOW/SH, both dialects, missing/extra tokens | Exact usage before any CTY callback; valid prefix/portable controls still resolve | Looking up the first token then ignoring the tail | Processor callback count and literal results; real checkout commands / commands, runner |
| Logout arity | BYE/QUIT/EXIT with extra tokens and mixed case/spacing | Usage rather than sentinel; no live/revision/effective/preset/disk change | A close message or new connection masks a disconnected original | Existing exact reply reader, correlated GET and subsequent SHOW OWN on the same socket / commands, runner |
| Valid logout | Each alias on its own admitted connection | Farewell and actual remote EOF within existing deadline; owned fixture retirement still joins | Harness closes locally after seeing farewell | Explicit EOF control before local cleanup / runner |
| Compatibility | Original CTY metadata/portable lookup and valid aliases | Existing tests and complete baseline remain green | A blanket refusal passes malformed-input tests | Valid controls and complete selected lane / commands, telnet |

This is a directly falsifiable parser boundary with no architecture fork or
scientific claim. Lead final review remains sufficient; no separate reviewer is
required for the two localized guards. Select ordinary baseline, targeted/race
packages, actual ordinary/race server cases and the existing record/map checks.

The first actual attempt `20261009-184801-e50f9482` retained an incorrect checker
expectation for SHOW OWN: the full SSID belongs on its Login call line, while
Own call is the baseline station. GET and SHOW OWN on the original connection
had already succeeded after the refused logout. Correct the control to require
both independent literal identities; do not change this existing display
contract. The failed attempt is retained and excluded from qualification.

Observed final validation:

| Check | Result |
|---|---|
| Focused processor controls | Both dialects, SHOW/SH, portable lookup and all logout aliases passed; rejected DXCC performs zero CTY callbacks |
| Complete commands/telnet packages | Ordinary and race suites passed |
| Complete ordinary baseline | All 70 packages completed successfully, with no-test packages build-checked |
| Runner controls | All 39 passed, including remote EOF before local close and failure cleanup even when receipt writing fails |
| Actual checkout runs | Ordinary `20261009-184949-1a6fac18` and race `20261009-185252-45024a94`: all 86 required cases passed per build; 36 main and 7 reputation cases per transport |
| Transport and child ownership | Zero strict wire or cleanup failures; all eight retirement receipts per build confirm clients retired, consumer joined and archive stopped |
| Whole-repository analyzers | vet, Staticcheck and lint passed; zero lint issues |
| Support and records | Troubleshooting-record checks passed; the mechanical crawler-entry checker had zero changed candidates; existing processor entry/lookup comments were reviewed directly |

Both actual builds and their children report Go/module source digest
`294182a3d234702c471eb0307289ddac989682d35cf8720078aa53be7df4e4a9`.
Python was frozen for both builds; this digest does not hash Python, docs or
configuration. Fresh lead final source, test, diff and retirement-receipt review
found no unresolved material defect. The two token-count guards enforce the
documented contract without replacing the tokenizer or writer. Literal callback,
same-socket and EOF controls qualify this boundary; no new lexical parser,
performance or broader lifecycle claim is made.

Implementation maps to `commands/processor.go`, permanent processor arity tests
and the cumulative checkout runner. Support-agent impact: Required; the telnet
guide and support routing explain usage/refusal versus valid closure. TSR-0058
records the failure and checker limits; no new ADR is needed for enforcement of
existing documented usage. Generated-map and whitespace checks complete
closeout. No required check was waived; no full-repository race, production-load
or external-cluster qualification is claimed.

## Checkpoint 12: fixed-argument settings

Status: complete. Current discovery inspected actual dispatch, detached human
coordination and publication, all setting handlers, HELP usage, diagnostic
formatting/readback, scheduling state, existing setting tests and fixture inputs.
DIALECT, NOISE, PATHSAMPLES, SOLAR, DIAG and SET/SHOW DEDUPE can ignore trailing
tokens. GRID already has exact arity from 09 and remains a compatibility control.
The approved strict-arity contract and existing documented forms resolve behavior;
no policy choice remains. Preserve all existing valid values and dialect aliases,
including CONFIDENCE for diagnostics.

Architecture: add token-count admission in the existing handlers before value
lookup or mutation. Persistent settings continue through the same per-login
lease, detached candidate, resource admission, live publication and exact-byte
save owner. DIAG remains session-only atomic state. Invalid SOLAR has no forced
rescheduling intent; valid human SOLAR deliberately reschedules even when its
cadence is unchanged. DEDUPE requested/effective wording remains checkpoint 13,
SOLAR availability wording remains 14. No schema, runtime default, sentinel,
tokenizer, queue, goroutine, lifecycle or shared-interface change is proposed.

Configuration ownership: dialect, grid, noise, dedupe, sample floor and cadence
remain schema-4 user settings. PATHSAMPLES DEFAULT remains stored zero; SOLAR OFF
remains zero. YAML loader and machine validation are unchanged. The isolated
fixture will construct the existing predictor from its already-loaded required
path_reliability.yaml and its allowed bands, making successful PATHSAMPLES reach
real dispatch. No DefaultConfig runtime fallback, observation feed, prediction
model change or scientific-quality claim is added. Constructor inspection found
only child-owned empty bounded store structures, with no spawned worker or
external I/O. Display and ingestion remain absent from this fixture.

Lead-owned preimplementation falsifiability matrix:

| Contract | Stimulus / fault | Required evidence | False-green risk | Checker / owner |
|---|---|---|---|---|
| Exact setting arity | Both dialects; missing/extra tokens for every fixed setting and extra DIALECT query/LIST or SHOW DEDUPE | Exact usage before mutation; zero commits and unchanged live/configured/revision/preset/disk state | A valid first token hides an ignored tail | Existing coordinator and exact checkout reply/GET, immediate saved bytes / telnet, runner |
| SOLAR scheduling | Different/unchanged valid cadence with an extra token; valid unchanged cadence after clock movement | Invalid commands preserve cadence and exact next deadline; valid unchanged human command advances schedule deliberately | GET reports cadence but misses deadline reset | Injected existing clock and raw next-deadline assertion / telnet |
| Session DIAG | Every mode and CONFIDENCE alias; malformed opposite mode | Session mode unchanged on refusal, valid modes appear in status and consumer output; no durable change | Persistent configuration omits session-only state | Direct atomic/format controls and GET session status / telnet, runner |
| Positive compatibility | Every setting, all admitted noise/diagnostic/dedupe/cadence values, dialect queries/aliases, count and DEFAULT | Correct runtime value, exact saved setting where persistent, legitimate query with zero save | Blanket rejection looks safe | Literal value and immediate disk expectations; loaded real predictor / telnet, runner |
| Durability | Same saved settings loaded by a fresh owned child | Exact persisted configuration and effective sample floor; DIAG naturally resets | Live cache hides missing writes or persists session state | Existing two-child runner and strict readback / runner |

This is localized admission on existing owners, without a design fork or new
semantic uncertainty. Lead final verification is sufficient. Select focused and
complete telnet/commands/filter suites with race, cumulative actual ordinary/race
checkout tests, runner controls, complete ordinary baseline, analyzers and records.
The new guard does not replace lexical parsing; bounded staged tail fuzz will
challenge arbitrary rejected tails and unwanted publication/rescheduling intent.

The first focused run caught an incorrect new test literal for SHOW DEDUPE's
existing key label. Current source and output say cqzone for SLOW, rather than
the assumed expanded key description. Correct that compatibility oracle; do not
change production wording or preempt checkpoint 13. The failed run is retained.
The first offline rerun also caught a missing YAML name in the new saved-value
oracle. Reuse the already-pinned loader through the existing support module;
no dependency installation or harness decoder change is required.

Technical review disposition: no production guard or ownership defect was found.
The reviewer identified equality-only numeric checks: Python False compares equal
to integer zero, so a bad zero representation could be hidden by later valid
30-value controls before restart. Require exact configured/saved scalar types,
while preserving omitted integer fields as zero. Offline vectors admit omitted
or literal integer zero and refuse booleans, floats, strings and nulls; this
qualifies canonical setter output without changing legacy storage decoding.
The review was read-only and saw scope and implementation; it is technical
evidence, not independent scientific evidence or approval. Preliminary successful
ordinary/race runs are retained but excluded from final stronger-checker
qualification; rerun both after the oracle fix.

The initial lint pass found three time comparisons in the new positive tests
that require time.Time.Equal. This test-only correction preserves the scheduling
oracle; affected focused tests passed under race afterward. It does not change
production code or invalidate the completed package/baseline/fuzz evidence.
Both actual builds were repeated for final, matching Go provenance.

Observed final validation:

| Check | Result |
|---|---|
| Focused and complete package controls | Fixed-setting controls and complete filter/telnet/commands ordinary and race suites passed; final test-only time-comparison correction passed focused race checks |
| Tail fuzz | 80,229 executions in 15 seconds with two workers and seven permanent seeds; PASS |
| Complete ordinary baseline | All 70 packages completed successfully before the final test-only comparison adjustment; no-test packages build-checked |
| Runner controls | All 42 passed, including exact integer-zero representation and session-only diagnostic preservation |
| Final actual checkout runs | Ordinary `20261009-214029-6e8cac45` and race `20261009-214224-a43585e0`: all 90 required cases passed per build; 38 main and 7 reputation cases per transport |
| Transport and child ownership | Zero strict wire or cleanup failures; all eight retirement receipts per build confirm clients retired, consumer joined and archive stopped |
| Whole-repository analyzers | vet and Staticcheck passed; final lint passed with zero issues |
| Support and records | Troubleshooting-record and crawler-entry checks passed; one changed support-critical candidate reviewed |

Both final actual builds and their children report Go/module source digest
`cfa922e8eb948e4fe25fb41d7169d3d594992863f2b58d81ab746347444eb918`.
Python was frozen for both; the digest does not hash Python, docs or configuration.
Earlier successful runs with the preceding test-file digest are retained; final
qualification above identifies the exact matching final builds.

Fresh lead final source, tests, direct diff and retirement-receipt review found
no unresolved material defect. Seven localized arity checks enforce the existing
forms, and the existing predictor constructor supplies the loaded PATHSAMPLES
consumer. Implementation maps to `telnet/server.go`, permanent fixed-setting
tests, the fixture options and cumulative runner. HELP SET DIAG adds the preserved
CONFIDENCE alias. Support-agent impact: Required; the telnet guide and support
routing explain refusals and scheduling preservation. TSR-0059 records the
learning; no new ADR is needed for existing contract enforcement. Generated-map
and whitespace checks complete closeout. No required check was waived; no
full-repository race, production-load or external-cluster qualification is claimed.

## Checkpoint 13: configured and effective DEDUPE readbacks

Status: complete. Read-only discovery inspected current stored-version
decoding, login preparation/publication, SHOW dispatch and its lease, machine
validation, human formatting, preset fallback tests, runtime lane construction,
loaded dedupe.yaml, and fixture ownership. Marked records retain exact setting
strings. A stored SLOW can remain configured after login resolves it to FAST;
SHOW DEDUPE currently mislabels the runtime atomic value as the saved choice.
SHOW SETTINGS keeps the raw choice but can call a recognized padded token
unavailable, or an unrecognized token unavailable. Ordinary empty selections use
the server default; nonempty unknown strings retain the existing MED interpretation
before lane availability fallback. These runtime and persistence rules do not change.

Architecture: read exact configured DEDUPE under the existing full-SSID lease,
alongside the current effective lane and usable NEARBY state. The SHOW path already
bypasses detached human mutation and does not clone a filter merely to read one
setting. Preserve its compact output for recognized matching policies. Use the
existing human-response counting pass, ASCII quoting, 78-column lines and final
65,536-byte cap for unusual values and expanded feedback. Refuse over-budget
readback without partial output. Correct SHOW SETTINGS token recognition and
explanation without changing the shared response or writer owners. All disabled
lanes report disabled suppression, not an active FAST sentinel.

No schema, default, saved-byte normalization, SET/LOAD applied-fallback behavior,
machine admission, broadcast lane, diagnostic consumer, pause, queue, goroutine,
lifecycle or retained-state change is proposed. Existing contracts and approval
resolve behavior; no new policy or ADR is needed. The fixture will derive its
lane flags from loaded secondary-window settings, matching production construction.
A separate private FAST-only profile with literal marked SLOW records proves real
login and fresh-process restoration. It adds no secondary deduper or production
pipeline claim. Startup and retirement receipts must expose and verify loaded lane
availability; unchanged profiles retain their shipped positive windows.

Lead-owned preimplementation falsifiability matrix:

| Contract | Stimulus / fault | Required evidence | False-green risk | Checker / owner |
|---|---|---|---|---|
| Configured versus effective | Marked SLOW, FAST-only loaded server; both dialects and a fresh child | SHOW DEDUPE and SHOW SETTINGS identify SLOW and effective FAST; GET and disk keep SLOW | Changing the setting to make the readback agree | Literal profile, exact replies, full configuration/revision/preset and immediate disk equality / telnet, runner |
| No read effects | Repeated reads after normal login establishes revision | Zero commits; exact configured/runtime/filter/preset/revision/disk unchanged | Initializing a legacy test adapter mistaken for a read mutation | Restored initialized clients and existing lease / telnet |
| Exact unusual tokens | Empty, mixed/padded recognized, whitespace-only and unknown values | Exact recoverable value; correct DEFAULT, unavailable or unrecognized explanation; unchanged runtime interpretation | EqualFold without existing trim, or assuming unknown means FAST | Literal runtime/wording and independent unquote controls / telnet |
| Effective overrides | Usable/inactive NEARBY; FAST-disabled fallback; all lanes disabled | Actual effective key, NEARBY reason, disabled-suppression wording | Reporting internal FAST sentinel as active suppression | Direct existing consumer/runtime controls and human output / telnet |
| Bounded output | Escape-expanding and long marked values, complete response boundary | ASCII/CRLF, every line at most 78 bytes, complete escapes and lossless pieces; clear refusal with no effects | Quoting the whole string or returning a partial response | Existing formatter and literal boundary/unquote tests / telnet |
| Positive compatibility | All enabled policies; human SET unavailable SLOW; machine refusal and existing LOAD fallback | Compact output and current applied-fallback save behavior retained | Blanket refusal or changing admission semantics to enable the test | Existing and focused controls; immediate saved configuration / telnet, runner |

Select focused and complete telnet/filter/commands ordinary and race suites,
cumulative actual ordinary/race server profiles, offline oracle controls,
complete ordinary baseline, analyzers, records, comments and maps. Fresh lead
verification will challenge ownership and raw-string bounds directly. Earlier
read-only delegated discovery was technical evidence shown the scope; it was
not independent scientific evidence and did not transfer lead authority.

### Review disposition

The bounded technical reviewer found that automatic pause could append its
footer after body admission. The literal 49,366-character marked choice produces
an exactly 65,536-byte body; the existing footer would exceed the final cap.
The formatter now reserves a conservative complete CRLF footer only when
automatic pause would trigger. A direct send-path test executes that boundary
and a fitting 49,200-character control, compares actual writer normalization
with a literal CRLF expectation, and checks the complete wire and every line.
The shared pause and writer owners remain unchanged.

Review then found that the actual runner's post-terminator GET discarded its
prefix. Leaked output after a refusal or footer could escape admission evidence.
The runner now uses an explicit correlated barrier, rejects trailing content,
accepts only CRLF framing and one expected echo, and counts residual framing
against the remaining response budget. New offline controls challenge leaked
content after both terminators, oversized blank padding, malformed framing,
duplicate/unexpected echo and invalid readback documents. All 48 offline controls
passed. The original telnet harness remains unchanged. A source-only follow-up
review found no material issue in the correction; it did not execute tests.

The earlier actual run `20261009-220223-3a1b918c` passed with the weaker barrier
and is preliminary evidence only. Final ordinary and race profiles below reran
with the corrected checker before closeout. These reviews were technical,
shown the scope, and did not constitute independent scientific evidence.

### Observed final validation

| Check | Result |
|---|---|
| Package controls | Focused DEDUPE/owner/budget and complete filter/telnet/commands controls passed; final complete three-package race suite reran after review corrections |
| Complete ordinary baseline | All 70 packages completed successfully on final Go state; no-test packages build-checked |
| Runner controls | All 48 passed, including exact owners/availability, lossless value, complete final wire and trailing-prefix refusal controls |
| Final actual checkout runs | Ordinary `20261009-220847-f4102c5a` and race `20261009-221029-4473bf4c`: all 104 required cases passed per build; 40 main, 7 reputation and 5 FAST-only DEDUPE cases per transport |
| Transport and child ownership | Zero wire or cleanup failures; all 12 retirement receipts per build confirm clients retired, consumer joined, archive stopped and exact loaded lane availability |
| Whole-repository analyzers | vet and Staticcheck passed; lint passed with zero issues |
| Support and records | Troubleshooting-record and changed crawler-entry checks passed; two support-critical candidates reviewed |

Both final actual builds and their children report Go/module source digest
`3b241e050447bf7029005cfdc8ea7826c3aba4531a8a312f8e6fc5cf8c0bf6a7`.
Python was frozen for both builds; the digest excludes Python, docs and configuration.
Private staged configurations are separately hashed and verified in receipts.

Fresh lead source, tests, direct diff and retirement-receipt verification found
no unresolved material defect. Implementation maps to the dedicated bounded
`telnet/dedupe_readback.go`, narrow SHOW SETTINGS recognition/reasons, permanent
owner/quoting/send-path tests, loaded fixture lane flags and the cumulative runner.
Support-agent impact: Required; the telnet guide and support route explain exact
saved choices versus effective lanes. TSR-0060 preserves the failure mechanism,
footer and barrier lessons, and checker limits. Existing runtime interpretation
and publication contracts remain; no new ADR is warranted. Generated maps and
whitespace checks close the slice. No production suppression-quality, external
cluster, load, or full-repository race claim is made.

## Checkpoint 14: SOLAR availability and conditional outcomes

Status: complete. Discovery inspected the concrete manager, production
construction, Summary/Snapshot, telnet emission and cadence ownership, detached
publication, machine effective status, human settings, HELP, configuration and
existing scheduling tests. Production supplies a manager only when the loaded
solarweather feature-root enables it. An empty Summary is not an availability
signal: enabled quiet or unobserved conditions also return empty. Due ticks
already advance when no summary is emitted.

Architecture: retain the concrete manager and its existing model-facing
OverrideGlyph dependency. Add a read-only query of its immutable enablement;
effective machine cadence is zero when unavailable, while configured and saved
cadence remain exact. Human acknowledgements, settings and HELP describe
conditional delivery on enabled nodes. Preserve OFF and deliberate human
same-value rescheduling, existing machine publication and LOAD behavior.

Extract only the current private message-dispatch loop for direct queue/deadline
tests. Keep Summary generation, polling, model thresholds, gates and real timer
behavior unchanged. A separate technical falsifiability investigation verified
that an unpolled manager is not a genuine calm oracle, and that negative dispatch
tests need a usable control queue and positive delivery. The smaller composed
evidence design is selected: actual solarweather owner tests generate active
and fresh calm outcomes; dispatcher tests qualify supplied-summary scheduling.
The unchanged wrapper's connection to Summary is verified from current source.
This does not qualify a live polling-to-wire bridge or polling-loop retirement.
A new interface would touch the override dependency or add a second source;
parent-context polling has no exposed join. Neither is needed for this bounded
feedback change. The investigation was inherited-context, read-only technical
evidence, not independent scientific review.

The checkout fixture will construct the manager from its already-loaded required
config on enabled profiles without starting fetch polling. Disabled profiles
keep it absent, matching production construction. Startup/retirement receipts
must verify actual availability. No runtime YAML/default/schema change, external
HTTP, model-quality claim, new production worker, lifecycle fix or durable
decision is proposed. Existing approval settles product policy; no new ADR is
needed.

Lead-owned preimplementation falsifiability matrix:

| Contract | Stimulus / fault | Required evidence | False-green risk | Checker / owner |
|---|---|---|---|---|
| Availability versus choice | Nil and explicitly disabled managers; enabled but unobserved manager; 15/30/60 and OFF | Exact saved cadence, effective zero only when unavailable, conditional wording on enabled node | Clearing saved settings or using empty Summary as enablement | Immutable manager query, literal readback/response and immediate saved bytes / telnet, solarweather, runner |
| Genuine calm and active generation | Fresh low observations, no holds; existing R3/G2 literal | Empty calm summary with both stale flags false and negative active indices; exact active message | An unpolled manager misrepresented as quiet conditions | Owner-state controls / solarweather |
| Selected ticks | Real control queues for 15/30/60/OFF; before/exact/repeated/late literal UTC boundaries | Exact messages/counts and next future deadlines; no catch-up duplicates | Nil queue makes all negative assertions pass | Current dispatch owner and literal deadlines / telnet |
| Empty tick consumes its slot | Empty at 12:15, active at 12:16 and 12:30 | No immediate message at 12:16; one at 12:30; exact deadline advancement | Ignoring the deadline after an empty result | Current dispatch owner / telnet |
| Publication preservation | Valid unchanged human SET, unchanged machine update, LOAD; invalid commands | Existing rescheduling/preservation distinctions, zero effects on invalid input | Readback fix changes clock ownership | Existing publication/arity controls plus exact schedule snapshots / telnet |
| Persistence and reads | Commit failure, both dialects, machine candidate status, repeated reads, fresh child, schemas 1–4 | Truthful warnings; configured/effective distinction; no read-induced revision/disk/deadline change | Logout save repairs missing command persistence | Immediate disk and owner comparison, isolated replacement children / telnet, runner |

Select focused telnet/solarweather/commands ordinary and race controls, cumulative
actual ordinary/race checkout profiles, offline checker faults, complete ordinary
baseline, analyzers and final documentation/maps/diff checks. No scientific
algorithm, model inputs or quality claims change; independent scientific review
is not triggered by this feedback correction.

### Observed final validation

| Check | Result |
|---|---|
| Package controls | Focused telnet/solarweather/commands controls and complete filter/telnet/solarweather/commands ordinary and race suites passed |
| Complete ordinary baseline | All 70 packages completed successfully; no-test packages build-checked; `checkpoint-14-all-packages.log` |
| Runner controls | All 50 offline controls passed, including strict loaded availability, numeric types, conditional wording, duplicate rows and read-owner faults; `checkpoint-14-runner-controls.log` |
| Actual checkout runs | Ordinary `20261009-224756-08cdaacd` and race `20261009-225016-dbf68a8d`: all 112 required cases passed per build; 42 main, 7 reputation and 7 disabled-SOLAR/FAST-only DEDUPE cases per transport |
| Transport and child ownership | Zero wire or cleanup failures; all 12 retirement receipts per build confirm clients retired, consumer joined, archive stopped and loaded availability; 8 enabled and 4 disabled solar profiles |
| Whole-repository analyzers | vet and Staticcheck passed; lint passed with zero issues |

Both actual builds and all their children report Go/module source digest
`4b72909e82b654fe7e836eadd1e6d3aad5a566ab5db40ba181504b6f38d95717`.
Go and Python were frozen for both runs. The digest excludes Python, docs and
configuration; private loaded configurations have separately verified hashes.

Fresh lead verification reviewed the final source, supplied-summary/owner test
boundaries, literal deadlines, direct diff, exact saved-byte checks and actual
retirement receipts. No unresolved material defect was found. The small private
dispatch extraction preserves the existing queue, timer and deadline owners;
the new immutable availability query introduces no polling or retained state.
The composed evidence does not establish live feed polling, a polling-to-wire
bridge, solar poll-loop retirement, physical/model accuracy or production load.
No external-cluster or full-repository race qualification is claimed.

Implementation maps to `Manager.Enabled`, conditional telnet acknowledgements
and settings, effective machine status, permanent owner/dispatcher controls,
loaded checkout availability and the cumulative runner. Support-agent impact:
Required; HELP/overview fixtures, README, operator/telnet guides and the support
route were updated. TSR-0061 retains the failure mechanism and checker limits.
The LOAD guide now describes scheduling separately from conditional delivery.
No new ADR is warranted. Record, crawler-entry, generated-map, whitespace and
direct staged-diff checks close the slice.

## Checkpoint 15: configured taxonomy underscores at human ingress

Status: complete. Discovery inspected taxonomy name admission, canonical
MODE/EVENT normalization, filter dispatch, history selection, the shared reader,
machine intent and header validation, login callers, loaded checkout taxonomy,
editing and physical-line recovery controls. `normalizeTaxonomyName` already
admits underscores. `gopls references` confirms only the post-login command
reader and the unchanged ReadLine adapter call the shared input implementation.
Login uses the adapter without machine awareness; machine request IDs remain
alphanumeric or hyphen, with a separate unchanged parser check.

Architecture: admit the one additional ASCII character only on the post-login
human path, before recognized machine intent. Preserve uppercase human delivery
and echo, byte limits, editing, printable COMMENT handling and recovery. Once
GET/PUT/PATCH/VALIDATE intent is recognized, the current character restriction
and terminal failure behavior remain. No signature, taxonomy configuration
contract, dispatch grammar, storage, worker, timer or retained-state change is
required. Unknown names still fail at the existing semantic handlers.

Lead-owned preimplementation falsifiability matrix:

| Contract | Stimulus / fault | Required evidence | False-green risk | Checker / owner |
|---|---|---|---|---|
| Loaded names are reachable | Actual loaded mode/event names and a mode alias with underscores; GO/CC and both codecs | Exact canonical rules, actual matching, immediate saved bytes and fresh-child restoration | Calling handlers directly bypasses the faulty reader | Real reader plus publication/matcher controls; loaded checkout capability and configuration / telnet, runner |
| Editing and bounds stay exact | Lowercase, echo, BS/DEL/word/line erase; exact 128 bytes and one over | Literal human delivery/echo, complete refusal at overflow | Broad punctuation admission or underscore removal | Reader controls and independent ASCII-domain fuzz / telnet |
| Login remains restricted | Underscore in login, then a valid physical line | Rejection, preserved recovery boundary and next valid input | Accidentally changing the shared base alphabet | Both codecs through ReadLine / telnet |
| Machine restrictions remain | Underscore immediately after a reserved verb or in request ID, including after editing | Terminal machine error retaining intent; unchanged direct header refusal | Accepting the underscore before parsing and keeping an unsafe upload session | Both-codec reader and direct header controls / telnet; cumulative checkout retains valid machine commands, without an early-error framing claim |
| Unsupported names have no effects | Underscored unknown MODE/EVENT and mixed valid/invalid lists | No publication/commit, exact live/revision/preset/disk unchanged | Testing syntax acceptance without semantic admission | Detached coordinator and actual immediate owner checks / telnet, runner |

The matrix refines checker detail within approved checkpoint 15. It introduces
no product-policy, scientific or ownership fork. Independent scientific review
is not triggered. Select focused reader/consumer tests, complete telnet/filter/
commands/spot ordinary and race suites, bounded reader fuzz, cumulative actual
ordinary/race profiles, offline oracle faults, full ordinary baseline, analyzers,
documentation/support records, maps and fresh final diff verification.

### Review disposition and observed validation

The fresh inherited-context technical review found no production ingress defect.
It identified an archive-oracle false green: a word-boundary match admitted
`TEST_MODE-X` while looking for `TEST_MODE`. The checker now requires the exact
24-column mode/comment field. Valid-layout mode and label suffix corruptions
are permanent negative controls. Both actual builds were rerun after this
Python-only correction; the earlier checkout runs are preliminary evidence.

Initial composed Go controls also compared decoded storage internals with
reflect.DeepEqual. Reload normalizes empty collections and excludes runtime
caches. Final controls compare every configured filter/settings/baseline value
semantically, retaining exact canonical selected-rule maps, association name,
stable live pointer and zero-commit/disk-byte preservation on refusal.

| Check | Observed result |
|---|---|
| Focused and complete package controls | Reader/taxonomy/machine/history controls and complete telnet/filter/commands/spot ordinary and race suites passed |
| Bounded reader fuzz | 113,590 inputs passed in the selected 15-second run; independent ASCII alphabet, editing/framing covered by curated controls |
| Final runner controls | All 52 passed, including valid-layout mode/label suffix corruption and exact rule/type faults; `checkpoint-15-runner-controls-final.log` |
| Final actual checkout | Ordinary `20261009-233825-d878a7e9` and race `20261009-234014-73110f12`: all 116 required cases per build passed; 44 main, 7 reputation and 7 disabled-SOLAR/FAST-only cases per transport |
| Transport and retirement | Zero wire/cleanup failures; all 12 retirement receipts per build confirm clients retired, consumer joined and archive stopped; loaded availability remains 8 enabled and 4 disabled solar children |
| Complete ordinary baseline | All 70 packages completed successfully, including build checks for no-test packages; before removal of one unused test-helper argument, with final focused taxonomy race checks refreshed afterward |
| Whole-repository analyzers | Final vet and Staticcheck passed; final lint passed with zero issues |

Both final actual builds and their children report Go/module source digest
`b292b14a2ca4f01710540631c8ef77046b3f7f440be1272edfb8c6767d23c841`.
Go and Python were frozen across the final pair. The digest excludes Python,
docs and configuration; the loaded private configurations have verified hashes.
Lint found one unused test-helper argument. Removing it changed no runtime code
or test assertion, so broader package and fuzz evidence remain applicable.
Focused taxonomy race checks, all analyzers and both actual builds were refreshed
on the final source state. The earlier checkout pairs are preliminary evidence.

Implementation maps to the narrow readInputLine guard, composed permanent
reader/publication/storage/matcher controls and loaded checkout profiles.
Support-agent impact: Required; telnet/validation guides, the common-question
route and TSR-0062 explain reachable configured names and unchanged restrictions.
ADR-0069 already makes the loaded taxonomy authoritative; no new ADR is needed.
These checks do not establish event inference, model quality, production load,
external-cluster behavior, full-repository race qualification or early
machine-error wire framing. No new runtime owner or retained state is added.
Fresh lead verification directly reviewed final source, exact rule/record
comparisons, oracle fault controls, documentation and the final receipts. The
technical review was inherited-context, not independent scientific evidence.
Record, crawler-entry, generated-map, whitespace and direct staged-diff checks
close the slice; no required check is waived.

## Checkpoint 16: COMMENT mode aliases and literal history clauses

Status: complete. Current discovery inspected ParseHistoryCommand, the
generic processor's raw-input handoff, BAND/MODE list parsing, the per-byte
COMMENT prefix scanner and connection-owned history publication. Semantic
references confirm the generic processor and telnet history owner are the
production parser callers. ADR-0261/0263 establish literal remainder and comma
ownership; approved v3 settles COMMENT alias precedence.

Architecture: commands owns one local keyword/comma boundary transition. Use
the same keyword-value decision in the existing typed selection parser and in
the COMMENT cut. Expose only a borrowed ASCII-prefix predicate to telnet's
history branch. Preserve the existing tokenizer adapters: generic strings.Fields
for API prefix fields, restricted ASCII spaces at ingress, exact raw phrase
slicing/ASCII-edge trimming/detachment. The predicate adds no validation, CTY
lookup, retained state, worker, lock, cursor or archive ownership.

The separate inherited-context design/falsifiability investigation found two
currently divergent COMMENT scans. Duplicating another scanner risks drift;
calling the full parser per input byte would add allocation and validation/CTY
work. The shared local lexical transition is the bounded choice. Preserve
standalone-word clause boundaries: comma-embedded COMMENT remains a list value
and is rejected as unsupported when the alias is absent. Retain the reader's
existing standalone NEXT guard, unrelated to this repair. No new product choice
or independent scientific evidence is required.

Lead-owned post-approval falsifiability matrix:

| Contract | Stimulus / fault | Required evidence | False-green risk | Checker / owner |
|---|---|---|---|---|
| Supported alias is a value | COMMENT first, after comma, embedded in comma list; repeated COMMENT then real marker | Canonical CW selection; literal phrase only at actual clause | First COMMENT always cut or arbitrary punctuation enabled too early | Independent literal vectors through parser/generic/readers / commands, telnet |
| Unsupported alias is a clause | Absent and non-filter-visible aliases, including trailing empty comma fields | Existing phrase semantics; embedded unsupported value still refused | Every COMMENT always treated as a value | Real loaded taxonomy variants and literal query expectations / commands, telnet |
| Raw phrase remains exact | Double spaces, punctuation, repeated words, 64/65 bytes and invalid whitespace | Literal needle and matching/count-one results with decoys first | Rejoining fields silently changes spacing or count predicate | Generic processor, existing phrase goldens and real reader / commands, telnet |
| Editing and alphabet | Erase alias/marker, then punctuation; valid second marker and next physical line | Zero premature punctuation admission; exact case/echo/recovery | Direct parser tests bypass ingress | Both codecs plus allocation and bounded fuzz controls / telnet |
| Invalid request preserves continuation | Prior accepted query/token/position/flags/generation, reader and parser refusals, then NEXT | Zero archive calls on refusal; same detached query and exact continued matching; configuration/revision/disk unchanged | Comparing only token or accepting a fabricated page | Composed history owner and actual checkout readbacks / telnet, runner |
| Existing grammar/ownership | BAND/MODE aliases, empty commas, NEXT and dialect/nil-filter guards | Existing controls remain green; zero allocations per borrowed prefix scan | Broad parser cleanup changes compatibility or cursor lifetime | Current parser/session controls, selected baseline/race lane / commands, telnet |

Select focused controls and complete commands/telnet/filter/spot ordinary/race,
bounded prefix/parser fuzz, cumulative actual ordinary/race profiles, offline
oracle faults, complete ordinary baseline and analyzers. Run analyzers before
final actual builds so test-helper cleanup cannot stale their source receipts.
Record the durable alias/clause rule in ADR-0273 and its failure learning in
TSR-0063; update human/history/support documentation and generated maps.

Implementation follows the shared lexical transition, with no full parser at
per-byte ingress and no change to archive/cursor/configuration ownership.
The new commands controls cover six history spellings, loaded alias visibility,
raw phrases, nil-filter/dialect guards, count-one decoys and zero borrowed-prefix
allocations. Composed telnet controls cover 48 selection cases and 12 cursor
scenarios across both codecs/dialects and three taxonomy profiles. Refusals
preserve query contents, position, flags, token, generation, stable filter,
configuration, preset, revision and exact disk bytes without a commit or read.

The actual profiles use two matching rows behind newer wrong-mode and
wrong-spacing decoys. First/after-comma aliases preserve the literal phrase;
invalid reader/parser input leaves the returned NEXT usable, then the second
match and final empty page complete the search. Every complete response is
followed by a correlated configuration barrier. Offline row negatives retain
valid 78-column layout so a spacing mismatch cannot pass merely by breaking
framing. The read-only technical review found no material implementation defect;
its documentation finding was corrected from fabricated to malformed token
shapes. Actual NEXT and the connection owner check token validity. This review
used inherited context; it is not independent scientific evidence.

| Selected evidence | Observed result |
|---|---|
| Focused history/parser/reader controls | PASS; `checkpoint-16-focused.log` |
| Complete ordinary baseline | All 70 packages passed, including build checks for no-test packages; `checkpoint-16-all-packages.log` |
| Complete affected-package race lane | commands, telnet, filter and spot passed; `checkpoint-16-race.log` |
| Bounded fuzz controls | Parser/prefix target: 155,640 inputs; reader target: 115,390 inputs; both passed with two workers and 15-second budgets; `checkpoint-16-fuzz.log` |
| Offline runner faults | 55 controls passed; `checkpoint-16-runner-controls-final.log` |
| Whole-repository analyzers | Vet and Staticcheck passed; lint passed with zero issues |
| Ordinary actual checkout | `20261010-075603-c575e849`: 120 cases passed, zero wire/cleanup failures; all 12 retirement receipts confirm clients retired, consumer joined and archive stopped |
| Race actual checkout | `20261010-075801-4a1a3251`: same 120 cases and cleanup outcome |

Both actual builds report Go/module source digest
`11cccfc3afbca7c3a227c03d1391839576a725f6ab5dd65e643fcdb768451cae`.
Go/Python stayed frozen across the final pair; loaded private configurations
have separate verified hashes. The digest excludes Python, docs and config.
No check here establishes production load, event inference, model quality,
external-cluster behavior or early machine-error wire framing.

Support-agent documentation impact: Required. Commands/telnet history guides,
the validation guide and configuration support card explain the actual boundary
and preserve the returned NEXT workflow. ADR-0273 refines only ADR-0263's
COMMENT boundary; TSR-0063 records the diverging scanners and checker limits.
The remaining detailed HELP catalog reconciliation stays in approved 17/18.
Fresh lead verification reviewed all touched production and new test files,
literal failure oracles, complete response barriers and final source/retirement
receipts. Record and crawler-entry checks passed; final generated maps and
direct staged-diff/whitespace review close the slice before commit.

## Checkpoint 17: category-specific CC HELP grammar

Status: complete. Current discovery inspected the compiled catalog, shared
filter reference, concrete examples, CC normalization, classic fallback,
registered owners, argument controls, recipe consumers and ADR-0264/0258/0261/
0272. A bounded read-only specialist enumerated actual grammar and identified
the existing positive helper's failure to reject Unknown responses. Source
inspection confirms that ordinary PASS/REJECT already execute through the CC
fallback. No new parser semantics or product choice is needed.

Architecture: change only compiled HELP construction and affected guides.
Separate the 18 ordinary list types, SOURCE's single value, feature switches
and special grammar. COMMENT has no SET/UNSET mutation alias; MINSNR needs modes
plus threshold or the verb-specific clear sentinel; NEARBY takes explicit ON/OFF
with either verb; NOFILTER permits only SET and /ON; DXBM uses numeric band codes
and SET's existing PASS/REJECT variants. UNSET never strips inline /ON or /OFF.
Use existing bounded rendering/wrapping and immutable startup vocabulary;
add no handler, alias, state, lock, worker, configuration or persistence change.
Retain preferred shared PASS/REJECT examples without denying accepted fallback.
Dynamic HELP routing/suggestions remain separate approved 18a/18b checkpoints.

| Contract | Stimulus / fault | Required evidence | False-green risk | Checker / owner |
|---|---|---|---|---|
| Advertised list types execute | Independent literal 18-family/value table and both CC verbs | Intended live state and immediate stored configuration agree | Catalog/helper omits a family or Unknown passes as success | Catalog assertions plus composed real coordinator/storage controls / commands, telnet |
| Inline flags are category-specific | SET flags for lists/switches; all excluded special families and UNSET flags | Intended reset/delta, no argument tails; excluded forms preserve live/revision/disk and make zero commits | Blanket placeholder implies COMMENT/MINSNR/NEARBY/DXBM or UNSET support | Literal accepted/refused table and cumulative checkout / telnet, runner |
| Special grammars retain ownership | MINSNR numeric/clear sentinels, explicit NEARBY both verbs, switches, NOFILTER, DXBM and shared COMMENT | Exact owner state and saved readback; no reversed or invented verb effects | Generic allow/block interpretation hides value-owned behavior | Existing owner controls and new composed literal cases / telnet |
| HELP is truthful and bounded | HELP SET/FILTER, UNSET/FILTER and FILTERS in CC, unchanged GO catalog | Supported category lists exclude unsupported COMMENT aliases, qualified resets, 78-byte lines and working examples | Substring presence proves routing but not execution | Exact catalog groups, width/negative controls and actual server HELP / commands, runner |

Select focused catalog/advertised-form controls, commands/telnet ordinary/race,
offline runner faults and cumulative ordinary/race checkout profiles, complete
ordinary baseline and analyzers. Preserve existing parser/owner tests; no new
parser fuzz or scientific/model qualification is claimed. Record the catalog
root cause in TSR-0064; execution contracts and ADR-0264 remain unchanged, so a
new ADR is unnecessary. Update support and history HELP wording during the
applicable approved catalog checkpoints, then regenerate maps last.

The final catalog uses one finite category-specific constructor for both CC
verb entries and the reference. Literal Go controls cover all 18 ordinary
families, switches, numeric/clear MINSNR, explicit NEARBY, NOFILTER, DXBM and
shared COMMENT. Accepted forms check the intended selected state and commit once;
refusals preserve configuration, runtime owners, preset and exact durable bytes
with no commit. HELP remains metadata; executable dispatch is unchanged.

The cumulative profile adds four cases across transports and fresh children.
It consumes complete bounded CRLF HELP responses and their counted pause footer
before a correlated CONFIG barrier. Independent category/grammar assertions,
typed live state, immediate stored rules and forced positive transitions reject
Unknown acknowledgements, no-op false greens and hidden suffixes. Six new
offline methods exercise these oracles. Their initial positive fixture had a
trailing comma that introduced an empty category; the fixture was corrected,
then the full offline suite passed. No Go result was invalidated by that fix.

| Selected evidence | Observed result |
|---|---|
| Focused catalog and composed advertised-form/refusal controls | PASS; `checkpoint-17-focused.log` |
| Complete ordinary baseline | All 70 packages passed, with build checks for no-test packages; `checkpoint-17-all-packages.log` |
| Complete affected-package race lane | commands and telnet passed; `checkpoint-17-race.log` |
| Offline runner controls | 61 passed; `checkpoint-17-runner-controls-final.log` |
| Whole-repository analyzers | Vet and Staticcheck passed; lint reported zero issues |
| Ordinary actual checkout | `20261010-082328-deaa45ad`: 124 cases passed, zero wire/cleanup failures; all 12 receipts confirm clients retired, consumer joined and archive stopped |
| Race actual checkout | `20261010-082554-30100968`: same 124 cases and cleanup outcome |

Both actual builds and all retirement receipts have Go/module source digest
`dc33539dd9c322c7e70c995f8b945c8aa73b51eddfb4882445c5bc73204bffe5`.
Go/Python stayed frozen across the pair; private loaded config hashes are
verified separately. The source digest excludes Python, docs and config.
No production load, model quality, external server or new parser qualification
is claimed. Dynamic routing, suggestions and remaining wording belong to 18a/b.

Support-agent documentation impact: Required. Commands/telnet guides, the
validation guide and configuration support card describe the actual CC grammar;
TSR-0064 records the category/owner mismatch and checker limits. Fresh lead
verification reviewed all production and new tests, bounded response handling,
independent literal oracles and final receipts. Record and whitespace checks
passed. The crawler candidate scan selected no files; the new catalog header
and existing production entry comments were reviewed directly. Maps are
regenerated last, followed by direct staged-diff review before commit.

## Checkpoint 18a: complete HELP routing and aliases

Status: complete. A bounded read-only specialist derived command families
from processor, history, configuration readback, filter, preset, propagation,
machine and session dispatch independently of HELP. Lead inspection confirms
missing SH/argument routes, CC's premature fallback return, both-dialect slash
filter readbacks and character-prefix collisions. Loaded taxonomy already owns
dynamic CC mode recognition; fixed shortcuts take precedence. No policy or
executable grammar change is required. Unsupported SH FILTER/SETTINGS/DEDUPE/
HOLD and SHOW/MYDX must remain unsupported; GO slash history remains CC-only.

Architecture: keep routing metadata in the existing request-owned finite HELP
catalog. Install only source-verified aliases, then select the longest complete
catalog head, preserving specific COMMENT/MINSNR/NEARBY topics. Match a whole
head or a following ASCII space, never a character prefix. Bare SHOW/SH do not
absorb unknown subcommands. Resolve fixed CC shortcut entries before checking
the first token against the loaded mode taxonomy. Argument tails select usage
help without invoking command owners or duplicating their argument validation.
No new executable alias, state, lock, worker, persistence or parser admission
change. Repair the two crawler comments that incorrectly name SHOW/MYDX.

| Contract | Stimulus / fault | Required evidence | False-green risk | Checker / owner |
|---|---|---|---|---|
| Executable aliases and argument topics resolve | Literal dispatch-derived heads, SH forms, settings/presets/machine tails and CC fallback in both dialects | Specific intended complete HELP entry | Catalog-derived coverage misses an absent executable alias | Independent literal routing table plus existing owner controls / commands, telnet |
| HELP advertises only real aliases | Reverse check of alias metadata against independent executable heads | No invented SH readback or GO slash-history support | Alias lookup succeeds although owner rejects it | Literal negative/reverse controls and composed readback/setting/preset ownership / commands, telnet |
| Longest complete head wins | DXCC vs DX, COMMENT/MINSNR vs generic list, prefix-collision and unknown SH heads | Correct narrow topic or explicit unknown help | Generic parent/character prefix hides a miss | Exact-header and collision controls / commands |
| Loaded shortcuts retain precedence | Custom mode/alias, disabled shortcut, unknown mode, fixed shortcut overlap | Loaded eligible token gets mode usage; fixed shortcut stays fixed | Hard-coded default mode list or taxonomy overrides fixed commands | Isolated taxonomy controls and cumulative checkout / spot, commands, runner |
| HELP has no command effects | Actual alias/topic queries separated by correlated configuration barriers | Complete responses; live/revision/preset and exact saved bytes unchanged | HELP accidentally dispatches a mutation or leaves unread output | Composed and actual checkout controls with fresh-child saved-state proof / telnet, runner |

Select focused literal/owner routing controls, complete commands/telnet race,
bounded routing fuzz, cumulative ordinary/race checkout and offline faults,
complete ordinary baseline and analyzers. Preserve widths, landing fixtures and
README synchronization. Category wording/suggestions remain separate 18b.
Update affected guides/support and record routing RCA in TSR-0065; ADR-0264's
ownership is unchanged, so no new ADR is required. Regenerate maps last.

Checker refinement for actual response receipts: short HELP entries lack a protocol
terminator. Consume a source-verified literal final line, and when the rendered
row count reaches the configured automatic-pause threshold consume its complete
footer, before sending CONFIG. A strict barrier rejects any hidden suffix.
No pipelining, sleep-based completeness assumption, runtime-config change or new
production seam is needed. Actual fixed topics and loaded aliases are separate
vectors; isolated Go taxonomy controls own overlapping alias precedence.

The final implementation replaces the duplicated normalizer with whole-head
catalog matching and verified alias metadata. Canonical entries display the
working aliases. Independent literal forward/reverse and visible-row controls
guard omissions, invented spellings, prefix collisions and narrow-topic
precedence. Composed real codec/dispatch/storage controls preserve filter
pointers, settings, revisions, scheduling, diagnostics, cursors, archive calls
and exact user/library bytes. Their initial mixed-case human request ID expected
case preservation, although the existing human reader uppercases that input;
the test expectation was corrected. Direct machine input separately retains its
correlated lowercase ID. No production reader behavior changed.

The cumulative runner adds four required cases across transports and fresh
children. It checks full preference/preset ownership, exact intended headers,
usage and visible aliases, unknown topics and complete bounded receipts before
CONFIG. Seven new offline methods falsify plausible wrong headers/usages,
unsupported aliases, malformed or hidden suffixes, incomplete pause footers,
incorrect barriers and preference/disk effects. The literal contract tables stay
together for source-owner comparison; they do not derive expected routes from
the implementation being checked.

| Selected evidence | Observed result |
|---|---|
| Focused literal/visible-alias and composed owner checks | PASS; `checkpoint-18a-focused.log` |
| Complete ordinary baseline | All 70 packages passed, including build checks for no-test packages; `checkpoint-18a-all-packages.log` |
| Complete affected race lane | commands and telnet passed; `checkpoint-18a-race.log` |
| Bounded routing fuzz | 19,885 executions passed with two workers for 15 seconds; `checkpoint-18a-fuzz.log` |
| Offline runner controls | 68 passed; `checkpoint-18a-runner-controls.log` |
| Whole-repository analyzers | Vet and Staticcheck passed; lint reported zero issues |
| Ordinary actual checkout | `20261010-085643-7fdc9c83`: all 128 cases passed, zero wire/cleanup failures; all 12 retirement receipts confirm clients retired, consumer joined and archive stopped |
| Race actual checkout | `20261010-085928-c78e8564`: same 128 cases and cleanup outcome |

Both builds and all retirement receipts have Go/module source digest
`ab700aa4d721cb6d2eb1761073dea1da9dcb1fcfb7ad8210c22dddfed018a0de`.
Go/Python stayed frozen across the pair. Loaded private config hashes are
verified separately; the source digest excludes Python, docs and config.
No production load, external cluster, inference/model quality or all-taxonomy
qualification is claimed. Category wording and suggestions remain in 18b.

Support-agent documentation impact: Required. Commands/telnet guides, the
validation guide and configuration support card describe alias/argument HELP
and its ownership limits. TSR-0065 records the routing RCA. ADR-0264 and
ADR-0089 were inspected and remain authoritative; no durable decision changed.
Fresh lead verification reviewed the production diff, all new Go/Python controls
and final receipts. Record and whitespace checks passed. The crawler candidate
scan selected no files; new routing headers and production entry comments were
reviewed directly. Maps are regenerated last, followed by staged diff review.

## Checkpoint 18b: accurate suggestions, categories and wording

Status: complete. Current source discovery inspected all filter domains and
COMMENT interception, shared usage/error callers, DX admission, MODE versus
generic list transitions, the 73-code state vocabulary and affected recipes.
A bounded inherited-context source review found no unresolved policy choice.
MINSNR is already present in current suggestions; remaining omissions include
COMMENT, DXBM, NEARBY, SELF, TOXIC and PASS-only NOFILTER. PC93 is an ANNOUNCE
alias. Both dialects accept shared PASS/REJECT; REJECT NOFILTER stays invalid.
Shared DXBM errors currently recommend CC-only syntax. Several docs invent
RESET MODE, and the landing overview/README overgeneralize MODE visibility
preservation to other lists. Province-only/FCC-only descriptions omit part of
the shared US/Canadian registered-address vocabulary.

Architecture: keep feedback as finite compiled text owned by telnet. Replace
the giant mixed-grammar usage line with bounded action-aware category suggestions
and short special-form guidance, directing full syntax/loaded values to existing
HELP PASS/REJECT. Keep these replies below the fixture's ten-row automatic-pause
threshold; add no owner, alias, parser or state/config change. Shared DXBM errors
use shared PASS/REJECT and REJECT MODE guidance. HELP and docs describe actual
DX preconditions, station-baseline admission and queued versus delivered
outcomes without exposing reputation reasons. State wording reflects all 73
registered mailing codes. MODE alone retains the unnamed-visibility promise;
exclusive recipes continue to block ALL before passing selected names.

Correct the unsupported RESET MODE wording in the operational MODE recipes and
the factual command sentence of ADR-0052, ADR-0272 and TSR-0053. These are
documentation corrections, not reversals of the accepted inference or MODE
decisions. Scientific/model behavior, taxonomy, admission and persistence stay
unchanged; no new ADR is required. Keep the decision history and explain the
correction in the execution record/TSR-0066. Sync both reviewed HELP fixtures and
README's generated block, preserving the existing 78-byte rendering contract.

| Contract | Stimulus / fault | Required evidence | False-green risk | Checker / owner |
|---|---|---|---|---|
| Suggestions describe executable families | Independent 29-family inventory, both actions, bare classic/CC commands and unknown categories | PASS includes NOFILTER; REJECT omits it; PC93 is identified as an alias; finite bounded complete text | Catalog/helper repeats an omission or advertises an unsupported verb | Literal inventory/reverse-domain controls, composed and actual checkout / telnet, runner |
| Errors preserve ownership | Unknown category, missing/invalid DXBM and state values in both dialects | Intended complete shared usage and zero live/revision/runtime/preset/disk changes or commits | Generic Unknown or CC-only guidance passes without checking effects | Exact literal errors and full-owner snapshots / telnet, runner |
| Wording matches current semantics | DX HELP, state categories/values, generic versus MODE explanation and all recipe references | Real admission qualifications, 73 codes, explicit supported exclusive/opening MODE commands and no RESET MODE advertisement | Synchronized snapshots repeat a false claim | Literal assertions, actual HELP receipts and existing executable recipe/admission tests / commands, telnet |
| Discovery stays synchronized and bounded | GO/CC overview, detailed help and README block | Reviewed fixtures, 78-byte lines, machine topics still available and no invented syntax | Updating expected fixtures alone hides broken navigation or recipes | Existing synchronization/recipe tests plus cumulative 18a routes and new wording controls / commands, runner |

Select focused wording/suggestion/refusal controls, complete ordinary baseline,
commands/telnet race, offline runner faults, cumulative ordinary/race checkout
and analyzers. Retain 07/17/18a semantic and ownership evidence; no new parser,
scientific or performance qualification is claimed. Update affected contributor,
operator and support wording, record TSR-0066, then regenerate maps last.

Implementation retains the existing owners and changes only their discovery
text. Literal inventories verify all 29 families and 73 mailing codes, the
PASS-only NOFILTER boundary, PC93 alias and MODE-specific visibility claim.
Composed real codec/owner/storage refusals preserve full live, cursor, runtime,
preset and exact-file ownership with zero commit attempts. Their first wording
test incorrectly required mailing values in HELP SYMBOLS; source shows that
topic owns the confidence/path legends, so the vector was corrected.

The cumulative runner adds four required cases with full settings and detached
preset baselines across transports and fresh children. Short replies are
consumed exactly before a strict correlated CONFIG barrier; file receipts also
check identity, size and modification time. Complete HELP receipts check six
wording surfaces per dialect and their deliberate pause behavior. The initial
actual run `20261010-093111-b20fe27c` failed two native discovery cases because
the HELP checker treated educational Unknown-state/prefix prose as rejection
feedback. It had zero wire/cleanup failures and both children retired. The
checker now exempts only those two exact educational rows in Filter reference,
with a valid-receipt control plus nested-rejection controls. The complete
ordinary/race pair was repeated. No production change followed from that fault.

Lint also identified a redundant embedded-field selector in one new test.
Removing that selector does not change its assertion or production behavior;
the affected discovery race tests were rerun successfully. Nine new offline
methods include the refinement above, bringing the final runner controls to 77.

| Selected evidence | Observed result |
|---|---|
| Focused literal wording and composed owner checks | PASS; `checkpoint-18b-focused.log` |
| Complete ordinary baseline | All 70 packages passed, with no-test packages build-checked; `checkpoint-18b-all-packages.log` |
| Complete affected race lane | commands and telnet passed; `checkpoint-18b-race.log` |
| Review-fix checks | Discovery race controls passed after the test selector cleanup; `checkpoint-18b-review-targeted.log` |
| Final offline runner controls | All 77 passed; `checkpoint-18b-runner-controls-final.log` |
| Whole-repository analyzers | Vet and Staticcheck passed; final lint reported zero issues |
| Ordinary actual checkout | `20261010-093426-2c442251`: all 132 cases passed, zero wire/cleanup failures; all 12 retirement receipts confirm clients retired, consumer joined and archive stopped |
| Race actual checkout | `20261010-093724-a4070a82`: all 132 cases passed, zero wire/cleanup failures; all 12 retirement receipts confirm the same retirement boundaries |

Both final builds and all 24 retirement receipts identify Go/module digest
`06988de0c79ed21e9fb5da833ad42b939f5f499104b8ad39d4ddd782b964f761`.
All 1,117 current source-file hashes matched the ordinary manifest. Go and
Python stayed frozen across the final pair; saved Python hashes were unchanged
after the race run. Private loaded config identity is checked separately; these
source digests exclude Python, docs and config. Complete baseline evidence is
retained before the equivalent test-only selector cleanup, with the affected
race rerun and final lint on that correction. No production load, deployed
server, scientific/model quality or location/delivery guarantee is claimed.

Support-agent documentation impact: Required. Contributor/telnet guides, the
validation guide and configuration support card now explain accurate discovery,
MODE recipes, mailing codes and DX outcomes. ADR-0052 and ADR-0272 retain their
accepted policies with dated corrections; TSR-0053's unsupported recipe is
corrected, and indexed TSR-0066 records the wording RCA. No new durable decision
was made. Fresh lead verification reviewed the final production and test diff,
checker refinements, source identity and retirement receipts. Record, crawler
and whitespace checks passed; the crawler scan selected three support-critical
files, and the new usage header was reviewed directly. Maps are regenerated
last, followed by the staged diff review.

## Checkpoint 19: token-based SHOW HOLD and strict arity

Status: complete. Root discovery and a bounded inherited-context source
review inspected handleReadPauseCommand, its session dispatch, both real reader
codecs, human readback interception, generic SHOW delegation, status/suppression
owners and existing writer/handshake tests. The exact normalized-string match
declines repeated internal spaces and extra tokens. Fallback can trigger
automatic pausing, contrary to the dedicated control-reply path. SH HOLD and
slash forms are unsupported. Both wire readers reject TAB; owner/API whitespace
tolerance must not imply new TAB wire admission.

Architecture: use the existing request-local fields to recognize the complete
SHOW/HOLD token pair; require exactly two tokens and return handled Usage: SHOW
HOLD for extra arguments. Keep current literal status rendering, PAUSE, RESUME,
aliases and reader alphabet unchanged. Recognized valid and malformed HOLD stay
on sendClientMessage, bypassing automatic pause. Status still uses the existing
coherent read-only snapshot; add no state, timer, queue, synchronization,
persistence operation, exported interface or production seam.

| Contract | Stimulus / fault | Required evidence | False-green risk | Checker / owner |
|---|---|---|---|---|
| Whole-token recognition and arity | Case, repeated/outer ASCII spaces, extra tokens and nearby/unsupported heads in both dialects/codecs | Exact intended status or handled usage; no new aliases; TAB line rejected safely | Direct owner accepts whitespace that wire ingress rejects, or broad prefix captures unrelated commands | Literal owner table, real reader/session transcripts and independent fuzz oracle / telnet |
| Read-only state | Inactive/retained count, finite/pending/expired/closed owners, malformed command with populated preferences/preset/history | No deadline/cutoff/epoch/pending/closed/counter, revision, cursor, settings, runtime or exact-file/commit effects | Status text succeeds after resetting a counter or rewriting the record | Full-owner snapshots, zero-call composed controls and actual saved-byte/stat receipts / telnet, runner |
| Control replies do not pause | Successful status and one-row malformed usage with automatic threshold one | Inactive stays inactive; active deadlines/counters stay unchanged | Main fixture threshold ten hides an incorrectly generic response path | Existing real handshake/writer session with threshold one, both codecs/dialects / telnet |
| Counters and delivery timing remain truthful | Actual enqueue/writer suppression, gated response write/flush, reading interval and expiry | Counts produced by suppression, pending status before completion and full finite interval afterward; stale spots stay discarded | Injected count alone or wall-clock sleep hides premature completion | Existing suppression owners and gated writer with bounded release/close/join cleanup / telnet |
| Checkout and fresh state remain coherent | Four new required cumulative cases; strict complete response then CONFIG barrier | Full preferences/preset/revision and exact files survive; pause countdown only advances naturally | Acknowledgement or loose collector hides suffix/state effects | Runner, offline refusal controls, ordinary/race checkout / scripts |

Run focused token/status/session/ownership tests, update the existing independent
read-pause fuzz oracle, run bounded fuzz, complete telnet race and ordinary
baseline, analyzers and cumulative ordinary/race checkout plus offline faults.
The main fixture retains its existing threshold ten; threshold-one routing and
gated delivery evidence belongs to the real Go session tests. ADR-0195,
ADR-0243 and ADR-0246 already establish the control/status/delivery ownership;
no durable decision changes. Update relevant contributor/operator/support
guidance and record the recognition RCA in TSR-0067, then regenerate maps last.

Test-seam disposition: the real handshake helper closes both pipe ends and
joins handleClient before restoring global storage. The production session
starts its writer without a test-visible completion handle, so this evidence
does not claim directly joining that hidden writer. Closing the pipes/done
channel retires its I/O; the writer does not own global persistence. The new
gated test explicitly owns, releases/closes and joins its writer on all failure
paths. This is control/parser evidence, not telnet goroutine-leak qualification;
no production lifecycle seam or stack-polling claim is added.

Implementation changes only recognition and extra-argument handling in the
existing pause-control owner. Six new permanent tests cover literal states,
whole tokens, reader editing/TAB refusal, full preferences/history/storage,
threshold-one real sessions and actual gated writer completion. The first
focused run caught a fixture assumption: changing the server dialect default
does not select CC over restored default user settings. The real-session test
now selects CC through DIALECT, consumes its automatic-pause reply and resumes
before taking its initial snapshot. The corrected focused run passed.

Seven new offline controls bring the runner suite to 84 methods. Four required
checkout cases bring the cumulative total to 136 per build. Actual cases produce
one admitted archived/suppressed spot and require a distinct fresh live row
after RESUME. They preserve full preferences, revision, preset association and
file bytes/stat identity across fresh children. Quantized public seconds are
bounded by measured send/receive intervals; exact nanos, cutoff and epoch remain
Go evidence. This fixture bridges archive and telnet consumers and does not
qualify the complete production output pipeline.

Lint found the handshake helper's old unparam suppression unused after the new
tests exercised a second timeout value. Removing that comment does not change
runtime or test behavior. The affected HOLD race checks were rerun and final
lint reports zero issues. Complete baseline, full telnet race, Vet, Staticcheck
and bounded fuzz evidence preceded only that comment removal; their behavior
evidence remains applicable. Both actual builds use the final source.

| Selected evidence | Observed result |
|---|---|
| Focused token, status, owner and writer checks | PASS; `checkpoint-19-focused-final.log` |
| Independent read-pause fuzz oracle | PASS; 85,073 executions in 15 seconds; `checkpoint-19-fuzz.log` |
| Complete ordinary baseline | All 70 packages passed, including build checks for no-test packages; `checkpoint-19-all-packages.log` |
| Complete affected race lane | Full telnet package passed; `checkpoint-19-race.log` |
| Final comment-cleanup checks | HOLD race controls passed; `checkpoint-19-review-targeted.log` |
| Offline runner controls | All 84 passed; `checkpoint-19-runner-controls.log` |
| Whole-repository analyzers | Vet and Staticcheck passed; final lint reported zero issues |
| Ordinary actual checkout | `20261010-100701-f7436388`: all 136 cases passed, zero wire/cleanup failures; all 12 retirement receipts confirm clients retired, consumer joined and archive stopped |
| Race actual checkout | `20261010-101031-1547e260`: all 136 cases passed, zero wire/cleanup failures; all 12 retirement receipts confirm the same retirement boundaries |

Both final builds and all 24 retirement receipts identify Go/module digest
`fedba553d6f034d4d71fe00861d174a45586c2c7f70095ec2c31c4293eaaa67b`.
All 1,119 current Go/module file hashes matched the ordinary manifest. Go and
Python stayed frozen across the final pair; saved Python hashes were unchanged
after the race run. Source identity excludes Python, docs and private config;
the driver verifies loaded config separately. No deployed server, production
load, pipeline shutdown or goroutine-leak qualification is claimed.

Support-agent documentation impact: Required. Contributor/telnet guides, the
validation guide and configuration support card explain strict token/arity,
unsupported aliases, TAB and state preservation. Indexed TSR-0067 records the
recognition RCA; accepted pause ADRs retain their policies, with no new durable
decision. Fresh lead verification directly reviewed the final production/tests,
independent literal/fuzz controls, fixture refinements, complete reply barriers,
source hashes and retirement receipts. Record, crawler and whitespace checks
passed; the crawler selected one support-critical production file. Maps are
regenerated last, followed by the staged diff review.

## Checkpoint 20a: terminal classifier retirement

Status: complete. Current source, semantic gopls references, go list package
and test dependencies, classifier/client/cache tests, ADR-0114 and the production
pipeline callers establish classifier ownership. Start is unguarded; Stop joins
without canceling Background-derived HTTP work or retiring queued/results
references; admission has no terminal fence. Results consumers use one-valued
receives, so the channel must remain open. A bounded inherited-context design
and falsifiability review confirmed these gaps and the approved 20a/20b boundary.

Architecture: one short classifier-owned mutex linearizes Start, admission and
terminal publication. Start adds the complete bounded worker group once before
releasing ownership. Constructor-owned cancellation feeds request timeouts; it
is independent of runtime cancellation so 20b can retain its grace opportunity.
One Stop operation marks terminal, cancels, unlocks, joins every worker and then
solely retires queued jobs and buffered results. Concurrent Stop callers wait
for that same complete operation. Never hold this mutex across HTTP, blocking
result publication or joins. Keep pre-Start admission, constructor disable
semantics, exported method signatures, channel identity and cache bounds/copies.

Eligible UNKNOWN human submissions after terminal publication become synchronous
UNAVAILABLE before safe-gate/cache checks. Non-human and already-classified
bypasses remain. Pending counts accepted queued/in-flight/blocked-publication
work, separately from result backlog: every path decrements its own accepted
job exactly once, never through a blanket reset. Terminally discarded work is
not stamped or counted as a timeout/malformed/unavailable outcome. A decision
committed before Stop may retain normal cache/outcome counters even if its
unconsumed result is later discarded. Post-Stop fail-open increments Unavailable
only; existing outcome, local/cache, queue-full and AI counters stay truthful.
Already-received results belong to the consumer; Stop cannot revoke them.

Design disposition: a controller goroutine is viable but adds pre-Start control,
acknowledgment and shutdown ownership; no selected responsibility requires it.
Separate once guards alone cannot fence Add versus Wait. Mutex plus one terminal
operation is the smallest supported choice; keeping a separate stop channel is
a local choice, not a material fork. All accounting/ownership findings are
covered below. No policy or scope change remains. Production pipeline grace and
handoff ordering are deliberately checkpoint 20b, not a claim of this patch.

| Contract | Stimulus / fault | Required evidence | False-green risk | Checker / owner |
|---|---|---|---|---|
| One worker group and fenced admission | Repeated/concurrent Start, Stop and unique submissions with active barriers | Active calls never exceed workers; no Add/Wait race; post-return admissions cannot enqueue/restart | Sequential starts or zero exercised workers hide duplicate groups | Permanent synchronized unit controls and race / toxicity |
| Pre-Start behavior | Accepted queued work, later Start; Stop before Start | Normal result after Start, or exact retirement/no later AI work after terminal Stop | Tests admit only after Start | Pre-Start/terminal controls / toxicity |
| Actual HTTP cancellation | Long ordinary timeout; header stall and first body-read barrier | Stop cancels each real request, joins workers well before ordinary timeout | Fake cancellation or unobserved headers never reaches stalled body | Local net/http integration with independent release/connection cleanup / toxicity |
| Queue/results retirement | Full capacity-one results, blocked publisher and another queued spot | Jobs/results empty, pending/backlog zero; spots unchanged; Results still open | Pending reset or empty queues hides retained references | Publication, direct queue and open-channel controls / toxicity |
| Outcome accounting and precedence | Natural timeout/error versus terminal cancellation; empty/routine/cached toxic/uncached post-Stop comments | Exact independent counters; no local/cache/queue/AI work after terminal admission | Any unavailable increment accepts wrong diagnosis | Counter and precedence tables / toxicity |
| Received ownership and cache bounds | Received result retained across Stop; category mutations; cap/TTL/churn | Consumer can still apply its result; cache copies and cardinality hold | Clearing results accidentally closes channel or mutates transferred data | Result ownership and existing/new cache controls / toxicity |
| Normal-path resource cost | Non-human, local, cache and queue-full single-item paths | No added per-comment allocations; short human-only mutex, no I/O/joins under it | Leak fix adds general queues/helpers or claims performance from code shape | Source caller-shape review and before/after guarded benchmarks / toxicity |

The jobs/results queues retain at most QueueSize entries each; active workers
are bounded by Workers. Cache keeps its existing cap/TTL and cloned decisions,
without spot pointers. Constructor context/terminal flags are constant-size
component state; no new registry, goroutine or metrics owner is needed. Existing
Pending, ResultBacklog and CacheEntries observe cardinality. The production
client must honor context cancellation; do not manufacture retirement by
abandoning an uncooperative client in another goroutine.

Select focused lifecycle/HTTP/cache tests, complete ordinary and race baselines,
analyzers and the cumulative ordinary/race telnet checkout pair. Permanent Go
tests extend the existing unattended Windows runner's package selection; the
telnet fixture alone does not qualify classifier or runtime shutdown. Record
the durable terminal contract and troubleshooting RCA, update operator/support
guidance and maps last. Benchmarks measure normal-path overhead locally; no
production p99, external model quality or runtime leak verdict is authorized or
claimed. A future comparable load/profile capture would be needed for those
claims, not for this directly tested terminal correctness contract.

Implementation and targeted evidence: `internal/toxicity/classifier.go` owns the
terminal boundary without exported API or configuration changes. Eight new unit
controls cover pre-Start admission, no revival, bounded repeated Start, twelve
racing lifecycle rounds, full-result blocked publication, six post-Stop
precedence cases, received ownership and a success returned after cancellation.
Two new cache controls verify detached category slices and 1,000 insertions with
both indexes bounded, followed by exact lazy-TTL removal. The real HTTP table
exercises header and first-body-read stalls with a 30-second ordinary deadline,
one active and two queued jobs, concurrent/repeated Stop, actual cancellation,
handler/worker joins, unchanged discarded spots and literal outcome accounting.
Independent fixture releases/connections make failure cleanup separate from
the success cancellation oracle. New tests retain existing queue/cache bounds.

The existing natural-timeout/error test now checks the complete literal Stats
after retirement. The error control gets its own one-second deadline instead of
sharing the timeout control's one millisecond, so delayed scheduling cannot
mislabel an intended immediate error. Focused ordinary checks passed, followed
by final focused race checks after this review refinement. Complete ordinary
and race baselines each passed all 70 packages. Vet/Staticcheck passed and lint
reported zero issues. Final checkout reconciliation also passed below.

Guarded five-sample before/after admission benchmarks passed their decision and
cardinality checks. Non-human/local/cache/full-queue allocations stayed exactly
0/3/6/4 allocs and 0/32/104/88 bytes per operation. Observed after CPU ranges were
3.291-3.424 / 203.6-240.1 / 582.0-623.1 / 756.1-859.6 ns/op respectively;
before ranges were 3.833-4.115 / 186.1-228.2 / 576.6-651.2 / 765.9-803.2.
These local, uncontended samples qualify the allocation target and do not prove
a CPU improvement, contended latency, production p99 or process-wide leak result.
Logs: `checkpoint-20a-admission-before.log` and `checkpoint-20a-admission-after.log`.

| Selected final evidence | Observed result |
|---|---|
| Focused classifier/cache/pipeline controls | Ordinary PASS (`checkpoint-20a-focused.log`); final race PASS after the natural-error review refinement (`checkpoint-20a-focused-final.log`) |
| Complete ordinary baseline | All 70 packages passed/build-checked; `checkpoint-20a-all-packages.log` |
| Complete race baseline | All 70 packages passed/build-checked, no race finding; `checkpoint-20a-race.log` |
| Whole-repository analyzers | Vet and Staticcheck passed; lint zero issues; `checkpoint-20a-vet.log`, `checkpoint-20a-staticcheck.log`, `checkpoint-20a-lint.log` |
| Offline runner controls | All 84 passed; `checkpoint-20a-runner-controls.log` |
| Ordinary actual checkout | `20261010-104916-23f25d96`: 136 PASS, zero wire/cleanup failures; 12 qualified stop receipts |
| Race actual checkout | `20261010-105247-7303d2de`: 136 PASS, zero wire/cleanup failures; 12 qualified stop receipts |

Both actual builds and all 24 stop receipts identify Go/module digest
`e04632017283b97e991f1b3d9c33aa228ede4ca2f7460ff1b7fc70ae63be964c`.
All 1,124 current Go/module hashes and the current Git source inventory matched
the final ordinary manifest. The five saved Python driver/support/harness/control
hashes stayed unchanged across the final pair. Config identity is separately
checked by the driver; Go source identity excludes Python/docs/private config.
These telnet receipts qualify their fixture consumer/archive retirement, not
the classifier or full production pipeline. Classifier evidence comes from the
permanent Go controls and real local HTTP barriers above.

Support-agent impact: Required. Config guidance, the toxicity support card and
symptom index distinguish terminal cleanup, ordinary AI failure and post-Stop
fail-open behavior. Indexed ADR-0274 refines ADR-0114's lifecycle only;
TSR-0068 records the ownership RCA and local qualification. The output/runtime
grace and downstream join defects remain 20b; neither this patch nor its tests
claim full runtime ordering, process-wide leak freedom, external model quality
or production latency. Design/test reviews inherited context; fresh final
verification was lead-owned, not independent. It directly checked the production
diff, all five new Go test/benchmark files, existing-error refinement, counter
oracles, cancellation/cleanup ownership, docs and final source/receipt identity.
Record and whitespace checks passed; crawler selection was zero files, so the
classifier's new support-critical header was reviewed manually. Maps are
regenerated last, then the staged final diff is checked before commit.

## Checkpoint 20b: production output retirement and handoff

Status: complete. This design and matrix preceded implementation. Authority
is v3 section 3 and checkpoint 20b; no new product policy is selected.

Current discovery: semantic gopls references establish that the bootstrap
`processOutputSpots` wrapper has one runtime caller, and stabilizer admission
has the initial-delivery and delayed-retry callers. The runtime starts classifier
workers and its output loop before network startup can fail. Primary dedup Stop
does not close its output channel and is not idempotent, so waiting for channel
closure or closing that channel externally is not a safe retirement mechanism.
The output loop owns classifier consumption and temporal/FT state, while the
stabilizer's separate release consumer currently has no join. Scheduler Stop
alone cannot prove final fanout completion, and retries can reserve work after
that scheduler exits. Source/callers, existing tests, ADR-0114/0274, ADR-0038,
ADR-0148, config normalization and shutdown ordering were inspected.

Selected architecture: keep a private runtime-owned output-pipeline handle,
installed before launching its goroutine. Its stop request and completion are
constant-size state. The same once-owned retirement helper serves signal
shutdown and partial-startup close, stopping primary dedup exactly once without
changing that package's exported contract. It requests and joins the output
loop before peer/telnet/archive/grid/store dependencies stop. If startup failed
before a pipeline exists, the helper terminally stops the constructed classifier.
Remove the now-redundant single-caller bootstrap wrapper rather than retaining
two startup owners. This is covered by the approved lifecycle boundary.

The first retirement request starts the classifier's existing grace budget,
even if an already received result is blocked in normal processing. One owned
timer goroutine waits for expiry or early drain completion, invokes the existing
terminal classifier Stop and signals its own completion. It never consumes or
applies results. The pipeline remains the only processing consumer, checks its
stop request before ordinary receives, and admits selected results to processing
only within grace. Physical receive is followed by an explicit expiry check
before ApplyResult: a select can choose ready data after its timer expires.
An expired value is consumer-owned and discarded without spot mutation; a result
admitted to processing remains owned through normal handoff, even after expiry.
Expiry cancels other HTTP work but cannot revoke admitted processing. Pipeline retirement
joins terminal classifier completion and all transferred processing. The grace
budget is not an overall shutdown deadline, and synchronous downstream callbacks
remain responsible for returning. No helper abandons blocked work.

After classifier drain, force-retire temporal holds using actual UTC time and
the existing decoder's force classification, then FT holds, including those
created by late results or temporal release. Stop/clear both pipeline timers.
Stop the stabilizer scheduler and join its release consumer before reporting
pipeline completion. Fence terminal stabilizer admissions/retries and retire
its still-held input/heap references and pending reservations as existing
shutdown drops; already released envelopes continue normal final gates/handoff.
Do not force-deliver unreleased stabilizer holds or change retry, suppression,
overflow, secondary dedupe, queue-drop or model contracts.

Design challenge disposition: waiting for dedup closure cannot work; closing
dedup output requires a broader sender/producer join contract and is rejected.
Global parent cancellation can destroy dependencies before grace and peer
withdrawal, so a private pipeline handle is selected. A pipeline-specific
context is equivalent here but adds no required ownership. A single owned timer
and an owned AfterFunc are both viable expiry mechanisms; the timer's explicit
completion avoids callback-cancellation bookkeeping. Sequential drain/process/
Stop is rejected because a transferred callback can postpone cancellation beyond
grace. This is an implementation refinement under selected v3, not a new policy.
Design and test-strategy reviews inherited context and are not independent.

Scientific timing review actually occurred in a separate non-steered context.
That reviewer inspected committed source at 4b86648, causal timing arithmetic,
ADR-0038 and config/replay/metric contracts without this ledger, intended design,
diff or test expectations. Root verified its material source observations.
Normal and forced evaluation use elapsed actual time t-t0; force ends waiting
but retains nonempty-winner and inclusive score/margin gates. Evidence ends at
min(t,t0+max-wait), while elapsed age is not capped to max-wait. A supplied future
clock can contaminate age; it does not manufacture observations or necessarily
change the winner when all stored evidence precedes actual shutdown.

Representative independently derived timing vectors (UTC t0 is observation;
scores are conditional dimensionless inputs, not decoder-score predictions):

| Inputs | Expected categorical/time rule | Provenance and failure detected |
|---|---|---|
| Lag/max-wait 15s, actual t=t0+500ms, force, winner present, score >=0, margin >=80 | Terminal apply, cutoff t, age exactly 500ms | Causal elapsed time and inclusive selected gates; catches future-clock metric contamination |
| Same, margin 79 or score -1 or no winner | Selected configured fallback, age 500ms | Existing product classification plus independent boundary arithmetic; force cannot manufacture confidence |
| Lag/max-wait 15s, no force, t=t0+15s-1ns / 15s / 15s+1ns, gates pass | Defer / apply / apply; last cutoff remains 15s, age 15s+1ns | Exact lag and max-wait boundaries; catches off-by-one and age capping |
| Lag 2s, max-wait 6s, failed gates, t=t0+6s-1ns / 6s / 6s+1ns | Defer / configured fallback / configured fallback | Max-wait boundary arithmetic, no invented success |
| Max-wait 6s, evaluation at 7s; evidence at 6s / 6s+1ns | Endpoint eligible / later event excluded; age 7s | Independent inclusive window boundary and truthful age |
| Actual age 500ms, artificially supplied t+24h | Truthful age 500ms; synthetic age would be 86,400,500ms | Independent arithmetic, catches existing shutdown shortcut |
| Accepted lag/max-wait 25h, actual age 500ms, force | Terminal evaluation at actual time, although due exceeds now+24h | Accepted config domain and force meaning; catches stranded future-due holds |

This establishes timing/retirement semantics, not callsign truth, calibrated
probabilities or optimal thresholds. Independent oracle missing for those
stronger claims: no held-out truth/calibration authority was supplied, and no
such change or claim belongs to this patch. Decoder scoring and resolver rails
remain unchanged.

### Contract-to-test matrix, completed before implementation

| Contract / failure | Stimulus or fault | Observable evidence | False-green risk | Checker / owner |
|---|---|---|---|---|
| Runtime owns completion; dedup output need not close | Actual runtime start then shutdown/partial close with dedup output left open | Owned stop/completion, dependency closure only afterward; repeated shared retirement safe | A synthetic closed channel hides production's open output | Permanent runtime integration and race / cluster |
| One processing owner through grace | Independently gated CW results; stop observed before releasing blocked ordinary processing | Late received result retains classification, model and normal handoff | A second test/result receiver or ordinary select consumes the grace control | Synchronized runtime callback barriers, source priority review / cluster |
| Processing admission remains inside grace | Selection resumes across expiry; literal before/exact/after boundaries | Expired selected value is discarded before ApplyResult or handoff; admitted processing survives expiry | Preselect check only; timer Stop still joining makes completion a stale gate | Shared ordinary/drain admission, literal time and unchanged-spot controls / cluster |
| Cancellation boundary independent of transferred processing | Received CW-A blocks in existing grid callback; completed CW-B remains buffered; another real HTTP request stalls with an unexpired long ordinary deadline | Request canceled at grace expiry before its ordinary deadline while A/completion/resources remain blocked; A later hands off; B stays unstamped and is discarded | Sequential drain passes uncomplicated stalls; natural timeout or fixture cleanup manufactures cancellation; timer applies results or late-receives B | Real local HTTP headers/body, exact A/B ownership and coupled barriers / cluster |
| Late FT holds retire | FT result completes during grace after prior CW barrier; long actual FT hold | Final normal FT handoff and zero groups/pending/timers after join | Flushing holds before drain loses newly created holds; ring-only check misses downstream | Gated runtime results and archive/telnet handoff / cluster |
| Temporal force uses actual clock and future-due items | Accepted 25h lag, 500ms actual age; equal-due/order controls and failed gates | Immediate release, literal actual age/gate action and empty pending/heap | now+24h superficially flushes usual configs; expected values copied from scoring | Independent timing vectors plus controller/pipeline controls / cluster |
| Normal downstream semantics remain | Suppressed MED duplicate and saturated bounded archive/telnet queues | Suppression/drop counters and unchanged sentinels; accepted handoff distinguished from delivery | Requiring every spot on a socket ignores intended gates/drops | Permanent delivery controls using real consumers / cluster |
| Scheduler join includes release processing | Already released envelope blocks in existing grid callback; risky retry and long-held work | Scheduler exit alone insufficient; completion waits consumer; terminal enqueue refused, reservations/references retire | Joined scheduler hides unowned consumer; empty fixture hides queue retention | Synchronization, post-Stop controls, exact pending/channel counts and race / cluster |
| Partial startup and cancellation cleanup | Constructed classifier before pipeline; real occupied listener failure after output startup | Same terminal owner, pending/backlog zero, actual workers and callback consumers joined | Test directly calls classifier Stop instead of production cleanup | Runtime startup-failure controls and independently released fixtures / cluster |

Final post-approval test review is adequate. Its checker refinements are covered:
repeat a retirement request while received CW-A remains blocked, without
extending the first deadline; retain completed unreceived CW-B and require its
unstamped discard; gate every drain receive by the actual deadline rather than
only terminal-worker completion. Refine the classifier Stop comment so grace
transfer and completion of transferred processing are distinct. These are
checker/comment refinements within approved 20b, not new product policy.
`go list -deps -test -json` confirms the cluster's production consumers and the
toxicity client/test dependencies; the saved package inventory is
`checkpoint-20b-dependencies.json`. No exported interface or schema changes.

Post-implementation review identified a preselect scheduling window: ready data
can win select after expiry while terminal Stop still joins. Disposition: use
the same post-receive processing admission in ordinary and grace paths. This
does not revoke consumer ownership under ADR-0274, which permits applying a
received result; v3 limits grace consumption and does not promise processing
every mechanically selected value. Already admitted A retains its normal
handoff. Added independent literal before/exact/after deadline and expired
selected-value controls; the real coupled fault covers admitted A/unreceived B.
The design/test refinements are within selected 20b, not a policy change.

Existing temporal/FT caps and stabilizer max-pending/input/release capacities
remain. New lifecycle state has constant cardinality; one terminal timer worker
per pipeline joins before completion, with no per-result goroutine or queue.
Pending/backlog and controller/scheduler counts qualify local convergence.
Preserve normal single-item output allocation behavior; measure existing guarded
ownership benchmarks and a terminal-admission benchmark if fencing adds cost.
No performance improvement or production leak claim is intended. Long-running
load/profile evidence would be required for those claims.

Select targeted runtime, temporal, FT, stabilizer and real HTTP ordinary/race
controls; complete ordinary/race package baselines, analyzers, offline runner
controls and cumulative ordinary/race checkout reconciliation. Read the final
diff freshly, document durable ownership/timing and troubleshooting boundaries,
update support routing, and regenerate maps last. Production external model,
external cluster, socket receipt, process-wide ingress retirement and deployment
remain outside the evidence claimed by this checkpoint.

### Implementation and review corrections

The runtime installs the pipeline handle before goroutine launch; normal
shutdown and partial-startup cleanup share an idempotent completion owner.
The pipeline's first request starts one existing grace budget and a joined
expiry worker. Selected results cross an explicit processing-admission boundary
before ApplyResult. Admitted processing, late temporal/FT holds and the separate
stabilizer release consumer finish before dependent resource closure. Terminal
stabilizer input/heap retirement releases references and each reservation; no
blanket count reset hides lost ownership. Existing normal gates and queue drops
remain. The single-caller bootstrap wrapper is removed; no exported API changes.

Permanent controls use real runtime retirement with dedup output left open,
independently gated CW/FT work, actual local HTTP header/body stalls, blocked
normal callbacks, suppressed MED output, saturated archive/telnet handoffs,
partial-startup failure, terminal retry policies, future-due temporal holds and
stabilizer scheduler/consumer barriers. The linear HTTP fault scenario keeps
its ownership and cancellation witnesses together; splitting its longer
table-driven test would obscure the order that prevents false-green shutdown
claims. Neither fixture consumes classifier Results itself.

The initial compile exposed a now-unused wrapper import, which was removed.
The first executed focused attempt exposed fixture defects: NewSpot's raw
status is empty (logically UNKNOWN), and an ordinary loopback reservation did
not exclude Windows production SO_REUSEADDR. Controls now initialize explicit
UNKNOWN, and a Windows-only test reservation uses SO_EXCLUSIVEADDRUSE before
the matching wildcard bind. The observed actual telnet startup now fails at
that occupied listener; production binding policy is unchanged.

The first crawler check identified missing entry comments in two existing
touched files; ownership/force comments were added and the check passed.
Lint identified two fixture inputs that every caller supplied identically.
The fixture now fixes probable state and derives its two-candidate weighted
total; all five selections retain the same 5,000 total. Affected ordinary/race
controls and analyzers are rerun. These test-helper/comment refinements do not
invalidate the completed broader production-behavior baseline.

### Observed validation and boundaries

| Check | Observed result |
|---|---|
| Final focused ordinary/race controls | Both PASS; `checkpoint-20b-focused-final.log`, `checkpoint-20b-focused-race-final.log` |
| Complete ordinary baseline | All 70 packages passed/build-checked; `checkpoint-20b-all-packages.log` |
| Complete race baseline | All 70 passed/build-checked, no race finding; `checkpoint-20b-race.log` |
| Final whole-repository analyzers | Vet and Staticcheck PASS; lint zero issues; `checkpoint-20b-{vet,staticcheck,lint}-final.log` |
| Offline controls | All 84 runner and 29 collector controls passed; `checkpoint-20b-runner-controls.log`, `checkpoint-20b-collector-controls.log` |
| Ordinary actual checkout | `20261010-120521-8b2e52e8`: 136 PASS, zero wire/cleanup failures; 12 qualified retirement receipts |
| Race actual checkout | `20261010-121008-5841e0dc`: 136 PASS, zero wire/cleanup failures; 12 qualified retirement receipts |

Final guarded allocation measurements used one fixed spot, bounded channels,
no stabilizer scheduler, and per-iteration reservation retirement. Five samples
per benchmark retained these results:

| Path | Before / after allocations and bytes | Before / final after nanoseconds |
|---|---|---|
| PrepareSpotContext | 0 allocations, 0 bytes / unchanged | 46.69-53.94 / 46.49-58.41 |
| EmitSpotOwnership | 1 allocation, 896 bytes / unchanged | 286.1-375.9 / 203.4-467.2 |
| Stabilizer pre-Start admission | 1 allocation, 96 bytes / unchanged | 90.96-105.2 / 99.21-118.5 |

Source guards passed for each build/execution. Logs are
`checkpoint-20b-output-before.log`, `checkpoint-20b-stabilizer-before.log` and
`checkpoint-20b-admission-after-final.log`. The short terminal fence retains
ordinary allocation behavior; these variable timing samples do not establish
production latency, capacity or a speed improvement.

The final actual pair, all 24 stop receipts and every current Go/module hash
identify digest `1294fb524e8769fdc2b8277024a69618856f30bc60b73ffa87ac9a89ca04969e`.
The current Git source inventory and all 1,134 hashes match both manifests.
Five Python driver/support/harness/control hashes stayed unchanged across the
pair; config identity is separately checked by the driver. Source identity
excludes Python/docs/private config. `checkpoint-20b-identity.log` records this
reconciliation. Wire fixtures qualify their command/consumer/archive boundary;
production runtime retirement is established by the permanent Go controls.

Support-agent impact: Required. Updated config guidance, the toxicity support
card and symptom routing separate grace, processing ownership and downstream
completion. Indexed ADR-0275 and TSR-0069 cross-link the classifier, temporal
and stabilizer records without replacing their other policies. The native
occupied-listener control follows the [Windows socket reservation contract](https://learn.microsoft.com/en-us/windows/win32/winsock/using-so-reuseaddr-and-so-exclusiveaddruse).
The independent timing oracle and its claim limits are recorded above;
design/test/quality reviews inherited context. Fresh final verification was
lead-owned and directly covered the production diff, test ownership/barriers,
literal clocks, normal gate/drop controls, raw-status and listener corrections,
fixture simplification, source identity, support guidance and decision records.

Record, crawler-entry and whitespace checks passed. Maps are regenerated last,
then checked and the staged final diff reviewed before commit. This qualifies
native Windows local behavior. External model/cluster behavior, durable archive
receipt, socket receipt, deployment, process-wide ingress retirement, long-run
production leak freedom and latency remain unclaimed.

## Checkpoint 21: optional toxicity configuration values

Status: complete. Authority: approved v3 checkpoint 21 and
the explicit user choice that zero requests the existing six defaults.

Current evidence: `internal/toxicity/config.go` is the strict optional-file
loader; `clusterRuntime.initializeToxicityClassifier` is its production caller.
Absent/blank files disable the feature. Present files require all ten non-null
keys even when disabled. The YAML decoder already owns scalar conversion;
this patch does not invent an integer spelling grammar. Configuration is
optional deployment/runtime tool-boundary configuration, not a main-config
merge. Enabled credential validation reads only the named environment variable.
The safe-gate reference table is loaded only for the enabled feature.

The loader already normalizes explicit zero to 1200 ms, 2 workers, queue 128,
cache 4096, TTL 86400 seconds and payload 512 bytes, and refuses negatives.
However, a positive timeout over 2147483647 silently becomes 1200 ms in the
consumer, and a TTL over 9223372036 seconds overflows `time.Duration` conversion
on 64-bit hosts. Add two loader admission checks before enabled credential
validation. Preserve the raw constructors' existing disable/default behavior;
do not add arbitrary capacity limits or allocate boundary-sized capacities.
Native integer decoding limits remain authoritative on 32-bit hosts.

Architecture: synchronous startup validation before constructing queues/cache,
starting workers or converting TTL. No new goroutine, retained state, interface,
hot-path operation, defaults, parser grammar or shutdown behavior. The accepted
timeout upper bound also makes downstream doubling safe; existing 500 ms to
five-second drain clamp remains unchanged. Current semantic references and the
checkpoint-20b Go dependency inventory cover toxicity and its runtime consumer;
imports remain unchanged. The config discovery reviewer was inherited-context,
read-only; this is not scientific or independent review. No policy is reopened.

Lead-owned falsifiability review: literal default snapshots and consumer
observations prevent a normalize-only false green; exact limits and one-past
refusals distinguish representability from silent fallback. A separate runtime
initializer test proves production loading/startup, since the command fixture
intentionally omits the optional classifier. Fractional scalar conversion and
arbitrary raw-constructor overflow remain outside this approved preservation.

| Contract | Failure/boundary | Stimulus | Observable result | Level | False-green risk | Exact checker | Owner |
|---|---|---|---|---|---|---|---|
| Required presence | Missing/null/unknown or wrong-type keys | Enabled and disabled complete files, one altered key | Error and no admitted config | Loader | Disabled bypass accepts invalid YAML | Config contract table | toxicity |
| Explicit zero | Six sentinels lost or treated as disabled | Each zero and all-zero YAML | Literal six defaults; enabled flag retained | Loader/consumer | Test repeats production constants | Snapshot, queue/cache/payload/deadline tests | toxicity |
| Negative/positive | Negative admitted or positive re-defaulted | Each -1 and 1; native integer maximum capacities load-only | Negative refused; valid literal positives retained | Loader | Huge test allocation or unrelated auth failure | Enabled/disabled values table | toxicity |
| Duration admission | Conversion overflow or hidden timeout fallback | Exact upper bounds and one-past; 32-bit architecture guard | Exact effective duration or key-specific refusal | Loader/consumer | Error comes from missing secret or native int decoder | Boundary table and effective cache expiry/deadline | toxicity |
| Constructor preservation | Direct calls acquire loader semantics | Raw nonpositive settings and timeout fallback | Existing disable/cache-off/untruncated/default behavior | Consumer | Only loaded config exercised | Constructor and literal drain vectors | toxicity |
| Production ownership | Loader bypass or optional file made required | Actual initializer absent/disabled/all-zero enabled/bad duration | Expected startup result and terminal worker join; no HTTP request | Runtime/race | Wire fixture omits feature and is miscredited | Runtime initializer controls | cluster |

Selected evidence: focused ordinary/race controls, complete ordinary Windows
lane and repository analyzers; cumulative runner/collector offline controls and
ordinary/race actual checkout command reconciliation. Existing 20b full-race
baseline remains relevant; repeat focused race for the new consumers. No parser
grammar or performance change requires new fuzz/benchmark claims. Record the
explicit narrow zero exception in ADR-0276, operator YAML/support guidance and
the configuration decision chain; preserve existing accepted history.

### Implementation and observed qualification

The two guards are in `normalizeConfig` after explicit-zero normalization and
negative rejection, before enabled credential checks. No raw constructor,
timeout formula, queue, cache, worker or decoder changes. Permanent loader
controls cover all ten keys missing/null/wrong-type, the six values negative,
zero and one with enabled/disabled files, unknown keys, optional paths, native
integer capacity boundaries without constructing them, and exact/one-past
duration limits. Consumer controls observe real classified payload and context
deadline, literal queue/cache defaults, exact cache expiry and drain endpoints.
Actual runtime initializer controls start and terminally join the loaded
classifier, exercise a synchronous local decision without HTTP, and refuse
invalid duration. Missing/malformed gate files succeed only while disabled.

The inherited read-only reviewer identified a false-green fixture that always
wrote a valid gate. The paired enabled/disabled absent/malformed controls resolve
it; focused race and the final complete ordinary lane include the final controls.
No remaining material blocker was reported. Root reviewed the production diff
and final tests directly; neither pass is described as independent review.

| Evidence | Observed result |
|---|---|
| Focused ordinary controls | PASS; `checkpoint-21-focused.log` (before the gate-only fixture refinement) |
| Final focused race controls | PASS; `checkpoint-21-focused-race.log`, including paired gate controls |
| Complete ordinary Windows baseline | All 70 packages passed/build-checked; `checkpoint-21-all-packages.log` |
| Whole-repository analyzers | Vet and Staticcheck PASS; lint zero issues; `checkpoint-21-{vet,staticcheck,lint}.log` |
| Native Windows 386 config/consumer controls | Compiled and executed successfully; `checkpoint-21-386-final.log` |
| Offline runner/collector controls | 84 and 29 PASS; `checkpoint-21-runner-controls.log`, `checkpoint-21-collector-controls.log` |
| Ordinary actual checkout | `20261010-123529-679c021b`: 136 PASS, no wire/cleanup failures and 12 source-matched retirement receipts |
| Race actual checkout | `20261010-123900-809fd19c`: 136 PASS, no wire/cleanup failures and 12 source-matched retirement receipts |

The pair's 1137 Go/module file hashes and Git source inventory match the final
current checkout; five Python driver/support/harness/control hashes remained
unchanged. All 24 retirement receipts match the source digest and report started,
clients retired, consumer joined and archive stopped. `checkpoint-21-identity.log`
records digest `8835fce47963aed407e870b5552e0b3415a7a3e86ddc536c99b65520ff7edde8`.
Manifests identify HEAD 1e016bd plus these working-tree hashes; no clean-HEAD
binary equivalence is inferred. Fixture-private configuration is separately
qualified by the driver.

The initial extra 386 invocation compiled successfully but PowerShell split an
unquoted dotted test flag; the binary refused `-test` before execution (status
2). Reinvoking the same saved binary with a literal argument array passed. The
failed command is retained in `checkpoint-21-386.log`; it is not a passing test.

The selected full baseline covers final production source. The existing full
race baseline from 20b is retained; new focused race exercises the added
consumers, with no production concurrency changes to invalidate the earlier
broader result. YAML headers passed for all 20 files; comment-only comparison
against HEAD passed and reported no added boolean inline warnings. Troubleshooting
records passed index/status/RCA/link checks. Support routing, toxicity card,
operator settings and ADR-0067/0114 cross-links now point to ADR-0276/TSR-0070.

Limits: the cumulative wire fixture intentionally omits optional toxicity and
does not establish its startup semantics; permanent actual initializer and
consumer tests do. Native 386 evidence covers toxicity config/consumers, not an
entire 32-bit runtime. No external Worker, model accuracy, production sizing,
performance or raw malformed TTL-overflow claim is made. No new scientific,
hot-path, lifecycle or parser-grammar policy requires a separate oracle here.

Fresh lead verification checked the approved item, final guards/consumer tests,
review disposition, exact artifacts, support and decision records, and claim
limits. No source changed after qualification. Maps are generated last, checked
and directly reviewed with the final staged diff before this checkpoint commit.

## Checkpoint 22: root executable argument boundary

Status: complete; authority is approved v3 checkpoint 22.

Current source: root `main` resolves binary identity, checks version tokens,
then delegates every other invocation to `cluster.Run`. Semantic references
confirm this is Run's only production caller; shouldPrintVersion also has its
existing root test caller. Run loads configuration before runtime construction;
its loader conditionally tries the relative `data/config` fallback when its
missing-file check permits it. Excluding that fallback with an isolated cwd is
required rather than assuming an absent override alone proves isolation.
Root main/version tests, README
build/config notes and the startup support card are the affected contracts.
The root dependency/test inventory is saved in `checkpoint-22-dependencies.json`.

Architecture: root main retains version-token-anywhere precedence, including
case normalization/trimming and the existing three version tokens. After that
check, only conventional `-h`, `-help` and `--help` arguments are accepted;
unrecognized options or positional arguments exit 2 with usage before Run.
All arguments are checked, so help cannot mask an unknown argument. No arguments
retain normal startup and its current failure status. A private bounded argv
scan returns the first unknown token without effects; help printing remains at
main's process boundary. Failed help output exits unsuccessfully rather than
starting runtime. No config, cluster.Run interface, metadata, runtime tuning,
worker, persistence, networking or version-output behavior changes.

Actual-main subprocess controls use the runner's saved root test executable,
an explicit child marker, isolated empty working directory, absent override,
bounded execution and joined child. The helper calls production main; it does
not substitute cluster.Run. A no-argument startup-failure control proves the
fixture really excludes both config paths. It omits child paniconexit0 solely
because returning from production main deliberately exits zero; the parent
test binary retains the runner's flag. Pure argv fuzzing supplements actual
exit/effect controls rather than replacing them.

| Contract | Failure/boundary | Stimulus | Observable result | Level | False-green risk | Exact checker | Owner |
|---|---|---|---|---|---|---|---|
| Help before startup | Help enters config/runtime | Each help flag, repeated help in isolated empty cwd | Exit 0, useful usage, empty stderr, no files | Actual main | Repository default config remains available | Root subprocess controls and no-arg failure witness | root |
| Version precedence | Unknown/help masks version | Each token anywhere, mixed case/trim, help+unknown+version | Exit 0, literal fixture identity, no usage/startup | Actual main | Predicate-only tests never invoke main | Version subprocess cases plus existing tests | root |
| Unknown admission | Partial argv accepted or positional ignored | Unknown before/after help, empty/Unicode/control input and extra positional values | Exit 2, diagnostic/usage, no config attempt/files | Actual main | Help short-circuits later invalid token | Exact status/streams/empty-directory checks | root |
| Startup preservation | New validation prevents ordinary launch | No arguments and no available config | Existing startup diagnostic and exit 1 | Actual main | All successful tests bypass a broken helper | Positive startup-failure control | root |
| Output failure | Help silently succeeds when unwritable | Helper closes only its stdout before production main | Exit 1 with output error, no startup/files | Actual main | Parent fails to launch rather than child's write failing | Closed-output child marker/control | root |
| Parser exactness | Help conceals or alters malformed token | Fuzz invalid positional values surrounded by valid flags | First unknown returned byte-for-byte | Fuzz | Parser oracle alone credited for process effects | Saved root binary fuzz plus main controls | root |

Lead-owned detailed test challenge finds no unresolved product-policy or oracle
gap. Conventional help aliases are local implementation detail of the approved
help fix; existing version precedence is controlling authority. Selected lane:
focused root tests, bounded argv fuzzing, all 70-package ordinary baseline and
whole-repository analyzers. No production concurrency change requires new race
qualification. Cumulative telnet remains covered by checkpoint 21 and final 35;
it cannot establish root CLI behavior. README/startup support and TSR-0071 retain
the operator finding; accepted build/version ADRs are preserved without a new
architecture decision solely for checkpoint bookkeeping. Maps are last.

### Implementation, qualification and final review

`main.go` adds one private argv scan and one usage constant. The existing version
predicate/output and Run delegation are unchanged. Main refuses the first
unknown token after the version check, prints help only for a fully valid help
invocation, checks help output failure and retains no-argument startup.
`main_arguments_test.go` adds 21 actual-main cases plus the pure argv fuzz target.
The explicit helper marker/literal version identity, separate stream checks,
bounded child join and empty-directory checks prevent skipped helpers, parent
launch errors and default-config availability from becoming passing evidence.

| Evidence | Observed result |
|---|---|
| Final focused actual-main/version tests | PASS; `checkpoint-22-focused-final.log` |
| Final complete root package | PASS after lint refinement; `checkpoint-22-root-final.log` |
| Instrumented bounded argv fuzz | PASS, 40934 executions in 10 seconds with two workers; `checkpoint-22-fuzz-final.log` |
| Complete ordinary Windows baseline | 70 passed/build-checked; `checkpoint-22-all-packages.log` |
| Final repository analyzers | Vet/Staticcheck PASS, lint zero issues; `checkpoint-22-{vet,staticcheck,lint}-final.log` |
| Record and whitespace checks | PASS; no inconsistent TSR index/status/RCA/link or diff whitespace |

Three non-green artifacts are retained. The initial focused test guessed the
final-fallback startup message, while the current loader reports `failed to stat
config path` directly; the assertion now proves actual config failure without
changing the loader. The initial stable binary fuzz run warned it lacked
coverage instrumentation, so it is not credited as guided fuzz evidence;
rebuilding the same path with `go test -c -fuzz=FuzzUnknownRootArgument` supplied
instrumentation for the passing final run. Lint flagged an ignored custom usage
helper error. Removing that unnecessary wrapper put the checked stdout write
directly in main and kept authoritative status 2 for invalid invocation.

The final root suite and repository analyzers reran after that local lint
refinement. The complete ordinary baseline preceded it; no other package,
runtime interface, config, build settings, concurrency or parser logic changed
to invalidate broader results. The fuzzed argv helper remains byte-for-byte
unchanged. No new race/performance/model evidence is needed for this cold CLI
boundary. Prior cumulative telnet results remain historical command evidence;
they are not credited for root CLI behavior or a newly hashed full checkout.

Root's fresh final pass reviewed all code/tests, actual artifacts, approved
boundaries, output/exit paths, existing version fixtures, README/startup support
and TSR-0071 routing. This pass is lead-owned, not independent. No durable
architecture or build/version decision changed; existing ADR-0236/0252 remain
authoritative. Support impact is operator invocation/discovery and is reflected
in README, Windows startup card and source map. Local main controls do not prove
real node readiness, production deployment or cross-platform runtime behavior.
Maps are generated last, checked and reviewed with the staged diff before commit.

## Checkpoint 23: sunspot watcher one-shot outcomes

Status: complete; authority is approved v3 checkpoint 23.

Current source/callers: `cmd/voacap_sunspot_watch/main.go` calls private run,
which calls private poll. Semantic references confirm neither has external Go
callers. Main already exits 1 on returned errors; run logs poll errors but returns
nil for `-once`. Report helpers ignore stdout errors. Each poll attempts its
existing report before its existing state write. The native PowerShell wrapper
already exits with the Go command status. Dependency inventory is in
`checkpoint-23-dependencies.json`. Inspected `internal/voacap/sunspot.go`, existing
sunspot/forecast fixtures and SSN decision routing establish that this is an
observational tool, not a new forecast or calibration claim.

Architecture: one-shot run returns polling/report/state errors. Continuous mode
keeps its current timestamped diagnostics and retry interval. Inject a private
io.Writer through run/poll/report helpers, with main supplying its existing
stdout. The caller owns the writer; do not close it. Join report errors with the
existing state write result, preserving the write attempt and order even if
reporting fails. Header failure is reported too, while preserving the existing
poll/state attempt; continuous header error is logged once. No new flag/default,
duration, scientific calculation, state format, HTTP bound, conditional fetch,
JSON parser or production lifecycle change. The existing ordered state attempts
resolve preservation; do not introduce a new skip-write-on-output-error policy.

Actual-main children use the saved watcher package executable, an explicit
helper marker, reset only their own flag set, private loopback HTTP and TempDir
state, bounded child join and request-count witnesses. Continuous retry evidence
deliberately terminates its child after observed retries and joins it; this is
not graceful production retirement evidence. A native PowerShell child executes
the unchanged wrapper with a local function standing in for the Go command,
capturing the forwarded argument array/cwd and successful invocation. A separate
native -File child compares a real Go-command preflight failure against the
wrapper's status. It neither contacts NOAA nor replaces an installed executable
or changes parent state.

| Contract | Failure/boundary | Stimulus | Observable result | Level | False-green risk | Exact checker | Owner |
|---|---|---|---|---|---|---|---|
| One-shot failures | Logged error still exits zero | HTTP failure/truncated body/bad JSON/no usable rows | Exit 1, causal diagnostic, one request, no state write | Actual main | Error arises in fixture startup/flag parsing instead | Request/body/output/state witnesses | watcher |
| Success and unchanged | Fix breaks valid/conditional output | Synthetic one-group/32-spot body, 304 and repeated observation | Exit 0, expected raw/rounded 42 or unchanged, valid saved state | Actual main | Only status asserted or source-dependent advice accepted | Literal sample fields, state decode and conditional header witnesses | watcher |
| State I/O | Directory fault happens before polling | Handler creates target directory after absent-state load | Valid sample and one request, then write-path error/exit 1 | Actual main | Initial load error masquerades as save fault | Late filesystem fault witness | watcher |
| Report I/O | Header/sample/unchanged error ignored or skips old effects | Closed stdout actual main; staged failing writers | Exit/error nonzero and existing state attempt still occurs | Main/consumer | Parent fails to launch or no actual poll/state branch | Closed-output child and report writer controls | watcher |
| Continuous retries | One-shot fix terminates loop on polling error | Failing then valid then failing loopback responses | Three requests, valid saved sample, expected report/error output, joined child | Actual main | Request witness precedes parsing/persistence | Third request plus saved-state/output checks | watcher |
| Wrapper propagation | Wrapper drops flag/status or starts NOAA | Native PS argument probe plus real -File failure, child-only invalid GOFLAGS | Exact array/cwd; actual Go preflight status propagated; no state | Native PS | -Command exit mapping or mock credited for Go failure | Captured JSON marker, real Go causal error and -File status | scripts/watcher tests |

Lead-owned detailed challenge resolves the earlier-error and durable-write
false-green paths above. Inherited operator-tool discovery is source evidence,
not independent verification. The selected scientific/data contracts are
unchanged; no new independent scientific verdict is needed for reporting status.
Selected lane: watcher targeted ordinary/race controls, full ordinary baseline,
repository analyzers, source-aware support/record/whitespace checks and maps last.
No new parser/protocol or performance change calls for fuzz/benchmark repetition.

Observed qualification: final watcher race suite passed, followed by the complete
ordinary 70-package baseline on the final source. Repository vet, Staticcheck and
golangci-lint passed (zero lint issues). Logs are retained as
`checkpoint-23-focused-race.log`, `checkpoint-23-all-packages.log` and the
checkpoint's analyzer logs. The initial PowerShell -Command status-7 fixture
failed because the host returned 1; that result is retained and not credited.
The repaired native -File control observes a real Go preflight failure, and its
copied no-exit negative control returns zero under the same failure. Final tests
check child execution bounds independently of status and refuse non-loopback
HTTP before DNS/dial. Both stdout/state error ownership and continuous retry
witnesses passed. These fixtures do not establish live NOAA health, scientific
accuracy or graceful continuous-mode retirement.

Root's fresh final pass reviewed source, test fault paths, request/state/output
witnesses and native wrapper controls against the approved boundary. This is
lead-owned verification, not independent review. No model or durable architecture
contract changed. Operator/support impact is reflected in the script guide,
wrapper comments, source map and TSR-0072. Record/crawler/whitespace checks and
generated maps are completed last and reviewed with the explicit staged diff.

## Checkpoint 24: skew-fetch command status

Status: complete; authority is approved v3 checkpoint 24.

Discovery inspected `cmd/rbnskewfetch/main.go`, skew Fetch/FetchAndWrite/
FilterEntries/WriteJSON, required config registry and test-copy patterns,
ADR-0031 and support routing. Semantic references show the command is the sole
caller of FetchAndWrite. Dependency inventory is retained as
`checkpoint-24-dependencies.json`. Main already exits on config error, but logs
a fetch/filter/write error and returns success. Shared functions already return
those errors. The command can exit 1 there without changing any shared API,
threshold, CSV parser, factor calculation, timeout, path/default or file format.
The existing direct-write ownership is retained; no atomic-output claim is made.
The final success report also returns status 1 if stdout fails, preserving the
table already written. This is the same approved write-failure boundary and
retains prior partial effects; it introduces no output transaction policy.

Architecture: return command errors through a private run function so its request
context cancellation completes before main logs and exits 1. Actual-main
children run the stable package executable with reset child-local flags, an
isolated cwd/config copy and owned loopback CSV. A child transport rejects other
destinations before DNS/dial; request counts distinguish config refusal from
fetch/parse/write failures. Context bounds and joined process exit are checked
separately from status. Selection remains inclusive absolute skew, with factor-1
rows retained, as specified by ADR-0031 and current FilterEntries.

| Contract | Failure/boundary | Stimulus | Observable result | Level | False-green risk | Exact checker | Owner |
|---|---|---|---|---|---|---|---|
| Successful table | Status fix changes selection/order | Full private config; +/- exact threshold and factor-1 CSV rows | Status 0, one request, literal decoded JSON rows/order and Wrote count | Actual main | Fixture fails config or copies selection code | Literal rows and request witness | rbnskewfetch |
| Fetch/read/parse failures | Logged error exits zero | HTTP 500, truncated body, malformed/incomplete/header-only CSV | Status 1, causal diagnostic, no Wrote or new output | Actual main | Launch/deadline/config error mistaken for intended branch | Bound, request count and exact error family | rbnskewfetch |
| Empty selection | All rows filtered still succeeds | Valid below-threshold rows | Status 1, no entries after filtering, prior output unchanged | Actual main | Parsing fails before filter | Known valid rows and specific diagnostic | rbnskewfetch |
| Durable output failure | False success or early setup error | Valid CSV, blocked parent or directory target | One request, file-path diagnostic, status 1 | Actual main | HTTP/config failure masks write path | Request plus causal mkdir/write error | rbnskewfetch |
| Preservation | Pre-write failure overwrites old table | Seed exact old bytes, then fetch/parse/filter fault | Old bytes unchanged | Actual main | Only missing output tested | Byte-for-byte old-file check | rbnskewfetch |
| Config failure | Regression reaches fetch on invalid config | Missing directory and malformed data.yaml in full copy | Status 1, config diagnostic, zero requests | Actual main | Blanket early refusal appears correct | Paired successful full-config fixture | rbnskewfetch |
| JSON/report output | Encoding or stdout failure ignored | Retained NaN factor; closed child stdout after valid input | Status 1; causal marshal/report error; preserve old table on encoding failure and newly written table on report failure | Actual main | Generic parse failure or child termination credited | Specific errors plus decoded/unchanged output witnesses | rbnskewfetch |

Lead-owned falsifiability review resolves fixture/exit false-green paths; inherited
operator review is read-only evidence, not independent verification. No material
ambiguity remains. Selected lane: focused actual-main suite, ordinary complete
baseline, analyzers, source-aware support/record/whitespace checks and maps last.
No parser, production concurrency or scientific behavior change requires new
fuzz, race or model qualification.

Observed qualification: actual-main skew command and existing skew suites passed.
The first complete run failed in the unchanged peer retry timing control:
98.9956 ms measured against its 99 ms lower acceptance bound for a 100 ms timer.
No peer source or test threshold was changed. Three focused repetitions passed,
then the complete ordinary 70-package rerun passed. These artifacts remain as
`checkpoint-24-all-packages.log`, `checkpoint-24-peer-retry.log` and
`checkpoint-24-all-packages-2.log`; the initial run is not credited as a pass.

Repository analyzers then caught the direct-exit/deferred-cancel path. The local
private-run refinement makes context cleanup finish before exit, preserving
status/messages and shared behavior. Final focused command/skew suites reran,
and repository vet, Staticcheck and lint passed (zero lint issues), retained as
`checkpoint-24-focused-final.log` and `checkpoint-24-*-final.log`. The complete
baseline preceded that command-local refinement; no shared API, package behavior,
config, build or production concurrency changed to invalidate other packages.
The final affected command suite and whole-repository analyzers are current.

Root's fresh direct review checked the final command, actual-main helpers and
all stage-specific failure witnesses against v3. The compact table-driven test
keeps the fault/expected-error pairs together; no hand-written file exceeds the
review threshold. This verification is lead-owned. Support impact is reflected
in the configuration guide, source map and TSR-0073. Existing selection and
write ownership remain authoritative; no ADR change is warranted. Record and
whitespace checks passed; crawler checks, map generation/check and staged-diff
review complete last. External feed health and cross-platform execution remain
outside this local qualification.
