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

Current progress: checkpoints 00 through 04d complete (04a used two commits);
38 approved checkpoints remain, beginning with 05a.

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
the same Go/script source digest:
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
used to declare this checkpoint complete.
