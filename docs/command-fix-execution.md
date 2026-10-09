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

Current progress: checkpoints 00 through 11b complete (04a used two commits);
29 approved checkpoints remain; 12 is next.

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
