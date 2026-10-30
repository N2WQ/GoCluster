# CC dialect v4 execution record

Authority: [approved scope](cc-dialect-v4-scope-ledger.md), exact conversation
`Approved v4`, baseline `7373e831`. Production data/deployment are excluded.

## Current state

- Approved v4 implementation is complete locally. Full ordinary and race
  acceptance each passed 11,926 cases across 72 packages, with 93 documented
  skips. Production deployment and CC User interoperability were not exercised.
- Discovery and pre-approval design/semantic/resource/scope reviews complete.
- Post-approval detailed falsifiability review is complete: F1-F7, T1-T8 and
  I1-I16 were dispositioned adequate by the lead, with no scope/policy change.
- Support-agent docs impact: Required. Current cards and developer/operator,
  source-map, question and troubleshooting routes now link the native CC/schema7
  contract. ADR-0330 records the durable ownership/reset/persistence decisions;
  no separate production-troubleshooting TSR is warranted.
- Implementation and observed validation are recorded below. Planned checker
  names in the design matrices are obligations, not independent pass claims.

## Detailed first-slice matrix

| Contract | Failure mechanism / stimulus | Observable oracle | Level / planned checker | Owner |
| --- | --- | --- | --- | --- |
| Same starting defaults | Both implementations accidentally deny every spot; custom HUMAN-only defaults | Equality plus literal expected positive/negative source/mode cases with identical controls/context | Unit: TestCCDefaultEquivalence | filter CC model |
| Mode/source selection | Empty selection becomes defaults; one source ignores mode; alias/UNKNOWN handling | Independent set-intersection truth table, including explicit empty deny-all | Unit: TestCCModeSourceTruthTable | filter CC model |
| Geographic conjunction | PASS OR broadens; DX/DE reversed; inequality mishandles unknown | Handwritten known-in/out/missing/invalid matrices; four categories AND, each list OR | Unit: TestCCGeographyTruthTable | filter CC model |
| Band ownership | OFF collapses to absence; identical global edit short-circuits | OFF overrides exactly one band; identical global edit clears only its category overrides; unrelated rows intact | Unit: TestCCGeographyOverrideStateAndReplacement | filter CC model |
| DXBM edit state | Active allow-all collapses into OFF; later edit reseeds base | Ordered first PASS/REJECT, whole-band/slot and reject-A/pass-A/pass-B sequences retain selected transition behavior | Unit: TestCCDXBMTransitions | filter CC model |
| Frequency classification | Expected table generated from production; cached band/observed frequency used | Independently transcribed 37-slot literals; below/at/above boundaries, final upper inclusion, gaps and contradictory cached bands | Unit: TestCCFrequencyPlanGoldenBoundaries | filter CC model |
| Immutability/bounds/performance | Input or returned canonical DTO aliases plan; duplicate-heavy raw input escapes limits; early rejection hides costs | Mutate both DTOs, reprepare changes, raw counts before cloning/sorting, atomic refusals, max all-pass/late-reject and zero warm allocations | Unit/race/benchmark: TestCCPreparedOwnershipAndBudgets, TestCCMatchingWarmAllocations, BenchmarkCCMatchingMaxPlan | filter CC model |

Lead dispositions: pair equivalence with independent expected outcomes; mutate both
input and returned canonical state; do not trust stale attestation; bound raw work
before deduplication. Existing shared state readers remain authoritative: valid
state tokens are not reclassified by a newly invented country gate.

## Integration obligations

The remaining detailed matrix must cover both-bank capture/publication and dirty
state, schema6/7 guards and stale revisions, exact whole-profile/two-reference
admission, protected originals, profile/library partial commits and concurrent
SSIDs, notice flush/acknowledgement interruption and stale ownership, both live
own-call paths, history A-B-A fencing, pause/resume positive delivery, malformed
uploads and reservation lifecycle/mixed queue drains. All material findings must
be dispositioned before the corresponding implementation slice.

The completed detailed review assigned these falsifiers (new checker names are
planned until their implementation is recorded):

| ID / contract | Stimulus and false-green risk | Required observable result / checker | Owner / level |
| --- | --- | --- | --- |
| T1 schema framing | Old/new exact limits, one byte over, header/body mismatch, LF/CRLF/IAC and command-looking tail | Frozen6 64 KiB; explicit7 256 KiB; unsafe tails terminal; TestYAMLSchemaVersionedFraming and existing receiver transport tests | telnet unit/TCP/fuzz |
| T2 input lifetime | Hold four decoder permits; exhaust 32 body leases; cancel receivers/waiters | Reservation acquired before reception and retained until body/AST release; budgets/permits converge; TestYAMLV7InputReservationLifecycle | telnet consumer/race |
| T3 output admission | Eight blocked responses, ninth GET, same-client second GET, invalid capture trap | Refuse before clone/render, one owner through flush; TestYAMLV7OutputAdmissionBeforeBuild | telnet consumer/TCP/race |
| T4 close fence | Producer holds reservation while close/final drain completes; queue full | No orphan enqueue; release once; TestYAMLV7ReservedEnqueueCloseFence | telnet consumer/race |
| T5 writer composition | Mixed bulletin/config owners; blocked write; write/flush failure; shutdown/drain | Both budgets release once, whole frames intact, enlarged batch drops before release; TestWriterMixedReservationLifecycle | telnet writer/TCP/race |
| T6 PATCH ownership | Identical global row, empty bands map, OFF/INHERIT, contradictory targets, empty lists | Presence semantics and next-command state exact; atomic refusal; TestV7PatchPresenceAndCategoryOwnership | telnet coordinator/fuzz |
| T7 legacy compatibility | Every6 resource/verb under GO/CC/DEFAULT, large native CC, result-switch attempts | Approved access matrix; no fake CC Boolean status or inactive overwrite; TestV6DialectCompatibilityMatrix | telnet coordinator/TCP |
| T8 preference revision | Old6 token, CC edit, GO-CC-GO, concurrent6/7 writes, metadata-only updates | One winner; stale refusals; metadata alone does not advance preference token; TestV7WholeProfileRevisionAcrossSchemas | telnet concurrency/race |
| I1 own-bank capture | Distinct banks and opposite controls, edits/switches/capture/SAVE/reconnect | Merge by program kind only, exact inactive state, stable Filter identity; TestV7ActiveCaptureOwnBank | telnet consumer/race |
| I2 dirty banks | Late GO save failure, CC switch/edit, return to GO, later shared save | Both unsaved banks retained and warning truthful; later complete save persists both; TestV7UnsavedBanksAcrossSwitch | telnet fault/TCP |
| I3 atomic publication | Concurrent alternating distinguishable plans, controls and geometry; retained old snapshots | Every read is complete old/new state; no alias; TestV7ConcurrentBankPublication | telnet consumer/race |
| I4 resource/preset boundaries | Wrong FILTER tag, inactive fields, mixed filter/selector PATCH, full CONFIG restore in CC, tagged LOAD | Exact approved ownership, readonly refs/notices refused; TestV7RestoreAndActiveEditOwnership and TestV7PresetBankOwnership | telnet coordinator/TCP |
| I5 commit ordering | Block/fail commit while matching/readback runs; overlarge record vs late IO failure | Machine/LOAD old state until commit; human admission before publication, late IO retained; TestV7CommitPublicationBoundaries | telnet fault/race |
| I6 replacement | 96 queued old-owner edits, blocked persistence/pacing, duplicate login and shutdown | Retired mutation/save/ack cannot overwrite successor; duplicate notice preserved; TestV7ReplacementBacklogOwnership | telnet TCP/race |
| I7 complete admission | Real canonical/wrapped maxima, two baselines plus metadata, alias/node/work bombs, operator defaults | Exact serializer/whole-document preflight; invalid combinations refused before activation; TestV7WholeProfileAdmissionAndDefaults | filter/telnet unit/fuzz |
| I8 literal wipe | Literal6 now-invalid expressions, unique old rules/baseline, nondefault controls/settings | Old fields discarded without compilation; both new defaults and six DEFAULT; exact originals backed up; no later reset; TestV7ProfileResetLiteralFixtures | filter/telnet unit/TCP |
| I9 shared reset | Library commit then profile failure, other SSID SAVE, retry/new SSID, mixed pre7/7 | Marker/reset one replacement; old gone; new preserved; TestV7LibraryResetRetryPreservesNewPresets | filter/telnet concurrency/TCP |
| I10 operation faults | Each file stage read/prepare/backup/write/sync/close/replace, initial vs wrapped ENOENT | File-specific partial outcomes, protected cleanup, safe retries; TestV7ResetFailureBoundaryMatrix | filter/telnet fault/TCP |
| I11 preset outcomes | Two refs, library success/association failure, failed LOAD, later overwrite/delete, crossSSID | Both prior refs preserved on failure; tagged single-bank LOAD; immutable baseline; TestV7PresetDualBaselineTransactions | telnet consumer/fault/TCP |
| I12 notice opportunity | Reset commit then missing/partial output, failed flush, PAUSE epoch changes | Flush-only receipt independent of PAUSE; later historical notice without another wipe; TestV7ResetNoticeFlushReceipt | telnet writer/TCP/fault |
| I13 notice ownership | Intervening edit/restore, newer library event, retirement, missing/bad/pre7 fresh profile, failed ack | Current-owner metadata-only captured acknowledgement; no factory/reset/revision/history mutation; TestV7ResetNoticeAcknowledgementOwnership | telnet fault/race/TCP |
| I14 actual delivery | Positive/negative human/skimmer after RESUME with fresh dedupe keys; both own-call paths and controls | Actual positive line before negative interpretation; retained exceptions/TOXIC; no replay; TestV7NativeDeliveryAndResume | telnet consumer/native+ziutek TCP |
| I15 history fencing | Block pending page, A-B-A, active edit, inactive-only restore, irrelevant controls | Stale page fenced; inactive-only edit preserves active cursor; explicit own-call selectors retained; TestV7HistoryBankActivationAndInactiveEdits | telnet history/race |
| I16 command ownership | Advertised commands and real HELP routing, taxonomy collisions, invalid tails, unsupported families, overlong list | Correct bank owner, atomic refusals, pause exception only for recognized readbacks, no autosplit; TestV7NativeCommandRoutingAndHelp | commands/telnet fixtures/TCP/fuzz |

Lead dispositions preserve independent expected outcomes and positive-delivery
evidence. Old tests asserting shared CC Boolean editing, preserved old controls,
old preset retention, or dialect-as-presentation history behavior must change to
the approved contract; GO-only assertions remain protected.

## Implementation handoff state

- `CurrentConfigurationVersion` is now 7 after coherent login, banked commands,
  storage, presets, writer and notice paths joined. Activation is local source
  only; no deployment or live data operation has occurred.
- Native CC model is in new `filter/cc*.go`; root added common snapshot bridging.
- Frozen v6 codecs/renderers now use an explicit v6 constant, independent of
  future activation. Historical headers/receiver limits remain unchanged.
- Production login and all current banked save/preset/machine paths use explicit
  v7 DTO APIs. The historical one-bank `UserRecord`/`SavedPreset` facades remain
  frozen at v6. Gopls reference checks of LoadUserRecord/PrepareUserRecord and
  call-path inspection confirmed no current banked production route needs an
  implicit projection through those carriers. Native CC serialization through
  a Boolean carrier is refused. This avoids dropping inactive state and retains
  independent v6 codec/capacity/failure oracles.
- Root owns existing telnet server, capture/publication, legacy adapter, history,
  human persistence, notice integration and public activation.
- Worker-owned native terminal/HELP, v7 machine handlers and notice modules join
  against root provider methods. Only one stable-runner window runs at a time.

## Validation evidence

| Command / scope | Observed result | Evidence boundary |
| --- | --- | --- |
| `pwsh -NoProfile -File scripts/test-windows.ps1 -Packages ./filter -Run '^TestCC' -Timeout 180s` | Exit 0; 87 passed cases; zero skips | Pure native model, independent boundary goldens and warm zero-allocation guards; no activation/storage integration claim |
| `pwsh -NoProfile -File scripts/test-windows.ps1 -Packages ./telnet -Run '^(TestYAML\|TestV[67]MachineHeader\|TestConfiguration(InputBudget\|Output))' -Timeout 180s` | Exit 0; 104 passed cases; zero skips | Receiver/header/budget helpers and legacy selected regressions; actual integrated writer/dispatch still pending |
| Stable Windows `./filter -Run '^TestV7'` ordinary and race | Both exit 0; 17 passed cases each, zero skips | Both-bank codecs, literal wipe, fault/retry, independent library marker/reset and new preset preservation; not a full runtime lane |
| Stable Windows `./commands` full; `./telnet -Run '^TestNativeCC'` | Exit 0; 416 and 15 passed cases respectively, zero skips | Initial HELP/native grammar/readback checks before later integration edits; final lane must cover the joined state |
| Stable Windows full `./filter`, after activation/frozen-facade correction | Exit 0; 825 passed cases, zero skips; `.tmp/cc-v4-filter-frozen-facades.log` | Full filter regression lane on that source state; race/fuzz/profile and final whole-repository lane remain separate |
| Stable Windows full `./telnet`, first activation run | Exit 1; 2,195 PASS / 202 FAIL / 3 SKIP events including subtests; `.tmp/cc-v4-telnet-activation.log` | Discovery run, not acceptance. Failures included coupled v6/current fixtures and four manually dequeued reserved responses without fixture release. Actual native and ziutek CC TCP mode/source/pause/switch/reconnect journeys passed. New capacity checks and subsequent corrections require the next run. |

The initial joined run also found an actual TOXIC shortcut dispatch gap: native
recognition delegated to an older parser that lacked SET/TOXIC and SET/NOTOXIC.
Native parsing now owns exact SET/NO/UNSET switch recognition and delegates only
the accepted switch mutation to the common control owner. The actual delivery
test and all-six shortcut matrix cover this correction. A nil retired-channel
notice fixture was repaired; production retirement ownership was unchanged.

The four-MiB reservation contamination was traced to four existing settings
tests that consumed `.raw` from a banked control message without running the
writer or releasing the transferred reservation. Fixture cleanup now follows
that explicit ownership. Production reservations still release only after
flush/discard; strict budget convergence assertions were retained.

## Fresh review and final evidence work

A separate fresh context performed the read-only Go code-quality review against
the approved scope and current source. It identified missing active preparation
charges in notice acknowledgement/final-save paths, the resulting writer-reporting
dependency, missing mixed-owner/failure-boundary evidence, and stale support routes.
The lead accepted these findings. Active work now acquires the shared preparation
budget; final teardown waits for mandatory writer cleanup without holding a charge.
Writer cleanup/drain/signalling precedes diagnostic logging and optional reporting.
Both machine and human enqueue reject preparation-only ownership. No new worker
or higher resource ceiling was introduced.

The lead's final pass also found that LIST and DELETE PRESET could decode a
complete library before acquiring a preparation owner. All four preset verbs
now reserve before storage access. A malformed-library trap under full process
budget proves refusal before capture, decoding, backup or replacement, with
unchanged profile/library bytes and converged ownership. This stays within the
approved process bounds; it does not raise limits or add a queue/worker.

The post-review targeted command selected `./filter,./telnet` with the reset
boundary, storage-fuzz seeds, preparation/mixed-writer/final-save, notice and
activation expressions. It exited 0 with 68 passing cases and zero skips in
`.tmp/cc-v4-review-fixes-targeted.log`. The 23 reset cases inject returned operation
faults through existing local seams, including actual partial write/sync/close/
replace helpers, then retry from another SSID, create FRESH and prove exact
backups, atomic library marker/deletion, no OLD resurrection and preserved current
edits/references. Directory-create/chmod/open faults and process/power-loss crashes
were not independently injected. Actual notice receipt/ownership uses the separate
telnet tests; the filter-only notice step is an explicitly simulated metadata save.

Mixed owner tests verify both bulletin and configuration charges converge after
flush, write failure, shutdown and final closed-queue drain. A deterministic
blocked-reporter test proves a final save completes after mandatory writer drain
without waiting for the optional callback. Exhaustion tests prove acknowledgement
and final save refuse before capture/load/write. These are local evidence, not
production latency or crash-durability measurements.

The machine capacity checks observed native CC 131,061 bytes with next-operand
refusal, complete CONFIG 196,608 bytes with +1 refusal, a two-reference profile
459,751 bytes and a widest complete CRLF frame 261,775 bytes. The widest fixture
includes both 32-byte association names, optional unsaved status, a 32-byte ID
and escaped 128-byte revision. Plain and escaped final frames have independent
encoder exact/+1 oracles at 64 and 256 KiB. All six native switch fields have
literal false/DEFAULT wiring checks. These synthetic storage-envelope limits do
not imply that every independent maximum or arbitrary settings value is a legal
terminal/machine preference.

The repository Python qualification scripts still target schema four and old CC
semantics. A baseline `git show 7373e831` comparison confirmed this drift already
existed when the server required schema six. No old cumulative profile is claimed
qualified, and no flat legacy state is fabricated from current banks. Current
native/ziutek TCP and strict Go protocol fixtures supply the release's actual
command/delivery/persistence evidence; the old Python harness needs a separate
explicit semantic port rather than a version-number substitution.

The additional `TestV7MachineRestoreOwnershipPresetsAndReconnectTCP` uses the
existing native/ziutek loopback fixture, not a new runner. It proves full schema7
restore of both banks while CC is active, native per-band/DXBM roundtrip, tagged
FILTER refusal/ownership, honest schema6 restrictions, tagged preset switching,
immediate version7 disk state/both references and reconnect equality. Its first
fixture correctly refused an unavailable MED preference; enabling the fixture's
three test policies makes its intended legal restore explicit. The corrected
cross-package selection exited 0 with 33 passing cases, zero skips in
`.tmp/cc-v4-crosspackage-targeted-2.log`.

The first broad run used an insufficient 180-second package limit and timed out
inside a native diagnostic fixture. Three apparent delivery failures occurred
earlier in fixture startup: they awaited the old `Self spots` acknowledgement
rather than the current explanatory `Own-call spots` wording, before submitting
the control spot. Five expected strings were corrected across the three relevant
test files. The corrected learning/delivery parents and native supervisor admission
all passed in the 33-case run. No test or matching assertion was removed. Current
process inspection found no remaining matching child; authenticated retirement
at that earlier package timeout itself was not recorded. The final broad lane
uses the normal ten-minute per-package allowance.

## Approved-item traceability

| Approved boundary | Implementation owners | Distinct evidence |
| --- | --- | --- |
| Native categories/defaults/frequency and immutable matching | `filter/cc_*.go`, `native_cc_runtime.go`, `match_snapshot.go`, `profile_v7.go` | Literal frequency endpoints/gaps, mode/source/unknown-state matrices, both defaults, maximum all-pass/late-reject allocation guards |
| Independent ownership, controls, revisions and inactive edits | `telnet/configuration_v7*.go`, publication/staging, banked schema6 and machine7 coordinators | `TestV7ActiveCaptureOwnBank`, unsaved banks across switches, concurrent plan/control/location capture, all six YAML switch pairs, stale tokens and inactive-state preservation |
| Profile/library wipe, protected originals and interruption retry | `filter/upgrade_v7*.go`, strict codecs and atomic file helpers | Literal old 0-6 TCP fixtures, 23 returned-operation failure/retry cases, concurrent SSIDs/FRESH survival, exact backups and marker/deletion images |
| Tagged presets and two associations | `filter/preset_v7*.go`, `telnet/preset_commands_v7.go` | Single-bank LOAD/switch, partial SAVE and retained immutable references, real TCP immediate storage/reconnect checks |
| Schema7/full restore and frozen6 compatibility | `machine_v7*.go`, `machine_v6_banked*.go`, receiver/header owners | Header/body mismatch/unsafe-tail cases, exact/+1 frames, active tag refusal, full both-bank restore in CC, honest CC schema6 views and native/ziutek TCP |
| Output/input/preparation budgets and teardown | `configuration_transfer.go`, common writer, composed final drain | Exhaustion before reception/capture, decode-permit hold, mixed owner flush/failure/shutdown, closed current owner, nonqueue authority and blocked-reporter final-save progress |
| Reset notice opportunity and metadata acknowledgement | `reset_notice_v7*.go`, login hooks and writer receipt | Successful-flush-only receipt, independent pause epochs, strict fresh reload/captured-event ownership, failed/retired/missing profile acknowledgement, no repeated reset |
| Live/history and familiar terminal UX | `native_cc_commands*.go`, readback/HELP, shared match/history snapshots | Actual positive socket witnesses before negatives in both transports, both own-call paths/TOXIC, no replay after RESUME, bank A-B-A and inactive-only cursor preservation |
| Complete admission, docs and performance boundaries | Full profile/two-reference codecs, readback admission, ADR-0330 and support routes | Exact resource/capacity cases, independent encoder limits, fuzz properties, local maximum matching benchmarks/profiles, final selected ordinary/race/static lanes |

## Fuzz and performance evidence

Stable-path instrumented test binaries were compiled with `go test -c -fuzz`
before timed campaigns; no cached test result is used as execution evidence.
Machine schema7: 93,834 executions/15 seconds with two workers; native terminal
ownership: 23,925/15 seconds; strict storage owners: 102,573/20 seconds with
per-input minimization limited to 100 attempts. All exited 0. Initial storage
campaigns spent their short window minimizing coverage inputs; their 9-10
executions are not presented as broad mutation evidence. Logs are
`.tmp/cc-v4-fuzz-machine.log`, `cc-v4-fuzz-native.log` and
`cc-v4-fuzz-storage-bounded-minimize.log`. These are bounded local campaigns,
not exhaustive parser proofs.

The ordinary filter binary ran `BenchmarkCCMatching` with `-benchmem`, three
one-second repetitions and CPU/allocation profiles. Maximum all-pass measured
317.5-429.5 ns/op; last-gate rejection 299.1-327.5 ns/op, each at zero B/op and
zero allocations/op. CPU samples locate work in bounded native matching, sorted
lookups and shared taxonomy normalization. `alloc_space` includes preparation/YAML
setup and is not retained heap or matching churn. There is no prior native CC
implementation for an equivalent before/after comparison, and no live-node p99
or production load improvement is claimed. Evidence:
`.tmp/cc-v4-matching-bench.log`, `cc-v4-matching-cpu-top.log`,
`cc-v4-matching-mem-top.log` and their `.pprof` files.

Worker runs are targeted development evidence, not the final ordinary/race lanes.
Record actual commands, scope, results and necessary excerpts as work proceeds;
do not replace a missing required check with a pass.

## Final verification

The fresh lead pass inspected the final storage/reset, current-owner notice,
both-bank capture/publication, preset, schema-six/seven ownership, history and
writer/resource changes against v4. It corrected the preset preparation gap
described above and two stale schema-six-only recovery references. No deployment,
production-data operation, service restart, commit or push was performed.

On the final Go source, `go vet ./...`, `staticcheck ./...` and
`golangci-lint run ./... --config=.golangci.yaml` all exited 0; lint reported
zero issues. Logs: `.tmp/cc-v4-vet-final.log`,
`.tmp/cc-v4-staticcheck-final.log`, `.tmp/cc-v4-lint-final.log`.
The final support-agent checker and changed-only crawler-entry checker also
exited 0; the latter scanned 26 tracked support-critical Go files. New production
files' role/ownership headers were inspected directly. `git diff --check` was
clean. Logs: `.tmp/cc-v4-support-check-final.log` and
`.tmp/cc-v4-crawler-check-final.log`.

The first complete ordinary lane exited 0: 56 executed packages, 16 build-only,
11,921 passing cases and 93 skips across 72 packages. Its log is
`.tmp/cc-v4-ordinary-final.log`. This preceded the final localized preset guard;
the repeated ordinary and final race lanes below qualify the frozen source.

The first full race attempt stopped at `internal/cluster` after
`TestFamilyDeliveryRunningPipelineImmediateAndHeld` received K1HOLD/P before its
barrier. No data race was reported. A separate read-only context traced the
unchanged resolver/pipeline: refreshing a tied reporter uses current time and
can turn a hysteresis-dependent Uncertain state into valid Probable at 50%,
which the default 25% low-P gate allows through. The 0.88-second failure cannot
be the configured two-second timer. An isolated race run and twenty repetitions
of the original fixture passed, so they do not establish a reliable oracle.

Only that fixture changed: its low-P threshold is now explicitly 51%, budget one,
and a lexically later competing seed keeps the initial winner Probable at exact
one-of-two support. The real resolver/scheduler, pending/held ownership, initial
archive/ring exclusion, final three-of-four 75% TCP receipt and release checks
remain. Production correction policy/configuration is unchanged. The pre-expiry
barrier still has its existing two-second host-scheduling assumption; no claim of
timing-free proof is made. The corrected fixture and all four preset-budget
refusals passed a two-package targeted race selection: six cases, zero skips,
`.tmp/cc-v4-final-fixes-race.log`. Final complete acceptance lanes follow this fix.
The separate reviewer inspected the final fixture diff and confirmed that its
initial state checks strengthened while all ownership, exclusion, receipt,
storage and exact 75% assertions remained; no production correction file changed.

The 93 ordinary skips comprise 56 external DXSpider observations without the
required external installation, 12 subprocess-only helpers, one unavailable
Windows H3 mapping and 24 opt-in live/data/profile/soak/controller qualifications.
They do not establish native CC/CC User interoperability, live-node performance,
power-loss durability or production readiness. The live CC test was not enabled;
CC User/per-band terminal integration remains the approved unverified follow-up.

After both final corrections, `scripts/test-windows.ps1 -Timeout 10m` exited 0
on the frozen source: 56 executed packages, 16 build-only, 11,926 passing cases,
93 skips across 72 packages. Log: `.tmp/cc-v4-ordinary-acceptance.log`. This is the
complete ordinary acceptance lane, including the strengthened running-pipeline
fixture and all four preset-budget refusal cases. The skip categories above are
unchanged.

`scripts/test-windows.ps1 -Race -Timeout 10m` then exited 0 on the same frozen
source: 56 executed packages, 16 build-only, 11,926 passing cases and 93 skips
across 72 packages, with no race report. Log:
`.tmp/cc-v4-race-acceptance.log`. The final ordinary and race lanes include the
corrected fixture and both native/ziutek configuration/preset/reconnect journeys.

The final source/diff review found no unresolved implementation defect in the
reviewed v4 scope. Remaining evidence boundaries are the explicitly unverified
CC User/native per-band terminal interface, bounded rather than exhaustive fuzz
campaigns, returned-operation rather than power-loss fault injection, opt-in
qualifications and local rather than production performance measurements.
After this record's final edit, repository maps are regenerated and checked
with `go run ./cmd/codemap generate -all` and `go run ./cmd/codemap check -all`;
command logs are `.tmp/cc-v4-codemap-generate.log` and
`.tmp/cc-v4-codemap-check.log`. The final generated diff and whitespace check
are reviewed before closeout. Work remains uncommitted on `boolean`.
