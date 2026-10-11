# Call-correction evidence execution record

Authority: user `Approved v2`, 2026-10-11; see
[approved ledger](call-correction-evidence-scope-ledger-v2.md).
Baseline: `3fbf87ff7ed379f72b3ce1479888a7f918e8f29f`.
Branch: `fix_correction_evidence`.

## Checkpoints

| Patch | State | Commit/evidence |
| --- | --- | --- |
| B0 | Complete | Original-content peer publication; checkpoint below |
| B1.1 | Complete | Receiver-level historical freshness; checkpoint below |
| B1.2 | Pending | Consumer-specific requirements |
| B1.3 | Pending | Independent family qualification |
| B1.4 | Pending | Capacity validation |
| B1.5 | Pending | Future static membership |
| B2.1 | Pending | Numerical/assets |
| B2.2 | Pending | SNR presence/domain |
| B3.1 | Pending | Cap-loss authority/retirement |
| B3.2 | Pending | Diagnostic meaning |

Implementation and validation advance sequentially. Full lanes use existing
stable Windows binaries; connected CustomSCP stages share a fresh real database.
No push/merge/deployment or external live-cluster tests are authorized here.

## B0 design and evidence obligations

Peer native transit already owns its original frame fields. Local publication
needs an owned compact source record captured before local output preparation,
propagated through temporal/FT/stabilizer owners and never recaptured from an
altered spot. Toxicity's separate normalized classifier comment does not replace
the accepted Spot comment. No independent retained state owner is added.

The falsifiable oracle is local delivered B and peer delivered original A with
the complete representable input payload, together with local ring/archive B.
Exact comments and original numeric frequency/IP, variant refusals, delayed
ownership, forwarding gates and deterministic absence barriers are required.
Support-agent docs impact: Required (peer publication and refusal contract).

Post-approval test-strategy review disposition: seven checker refinements
accepted before production edits. Keep independent original/local oracles and
valid modern inputs; prove native writer/reader admission plus independent byte
vectors; use destination FIFO or synchronous queue barriers for local refusal;
exercise production constructors and actual releases; retain native forced-
mutation proof alongside actual correction TCP integration; never equate the
publication return value with receipt; replace old inferred/override-comment
expectations. No product-policy/scope gap was found. The peer worker owns format,
validity, refusal, native-reader and fuzz tests; the lead owns pipeline capture,
all delayed transfer tests, TCP correction and storage observations. Existing
owner retirement/race/allocation checks cover compact record ownership.

| Contract/failure | Stimulus | Observable/checker | Owner |
| --- | --- | --- | --- |
| Capture aliases corrected Spot | Real A-to-B correction and averaging | Local TCP/ring/archive B, exact original peer tuple; correction counter | cluster integration |
| Native relay reads local fields | Existing dedupe-lock forced mutation | Original native writer/reader tuple | peer isolation |
| Toxicity resume loses fields | Blocking classifier | One resumed exact source publication; no early output | cluster integration/race |
| Temporal/stabilizer/FT transfer omitted | Production hold, retry, overflow, drain | Original per-input tuples; final counts and empty owners | cluster integration/race |
| Missing provenance falls back | Final B with no source record | Local B possible, peer queue empty | cluster consumer |
| Comment/IP/frequency rewriting | Independent byte/numeric vectors, per-variant limits | Exact admissible native payload or refusal | peer unit/native/fuzz |
| Refusal/gate falsely passes | Full queue, invalid input, FIFO valid barrier | Exact complete publication sequence/count | peer/cluster |
| Record retained after retirement | Fill caps, retry/pop/Stop, concurrent work | Coupled pending/backing references; immutable record | cluster ownership/race/benchmark |

Baseline red, before production edits: `scripts/test-windows.ps1 -Packages
./internal/cluster -Run '^TestB0OriginalPeerDeliveredAfterLocalCorrection$'
-Timeout 60s` compiled and ran the canonical cluster binary, exit 1. The test
positively observed one call and one frequency correction, local TCP/ring and
reopened archive K1ABC/14020.22. Actual peer receipt was K1ABC/14020.2 with trimmed
comment, versus independent original K1ABD/14020.12 and `  CW\t~CQ~  `.
This is a behavioral reproduction, not a compilation failure.

### B0 implementation and checkpoint evidence

- `peer_publication.go` captures one private immutable accepted-source tuple.
  `output_pipeline.go` captures before local preparation and restores the saved
  tuple on temporal reentry. Temporal/FT/stabilizer constructors, retry and final
  delivery carry it; missing provenance cannot publish a corrected fallback.
- `peer/format.go` validates the exact selected PC11/PC61 variant using existing
  admission rules, preserves comment/IP and numeric frequency, and reuses the
  bounded native sentence encoder. Native transit and existing source, age,
  forwarding, MED, suppression and queue gates remain intact.
- `PublishDX` retains its existing policy-admission return meaning. Neither that
  bool nor pipeline output counters establish destination receipt. Connected
  tests independently observe the actual recipient.
- Source-record lifetime is coupled to existing bounded owners, without a new
  map, queue, worker or persistence field. Temporal `Drain` deletes pending;
  FT `Drain` removes groups and clears scratch references; stabilizer heap pop
  and shutdown clear unreleased work. Already released envelopes remain owned
  by the existing pipeline consumer. This is inspected ownership plus local
  test evidence, not a long-running production leak assessment.

Fresh Go quality review reused its earlier planning context and was not an
independent scientific review. It found no production correctness defect; two
checker gaps were fixed and rereviewed: real final MED/family denial must remain
silent on the actual peer FIFO, and empty/caret comment refusal must preserve
local TCP/ring/reopened archive delivery. Existing direct delivery fixtures now
explicitly supply original provenance and independent peer identities; valid
positive fixtures supply representable comments/IP. Confidence oracles remain
local and unchanged.

| Check | Command/scope | Observed result |
| --- | --- | --- |
| Targeted delivered correction, attribution, ambiguity, temporal/overflow and FT expiry | `scripts/test-windows.ps1 -Packages @('./peer','./internal/cluster') -Run 'TestB0OriginalPeerDeliveredAfterLocalCorrection\|TestPeerOriginal\|TestLocalPeerPublication\|FuzzLocalPeerPublicationRoundTrip\|TestCallCorrectionMethodDeliveredAccounting\|TestCallCorrectionMethodDeliveredAmbiguity\|TestCallCorrectionTemporalDelivered\|TestCallCorrectionDelivered\|TestP4FTExpiryDelivered\|TestRuntimeTemporal.*Delivered' -Timeout 180s` | Initial fixture failures repaired; held-path rerun passed; final complete suite includes all cases |
| Full ordinary | `scripts/test-windows.ps1 -Timeout 300s` | Exit 0; `Windows tests complete: 70 packages; prepare-only=False; race=False` |
| Vet | `go vet ./...` | Exit 0 |
| Staticcheck | `staticcheck ./...` | Exit 0 |
| Lint | `golangci-lint run ./... --config=.golangci.yaml` | Exit 0, `0 issues` |
| Full race | `scripts/test-windows.ps1 -Race -Timeout 600s` | Exit 0; `Windows tests complete: 70 packages; prepare-only=False; race=True`; no race reports |
| Connected CustomSCP | `scripts/test-customscp-regression.ps1`, then `scripts/test-customscp-regression.ps1 -Race` | Both exit 0; producer/reopen/actual delivered-output handoff passes, including missing-history hold control |
| Instrumented protocol fuzz | `go test -c -fuzz=^FuzzLocalPeerPublicationRoundTrip$ -o .tmp/windows-tests/dxcluster/peer/package.test.exe dxcluster/peer`, then saved binary with `-test.run=^$ -test.fuzz=^FuzzLocalPeerPublicationRoundTrip$ -test.fuzztime=20s -test.fuzzcachedir=C:\src\gocluster\.tmp\fuzz\b0 -test.parallel=2 -test.paniconexit0 -test.timeout=60s` | Exit 0; coverage-guided 36,131 executions, 46 new interesting inputs |
| Allocations | Saved ordinary cluster binary, `-test.run=^$ -test.bench=BenchmarkOriginalPeerPublication\|BenchmarkOutputPipelinePrepareSpotContext\|BenchmarkOutputPipelineEmitSpotOwnership\|BenchmarkStabilizerPreStartAdmission -test.benchmem -test.benchtime=200ms -test.count=1 -test.paniconexit0 -test.timeout=60s` | Exit 0; metrics below |
| Support comments | `scripts/check-go-crawler-entry-comments.ps1 -ChangedOnly -FailOnMissing` | Exit 0; changed support-critical entry comments pass |
| Support bundle/routes | `scripts/check-support-agent.ps1` | Exit 0 |
| Support evaluation | `scripts/evaluate-support-agent.ps1` | Broader eval exit 1: unrelated SA-027 stale telnet README line range; affected peering SA-014 retrieval checks pass; all answer scoring skipped |

Benchmarks on native Windows/amd64, i9-10900, 200ms: capture 52.82 ns/op,
128 B/op, 1 alloc/op (120-byte record value); synchronous adapter 29.75 ns/op,
0 B/op, 0 alloc/op. Existing context preparation is 52.94 ns/op, 0 allocations;
sealed local ownership 427.6 ns/op, 896 B/op, 1 allocation; stabilizer pre-Start
admission 110.7 ns/op, 112 B/op, 1 allocation. These are bounded local cost
observations, without a before/after throughput or production-capacity claim.

The first direct fuzz invocation used an ordinary test build and explicitly
warned that coverage guidance was absent. It was superseded by the instrumented
build/run above, retaining the same executable path and exclusive runner lock.
All test execution was serial from the existing saved paths; no firewall rules,
new listening executable paths, deployment or external live feeds were used.

The unrelated support evaluation requests telnet README lines 85-199 while its
required DXCC text is now at lines 246-278. README, catalog and eval runner are
unchanged by B0. It is recorded as an outstanding broad-eval fixture defect,
not repaired in this approved peer patch and not reported as a passing eval.
ADR-0284 records original-content policy and partial ADR-0148 supersession;
domain/peer/support documentation and generated runtime code map were updated.

Final selected Go lanes passed with Go 1.27.1, Staticcheck 2026.2.1 and
golangci-lint 2.14.0. Maps were regenerated and `go run ./cmd/codemap check -all`
passed. Fresh lead-owned review verified the final diff against B0, immutable
value ownership and actual peer/local oracles; the broader support-eval fixture
failure remains explicitly separate. Final whitespace/staged checks precede the
local B0 commit. No wider deployment, probability calibration or production
performance claim follows from this checkpoint.

## B1.1 design and pre-implementation review

Current query construction prunes by retention, counts all retained receivers
and cells, and scores only the newest timestamp. The public-API baseline
regression compiled and failed behaviorally: 55/54-day observations plus one
fresh observation returned 3 qualified receivers at minimum score 5, rather
than 1; all 3 were retained. Stable-path command:
`scripts/test-windows.ps1 -Packages @('./spot') -Run
'^TestB11CustomSCPIndividualFreshness$' -Timeout 60s`, exit 1;
`.tmp/b11-baseline-red.log` contains the actual mismatch.

Separate non-steered scientific review occurred before implementation. The
reviewer received only a neutral question and observational source locations,
not the selected design, ledger or expected implementation results. Its
conditional set-cardinality derivation and literal inclusive tier boundaries
agree with the user's selected individually age-eligible policy. It also
identified the distinction between retained history, current-event agreement
and calibrated correctness, none of which are interchangeable. The existing
nonpositive ingestion-time fallback, compact uint32 timestamp saturation,
stored-second precision and process-clock admission remain unchanged. Future
exclusion is evaluated on stored seconds; no subsecond validity claim is made.

Post-approval test-strategy review was planning-context evidence, not scientific
independence. All refinements are accepted: differing core/floor score minima,
eligible-cell counting, literal boundary vectors, byte comparison before
decoding/reopen, deterministic delivered FIFO barriers, static-miss floor
controls, newest-observation/future quarantine and fully normalized concurrent
readers. Existing retention pruning remains allowed; eligibility filtering
alone must not delete rows inside the retained horizon.

One private bounded scan will select receivers whose own score meets the
calling rail's existing minimum and whose stored timestamp is not future.
Cells and latest/score will describe that same eligible population. Existing
lock/prune/index ownership and the normalized no-cache path stay intact; no new
retained state or filtered-row slice is introduced. Default <=64 receiver cell
scratch remains stack-based, targeting 0 query allocations; larger configured
caps retain the existing bounded scratch-map fallback. Baseline old snapshot
benchmark measured 0 allocations at 1/4/8/16/32 rows and approximately
297/313/351/443/722 ns; its old future-seeded fixture is corrected for the new
contract before final measurement. Microbenchmarks do not establish production
capacity; any production performance acceptance still requires live profiles.

Family selection, consumer coupling, capacity validation and static future
eligibility remain their separately approved later patches.

The independently seeded delivery baseline also failed behaviorally before the
production edit. `scripts/test-windows.ps1 -Packages @('./internal/cluster')
-Run 'TestCustomSCPFreshnessDelivered' -Timeout 120s`, exit 1, recorded in
`.tmp/b11-delivery-baseline-red.log`: stale history bypassed the expected hold
(`pending=0`, `ring=1`), delivered K1HST/S ahead of a positively received K1BAR/P
FIFO barrier on normal/DIAG TCP and original-content PC61, and added the same
extra K1HST/S row to ring/reopened archive. Observation-only static-miss history
also delivered S rather than the required ?. Fresh positive controls passed.
After the bounded query change, the same two delivery tests and controls passed
unchanged (`.tmp/b11-delivery-targeted.log`, exit 0).

The initial full ordinary run stopped at an older cleanup fixture that refreshed
at `now+1 minute` but queried support at the earlier `now`. Its raw bytes were
preserved; the lookup now correctly excludes that future row. The fixture's
recovery query was moved to the refreshed stored time, keeping its deletion
interleaving and raw-byte oracles unchanged. Targeted cleanup/freshness checks
then passed (`.tmp/b11-recovery-targeted.log`). A complete ordinary rerun is
required before this checkpoint is marked complete.

The old cleanup counterexample runner's producer-only overlay also compiles
historical source against current package tests. Private lookup signature and
new helper usage require matching historical store/benchmark test helpers and
excluding the new freshness tests only from that defective producer build.
Read-only compatibility review verified the necessary historical dependencies.
This is maintenance of the existing baseline-producer/current-consumer oracle,
without changing selected test names, ordinary/race paths, scheduling-only hook,
storage schema or consumer delivery expectations. Offline checks must distinguish
the exact historical sources, reject unavailable sources before build and prove
the ten-path overlay boundary and unchanged current consumer.

### B1.1 implementation and checkpoint evidence

`spot/custom_scp_store.go` passes the existing core or floor minimum into its
private snapshot path. `eligibleCustomSCPSnapshot` filters each receiver before
counting its receiver/cell/latest/score, preserving the same retained-row scan,
mutex, horizon prune, expiry and interner ownership. No new retained object,
public interface, record format or admission rule is introduced.

Fresh Go quality review found no material production defect and verified the
deferred boundaries. That review followed the separate scientific review and
was allowed implementation context; it is not described as independent science.
Its private-copy question was resolved by the preserved archive regression:
archive flush explicitly clones before normalization, while RecordSpot only
reads shared Spot fields. Fully normalized sealed inputs are positively admitted
with real initialized H3 mappings before concurrent readers/recorders/queries and
cleanup run; cell gates were not weakened to make the fixture pass.

| Approved obligation | Implementation/validation |
| --- | --- |
| Individual freshness and matching cells | Eligible snapshot; `TestB11CustomSCPIndividualFreshness`, `TestB11CustomSCPEligibleCells`, core/floor rail minima |
| Inclusive tiers and stored-second future exclusion | Literal b-1/b/b+1 age vectors, between-tier minima, same-second precision, future newest quarantine/catch-up and older-arrival refusal |
| Retention/bytes/reopen | Byte checks before lookup/decode, retained coupling, independent reopened counts, normalized concurrent readers and preserved archive private-copy tests |
| Actual delivered consequences | `TestCustomSCPFreshnessDeliveredStabilizer` and `TestCustomSCPFreshnessDeliveredSFloor`; independent raw seeds, normal/DIAG TCP, original peer FIFO, ring/reopened archive and fresh/static controls |
| Bounded cost | Corrected past/current benchmark fixture plus 64/128-row mixed and overflow cases; default stack scratch |
| Connected counterexample remains usable | Producer-only historical private-test overlay; exact ten-path/six-empty offline boundary, unavailable-source stops, actual old failure and repaired ordinary/race handoff |

| Check | Command/scope | Observed result |
| --- | --- | --- |
| Final targeted unit/cleanup | `scripts/test-windows.ps1 -Packages @('./spot') -Run 'TestB11\|TestCustomSCPCleanupCandidateRefresh' -Timeout 180s` | Exit 0; corrected recovery clock |
| Delivered freshness | `scripts/test-windows.ps1 -Packages @('./internal/cluster') -Run 'TestCustomSCPFreshnessDelivered' -Timeout 120s` | Exit 0; same cases failed behaviorally on baseline |
| Full ordinary | `scripts/test-windows.ps1 -Timeout 300s` | Final rerun exit 0, 70 packages; initial cleanup fixture failure recorded above |
| Vet / Staticcheck / lint | `go vet ./...`; `staticcheck ./...`; `golangci-lint run ./... --config=.golangci.yaml` | Each final invocation exit 0; lint 0 issues |
| Full race | `scripts/test-windows.ps1 -Race -Timeout 600s` | Exit 0, 70 packages; zero `WARNING: DATA RACE` reports |
| Repaired connected handoff | `scripts/test-customscp-regression.ps1`, then `-Race` | Both exit 0; actual DB/reopen/TCP handoff and missing-history hold controls |
| Defective connected baseline | `scripts/test-customscp-regression.ps1 -Baseline` | Producer builds/runs, persists refresh, then loses exact observation/static values; reopen support 0; current consumer fails positive delivery, exit 1 as required |
| Runner offline fixtures | `scripts/test-test-customscp-regression.ps1` | Exit 0; 16 checks, no Go commands executed |
| Support comments / routes | Changed-only crawler entry checker; `scripts/check-support-agent.ps1` | Exit 0 |
| Affected support eval | `scripts/evaluate-support-agent.ps1 -CaseId @('SA-006')` | Exit 0, retrieval-only; no answer scoring claimed |

Logs: `.tmp/b11-full-final.log`, `b11-race.log`, `b11-vet.log`,
`b11-staticcheck.log`, `b11-lint.log`, `b11-runner-offline.log`,
`b11-cleanup-baseline.log`, `b11-customscp.log`, `b11-customscp-race.log`.
The first baseline runner attempt failed compilation before any saved executable
ran (`e16d6bf861694aedb50fb0124625b252`); this was tool compatibility failure,
not storage-loss evidence. The corrected actual defective handoff
`648a5f8fcda54bf6be04952ae4410b0f` records raw values present before cleanup,
absent after cleanup/reopen, and consumer delivery failure. Repaired handoffs
`2896a5b79921482cb1e2ad96fa858ad1` and `f560d3af6e0944cf9b748d7eae8e0883`
pass ordinary and race respectively.

Ordinary local benchmarks, 200ms, i9-10900 Windows/amd64: healthy 1/4/8/16/32/64
receiver lookups approximately 314/336/376/475/816/1955 ns, all 0 B/0 allocs.
Mixed 64 retained/32 eligible is 1009 ns and 128 retained/64 eligible 2442 ns,
both 0 B/0 allocs. Configured 128 eligible distinct cells uses the existing bounded
overflow path: 8712 ns, 2040 B, 6 allocations. The old benchmark's future inputs
prevent strict before/after equivalence; no production p99/capacity claim is made.
Private eligibility/unchanged encoding adds no parser surface needing new fuzzing.

Support documentation impact is required and addressed in the confidence card,
spot README and ADR-0285; runner docs reflect matching historical helpers.
Go 1.27.1 Windows/amd64, Staticcheck 2026.2.1 (0.8.1) and golangci-lint 2.14.0
were rechecked. Full Go source validation is unchanged by the script-only fix;
its offline/connected lanes ran on the final scripts. Final maps/check and
lead-owned diff review precede the local commit. No firewall rules, listening
executable paths, deployment or external live feeds changed.

Final lead-owned verification inspected the approved boundary, production scan,
all touched test/doc/script diffs and their independence/ownership claims. Maps
were regenerated and `go run ./cmd/codemap check -all` passed; the support checker
passed again after generation. Staged whitespace review passes. This establishes
the selected historical accounting repair, not field accuracy or episode
association. The next patch may begin after the local B1.1 checkpoint commit.

## B1.2 - Consumer-specific history requirements (complete)

Current-source discovery identified shared score/cell maxima in runtime and
replay construction, and a loader rewrite that raises the common receiver
minimum from the stabilizer minimum. The latter also reaches resolver validation
and the independent FT recent floor. Replay omitted the configured static horizon.
CLI gopls references/implementations for RecentSupportStore confirmed the two
implementations and runtime/replay/correctionflow/spot callers; current rg/source
reads verified the consumer edges. The connector's stale line locations were
not used as current source evidence.

Design/test review disposition: use explicit scalar score/cell query requirements
on the single canonical store, preserving raw and normalized query paths. Avoid
interface views with extra lifecycle/stat forwarding. Recording, stats, cleanup,
checkpoint/integrity and shutdown retain the existing canonical pointer.
Receiver counts remain partial evidence even below a consumer's receiver minimum:
the Bayesian prior and diagnostics consume those counts. Erasing a 3-receiver
subject below a minimum of 4 would change a 4:3 prior to 4:0. Enforce receiver
minima at qualification gates instead. Resolver/stabilizer base minima are the
maximum of their own custom minimum and the unraised common minimum. Plus1 and
edit-neighbor rails additionally apply their own receiver thresholds. Dedicated
custom S-floor thresholds remain separate; FT retains the unraised common minimum.
Preserve the existing resolver-local plus1 loader normalization, including its
default ordering; remove only the cross-consumer stabilizer rewrite.

The postapproval falsifiability review requires unequal score, cells and receivers
as separate dimensions, reversed query order, literal partial-count diagnostics,
actual config.Load, runtime/replay construction, canonical recording/ownership,
and actual delivered stabilizer/floor results with positive FIFO barriers.
No additional scientific claim or product-policy choice is introduced. The
earlier non-steered freshness oracle applies; this engineering review is not a
new independent scientific assessment. Capacity coercion/family selection/future
static eligibility remain for their subsequent approved patches.

Implementation keeps the RecentSupportStore lifecycle interface unchanged and
adds optional scalar requirement queries to the canonical Custom SCP store.
Runtime and replay default core queries now use resolver requirements; stabilizer
queries pass their own requirements without changing options or ownership.
Replay supplies StaticHorizonDays. The three-line stabilizer-to-common loader
rewrite is removed; resolver-local plus1 normalization remains.

The defective-source overlay at `290674d` fails the final delivery fixture
behaviorally: a permissive stabilizer is wrongly held by stricter core score/cell
settings, while a stricter stabilizer receiver gate is bypassed. Normal/DIAG TCP,
original PC61, ring and reopened archive disagree with the independently selected
consumer outcome. A published 2-versus-1 minority P spot supplies the positive
FIFO barrier without recording new historical V evidence. An actual config.Load
FT fixture separately reproduces stabilizer count 5 changing common minimum 2
and delivered S to ?. The old replay constructor overlay reproduces both stricter
core coupling and omitted nondefault 90-day static-horizon wiring.

Initial delivery probes used an invalid barrier: enabled correction erased a
preassigned P, and a proposed 1-versus-1 barrier was uncertain after resolver
sanitization. Those timeouts/precondition failures are fixture defects, not
delivered behavioral evidence. The corrected final baseline runs have no such
failure. Initial analyzers found the now-unused maxInt helper and an always-two
test parameter; both were removed without changing behavior. The changed-file
crawler checker caught the missing shared-interface entry comment, now added.

Two context-influenced engineering reviewers found no material implementation
defect and verified the consumer boundaries. Their README capacity qualification
finding was corrected; boolean capacity coercion still belongs to B1.4. These
reviews are not independent scientific evidence. Final lead-owned diff inspection
verified scalar query ownership, partial dissent counts, recording/cleanup owner
identity, loaded minima and replay horizon wiring against current source.

Ordinary 200ms benchmarks on the i9-10900 Windows/amd64 host measure the explicit
normalized four/eight-receiver query at 331.9/344.8 ns, both 0 B and 0 allocations.
The complete Custom SCP edit-neighbor EvaluateStabilizerDelay case measures
34,729 ns, 763 B and 143 allocations; the existing legacy-store edit-neighbor
benchmark measures 33,714 ns, 2296 B and 287 allocations. These are different
store fixtures, not matched before/after evidence. The scalar query preserves
its zero-allocation contract; no zero-allocation claim applies to the full
variant scan, and no production capacity or latency improvement is claimed.

| Approved obligation | Implementation / regression evidence |
| --- | --- |
| Separate score/cell/receiver gates | Scalar CustomSCPRequirements queries, BuildCorrectionSettings and stabilizer query routing; B12 unit, flow and actual loaded-config tests |
| One recording/lifecycle owner | Runtime/replay constructor tests, concrete admission, stats, StopCleanup/Close/reopen and connected handoff |
| No query-order mutation or erased partial dissent | Alternating raw/normalized queries, unchanged options/entry owner/row bytes/stats; literal 4:3 Bayesian diagnostics |
| Independent common, floor and FT minima | Loader removes only stabilizer rewrite; actual loaded FT floor remains S under stabilizer counts 2 and 5 |
| Runtime/replay delivered behavior | Independent score/cell/count stabilizer fixtures, positive minority-P FIFO barrier, normal/DIAG TCP, original PC61, ring/reopened archive; replay 90/395-day static horizon controls |
| Bounded lookup cost | Explicit normalized query benchmark is zero-allocation; complete edit-neighbor measurement reported separately |

| Check | Command / scope | Observed result |
| --- | --- | --- |
| Config / flow baseline | Exact prepatch source overlay; separate loaded-config and flow invocations | Exit 1 on consumer coupling and gate outcomes |
| Delivered baseline | Final corrected stabilizer fixture overlay at 290674d; actual loaded FT fixture | Exit 1 on actual delivery; invalid early barrier attempts excluded from proof |
| Replay constructor baseline | Old runner constructor only, with current query APIs | Exit 1 on core coupling and omitted 90-day horizon; not described as full prepatch source |
| Final targeted | `scripts/test-windows.ps1 -Packages @('./spot','./internal/correctionflow','./internal/cluster') -Run '^(TestB12\|TestCustomSCPConsumer)' -Timeout 180s`; cluster/replay targeted invocation | Exit 0 |
| Full ordinary | `scripts/test-windows.ps1 -Timeout 300s` | Exit 0; 70 packages including no-test build checks |
| Full race | `scripts/test-windows.ps1 -Race -Timeout 600s` | Exit 0; 70 packages, zero race reports |
| Analyzers | `go vet ./...`; `staticcheck ./...`; `golangci-lint run ./... --config=.golangci.yaml` | Each final invocation exit 0; lint 0 issues |
| Connected history delivery | `scripts/test-customscp-regression.ps1`, then `-Race` | Both exit 0; real producer DB handoff, reopen and TCP delivery |
| Comments / YAML / support | Changed-only crawler checker, comment-only YAML rigor, support checker, affected SA-006 eval | Exit 0; eval is retrieval-only |

Ordinary completion preceded removal of the unused helper and equivalent test
fixture parameter. The affected final targeted run and complete final race lane
were rebuilt afterward; these changes cannot invalidate unrelated ordinary
behavior. Final analyzers ran after the fixes. No parser/protocol surface was
added, so new fuzzing was not selected.

Logs: `.tmp/b12-full.log`, `b12-race.log`, `b12-final-targeted.log`,
`b12-delivery-replay-targeted.log`, `b12-config-flow-baseline-red.log`,
`b12-flow-baseline-red.log`, `b12-delivery-baseline-final-red.log`,
`b12-ft-baseline-red.log`, `b12-replay-constructor-baseline-red.log`,
`b12-vet.log`, `b12-staticcheck.log`, `b12-lint.log`, `b12-comments.log`,
`b12-yaml.log`, `b12-support.log`, `b12-support-eval.log`,
`b12-spot-bench.log`, `b12-flow-bench.log`, `b12-customscp.log`,
`b12-customscp-race.log`. Repaired handoffs:
`2b88037e06e54dad8f38f32f609aa72a` ordinary and
`ae5b0781e68d4cc5a350762f230bb04e` race.

Support documentation impact is required and addressed in the confidence card,
spot README, comment-only pipeline guidance and ADR-0286. The final maps/check,
staged crawler/whitespace checks and fresh lead-owned closeout review precede
the local checkpoint commit. No firewall policy or listening executable path
changed; no push, merge, deployment or external live-feed test occurred.

## B1.3 - Independent family-member qualification (complete)

Checkpoint base is B1.2 commit `4f40cc0`. Current-source and CLI gopls references
confirm the only production family-floor caller is bootstrap's S-floor fallback;
the exact query remains first and static recognition remains an independent
alternative. The existing pair-versus-slice test has one trivial qualifier and
cannot expose ranking-before-gating. Both APIs currently pick the highest score,
then count, before checking all family requirements. A higher-ranked failure can
therefore conceal an independently qualifying member.

The user's selected any-independently-qualifying-member policy is explicit in
approved v2; no new policy is selected here. Design is a boolean OR of each
member's complete score/cell/receiver qualification, with no pooled or ranked
snapshot. Query every member previously visited, including after an earlier
success, to preserve query-driven horizon pruning. Retain the slice-free key-pair
path, duplicate-key handling, thresholds, current capacity coercion and owner.
No new state, lifecycle, encoding, admission or caller contract is introduced.

Postapproval falsifiability review requires fresh count/cell failures hiding an
older success, tied score/count with differing cells in both orders, missing and
score-ineligible populations, split gate successes and collectively sufficient
but individually failing support. Delivery seeds observation-only bytes with no
static membership, so independent recognition cannot hide the historical defect.
Exact-positive/family-negative and static-positive/history-zero controls preserve
the distinct contracts. Peer output asserts original fields, never a local glyph
or percentage. A targeted two-member benchmark must retain zero allocations.
Delivery family suppression and future static eligibility remain deferred.

Defective B1.2 source fails four unit qualification cases, including order-sensitive
score/count ties, and the static-negative delivered case. Actual normal/DIAG TCP,
ring and independently reopened archive contain `?` instead of required `S` for
W1AW/KH6 despite a qualifying older W1AW member. Original PC61 fidelity, negative
nonpooling, exact precedence and static-only controls remain green. Patched
targeted spot/cluster checks pass. Raw record comparisons run before decoding and
after both query/reopen cycles; no current percentage or accepted correction is
invented by recognition.

The source change is independent boolean qualification instead of snapshot
ranking. Snapshot visitation, mutexes, pruning, lifecycle and retained layout are
unchanged. Full ordinary/all analyzers plus full spot race, targeted family
delivery race and connected ordinary/race were selected; an additional all-package
race run would add no changed concurrency surface. Contextual Go quality review
found no material issue and agreed with this selection; it is not independent
scientific evidence. Final fresh verification remains lead-owned.

The historical regression file exceeds the code-quality review trigger and was
reviewed explicitly. Its family cases share historical byte/retention helpers and
the defective cleanup producer's existing whole-file overlay exclusion. Keeping
the cohesive historical checks there avoids another private-baseline compatibility
and runner patch. Production remains one small store-file change and actual
delivery has its own bounded test file.

Final-state validation passed: all 70 packages in the ordinary lane, vet,
Staticcheck, lint (zero issues), all spot tests under race, targeted actual
family-delivery tests under race, and connected CustomSCP ordinary/race handoffs.
The handoff evidence directories are `bad0d80d117b4e85b36dd4931d46f47d`
and `e740254d376b4cea8f2d8927b7150b9a`. These checks use the same stable
executables and do not require firewall changes. The warmed two-member pair
benchmark reports 2272 ns/op for the later qualifying member and 1923 ns/op for
neither qualifying, both zero bytes/allocations; these are different outcome
fixtures, not a before/after or production-capacity comparison.

Evidence logs in `.tmp`: `b13-unit-baseline-red.log`,
`b13-delivery-baseline-red.log`, `b13-targeted.log`, `b13-full.log`,
`b13-vet.log`, `b13-staticcheck.log`, `b13-lint.log`, `b13-spot-race.log`,
`b13-delivery-race.log`, `b13-customscp.log`, `b13-customscp-race.log`,
`b13-bench.log`, `b13-comments.log`, `b13-support.log` and
`b13-support-eval.log`. Source maps and final staged checks follow. Fresh
lead-owned diff verification confirms complete gates per member, no pooling,
preserved visitation/pruning and exact/static precedence. No scientific claim,
delivery suppressor change, external live-feed test, push or deployment occurred.
