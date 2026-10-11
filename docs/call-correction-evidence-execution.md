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
