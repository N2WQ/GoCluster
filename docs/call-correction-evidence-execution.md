# Call-correction evidence execution record

Authority: user `Approved v2`, 2026-10-11; see
[approved ledger](call-correction-evidence-scope-ledger-v2.md).
Baseline: `3fbf87ff7ed379f72b3ce1479888a7f918e8f29f`.
Branch: `fix_correction_evidence`.

## Checkpoints

| Patch | State | Commit/evidence |
| --- | --- | --- |
| B0 | Complete | Original-content peer publication; checkpoint below |
| B1.1 | Pending | Receiver-level historical freshness |
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
