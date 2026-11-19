# Call Correction v2: v8 Validation

Authority: exact `Approved v8`, [scope ledger](call-correction-v2-scope-ledger-v8.md).
Workspace: `C:/src/gocluster-call-correctionv2`, branch `call-correctionv2`.
Implementation began at `21c763e5`; the native study executable is frozen from
`cc225f08d5a64e78de98555e425a4d4fae8f4af0`. Later operator/result documentation
does not change that executable's recorded source identity.

Status: all four approved implementation/evaluation units completed. The full
code lane passed, but both development methods failed the unchanged objective
and reference guard. [Exact results](call-correction-v2-v8-results.md). C remains
default-off and B replay-only. Production activation and qualification are not
complete.

## Source To Approved Scope

| Unit | Implementation | Distinguishing evidence |
| --- | --- | --- |
| 1 shared C | `frequency_proposals.go` retains v1 admission/order; `selective_frequency.go` adds owned declines and early unusable-competitor checks | 162 ordinary and 162 race cases across flow/spot; useful rescues, raw bounds, credibility weights, retained dissent, aliases, directed Morse, distance caps, same-generation rows and concurrent immutable reads |
| 2 live C/config/replay | `resolver_correction_outcome.go`, `selective_rescue.go`, output stages and temporal pending; strict optional boolean; v2 replay/plan/evaluation extension | 160 targeted passes across config/cluster/replay, including genuine hold/overflow distinction, no-B license rejection, ordinary-only scheduled retry, full distance-three/Bayes-comparator gates, metadata/grid refresh and store-specific learning |
| 3 experimental B | `receiver_compatibility*.go`; single selected replay gate candidate and post-selection temporal intersection | 175 targeted ordinary/race passes across flow/replay, literal weighted/raw and own-SNR vectors, all opposing identities, bounds, unavailable arithmetic, deadband, canonical joins, owned concurrent results, one-trial/no-fallback consumers |
| 4 final disposition | Native full lane, exact original controls, bounded C/B development captures/evaluator, contracts/support/decision docs and generated maps | Full ordinary/race/static passed; all eight captures and native paired evaluation completed; both empirical objectives/guards failed; maps generated and checked fresh |

Implementation checkpoints: `4da375f2` shared C, `d1b04242` live C/versioned
replay, `bb17e08c` B, and `cc225f08` wrapper-lint documentation/test-style
refinement. Ordinary outcome extraction preserves the old executed branches;
the new optional path cannot derive authority from a confidence glyph.

## Observed Checks

- Native full ordinary lane: **15,152 passed, 99 skipped, 76 packages**
  (61 executed and 15 build-only). No test-result caching.
- `go vet ./...`, `staticcheck ./...`, and
  `golangci-lint run ./... --config=.golangci.yaml`: passed.
- Native full race lane: **15,152 passed, 99 skipped, 76 packages**
  (61 executed and 15 build-only). B/C targeted race selection also passed
  175 cases without skips.
- Bounded method-wire fuzzing: 2,776 executions, PASS; both version seeds and
  exact structural bounds are included. This is finite fuzz evidence.
- Preparation script syntax/selfcheck passed: original 16/68 inventory and
  explicit v2 2/8 inventory, 19 refusal checks and publication-failure controls.
- YAML headers: 20 files passed. The added boolean-comment warning was reviewed:
  it describes an optional additional-correction side effect and shipped-off
  policy. The final YAML documentation edit preserves HEAD's parsed tokens.
- Changed support-critical crawler entries: 13 files passed; this is a
  mechanical entry check, supplemented by direct source review.
- `go run ./cmd/codemap generate -all` and `go run ./cmd/codemap check -all`:
  passed. The maps were refreshed again after final documentation, as the final
  repository write in validation; final diff whitespace checks passed.

Toolchain observed: Go 1.27.1 windows/amd64; Staticcheck 2026.2.1 (0.8.1);
golangci-lint 2.14.0 built with Go 1.27.0.

## Local Benchmarks

Native Windows/amd64, Intel i9-10900, one-second guarded workloads:

| Workload | Time/op | Bytes/op | Allocs/op |
| --- | ---: | ---: | ---: |
| C typical | 9,102 ns | 6,456 | 49 |
| C 5,120 raw incidences | 6,802,473 ns | 4,044,577 | 1,584 |
| B typical | 12,102 ns | 7,104 | 69 |
| B 5,120 incidences / 432 scores | 13,035,954 ns | 6,707,514 | 25,120 |
| Existing unchanged ordinary consumer | 810 ns | 0 | 0 |
| Existing missing-snapshot consumer | 472 ns | 0 | 0 |
| Existing ordinary stage plus bounded observation/drain | 2,490 ns | 1 | 0 |

These measured shapes are not universal worst-case latency, process memory,
production p99 or resource qualification. No production optimization campaign
was performed. The inherited failed resource gate remains deferred and unwaived.

## Review And Failure Disposition

Separate non-steered scientific/design reviews preceded the scope. The later
formula/checker review saw the proposed formula and independently recalculated
its arithmetic; it is not called blind RF evidence. Separate read-only source
reviews covered replay/preparer, live/config and B/consumer code and found no
remaining material defect. The lead verified findings and owns final claims.
The documentation review's bounds/SNR wording refinements were adopted.
The separate artifact review recounted all 1,340,516 unique ordered focal IDs,
applied/stable counts, changed-outcome unions and reference partitions, and
verified the paired/applied output hashes against COMPLETE. It confirmed both
failed objectives/guards and identified C's lost stable baseline receipt and
two ordinary-route changes outside active cells. Its corrections to unknown-
reason partition wording and causal-attribution limits were adopted. Those
artifacts do not independently establish RF truth or the precise history/state
cause of every final divergence.
The fresh lead-owned final pass compared the actual source/diff with all four
approved units, the native command results and final claims. It found no
remaining material source-completeness defect. This pass is not independent
review and does not turn the failed empirical method checks into a pass. No Go
source changed after the clean native study freeze; subsequent edits are
documentation, generated maps and a YAML comment preserving parsed values.

Preserved failed evidence distinguishes defects from setup problems:

- The first raw-basis fixture retained weighted metadata. Rebuilding the valid
  all-zero raw shape fixed that fixture without relaxing runtime admission.
- A replay competitor control published twice at one clock; publishing the full
  population once established its intended current-evidence precondition.
- The initial live freshness fixture did not assert current rival publication.
  The retained diagnostic showed Drain consuming ten receipts while six old
  rows remained published; a later Sweep published both rival rows and C
  correctly declined. The exact original timing failure was not reproduced.
  The final test explicitly checks its publication barrier, rival support and
  decline while preserving the same lifecycle generation.
- The retry fixture confused loss-only process-idle retirement with healthy
  observation expiry. Its final controlled observation sweep proves retirement
  before testing a fresh useful rescue on the ordinary-only retry route.
- A metadata test queried rejection reasons for an applied reason; it now uses
  the separate applied-reason accessor. An introduced unused test import was
  removed. No production semantic relaxation was made for these failures.
- Lint identified the intentionally retained wrapper parameters and one test
  switch style. A localized, explained unparam suppression preserves the
  approved wrapper signature; the test switch was simplified.
- One `RequireCases` invocation used an incomplete identity. The corrected
  `package::test` invocation ran the named probe; the temporary probe was removed.

## Evidence And Limits

Ignored receipts are under `.tmp/call-correctionv2/`, including
`v8-unit1-*`, `v8-unit2-*`, `v8-unit3-*`, `v8-method-fuzz.log`, and
`v8-final-*`. The native freeze is
`v8-development-exposed-v1/FREEZE.json`, SHA256
`b1a98f91d57e4be73f52ccb465e29bbb38c8e7c8a8a486fc8eb9337239abcf6f`.
All four disabled streams were actually captured and checked against the fixed
original SHA256/count authorities: **2,364,930 rows match byte for byte**.
Active development uses November 7/14 only; November 15/16 are parity controls.

No November 17+ archive contents, names or hashes were opened. The ordinal asset
was not refitted or installed. There is no live B loader. C ships false, and no
deployment, activation, restart, merge or push occurred. Existing reference
floors and production resource requirements remain unchanged. Archive agreement
and stability do not establish RF correctness.

Support-agent docs impact: **Required**; the confidence card and linked live,
configuration, domain and replay contracts were updated. Durable decision:
[ADR-0361](decisions/ADR-0361-selective-current-signal-call-rescue.md). Existing
ADR-0360/TSR-0139 history was inspected; no new TSR is warranted by the fixture
refinements alone. V8 did not alter the shared checkout. The later approved v9
integration preserves main's ADR-0358/0359 and TSR-0137 and assigns the correction
records ADR-0360/0361 and TSR-0139 to resolve the branch identifier collisions.
Frozen v8 executable and artifact identities remain unchanged.
