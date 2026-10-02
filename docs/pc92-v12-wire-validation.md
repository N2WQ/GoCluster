# PC92 v12 raw-identity correction evidence

Date: 2026-10-01. Authority: [Approved v12, V12-01](pc18-pc92-scope-ledger-v12.md).
Baseline: `2c0607986f3d3d9dc1921eb5b7c5ae00595143d2`.
This is focused development evidence for RA01, not the integrated final lane,
whole-v12 closeout, workload-latency acceptance or overall allocation proof.

## Implementation boundary

`config.IsRawPeeringCall` applies the pinned uppercase ASCII grammar without
trimming, case conversion, slash repair or SSID repair. It shares the existing
bounded grammar expression but does not change `CanonicalPeeringCall` or
private-recipient classification.

`DecodePC92` checks an original origin before stable canonicalization.
`DecodePC92Entry` removes only right ASCII spaces from the call portion before
the raw check. This preserves the receiver's entry-padding behavior inside the
existing printable frame envelope. A raw-valid call must still have a valid,
stable canonical representation. `W1AW/P` and `K1ABC-01/P` do not acquire new
Go authority merely because the raw receiver grammar permits them.

The complete record validates before normal receive, startup staging or
full-mailbox eligibility can change state. `EncodePC92` canonicalizes its local
origin copy and leaves the input unchanged. Accepted transit still uses
`Frame.Encode`, preserving original payload spelling and padding except the
transport hop. Authentication, local publication, PC93 ownership/classification,
numeric metadata and IP behavior are unchanged by this slice.

## Detailed review disposition and coverage

The post-approval review used the approved design and prior discovery. It was
design-aware, not independent normative review. The lead accepted all
checker-only refinements before implementation; no new policy was selected.

| Failure mechanism | Distinguishing evidence and owner |
| --- | --- |
| Repairing malformed origin/subject/member before validity | Config `TestRawPeeringCallV12Grammar`; peer `TestPC92V12RawIdentityRoles`, including all three audit vectors and A/C/D/K roles |
| Applying origin trimming or Unicode/control padding | `TestPC92V12RolePadding`, including standalone entry decoding with metadata after padding |
| Raw validity mistaken for stable authority; portable/SSID regression | `TestPC92V12StableIdentityBoundary`; unchanged `TestCanonicalPeeringCallDXGrammar` |
| Partial membership or implicit-subject validation bypass | `TestPC92V12WholeRecordAtomicity` |
| Invalid receive replaces metadata, authority, ingress or dedupe; external dual-watermark poisoning | `TestPC92V12MalformedNoAuthority` copies seeded node/member/ingress/freshness state, charges and cache counts; checks relay, malformed-key absence and corrected same-timestamp retry |
| Invalid startup changes metadata before rejecting a later member | `TestPC92V12StartupMalformedNoEffect` uses a tracked candidate, all three roles, remote version/build/bitmap, staging backing/counts and a valid positive control |
| Pressure converts malformed input into link closure | `TestPC92V12MailboxRawEligibility`: normal, count-full and byte-full paths, all three roles, positive current-owner refusal controls |
| Own-origin exclusion or stale ownership test becomes unreachable | Corrected raw-valid own-origin alias in `TestPC92MailboxEligibilityMatchesNormalPath`; unchanged `TestPC92MailboxStaleOwnerCannotGateReplacement` |
| Transit silently canonicalizes payload | Corrected raw-valid portable origin and padded member in `TestPC92TransitPreservesPayloadIdentity`; exact forwarded payload assertion |
| Decoder tightening breaks local encoder aliases or mutates input | `TestPC92V12EncoderLocalOrigin`: local aliases, A/C/D/K, implicit subjects, exact canonical authority and copied slice comparisons |
| Shared helper change breaks local service or private safety | Unchanged publication-alias, literal-authentication, PC93 mapping/ambiguity, unrepresentable-private, named-group and telnet current-owner/revision tests |
| Login-only reference oracle falsely certifies raw fields | New harness `raw_callsign` and `decode_pc92_entry` commands call unmodified receiver routines; `TestDXSpiderReferenceRawPC92Identity` also drives actual framed C records |
| Round-trip fuzz accepts an incorrectly repaired identity | `FuzzPC92V12RawIdentity` uses guaranteed-invalid mutations plus valid controls; existing `FuzzDecodePC92Atomic` independently exercises arbitrary wire parsing/encoding |
| Parser-only benchmark misses added decoder cost | `BenchmarkPC92V12Decode` covers small K, complete 62,171-byte C, 8,191-member C, invalid final member and long invalid call; `TestPC92V12DecodeAllocationBound` measures actual decode allocations |

The wire worker owns these additions and the harness; existing telnet checks
were run unchanged. The lead owns integration and final-state validation.

## Observed checks

All executions dot-sourced `D:/codex-gocluster-v12-20261001/env.ps1`. This uses
process-local D: Go cache/temp paths, the pinned WinLibs C compiler, the existing
portable Perl interpreter and the separate restored dependency library. No
global environment, receiver checkout or dependency installation was changed.
Logs and source snapshots are retained under that D: directory.

Normal controls passed: config 0.795s, peer 3.233s, telnet 0.101s.

```text
go test ./config ./peer ./telnet -run 'Test(RawPeeringCallV12|PC92V12|CanonicalPeeringCallDXGrammar|PC92PublicationCanonicalAliasLifecycle|PC92TransitPreservesPayloadIdentity|PC92Mailbox|InboundHandshakeCanonicalIdentity|PC93|CurrentDirectMessage|PC92EntryCountAndWireBounds|PC92ColonFlood)' -count=1 -timeout=120s
```

Focused race passed: peer 4.723s, telnet 1.120s; separate config race 1.114s.

```text
go test -race ./peer ./telnet -run 'Test(PC92V12|PC92Mailbox|PC93|CurrentDirectMessage|InboundHandshakeCanonicalIdentity)' -count=1 -timeout=120s
go test -race ./config -run '^Test(RawPeeringCallV12Grammar|CanonicalPeeringCallDXGrammar)$' -count=1 -timeout=60s
```

The real pinned receiver suite passed in 8.418s. Every selected test ran; none
skipped. It includes canonical identity, raw role behavior, all 1,000 users and
64 peers in the 62,171-byte C, and both Go handshake directions in PC9x and legacy
modes. Prerequisite imports were executed successfully beforehand.

```text
go test ./peer -run '^TestDXSpiderReference(RawPC92Identity|CanonicalIdentity|GoSessionStartup|Complete62171ByteSnapshot)$' -count=1 -v -timeout=120s
```

The receiver remains pinned to
`3e9b3621d94dd45c68702e4a0f896aac33f2a91d`. Raw grammar/decode results are kept
separate from login normalization and final receiver routing. In particular,
its malformed-member C can advance `lastid` and become empty; that behavior is
not Go's selected whole-record atomicity oracle.

Both fuzz runs passed with two workers: raw-identity 186,977 executions and
whole-decoder 186,938 executions.

```text
go test ./peer -run '^$' -fuzz '^FuzzPC92V12RawIdentity$' -fuzztime=30s -parallel=2
go test ./peer -run '^$' -fuzz '^FuzzDecodePC92Atomic$' -fuzztime=30s -parallel=2
```

Three checker corrections are retained explicitly:

- The first receiver test reused a canonical member before sending its portable
  spelling. The receiver's raw C difference calculation and later normalization
  can add then delete that route. Each raw-acceptance case now starts with an
  explicit empty C. No Go policy was changed to reproduce this separate receiver
  reconciliation behavior. The final combined reference run passed.
- Fuzzing found that noise `:` could move appended `!` into metadata. Arbitrary
  numeric metadata normalization is outside this identity contract. Forbidden
  punctuation is now prepended before noise. The minimized input is retained at
  `peer/testdata/fuzz/FuzzPC92V12RawIdentity/aecd40b1a5293684`; its regression and
  the corrected 30-second run passed.
- The first race run applied a production allocation ceiling to instrumented
  `sync.Pool` discard behavior. The allocation checker now uses the existing
  `graphScratchRaceInstrumented` distinction, preserving semantic checks under
  race and measuring the ceiling in normal builds. Corrected race passed. No
  production allocation limit was relaxed.

## Decoder measurements and their limits

The benchmark ran sequentially in an exclusive intensive-work window on
windows/amd64, Intel Core i9-10900, default 20 logical execution slots. The same
benchmark body/fixtures were used for both runs. An external Go overlay selected
the saved baseline versions of `config/peering_contract.go`,
`peer/pc92_codec.go`, and `peer/pc92_codec_entry.go`; the shared workspace was
never reverted. This is a comparison of the wire implementation versions, not
a checkout-wide final-source workload qualification.

```text
go test -overlay D:/codex-gocluster-v12-20261001/wire-baseline/overlay.json ./peer -run '^$' -bench '^BenchmarkPC92V12Decode$' -benchmem -count=5 -benchtime=500ms
go test ./peer -run '^$' -bench '^BenchmarkPC92V12Decode$' -benchmem -count=5 -benchtime=500ms
go test ./config -run '^TestRawPeeringCallV12Grammar$' -bench '^BenchmarkRawPeeringCallV12$' -benchmem -benchtime=500ms -count=1
go test ./peer -run '^TestPC92V12DecodeAllocationBound$' -count=1 -v -timeout=60s
```

| Decoder fixture | Baseline median | v12 median | Baseline/v12 median allocations per operation |
| --- | ---: | ---: | ---: |
| Small K | 1.341 us | 1.847 us | 5 / 5 |
| Complete 62,171-byte C | 1.058 ms | 1.376 ms | 2,263 / 2,263 |
| 8,191-member C | 3.855 ms | 5.942 ms | 8,197 / 8,197 |
| Invalid final member | 4.294 ms | 5.898 ms | 8,200 / 8,200 |
| Long invalid call | 74.385 us | 73.747 us | 8 / 8 |

The raw predicate alone measured 286.4ns/op, 0B/op and 0allocs/op. Required raw
validation adds CPU cost; the long-invalid difference does not establish a
speedup. Normal decoder-only allocation measurements ranged from 5,848 to
829,536 bytes for these fixtures, below the shared 5MiB mutation allowance.
That check does not prove combined decode/plan/commit scratch or retained memory.
It does not replace the complete scheduler service-cost gate, the 2P runtime
profile, long qualification, or the still-open overall allocation proof.

Evidence logs: `wire-controls.log`, `wire-race-corrected.log`,
`wire-config-race.log`, `wire-reference-final.log`,
`wire-fuzz-raw-corrected.log`, `wire-fuzz-atomic.log`, `wire-allocation.log`,
`wire-benchmark-baseline.log`, `wire-benchmark-after.log`, and
`wire-raw-predicate-benchmark.log`. Earlier failed checker logs remain retained.
