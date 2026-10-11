# Archive fanout prerequisite execution

Repository: `C:\src\gocluster`; branch: `fix_call_correction`.
Baseline: `fb86e750fcc5101312480c962283a8af966bbea5`.
Exact authorization: user supplied `Approved v2` after the revised ledger.

## Approved v2 delta

The original five correction/confidence patches retain their approved policies,
acceptance requirements and scientific limits. Add one separate prerequisite:
archive insertion must normalize a worker-private `Spot.Clone()` rather than
the immutable snapshot shared with peer, telnet and ring consumers. Preserve
owned queue pointer reuse, exact stored normalization/bytes, schema, batching,
drop policy and shutdown ownership. No scoring, configuration or protocol change.

P1 work was saved with `git stash push --include-untracked` before the archive
prerequisite. All archive checks and its commit run independently of P1. Restore
P1 afterward, finish its full checkpoint, then execute P2-P5 sequentially without
routine approval requests. No push, merge or deployment is authorized.

Required checks: baseline raw/partial/normalized source preservation and stored
bytes, concurrent sealed reads during insertion, normal/DIAG TCP and peer
receipt, ring storage and reopened archive; queue reuse/drop/drain preservation;
copy/batch costs; full native Windows ordinary/race suites, vet, Staticcheck,
golangci-lint, affected Linux ordinary/race and connected CustomSCP checks. Final
quality review, docs/maps and a fully tested local commit precede restored P1.

Read-only design, falsifiability and scope challenge occurred before v2 approval.
They established existing immutable handoff and exact normalization as controlling
contracts. Worker-private cloning was selected because it retains the existing
normalization/encoding path, including populated-cache precedence. Removing
normalization or cloning only at the caller would not cover raw public inputs.

## Evidence and status

The baseline connected `TestArchiveImmutableFanoutDelivered` passed ordinary
execution and its exact delivered fields, then failed native Windows race
execution. The detector reports `Spot.EnsureNormalized` at `spot.go:516`, called
by `Writer.flush`, writing concurrently with `peer.formatPC61`. Production was
unchanged. Logs: `.tmp/call-correction/archive/delivery-baseline-{ordinary,race}.log`.
This confirms a race, not corrupted archive contents or restart data loss.

The production delta is `s = s.Clone()` before normalization in `Writer.flush`.
`SnapshotForAsync` is deliberately not used: it reuses sealed pointers. The
fully normalized concurrent-reader regression catches the equal-value write;
raw and partially normalized inputs alone are insufficient. Literal version-6
bytes are compared before any decoder runs; `decodeRecord` field checks and
graceful reopened rows are separate observations.

Baseline archive regressions failed source-preservation assertions for raw,
partial and populated/whitespace-cache inputs; their persisted byte/field checks
passed. The normalized concurrent-read test independently failed under race.
Logs/binaries: `.tmp/call-correction/archive-prerequisite/`. Fixed targeted archive
and connected fanout race checks passed both packages.

## Completed checkpoint

Root-observed checks all exited successfully:

- `scripts/test-windows.ps1` and `-Race`: each built 70 packages, executed 53
  test packages and build-checked 17 without tests. Logs `full-ordinary.log` and
  `full-race.log`; no required check waived.
- `go vet ./...`, `staticcheck ./...`, `golangci-lint run ./...
  --config=.golangci.yaml`: exit 0, lint zero issues. After the test-only cleanup
  and byte-order changes, affected archive/cluster analyzers also exited 0.
- WSL Ubuntu-24.04 `go test` ordinary and `-race` for `./archive
  ./internal/cluster ./peer ./telnet ./buffer`, `-count=1 -timeout=180s`: all five
  passed. Final affected archive/connected tests reran ordinary and race on Linux.
- `scripts/test-customscp-regression.ps1`, ordinary and `-Race`: both connected
  producer/consumer runs passed (persisted observation/static membership, reopen
  recovery and exact TCP receipt plus missing-observation control). Run IDs:
  `3ca5d0cb6e7c43c0bc86be79ef5af614` and `04228da6eb604ab7930e83d186783474`.
- Final native ordinary affected rerun: archive and cluster passed
  `TestFlush|TestOwnedQueue|TestArchiveImmutableFanoutDelivered|TestEnqueue`.
  Broad race already compiled the final test state; `final-targeted-ordinary.log`
  and `linux-final-targeted.log` retain the affected reruns.

Fresh lead final verification and separate read-only Go quality review found no
remaining material finding. Review caught an assertion-failure fixture cleanup
gap; its bounded client-retirement wait now runs in cleanup before cwd restore.
No production lifecycle policy was changed. Final map generation/check,
troubleshooting-record checker and diff whitespace check passed.

Approved item mapping: archive private normalization -> `Writer.flush`, literal
byte/source-preservation/concurrent-reader tests; immutable delivered identity ->
connected normal/DIAG TCP, PC61, ring and graceful reopen; queue/drop/Stop bounds ->
exact-pointer/full-queue and five accepted rows across three batches; cost ->
one/64-row benchmarks; decision/support -> ADR-0281, TSR-0080, indexes and map.

This prerequisite is complete and ready for its local commit. P1-P5 resume after
restoring the saved method work; each retains its own required checkpoint.

| Local flush case | Baseline B / allocs per op | Private copy B / allocs per op |
| --- | --- | --- |
| normalized one row | 305 / 4 | 1204 / 5 |
| categorized one row | 346 / 5 | 1279 / 7 |
| normalized 64 rows | 19705 / 257 | 77734 / 321 |

Values are medians of three 150ms repetitions, including Pebble work but excluding
fixture construction. Cloning adds one Spot allocation and about 900 bytes
per row, plus one category-slice copy when present. Timing medians for normalized
one/64 rows were 6.78/120.94 us baseline and 10.21/173.68 us fixed. The fixed run
overlapped validation load, so these are descriptive costs, not a matched latency
or throughput verdict. Logs: baseline `archive-baseline-bench.log` and fixed
`.tmp/call-correction/archive/fixed-bench.log`. No performance win is claimed.

The copy is transient, owned by the existing single insertion worker and used
for one row's encoding. It adds no cache, registry, lock, goroutine or queue.
Measure allocation cost; local benchmarks do not establish sustained production
load. Live profiling remains an operational follow-up beyond this local batch.

Support-agent docs impact: Required. Updated troubleshooting routing; user
commands, glyph meanings and configuration remain unchanged. See
[TSR-0080](troubleshooting/TSR-0080-archive-shared-snapshot-normalization.md) and
[ADR-0281](decisions/ADR-0281-archive-worker-private-normalization.md).

Wider signal association remains deferred: fresh evidence inside an existing key
and recency window does not prove the original transmission episode. This
prerequisite does not alter confidence rules or establish overall call accuracy.
