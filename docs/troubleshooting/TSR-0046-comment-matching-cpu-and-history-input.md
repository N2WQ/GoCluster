# TSR-0046 - Comment Matching CPU And History Input

Status: Monitoring
Date Opened: 2026-10-08
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: filter, commands, telnet fanout, peer admission
Trigger Source: Chat request
Led To ADR(s): none
Tags: COMMENT, literal matching, CPU, history, printable ASCII

## RCA Summary

- What happened: Valid repeated-prefix COMMENT rules made each client's spot
  admission expensive. The generic command API also admitted invalid trailing
  phrase whitespace that the history parser rejects.
- Why: The matcher restarted byte comparisons at every candidate position.
  COMMENT preceded cheaper rejection gates, and generic `TrimSpace` removed
  invalid phrase bytes before history parsing.
- What fixed it: Stack-only single-word Shift-And matching, saved COMMENT after
  ordinary filter gates, archive identity before explicit phrase selection,
  and original input passed to the history parser.
- How we know: Independent literal-search oracle, endpoint regression goldens,
  identical before/after benchmarks, guarded actual fanout and CPU profiles,
  and admitted peer-frame tail-match fixtures. Local evidence is described below.
- Operator/support answer: Keep the existing 32-entry-per-list and 64-byte-phrase
  limits. Search covers the entire stored comment. CPU still scales with comment
  bytes, active rules and eligible clients; local results are not production
  latency or release-clearance evidence.

## Triggering Request

- Request date: 2026-10-08.
- Request summary: Review of comment management identified repeated near-match
  CPU cost and trailing tab/nonbreaking-space acceptance through the generic API.
- Request reference: [feature commit d8ee076](https://github.com/N2WQ/GoCluster/commit/d8ee076a269c725bacd0b303bcd43fc8ded017af).

## Symptoms and Impact

- The supplied review measured 32 valid reject phrases (61 A bytes plus three
  digits) against repeated A comments: 0.127 ms at 128 bytes, 2.08 ms at 1,024
  bytes and 128.4 ms at 65,500 bytes per client filter. Those are the reviewer's
  local results, independently reproduced here with different absolute timings.
- Live delivery runs matching once per eligible nonself client while holding
  that client's filter read lock. Unwanted bands/modes formerly paid the scan cost.
- Archive phrase scans formerly preceded exact-call/entity exclusion.
- Generic history input ending in tab/NBSP formerly became valid after trimming.
  Connected telnet history uses its separate raw-input path.

## Timeline

1. 2026-10-08 - Review identified both defects in d8ee076.
2. 2026-10-08 - Read-only source/caller review confirmed the failure mechanisms.
3. 2026-10-08 - Owner approved Scope Ledger v2; regression fixtures were added
   before production changes to capture the old baseline.
4. 2026-10-08 - Matching and work ordering were corrected within ADR-0261.

## Hypotheses and Tests

1. Zero allocations and short easy-mismatch benchmarks established safe CPU cost.
   - Evidence: The older benchmarks omit repeated prefixes and long comments.
   - Outcome: Rejected. Allocation bounds do not establish a work bound.
2. Moving rejection gates alone sufficiently corrects the matcher.
   - Evidence: Otherwise eligible clients still scan every active phrase.
   - Outcome: Rejected. The matcher itself needs predictable work.
3. Trimming generic command edges preserves the phrase contract.
   - Evidence: New generic-endpoint goldens failed before the production fix for
     trailing tab, NBSP, CR/LF, vertical tab and form feed.
   - Outcome: Rejected. Phrase edges trim only ASCII spaces.

## Findings

- Root cause: Nested repeated comparisons and generic normalization before a
  command-specific literal boundary were independent defects.
- The 64-byte phrase cap fits one unsigned match-state word, including bit 63.
  ASCII masks occupy a fixed 1 KiB local table; high bytes reset state instead
  of bridging a literal match. Work per phrase is O(N + M + 128), with no
  retained matcher cache or scratch heap allocation.
- Deferring COMMENT accepts extra ordinary-gate work for early COMMENT
  rejections. That tradeoff must be measured rather than hidden in passing cases.

## Decision Linkage

- ADR created/updated: none. [ADR-0261](../decisions/ADR-0261-literal-comment-filter-and-history.md)
  already governs semantics, limits, exact ownership and no retained cache.
- Decision delta summary: Correct the implementation of that bounded literal
  contract; no schema, persistence, ingestion limit or self-filter policy change.

## Verification and Monitoring

Seven 100 ms benchmark samples on native Windows, Go 1.27.1,
Intel Core i9-10900, default GOMAXPROCS=20, produced these medians. The fixtures
are identical across the old and corrected production implementations.

| Fixture | Old | Corrected | Result |
| --- | ---: | ---: | --- |
| 32 hostile rejects, 1,024-byte synthetic comment | 1.517 ms | 0.0323 ms | 46.9x faster |
| 32 hostile rejects, 65,500-byte synthetic comment | 95.868 ms | 2.560 ms | 37.4x faster |
| Actual fanout, eight eligible clients, 65,500 bytes | 785.685 ms | 16.568 ms | 47.4x faster |
| Existing passing filter, 32 entries in each list | 14.026 us | 10.365 us | 26.1% lower time |
| 32 digit-leading suffix rules, 128-byte comment | 4.308 us | 8.093 us | 1.88x slower |
| First short REJECT match | 16.38 ns | 356.7 ns | Added ordinary-gate work |
| Short PASS miss | 27.41 ns | 381.9 ns | Added ordinary-gate work |

Matcher/normalized-filter fixtures reported 0 B/op and 0 allocs/op. Fanout
continues to allocate one existing delivery envelope per admitted client.
The two early rejection budgets were 447.76 ns and 468.21 ns respectively,
1.25 times their corrected ordinary-only plus isolated-matcher component sums.
Common passing/no-COMMENT fixtures met the 25% regression ceiling.
Fixed table setup costs more for the short digit-leading easy-mismatch case;
the predictable-work correction does not improve every input shape.

Five-second fanout CPU profiles also include calibration calls, counted by the
fixture logs. Normalize cumulative `deliverJob` samples by all completed calls:

| Clients | Old calls / delivered | Old CPU per call | Corrected calls / delivered | Corrected CPU per call |
| --- | ---: | ---: | ---: | ---: |
| 1 | 105 / 105 | 96.57 ms | 3,284 / 3,284 | 1.84 ms |
| 8 | 8 / 64 | 775.00 ms | 482 / 3,856 | 14.69 ms |

Both profiles reduced sampled caller CPU per completed call by about 53x.
The matcher remains the dominant CPU consumer in this intentionally hostile
workload; its percentage alone would conceal the absolute improvement.
Compiler escape output reports nonescaping matcher inputs, and Windows/amd64
disassembly reserves exactly 1,024 bytes of stack for the mask table.

Admitted frame-envelope fixtures retain 65,473/65,463/65,471 comment bytes for
their concrete PC11/PC61/PC26 sentences respectively, including the tail match.
These sizes depend on the fixture's header/suffix, not a new comment limit.

Validation procedure and runtime acceptance are in
[telnet command validation](../telnet-command-validation.md#comment-cpu-and-literal-input-regression).
Before/after logs, operation counts and profiles are retained locally under
`.tmp/comment-v2/`; the reusable fixtures live in the repository's test files.

Local correctness/tool validation passed:

- `go test ./... -count=1 -timeout=180s`, `go vet ./...`,
  `staticcheck ./...` and `golangci-lint run ./... --config=.golangci.yaml`.
- Race checks passed for filter, commands and telnet. The broader peer run first
  failed an unchanged outbound retry timing assertion (98.8519 ms against a
  100 ms minimum with 1 ms tolerance). Five isolated race repeats of that test
  passed, followed by a passing full peer race rerun. This does not establish
  historical flakiness or a retry-code diagnosis.
- Thirty-second, four-worker fuzz runs passed 578,052 matcher executions and
  534,564 history parser/generic-endpoint executions.
- All 22 offline Python harness fixtures passed. The October 7 remote run is
  separate earlier evidence and does not validate COMMENT/schema 4.

The fixes remain under ADR-0261. Support-agent impact is required for this
diagnosis: `customgpt/source-map.md` links this record without changing the
existing command, configuration or phrase contract.

- Signals to monitor: CPU and admission latency per spot under representative
  comment lengths/rule counts/client counts; filter-writer wait, worker/client
  drops and queue backlog under steady and burst load.
- Production acceptance: Replay the same delivered workload and verify CPU per
  admitted spot, latency percentiles, filter-update wait and drop/backlog behavior
  against the deployment's capacity budget. No representative production load or
  live-server regression was run for this correction.
- Rollback/reassessment triggers: Wrong literal results, phrase acceptance
  divergence, delivery losses, heap allocations or a missed performance budget.

## References

- [ADR-0241 peer framing](../decisions/ADR-0241-peer-frame-payload-and-comment-framing.md).
- [TSR-0041 history selection](TSR-0041-exact-call-history-and-scan-cap.md).
- `filter/comment*_test.go`, `commands/comment_history*_test.go`,
  `telnet/comment_benchmark_test.go`, `peer/comment_filter_boundary_test.go`.
