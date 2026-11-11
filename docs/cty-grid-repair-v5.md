# CTY and grid repair execution — approved v5

## Authority and window

- Owner authorization: exact `Approved v5` in this conversation.
- Checkout: `C:\src\gocluster`, branch `cty-to-grid-fix`.
- Starting revision: `28e5c24542e3b97d533ff6486227ebbb282a9690`.
- Start: 2026-11-11 01:37:11 America/New_York (06:37:11 UTC).
- Deadline: 2026-11-11 07:37:11 America/New_York (12:37:11 UTC).
- Reserve final validation from 10:37:11 UTC; start no slice that cannot be
  completed and checked within the remaining implementation window. Finish early
  if complete. At the deadline, checkpoint and pause unfinished work.
- Preserve and never stage the pre-existing `data/bulletins/history.json` edit.
- No elevation, installation, machine security/firewall/power changes, production
  access, reset/import, deployment, merge, push, or release.

## Approved implementation queue

| Slice | Contract | Status |
| --- | --- | --- |
| S0 | Baseline source/tool/test identity and comparable benchmarks | Passed targeted baseline |
| S1 | Exact-only entries remain full-key matches, never prefix fallback; preserve portable precedence and inventory | Targeted ordinary/race/fuzz passed |
| S2 | One CTY conversion negates west-positive longitude once in grid backfill and SHOW PROP; new estimates remain persisted | Targeted ordinary/race passed |
| S3 | Disk supplies grid/provenance, never CTY authority; positive/negative classification cache belongs to its CTY snapshot with bounded identity metadata | Targeted ordinary/race passed |
| S4 | Integrated/disposable backup, empty-start, recovery-isolation, restart/rollback and protected-input rehearsal; operator/support and decision records | Targeted ordinary/race and documentation checks passed |
| S5 | Final ordinary/race/analyzer/fuzz/benchmark/connected-regression/code-map/source review | Passed selected lane; existing skips audited |

The inherited pending merge (`grid_writer.go`) and complete grid retirement
(ADR-0338) are retained and regression-tested, not reimplemented. S1 and S2 are
independently useful; integrated S3/S4 results require their retained dependencies.
Only one implementation slice writes at a time; reviewers remain read-only.

## Engineering and evidence gates

- Follow the current AGENTS.md, code-quality standard, validation runbook and
  final review checklist. Source completeness precedes dependent implementation.
- Fixed real-file expectations and an independent longest-eligible-prefix oracle
  must expose the wrong CTY convention and exact-only fallback. Include actual
  country filtering and license-jurisdiction consumers.
- Retain raw PrefixInfo coordinates, the generic geographic converter,
  persisted encoding, reported-locator paths, portable/beacon normalization,
  queue limits, drops, timeout and grid shutdown ownership.
- Classification snapshot identity cannot reference a whole historical database
  or use an unbounded registry. Contradictory disk CTY fields must lose to active
  positive and negative results in both grid lookup paths.
- Force same-pending-batch persistence and barrier-controlled old/new snapshot
  interleavings. Verify durable results after actual store close/reopen.
- Compare matched baseline/candidate cold and warm measurements, allocations and
  bounds. No production performance or process-memory-ceiling claim follows.
- Rehearsal must reopen the backup and verify literal reported/derived rows,
  prove zero legacy rows, and exercise actual recovery selection. Protected-input
  evidence must force changed-content publication, prove permission-specific
  replacement denial and unchanged checksum, then verify startup consumption.
- After targeted ordinary/race gates, checkpoint a passing slice. Diagnose and
  repair failures only within scope. Defer a blocked slice and dependents;
  continue independent approved work. Restore only demonstrably owned unfinished
  edits to a recorded checkpoint. Never weaken assertions or waive a required
  check. Full-lane failure leaves overall qualification incomplete.
- Final state requires full native ordinary/race, vet, Staticcheck, lint,
  relevant connected CustomSCP ordinary/race, matching fuzz and performance
  evidence, affected docs/ADR/TSR/index checks, generated maps and direct review.

## Boundaries and review disposition

New CTY estimates are saved in Pebble with derived provenance and reported grids
win. There is no on-demand-only change. Persisted estimates can remain stale
after CTY refresh; clearing the combined cache can temporarily hide a pending
reported grid. Those freshness changes, portable policy, location-quality
learning, generic poles/antimeridian handling and all legacy-row imports remain
outside this scope. Locator length alone does not prove report authenticity.

The first stop of a live legacy build is not qualified by a new build's stop
tests. Actual production stop, backup and cutover evidence remains separate.
Existing overload drops and storage-error behavior remain; synchronous storage
and callbacks are owned until return, not subject to a new runtime deadline.

Preapproval lead analysis and a separate read-only challenge dispositioned:
already implemented merge/shutdown repairs, dependency-aware partial results,
comparable baseline measurements, contradictory disk oracles, old-snapshot
completion, actual recovery selection, and real replacement-denial evidence.
The earlier coordinate oracle was separately non-steered: supplied format/data,
not the handoff, preferred implementation or implementation-derived answers.
The shipped file is still SHA256
`5d9602e15886a5f16afd6282ad6a220d62d8dcd3e24665dca86e939b2bd34488`.

## Approved item trace

| Behavior | Implementation | Falsifying evidence |
| --- | --- | --- |
| Exact-only keys cannot classify longer calls | `cty/parser.go` trie construction | Shipped/synthetic prefix contracts, independent dictionary-scan fuzz oracle, actual country filtering and license routing |
| CTY longitude converts once; new estimates persist | `cty/maidenhead.go`, cluster `bootstrap.go`, telnet `show_prop.go` | Literal eastern/western/boundary vectors, real synchronous/asynchronous backfill, flush/reopen, cold restart, direct SHOW PROP |
| Disk supplies grids/provenance only | Cluster `call_meta_cache.go` ApplyRecord | Contradictory disk fields over warm/cold active hits/misses, both workers, hydrate/expire/purge/relookup |
| Classification belongs to one bounded snapshot | `cty/snapshot.go`, cluster `call_meta_cache.go` LookupCTY/publication | Concurrent identity initialization, distinct databases, controlled late old publication with/without Clear, bounded churn and warm allocation checks |
| Retain pending merge and complete retirement | Inherited cluster `grid_writer.go` and runtime retirement owners; unchanged here | `TestGridWriterPendingProvenancePersistence` forces one pending batch then reopens; `grid_retirement_test.go` covers admitted hooks, workers, overflow, recovery, maintenance and terminal hooks |
| Prepare separately authorized empty rebuild | `cty_grid_rollout_test.go`, `cty_protected_input_windows_test.go`, operator runbook, ADR-0340, TSR-0126 and support routes | Actual disposable backup/readback, zero-row startup, recovery selection/isolation, cold restart/rollback and changed-content permission denial/restoration |

## Execution evidence

- S0: branch/starting revision and unrelated edit verified. Installed native
  tools: Go 1.27.1, Staticcheck 2026.2.1, golangci-lint 2.14.0 and GCC 16.2.0.
  No machine settings were changed. Windows firewall notifications are enabled;
  a denied operation remains a recorded gap rather than elevation authority.
- Raw logs and frozen benchmark artifacts: `.tmp/cty-grid-v5/`.
- S0 ordinary and race selections each passed 51 cases with zero skips across
  CTY, gridstore, cluster and telnet (`s0-baseline-{ordinary,race}.log`).
  Frozen CTY/cache binaries and five-sample baseline measurements are retained.
  Warm classification-cache positive/negative hits allocate zero; cold churn is
  three allocations and 240 bytes per lookup. The first negative benchmark used `ZZ9ABC`, which is
  actually Brazil in this dataset; its guard failed and the invalid fixture log
  is preserved. Verified miss `QZ9ABC` replaces it; corrected benchmark passes.
- One shell launch transiently failed to find `pwsh.dll`. A read-only fallback
  confirmed the installed files still exist; no provisioning or security change
  was attempted. Subsequent native execution must establish tool availability.
- S1 red evidence: shipped exact-only regressions, synthetic best ordinary
  ancestor, actual country-rule evaluation and real login license routing all
  failed on the inherited implementation (`s1-red*.log`). Trie construction now
  excludes exact-only entries while Data/Keys and full-key matching are intact.
  Selected ordinary/race runs each passed 68 cases, zero skips
  (`s1-{green,race}.log`). The independent dictionary-scan fuzz oracle passed
  1,718,289 executions in 30 seconds (`s1-fuzz.log`). A separate read-only
  source/test reviewer found no material issue; no reviewer test execution is
  claimed. Native PowerShell subsequently ran successfully without provisioning.
- S2 red evidence: both production consumers returned mirrored literal grids,
  including durable `K1ABC -> MN52` (`s2-red-{backfill,showprop}.log`). An initial
  new test double-closed its store during failure cleanup; cleanup was repaired
  and the numeric regression rerun before production mutation. Shared
  `Grid4FromPrefixInfo` now owns sign conversion; mock CTY longitudes represent
  their intended geographic locations. Both modes of real backfill, flush/reopen
  and cold restart preserve derived grids. Selected ordinary/race each passed
  64 cases, zero skips (`s2-{green,race}.log`). Separate source/test review found
  no material issue. The independent non-steered oracle additionally derived
  `VK2ZZZ -> VK2(-34,-151) -> QF56`, with latitude bounds [-34,-33) and adjacent
  cases; no expected value or implementation was supplied to that reviewer.

- S3 red evidence reproduced disk replacement of active hits/misses and literal
  `JJ00` after hydrate/expire/purge/relookup (`s3-red.log`). Runtime hydration now
  supplies grids/provenance only. Positive/negative cache entries carry a stable
  token from the passed database, including late publication after refresh.
  Tokens contain one byte, no database/map/closure/back-reference; the existing
  per-shard LRU owns and evicts entry tokens, with no generation registry.
  Concurrent first use, independently loaded/constructed snapshots, both worker
  paths, contradictory disk fields, positive/negative late publication with and
  without clear, 512-snapshot churn and zero warm allocations are covered.
  Selected ordinary/race each passed 82 cases, zero skips
  (`s3-{green,race}.log`). A separate source/test reviewer found no material issue.
  Initial matched-workload benchmark allocation counts stay zero warm and three
  cold; cold bytes increase from 240 to 256 due to the entry's identity field
  and allocator size class. Timing is provisional until final paired measurements.

- S4 ordinary rehearsal verifies exact reported/derived rows in a copied verified
  backup, zero-row production grid startup, actual recovery rejection of a newer
  external legacy tree, active-only selection, corrected fallback/new report,
  cold restart and legacy storage rollback. An initial missing copy destination
  was a fixture error and was fixed without changing production copy behavior.
  Review identified possible hourly checkpoint interference; final selection now
  occurs after all background owners join and the controlled final checkpoint is
  strictly newer than all retained active candidates. No wall-clock scheduling
  assumption determines the selected path.
- The Windows input rehearsal allows temp-file creation, denies owned destination
  write/delete and parent delete-child, forces changed HTTP 200 content through
  real refresh/removal, verifies ErrPermission and unchanged checksum, loads that
  copy into grid startup, restores/compares original DACL/protection and proves
  subsequent publication succeeds. No machine policy, production permission or
  elevation changed. Initial fixture build typing error is retained separately.
- Support-agent docs impact: Required. Operator procedure, routing/source maps,
  developer index, ADR-0340, TSR-0126 and their indexes cover corrected authority,
  persistence, empty rebuild and unchanged limitations. TSR index checker passed
  after adding its required explicit root-cause bullet; original failure retained.
- Final S4 ordinary/race each passed both rehearsal cases without skips
  (`s4-final-{ordinary,race}.log`). Separate review found only the hourly-candidate
  test ordering issue, now dispositioned above; ACL restoration/resource handling
  and real publication positive/negative controls had no material findings.
  The initial support search fixture failed its common-query response-budget
  assertion after introductory routing text changed corpus region layout. A
  read-only baseline-content override passed, establishing the documentation
  interaction. Moving the focused CTY routes into their own appended sections
  restored all original search assertions without changing worker/checker code
  or weakening any check (`s4-support-search-final.log`).

## S5 final verification

Production/test implementation revision: `5f630a1bc2d9beed31f3f7b7c629f043fb0a9b8a`.
Subsequent closeout changes are this evidence record and generated maps only.
Native tool availability was re-established; every required command below exited
zero without elevation, installation or machine-policy changes.

| Check | Observed result | Evidence under `.tmp/cty-grid-v5/` |
| --- | --- | --- |
| `scripts/test-windows.ps1 -Timeout 10m`, GOMAXPROCS=4 | 12,432 passing cases, 59 executed + 15 build-only packages, 93 skips | `final-native-ordinary.log` |
| Same runner with `-Race` | Same counts; no race reports | `final-native-race.log` |
| `go vet -p4 ./...`, `staticcheck ./...`, checked-in golangci-lint configuration; GOMAXPROCS=4 | All passed; lint reported zero issues | `final-vet.log`, `final-staticcheck.log`, `final-lint.log` |
| `scripts/test-customscp-regression.ps1`, ordinary then `-Race` | Connected producer/consumer, preserved delivery and missing-observation hold passed, exact TCP output read back | `final-connected-customscp-{ordinary,race}.log` |
| `go test ./cty -run '^$' -fuzz '^FuzzCTYPrefixEligibility$' -fuzztime 30s -parallel 2`, GOMAXPROCS=2 | 1,821,109 executions passed | `final-fuzz.log` |
| Frozen baseline/candidate benchmarks | Five paired alternating samples per variant/workload; all workload guards passed | `final-paired-*.txt`, `final-paired-summary.{json,csv}` |
| Troubleshooting records, support-critical comments, support-agent bundle/search | All passed without changing checker/worker assertions | `final-tsr-check.log`, `final-crawler.log`, `final-support-agent.log` |
| `go run ./cmd/codemap generate -all` then `check -all` | Both maps regenerated; runtime map content updated; freshness passed | `final-codemap-*.log` |

The two complete native lanes' 93 skip identities and reasons match exactly:
12 helper entry points, 56 external DXSpider evidence cases, 24 optional
live/profile/controlled-input campaigns, and one existing platform H3 mapping
limitation. Audit records are `ordinary-skip-audit.json` and
`race-skip-audit.json`. No new CTY/grid regression skipped; no check was waived.
These results do not establish the skipped external/live campaigns.

Connected fixture roots are `.tmp/customscp-regression/bdf4f3fe7b9f4fa884f963efa88db14d`
(ordinary) and `.tmp/customscp-regression/74da4b20241d41d3b0b65015ec0c6ad3`
(race). They retain producer/consumer readback evidence, not production data.

### Local performance and resource result

Intel Core i9-10900, native Windows amd64, Go 1.27.1, GOMAXPROCS=2,
300 ms per benchmark sample. Baseline production source is starting `28e5c245`;
candidate is `5f630a1b`. Benchmark scaffolding is unchanged from `d313040c`.
Saved binaries were compiled from those observed source states and reused for
the final comparison. SHA256, Go Build ID and build info are retained in
`benchmark-binary-identities.json` and `*.exe.buildinfo.txt`; Go test binaries
do not embed VCS settings, so revision provenance is the observed build context.
No heavy suite or reviewer ran concurrently with these paired measurements.

| Workload | Baseline median ns/op | Candidate median ns/op | Baseline -> candidate bytes / allocations |
| --- | ---: | ---: | --- |
| CTY K1ABC | 152.20 | 133.20 | 112 / 1 -> 112 / 1 |
| CTY K1DG | 153.30 | 139.90 | 112 / 1 -> 112 / 1 |
| CTY K1DW | 79.48 | 80.70 | 112 / 1 -> 112 / 1 |
| CTY JA1ABC | 157.10 | 157.30 | 112 / 1 -> 112 / 1 |
| CTY N2WQ/VE3 | 291.00 | 261.80 | 256 / 3 -> 256 / 3 |
| Cache warm hit | 33.79 | 33.37 | 0 / 0 -> 0 / 0 |
| Cache warm miss | 30.63 | 30.95 | 0 / 0 -> 0 / 0 |
| Cache cold churn | 651.50 | 673.50 | 240 / 3 -> 256 / 3 |

Warm classification remains allocation-free. Cold churn's measured median cost
increases 22 ns (3.38%) and 16 bytes, with unchanged allocation count. Baseline
cold samples span 635.2–754.1 ns and candidate 664.5–719.0 ns; warm timing ranges
also overlap. These are descriptive local samples, not a statistical speedup,
live p99, sustained throughput or process-memory-ceiling claim. The bounded
identity field/token is the accepted correctness cost. Cardinality/churn and
late-publication tests passed; no optimization beyond approved repairs was added.

### Final review and checkpoints

A fresh separate-context read-only Go quality reviewer inspected all six
production Go files, changed tests/docs, unchanged writer/retirement/recovery
owners, and S4 execution logs. No material final findings remained; that reviewer
ran no tests. The lead verified the actual production diff, selected-lane results,
skip audit, fixed workload allocation/timing evidence and approved-item trace.
The final caller search finds both production grid consumers using the shared
CTY boundary and no remaining direct consumer of the generic converter. The
shipped CTY checksum remains the pinned value above. Final map diff and whitespace
review complete the lead-owned fresh verification; this is not represented as
independent scientific review. The earlier coordinate oracle was separately
non-steered as described above.

Passing implementation checkpoints: `d313040c` (S0), `0eb7217d` (S1),
`6c5435a5` (S2), `15975496` (S3), `5f630a1b` (S4). The evidence/map closeout is
committed after those checkpoints. Raw local logs/binaries remain ignored and
available for inspection; none are deployment artifacts.

## Final result

All approved behavior and required selected checks passed, with existing skips
and residual limits recorded above. New CTY estimates remain persisted as derived;
reported locators retain precedence. The inherited merge and complete-retirement
repairs remain intact. No production store, service, backup, reset, import or
rollout was operated; no merge, push or release occurred. The unrelated bulletin
history edit remains unstaged. Persisted-estimate freshness and last-known-good
CTY publication remain follow-ups under the owner's chosen persistence policy.
