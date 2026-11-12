# Cache precedence and geography initialization: approved v6

## Authority and execution identity

The owner approved `Approved v6` and explicitly requested creation and checkout
of `more-stabilizer`. The complete proposed ledger was presented in the
conversation before approval. This record preserves its executable contracts
and subsequent evidence; progress entries do not expand scope.

- Workspace: `C:\src\gocluster`, branch `more-stabilizer`.
- Baseline: `9bcae8e355c71eba6d480841a12ea6acac556dda`.
- Unrelated modification to preserve: `data/bulletins/history.json`.
- Approval observed/start: 2026-11-11 22:01:58 EST (2026-11-12 03:01:58 UTC).
- Implementation cutoff: 2026-11-12 00:31:58 EST (05:31:58 UTC).
- Four-hour window end: 2026-11-12 02:01:58 EST (07:01:58 UTC).
- Final 90 minutes are reserved for review, final validation and handoff.
- Evidence: `.tmp/cache-geography-v6-*`; checkpoints are attributable local
  changes and this record, not commits.
- No commits, merges, pushes, deployment, production rebuild/restart, installs,
  firewall/security changes, credentials changes or persistent scheduler.

## Approved scope and boundaries

1. Strict shared load-only H3 initialization for live startup regardless of
   path prediction, and replay when call correction/CustomSCP is enabled.
   Missing/malformed required geography fails startup through existing cleanup.
   Prediction learning/display/disabled commands stay disabled.
2. Read both explicitly configured tables once per invocation, aggregate
   errors, close handles, and initialize from the captured bytes. Limit reads
   to expected bytes plus one: res1 6,737, res2 47,057. No runtime generation,
   rewriting, directory fallback, new table validation policy or H3 math.
   Validate each invocation outside the existing once-owned mapping lifetime;
   build both locals before publishing. Preserve latched initialization errors,
   file order, one-based IDs, legacy APIs and working-directory path semantics.
3. A private grid-component coordinator orders active lookup selection, cache
   admission, outgoing replies, report writes/coalescing/commit, invalidation
   and store replacement. Register worker lookups before candidate acquisition,
   after dequeue; retain ownership through publication/reply/cleanup. One
   selection owns grid, precision, provenance and success. No IO, callbacks or
   blocking admission under coordinator/cache locks.
4. Keep reported-over-derived authority, existing reported/report rules,
   persistent-store retention and cache TTL distinction. Failed/incomplete
   corrected-call lookup cannot bypass storage with direct CTY; old grid clears.
   Delayed release stays nonblocking and can deliver unknown, measured by the
   existing corrected-without-grid counter. Preserve all v5 stabilizer behavior.
5. Distinguish cache invalidation and store replacement epochs. Retired-store
   completions cannot publish into new-store authority; same-store commits
   after CTY clear can establish authority. Never let old commit A clobber newer
   live reported B, even if B's best-effort write is dropped/failed.
6. Temporary compact stored-reported copies, if needed, arbitrate only already
   successful reported reads. Never turn MISS/derived/error into a reported hit
   or pin an unpersisted live update. Release copies when last active lookup
   finishes. Nil-cache and immediately expired cache callers remain supported.
7. Bound active coordination by existing owners: 8,192 queued reports + one
   writer handoff + 500 batch entries + five active workers = 8,698 keys;
   at most five temporary locator copies. Clone normalized compact keys, bound
   copied text by existing input limits including normalization expansion,
   release empty backing, compact during anchored churn, and prevent identity
   wrap/ABA. No new workers, timers, queues or unbounded histories/labels.
   Copied-text bound is 8,698*K + 5*G: K=3*max(1024,effective command-line limit,15),
   G=3*max(65535,effective PSK payload limit,effective command-line limit,1024).
   Shipped limits imply about 26.4 MiB worst-case copied text plus bounded
   bookkeeping, not a process/RSS ceiling.
8. Fixed headless/cache-display diagnostics: active keys, copied text bytes,
   pending reports, superseded candidates. Gauges converge to zero; cumulative
   superseded count follows existing grid-metric lifetime. Cover narrow layouts.
9. Update affected docs, configuration reference wording, ADR/TSR/support routes
   and generated maps. No YAML defaults/schema change. Preserve historical
   run records and unrelated data.

Excluded: full replay/live stabilizer parity, earlier path-learning relocation,
historical grid/history repair, feed-specific provenance, storage retention or
best-effort durability changes, geographic math/table data/generator changes,
production tuning and production accuracy/p99 qualification.

## Ordered queue and gates

- Slice 0: verify baseline/tool/build ownership, capture comparable ordinary
  benchmarks, and finish detailed contract-to-test falsifiability review.
- Slice 1: complete shared geography wiring, fixtures and docs; targeted native
  ordinary/race and WSL CGO=0 ordinary, CGO=1 ordinary/race cold-start checks.
  Review source completeness before continuing.
- Slice 2: complete coordinator/cache/writer/reply/epoch/cleanup/diagnostic unit;
  deterministic failure/lifecycle/churn and performance checks plus prior v5
  regression protection. Do not retain a partial cache repair as complete.
- Slice 3: integrated docs/decision records, fresh code review, complete final
  lane and handoff. Slice 1 may be retained independently if slice 2 is safely
  deferred. All retained changes require final relevant validation.

Failed mandatory gates block dependents. Diagnose in-scope failures, preserve
failed evidence, never weaken tests/skip checks/change acceptance. New material
scope or policy defers affected work; safe independent approved work can
continue. Restore only safely attributable changes; never reset owner work.
No new implementation slice after the cutoff. Report incomplete validation
honestly if the lane cannot finish; OS/IO retirement is not guaranteed to meet
the wall-clock window.

## Acceptance and validation

Freeze concrete checks before implementation. Required evidence includes:

- Real worker reply and cache agree under stale derived/reported results,
  equal-text provenance and full precision. Expected successful lookup cannot
  pass through timeout/fallback.
- Cache clear/LRU/TTL and post-clear lookups/commits; old-store acknowledgements;
  newer live reports including later write failure/drop; independent calls.
- Nil/expired caches, concurrent positive reported reads, pre-purge positive
  versus post-purge MISS/derived, and temporary-copy invalidation.
- Queue overload, unavailable/error store, timeout, coalescing, shutdown and
  every ownership terminal; zero gauges and released backing after drain.
- Long churn with one held owner, maximum-size compact keys and comparable
  retained heap/backing observations. Race stimuli are concurrent.
- Block actual storage and observe nonblocking delayed release; late results
  cannot alter sealed v5 output.
- Fresh-process live/replay startup (no prewarming); distinct/same coarse-cell
  CustomSCP controls; GRID/NEARBY with prediction off and prediction still off.
- Missing/unreadable/truncated/wrong-count/oversized/growing tables; both errors,
  closed handles, no writes/generation; captured bytes usable after removal;
  invalid->valid, valid->invalid and previous sticky-failure sequences.

Benchmark ordinary builds with comparable workloads/five samples; warm grid/CTY
hits and unchanged updates retain zero allocations. Include all cold map and
compaction/pin costs. Reproducible >20% warm median CPU or contended throughput
regression requires diagnosis/reassessment, not budget waiver.

Final lane: full native Windows ordinary + race (stable-path runner), exact
required-case execution, vet, staticcheck, golangci-lint, connected CustomSCP
ordinary + race, targeted WSL CGO=0 ordinary and CGO=1 ordinary + race, generated
maps last write/check, final source/diff/docs review. Fixtures use private
tables/configs/databases and no production data/network downloads.

Read-only design and falsifiability reviewers challenged initial/revised scope.
Lead verified material claims against current source. No independent scientific
validation was performed; existing geography math/table meaning remain intact.
Local measurements cannot establish production p99, memory ceiling or accuracy.

## Execution log

- 03:02 UTC: baseline and owner change verified; created/switched to
  `more-stabilizer`; no pre-existing code edits. Requested detailed post-approval
  falsifiability review before implementation. Tools/build qualification and
  baseline benchmarks pending.
- 03:07 UTC: post-approval detailed adversary review accepted. Proposed exact
  check families: `TestH3StrictReaderBounds`, `TestH3StrictReaderFailures`,
  `TestH3StrictCapturedBytes`, `TestH3StrictInitializationLifecycle`,
  `TestRuntimeGeographyStartup`, `TestRuntimeGeographyConsumersPredictorOff`,
  `TestReplayGeographyStartup`, `TestReplayGeographyConsumers`, and
  `TestGridAuthority*` (late completion, invalidation, pending commit, commit
  winner, epochs, per-call isolation, stored reported, pins, ownership, bounds,
  churn, successful sync reply, diagnostics, warm allocations). Freeze actual
  implementation identities before checks; require exact executed leaf cases.
  Accepted checker refinements: replay globally disabled keeps its existing
  config error; inject portable reader errors rather than skip permissions;
  cold child process and exact cases cannot be replaced by prewarming/parent
  passes; contention uses identical baseline/candidate harness; CGO=0 is
  ordinary only, Linux CGO=1 includes race. No material policy/scope gap found.
  Geography worker released as sole implementation writer.
- Baseline prerequisites passed: native Go1.27.1, Staticcheck2026.2.1,
  golangci-lint2.14.0, WSL Go1.27.1/GCC13; Linux CGO=1 pathreliability selected
  test compiled/executed successfully. Native cluster prepare-only passed;
  59 existing grid/CTY/retirement cases passed with no skips (baseline-targeted
  log). Initial direct benchmark invocation failed due to unquoted dotted
  PowerShell flags; corrected invocation quoted every flag, passed. Baseline
  five-sample warm medians: cache hit91.73ns/op; unchanged update94.67ns/op;
  both zero allocations. Full benchmark log includes CTY hit/miss/cold churn.
- Owner bulletin-history SHA256 observed before runtime fixture qualification:
  `CEC97BF555F464F789436F9B7A3CDD930D5943B708695B98E7E13CF016BA4F13`.
- Slice 1 source checks: actual original live/replay startup failed the new cold
  positive controls (baseline-live/replay-failure logs); implementation restored
  byte-for-byte after controlled negative runs. Interim ordinary/race/platform
  checks passed but are not final evidence after fixture refinements. Corrected
  fixture eligibility/publication fence for FT8 wire evidence, and corrected a
  false-green captured-byte test: it must execute the complete production init
  body through a private per-invocation reader seam in a cold child, rather than
  manually constructing a mapper. Final reruns/source review pending.
- Cache implementation refinement within approved scope: warm valid cache hits
  keep the existing shard-lock linearization; unchanged, unexpired reported
  updates use a check-only shard fast path preserving LRU recency. Changed work
  rechecks under coordinator->shard. Bind ownership before CTY/recovery; capture
  the concrete store per lookup; carry store epoch and the actual reported
  locator's revision through coalescing (not aggregate UpdatedAt). Negative
  reads invalidate temporary-pin publication identities. Pending actual reports
  plus absent usable reported cache are contested; preserve that witness even
  if a failed write retires before completion. CTY/derived-only pending work is
  not contested reported authority. No new product policy or validation waiver.
- 03:48 UTC: slice 1 GATE PASS. Final full native affected ordinary run
  (`geo-native-affected-ordinary-final.log`): 3 packages, 2,974 passed cases,
  17 existing gated/platform skips, all 38 required identities executed/passed.
  Focused native ordinary/race each passed 37 new cases with zero skips; the
  repaired existing executable fixture separately passed ordinary/race. WSL
  CGO=0 ordinary, CGO=1 ordinary/race each passed all 37 new + one existing
  fixture cases with zero skips/failures (geo-wsl receipt and JSON logs).
  Initial broader run's 180s package limit was insufficient for existing
  diagnostics and was replaced by the normal 10m limit. Its next complete run
  identified one valid-start fixture lacking private H3 inputs; that fixture
  now stages copies and an explicit path, preserving its assertions. Failed
  logs are retained. Source reviews found no remaining material issue; eight
  Go-file hashes frozen in geo-source-sha256.txt. Documentation, TSR, YAML
  semantic-equality/header, comments and whitespace checks passed.
  A byte-copy checkpoint of 25 owned geography files is under
  `.tmp/cache-geography-v6-geography-checkpoint/manifest.json`; owner bulletin
  and this evolving record are excluded. Cache worker released as sole code
  writer. All slice evidence remains provisional to the final integrated lane.
- 03:50 UTC: cache regression captured red before production cache edits:
  `TestGridAuthorityLateDerivedCache` left committed `FN31AB`/reported masked
  by cached `FN20`/derived after a late lookup plus Clear. See
  `cache-geography-v6-cache-baseline-red.log`. Identical baseline five-sample
  ordinary warm/parallel harness passed (cache-baseline-bench.log), including
  same-key and sharded hits, unchanged reports and 90/10 mixed traffic with
  zero allocations. Cache production implementation then started.
- 04:02 UTC: central cache source compiles and original regression is green;
  initial broader grid checks passed 89 cases with no skips. Lead review
  refinements implemented: current-store pending counts distinct from total
  owners; separate live-report and lookup-publication identities so old A's
  acknowledgement cannot invalidate newer B's queued commit; reported hydration
  fences old derived candidates; pinning follows actual selection disposition;
  per-request ownership lasts through the actual buffered response.
  Read-only substantial review verified epoch/lock/owner/backing paths and
  found one additional defect in the draft: a derived completion's negative
  witness rejected a genuinely reported raw-fallback read. The witness now
  restricts borrowing/recreating temporary pins, rather than rejecting that
  independently successful report. Deterministic raw-fallback and purge
  controls were added; pre-purge positive overlap is retained, not strengthened
  into a new purge policy. Final targeted/race/source checks remain pending.
- Preliminary identical five-sample performance comparison: ordinary warm hit
  94.29->85.75ns, unchanged report 97.24->65.38ns, zero allocations. Parallel
  same-key hit/update/mixed changes -2.6%/-13.9%/-7.4%; sharded changes
  +0.2%/+2.8%/+1.1%, zero allocations. All below the 20% regression boundary.
  These are local microbenchmarks, not production latency claims. Source
  relevance and final cold/churn/profile evidence still require closeout.
- 04:14 UTC: owner bulletin-history SHA256 remains unchanged. Cache/UI focused
  checks have interim green results; resource-test review additionally requires
  actual enqueued substring backing comparisons (reconstructing equal text
  would be false-green), real normalizer expansion from input-sized fields,
  and final historical-backing/cleanup evidence. No parser caps or gridstore
  encoding changes are authorized or introduced by those checker refinements.
- 04:24 UTC: final cache source review cleared the original defect and two
  evidence gaps. Anchored churn now holds an actual old read across 12,000
  churn operations/rebuilds, THEN commits a same-call report and forces LRU
  eviction before completing the stale read. The mutation must reach the
  original held entry. A four-worker cold benchmark plus one held anchor
  supplies genuine coordinator contention within the five-owner boundary.
  Native targeted race passed 146 cases/zero skips; final backing-order race
  passed five cases, with 96 rebuilds and held GC heap 19,548,280->19,459,176
  bytes. Whole UI ordinary passed 103 cases/zero skips. Cold five-sample medians:
  empty reset317.5ns/410B/10alloc; held anchor233.2ns/156B/8alloc; concurrent
  four workers+anchor418ns/156B/8alloc. Rate-1 alloc/inuse/mutex profiles are
  post-retirement, paired with actual during-held GC observations; inuse5.50MiB
  is primarily existing ULS initialization5.44MiB, not a process/RSS claim.
- 04:25 UTC: initial integrated ordinary lane started. Subsequent source review
  found cold-only diagnostic gaps (original async/sync warning context and
  uncounted timestamp/pin replacements) and eligibility for deferred synthesized
  persistence. Those were repaired with exact counter and real worker/drained
  store assertions. Rejected raw derived data cannot be enqueued merely because
  a different selected cache value is also derived. Because source changed
  during this initial integrated run, it is PROVISIONAL and the entire ordinary
  lane will repeat on the final frozen source. Full race will use that final
  source too. No initial integrated pass is promoted to final-state evidence.
- 04:35 UTC: vet and standalone Staticcheck passed; golangci-lint reported eight
  style/analyzer issues in owned files (large range-value copies, an unused
  private timestamp result, a tagged-switch suggestion, and wrapped-error
  comparison in the H3 test). Bounded mechanical fixes requested before the
  final freeze and repeated analyzers; no lint waiver or new behavior.
- 04:37 UTC: all eight lint issues repaired mechanically; golangci-lint exits0,
  reports0issues (cache-lint-repair.log). Source refrozen at04:37:29UTC with
  manifest combinedSHA256
  `0281bbcd6a8370866189398a996a9828365d58c566dbda88f127c6043515c797`.
  Repeated whole-workspace vet and Staticcheck on that source both exit0.
  Initial integrated ordinary completes12,611passed/93existing skips across
  59executed+15build-only packages, but remains provisional. Full frozen
  ordinary starts04:40UTC with96required identities (38geography+58cache/UI).
  WSL first invocation failed before builds because a non-login shell lacked
  Go on PATH; retained path-failure.log and restarted through the already
  qualified login shell, without installing or modifying host configuration.
- Documentation gates: troubleshooting records pass; tracked crawler-entry
  checker passes6criticalfiles; additional untracked-file scan found one new
  formatter missing a discovery header. Lead added only a two-line file-role/
  ADR comment to grid_authority_stats.go (no executable change), then covers
  all new non-test files explicitly. YAML20headers/comment-only tokens pass;
  boolean-comment warning reviewed: db_check_on_miss value is unchanged, and
  the revised comment correctly describes async plus caller-opted bounded sync.
- 04:43 UTC: final WSL command and JSON verification pass in all three variants:
  CGO=0 ordinary, CGO=1 ordinary and CGO=1 race; four packages each, all96exact
  required RUN/PASS identities, zero failures/skips. See final-wsl-receipt.log.
  Explicit new-source header scan passes all3new non-test Go files; read-only
  gofmt check passes all21owned Go files. No executable source changes remain.
- 04:53 UTC: FINAL frozen ordinary lane exits0: 12,612passed, 93existing gated/
  platform skips; 59executed+15build-only packages; all96required identities
  executed/passed (final-ordinary-frozen.log). Original provisional integrated
  result remains separate. Refreshing cold benchmark on this final ordinary
  executable, then full race and connected CustomSCP handoff remain pending.
- Final ordinary executable cold medians (five samples): concurrent four
  workers+anchor418.2ns/156B/8alloc; empty reset314.8ns/410B/10alloc;
  held anchor234.2ns/156B/8alloc. This includes the final counter/eligibility
  fixes and confirms unchanged allocation costs. Full race started04:53UTC;
  required identities remain96. At04:58UTC branch remainsmore-stabilizer and
  the owner bulletin-history hash is still unchanged.

## Implementation and evidence map

| Approved obligation | Implementation | Distinguishing evidence |
| --- | --- | --- |
| Required shared geography, strict reads, unchanged map meaning | h3required.go and shared decoder; early main_runtime.go and replay runner owners | Original cold-start negative controls; captured-removal full-init child; input faults/Once sequences; actual GRID/NEARBY/CustomSCP and prediction-off controls; all96required cases in three final WSL variants |
| Reported authority across cache, reply and writer | grid_authority.go, call_meta_cache.go, bootstrap.go and grid_writer.go | Actual buffered replies, raw fallback, reported hydration/cache loss, successful/failed/dropped commits, source-rejected derived persistence and exact supersession counts |
| Epochs, bounded ownership and retirement | Stable cache/handle binding, owned write envelopes, per-request worker lifetime, store-local pending counts | Store replacement/Clear/negative controls; unchanged retirement and delayed-release regressions; coalescing/overflow/failure/terminal drain |
| Bounded text, pins, identities and map backing | Compact copies; 8,698-key/five-pin ownership proof; nonzero identities; mutation-triggered map rebuild and empty reset | Maximum owner population, actual source backing comparisons, UTF-8 expansion, actual stale selection after anchored rebuilds/report/eviction, during-held GC observations and local cold/contention profiles |
| Fixed diagnostics and preserved operator behavior | Four headless/cache rows, unsigned formatting, cache-pane height13, preserved async/sync warnings | Maximum unsigned/unavailable formatting; actual60/120-column pane drawing; zero gauges after drain; operator/support documentation |
| Durable decisions and support retrieval | ADR0344/0345, TSR0128/0129, partial supersession links, runtime/operator/replay/H3/cache references and customgpt routes | TSR/link/header/YAML checks and direct source/doc review; generated maps and final review recorded below |

No stabilizer letter/percentage/budget/age policy, storage encoding/retention,
historical repair, source parser cap, geographic calculation or production
configuration value was changed. Required H3 startup availability is the
explicit owner-selected compatibility change. Production deployment, accuracy,
p99 and whole-process memory qualification remain outside this local run.

## Final validation results

- Native ordinary: exit0, 12,612passed/93skipped, 59executed+15build-only
  packages; all96required identities RUN/PASS. Evidence: final-ordinary-frozen.log.
- Native race: exit0 at05:17UTC, same12,612passed/93skipped and74packages;
  all96required identities RUN/PASS, no race report. Evidence: final-race.log.
- Skip identity audit against priorv5 ordinary: exactly the same93names, no
  additions/removals. Skips are existing helper-process, opt-in qualification/
  live/profile/reference-fixture or platform gates; no required new case skipped.
- WSL final: CGO=0 ordinary, CGO=1 ordinary/race each4packages/all96required
  exact RUN/PASS identities, zero failures/skips. JSON logs and final receipt
  verify actual case execution, not compilation or parent-only success.
- Vet, standaloneStaticcheck and golangci-lint: exit0; lint0issues after the
  recorded mechanical repairs. Final executable-only changes were checked in
  the complete repeated native and WSL lanes.
- Connected CustomSCP ordinary/race: exit0 at05:18UTC; fresh producer/consumer
  database handoff, exact TCP line, persisted delivery and same-static-membership
  held negative control passed. Raw evidence directories:
  `300cb2830e7347ca855f343e2041e3a9` and `8f38338cca9b454e8f9aa934989f07a2`
  under `.tmp/customscp-regression/`; final-connected-ordinary/race.log.
- Performance/resource acceptance: warm paths0alloc; maximum paired warm
  contention regression2.8% (below20%); final cold allocations/maintenance and
  actual anchored GC/backing checks recorded above. No budget was relaxed.
- Documentation: TSR consistency, link-target review, YAML20headers and
  comment-only token equality pass; all21owned Go files formatted,6tracked
  critical headers plus3new non-test headers reviewed. No config values changed.
- Source review: lead-owned completeness/diff review plus separate read-only
  cache/geography reviews. All material findings were fixed and distinguishing
  checks added; no unresolved implementation finding or validation waiver.
  These reviews are not independent scientific/production qualification.

## Completed outcome

Approved v6 is complete: geography and cache units are retained and validated,
with no deferred slice, rejected optimization, missing required check or waiver.
All source/doc findings were dispositioned before this closeout. Work finished
at approximately2026-11-12 00:21EST (05:21UTC), inside the approved four-hour
window and before the new-implementation cutoff.

Code maps generated and checked successfully; direct diff review confirms only
the path-reliability and runtime/ingest maps changed as expected for new files
and exported initialization. Final whitespace check passes, including an
explicit scan of all17untracked new text files. The final21Go-file hash manifest
is `.tmp/cache-geography-v6-final-source-manifest.json`. Regenerate/check maps
after this last documentation write under the runbook; final-maps-generate.log
and final-maps-check.log retain the authoritative command results.

Final identity: `C:\src\gocluster`, branch`more-stabilizer`, unchanged HEAD
`9bcae8e355c71eba6d480841a12ea6acac556dda`, local uncommitted changes, nothing
staged. No commits, merges, pushes, deployment, production rebuild/restart,
installs, host-policy changes or UAC requests occurred. The owner's bulletin
history retains its original recorded SHA256. ADR0344/0345 and TSR0128/0129,
support routes and indexes are current. Existing excluded work remains excluded;
no production accuracy, latency or whole-process memory qualification is claimed.
