# Sunspot-source switch: approved v6 execution

Status: implementation and combined validation complete on top of current main.
No production cluster has been restarted or deployed, and nothing is published.

Integration follow-up, 2026-11-13: the owner explicitly requested committing the
SSN changes, rebasing onto current `main`, validating the combined code, and
fast-forwarding `C:/src/gocluster`. At integration start `main` is clean at
`14c04f5d5d295306bd59f9f466660896057e4401`, containing both spot-comment commits.
This follow-up authorizes local integration; publishing and restart remain
excluded. The scope and evidence below preserve the earlier isolated phase.

Owner authorization: exact `Approved v6`, with explicit isolated-worktree and
unattended-execution instructions. No execution deadline was requested.

- Worktree: `C:/src/gocluster-ssn-v6`
- Branch: `voacap-sunspot-48h-v6`
- Baseline: `91b3740d018a143830d4b1cdcc3c57725a5a7f37`
- Protected checkout: `C:/src/gocluster`, branch `human-comment-integrity`,
  observed tip `e0904f54`; its files and runtime must remain untouched.
- Evidence: `.tmp/ssn-v6/` inside this worktree.
- Recovery: owned diff and this execution record; no commit authorization.
- Excluded: deployment, restart, commit, merge, push, glyph-policy changes,
  performance optimization/campaigns and new comparison logging.

## Executable Scope Ledger v6

One coherent changeset replaces the runtime solar-input contract while retaining
the current forecast machinery. The user selected old NOAA observation source,
48-hour exponential half-life, no flux blend and no percentage hold.

1. Fetch `sunspot_report.json` every 1800 seconds. Calculate local index
   `10 * groups + spots`; smooth new observation-time samples with 172800-second
   exponential half-life. Preserve full precision and round only final SSN.
   Cold replay all usable observations chronologically. Repeated bodies/304
   never advance the EWMA or observation watermark. Do not alter the manual
   Wolf experiment helpers or reuse their intermediate-rounded calculation.
2. Use a separately validated runtime reducer. Keep timestamp/station/observatory
   identity; collapse exact group duplicates, refuse contradictory group records
   and simultaneous competing stations; do not deduplicate by region number.
   Initial unambiguous correction records can contribute, but already accepted
   timestamps are not revised. Refuse an affected aggregate when a count is
   unusable, retaining unrelated usable aggregates; an unassignable malformed
   group refuses the representation. Reject ambiguous duplicate JSON members,
   malformed/future/fractional/negative/overflowing representations. Define the
   accepted local interpretation explicitly, without claiming undocumented JSON
   completeness or inventing status/revision precedence.
3. No missing/empty/unsupported representation becomes SSN zero. The JSON
   service's explicit spotless encoding and complete solar-disk coverage are not
   established. Retain last good compatible input and warn at 48-hour observation
   age on every poll outcome. Never-observed input is missing immediately.
4. Introduce monitor continuity v3: source/URL/units/estimator-reduction version/
   half-life identity. Reject old flux v2 and old Wolf v1 continuity and HTTP
   validators, then seed from the feed. Retain one bounded continuity record,
   maximum64KiB; no observation-history owner. On upgrade outage, serve valid
   cached forecasts but admit no new jobs until compatible count input exists.
5. Preserve one80-byte pointer-free provenance and88-byte snapshot, with at most
   128 added metadata bytes per forecast. Use source-neutral scalar meanings and
   source-tagged JSON: legacy flux ID1 retains truthful old SFU fields, count
   source uses count-specific fields, ID0 preserves unknown-provenance providers.
   Reject mixed/unsupported encodings. Keep forecast schema/model generation2;
   preserve legitimate existing flux forecasts, their actual SSN/provenance,
   original coverage and expiry. Do not relabel or recompute legacy provenance.
6. Preserve stable keys, rolling24-hour windows from worker start, immutable
   expiry, prior-forecast service during replacement, requested final-hour renewal,
   ordinary delay/backoff, one worker, queue1000 and two50000-entry caps. No idle
   work or source-change-wide enqueue. Same rounded SSN does not require a new
   engine run solely because source identity changed. Keep corrected longitude,
   calendar handling, glyph thresholds, reliability and observed-evidence rules.
7. Remove blend setting from required paths/defaults/struct/validation/wiring;
   reject retired `ssn_recent_flux_weight` with actionable merge/alias-aware
   validation. Keep polling/half-life/staleness/forecast/cache settings. No runtime
   source selector. Retain rejection of `recompute_delta_percent`.
8. Update affected operator/config/package/support documentation and supersede
   only ADR0347's solar-input decision, retaining its forecast contracts. Generate
   code maps as the final repository write and check freshness/final diff.

## Required acceptance evidence

- Independent literal vectors: groups3+4 in two groups ->27; first valid sample
  seeds itself; previous100/new200 after24h ->129.28932188134524, after48h ->150;
  previous100/new102 after48h ->101. Rounding boundary100.499/100.500/100.501 ->
  100/101/101. These are arithmetic oracles, not reception-accuracy calibration.
- Parser order/duplicate/station/status/conflict/malformed/zero-absence cases,
  overflow, future times and accepted-correction deferral; bounded parser fuzz.
- Uninterrupted versus restart continuation; v1/v2 rejection without validators;
  compatible v3 and identity mismatches; outage during first upgrade.
- Literal legacy flux/new count/unknown forecast JSON round-trips with immutable
  expiry and truthful units. Same-SSN transition; changed-SSN requested replacement
  and failed replacement retaining previous forecast.
- Feed -> monitor -> worker -> deck `SUNSPOT 101.` -> persisted provenance, proving
  a one-unit change is not suppressed. Use isolated native installation for native
  integration; no live runtime or engine invocation.
- Existing renewal, delay/backoff, queue/cap/coherent-dequeue invariants; physical
  80/88/128-byte envelope, warmed zero allocations and bounded-state checks.
- Targeted tests during work, complete Windows ordinary/race lanes, vet,
  Staticcheck, lint, fresh final source/diff/claim review; docs/support checks.
- Preserve earlier inconclusive timing evidence as inconclusive and deferred.

The prior107 replay estimate used legacy parsing and is not an acceptance oracle.
This is an experimental local sunspot index, without an official smoothed-SSN
equivalence or prediction-accuracy claim. Material changes to these semantics,
compatibility/resource guarantees or validation require reassessment and approval.

## Progress

- Worktree created clean from the approved baseline.
- Core Go/config/test implementation delegated as a single writing unit.
- Toolchain verified: Go 1.27.1 windows/amd64, Staticcheck 2026.2.1 (0.8.1),
  golangci-lint 2.14.0.
- Native engine copied into this worktree's `.tmp/ssn-v6/engine`; all 395 static
  files matched SHA-256 values from the earlier isolated installation. Its run
  directory started empty. Neither the live engine nor another worktree's run
  directory is used. Evidence: `.tmp/ssn-v6/engine-static-sha256.json`.
- Support-agent docs impact: Required. Updated the path support card and operator
  and troubleshooting routes, with authoritative package/operator/config docs.
- Decision disposition: ADR-0350 supersedes only ADR-0347's solar-input and
  monitor-continuity clauses. Historical geometry/performance evidence is kept;
  no new troubleshooting root cause requires a TSR at this checkpoint.
- YAML header/comment check passed for 20 runtime configuration files.
- Local support-agent bundle/routing check passed (`scripts/check-support-agent.ps1`);
  raw output is `.tmp/ssn-v6/support-agent.log`.
- Read-only scientific/parser review found a duplicate-member aggregate-isolation
  defect and missing count-provenance boundary checks. The implementation writer
  is addressing these with regressions before final validation. Analytical EWMA
  identities and the literal 27-count reduction agree with the inspected math.
- Pre-approval non-steered scientific review and scope challenge were completed
  in the owner conversation. Separate bounded implementation reviews covered
  parser/math and cache/provenance compatibility. The lead retains integration,
  final validation and final source/claims review.
- The parser/provenance review findings above are fixed and source-reviewed.
  A further constant-minimum floating-point case is fixed by the stable update
  `previous + alpha * (value - previous)`, with a continuation/state regression.
  The reviewer found no remaining material defect in its inspected paths.
- Targeted development runs: `targeted-01.log` passed 164 selected cases;
  `targeted-02.log` retained a transient test-build integer-type mismatch;
  `targeted-03.log` passed 50 selected cases after the correction. The opt-in
  native test was skipped in those targeted runs; this is not native evidence.
- Writer freeze: `targeted-04.log` passed 52 cases, with one expected native
  opt-in skip. The lead subsequently strengthened monitor-level fractional
  precision coverage and corrected lint wording/helper use; no runtime behavior
  changed in these final review fixes.
- Vet and Staticcheck passed; initial lint found two spelling issues and a test
  helper whose station argument was constant. These were corrected without
  suppressions. Final lint passed with zero issues. Logs: `vet-final.log`,
  `staticcheck-final.log`, `lint.log` (initial), `lint-final.log`.
- Initial full ordinary run reached `internal/cluster` after 37 successful
  packages. `TestGeographyStartup/prediction_off` failed because this fresh
  worktree lacked ignored call-correction assets used by existing startup tests.
  Copied only the four configured prior/model files into this worktree and
  verified SHA-256 identity; original files were read-only. Evidence and hashes:
  `windows-ordinary.log`, `local-prerequisite.txt`. No unrelated code was changed.
- Required native and review-fix run passed 24 cases, zero skips. Actual native
  execution consumed deck SSN 100 then 101 and persisted count provenance with
  fixed 24-hour expiry. The failed geography startup case also passed after the
  local prerequisite copy. Evidence: `native-and-review-fixes.log`.
- Initial instrumented parser fuzz run passed 234905 executions in 15 seconds
  with two workers (`parser-fuzz.log`).
- Final source review also required count provenance to place acceptance on or
  after observation, matching the monitor's existing causal-order rule. Negative
  acceptance times and observations after acceptance now fail literal-wire tests;
  valid count fixtures carry truthful acceptance times. Legacy flux is unchanged.
  The remaining ordinary run has not yet reached either affected owning package;
  final native/race/analyzer checks will use this final state.
- Concurrent spot-comments work in the protected checkout continued independently
  and added ADR-0349 after this task began. The new decision here uses ADR-0350
  to avoid that collision. This task has not changed the original checkout.
- Ordinary validation is complete across all 74 root packages. The initial 37
  successful packages were retained; the failed package and remaining 37 were
  run again after prerequisite preparation. The resumed run passed 9474 cases
  (29 executed packages, 8 build-only, 86 conditional skips). Exact inventory and
  resume boundary were checked before execution: `root-packages.txt`,
  `ordinary-resume-packages.txt`, `windows-ordinary-resumed.log`.
- Final native/count-source review checks passed 7 cases with zero skips on the
  final source (`native-final.log`). The final instrumented parser fuzz run
  passed 197750 executions in 15 seconds with two workers and 287 cumulative
  corpus entries (`parser-fuzz-final.log`). These are correctness checks, not
  capacity measurements. Initial evidence is retained separately.
- Final `go vet ./...`, `staticcheck ./...` and
  `golangci-lint run ./... --config=.golangci.yaml` passed on the final code;
  lint reported zero issues. The source/config receipt is
  `.tmp/ssn-v6/final-source-sha256.json`.
- Final YAML, support-critical comment and local support-agent checks passed
  (`yaml-final.log`, `comments-final.log`, `support-agent-final.log`).
- The full native Windows race lane passed all 74 packages: 59 executed,
  15 build-only, 12782 passed cases and 96 conditional skips. The short native
  count-source integration ran and passed against the isolated engine under the
  race detector (`windows-race.log`). No data race was reported. Conditional
  helper/platform/opt-in skips do not qualify the deferred campaigns; no such
  performance or production claim is made.
- Code-map generation/check preflight passed. The two generated maps retain
  261 pre-existing root-relative ADR links; the count is unchanged from the
  baseline. This is an existing generator limitation outside v6. Freshness is
  not a claim of relative-link correctness. Current solar-input policy is routed
  explicitly through the package/operator docs and current-contract map.
- Final deleted-test review found that the generic HTTP read-failure/body-close
  regression had been dropped while replacing flux-specific tests. Restored it
  without changing transport or application code. Its final targeted ordinary
  and race reruns each passed one required case with zero skips
  (`cleanup-ordinary-final.log`, `cleanup-race-final.log`); broader production
  results are unchanged.
  Vet, Staticcheck and lint also passed after restoration. The receipt was updated
  only for that test file, with all production hashes checked unchanged.

## Review and validation traceability

| Approved behavior | Owning implementation | Distinguishing evidence |
| --- | --- | --- |
| Coherent reported-group reduction; ambiguity, duplicate and future refusal | `internal/voacap/solar_count.go`, strict JSON object decoder in `solar_input.go` | Literal 27, status/Region/station/order/conflict/invalid-isolation regressions; parser fuzz target |
| Elapsed-time EWMA, final-only rounding, every integer change | `solar_count.go`, `sunspot_monitor.go` | Independent 24/48/96-hour vectors; monitor fractional-value assertion; native 100-to-101 pipeline |
| Count-only configuration and wiring | `pathreliability/config.go`, `internal/cluster/main_runtime.go`, shipped YAML | Required-setting and retired direct/merged/aliased key tests |
| v3 bounded continuity, no old validators, last-good health | `sunspot_monitor_state.go`, `sunspot_monitor_health.go` | Restart/cold equivalence, v1/v2/identity refusal, encoded 64-KiB guard, stale boundary/recovery/error/304/cancellation tests |
| Source-neutral fixed storage, truthful legacy/count/unknown wire | `solar_input.go`, fallback and disk validation call sites | Literal wire tests, raw/SSN consistency refusal, 80/88-byte tests and existing 128-byte forecast envelope |
| Valid legacy forecasts, original expiry and same-SSN transition | `pathreliability/voacap_sunspot_source_test.go`; unchanged cache admission/lifetime machinery | Live worker-enabled legacy restore test; no admission on missing input or same-SSN source transition; original-expiry refusal |
| Preserved scheduler, caps and glyph policies | Existing fallback, worker, forecaster and classification owners | Existing final-hour/delay/backoff/dequeue/failure/bounds/warm-allocation regression suite; direct production diff review |
| Operator/support and durable contract | Package/config/operator docs, CustomGPT routes, ADR-0350 | YAML/comment/support checks, local Markdown link checks, generated-map freshness and final diff review |

The lead reviewed the actual production diff and new source files, including
the worker and disk validation boundaries. No scheduler, classifier, geometry,
calendar, queue-cap or map-cap changes are present. Removed flux conversion code
had no remaining callers; manual Wolf primitives and experiment commands are
unchanged. The parser's transient maps are bounded by the 1-MiB default response
limit and discarded after polling. Monitor retained state is one scalar record;
no history, metadata registry, new goroutine or timer was added. The cache retains
the existing pointer-free provenance and metadata ownership.

Observed checks establish functional and resource contracts, not deployment,
production glyph impact, engine capacity or physical propagation accuracy.

## Final disposition

Approved v6 is implemented in the recorded worktree. Independent non-steered
scientific review occurred before approval; separate implementation reviews and
the lead's final source/diff/evidence review found and closed the material
findings documented above. The lead's final verification is not an independent
scientific review. No owner-policy decision or approved implementation item
remains unresolved.

The final changed-file inventory and source/config hash verification are retained
as `final-diff-name-status.txt` and `source-receipt-check.log` under the evidence
directory. Code maps were regenerated and checked after the restored test;
generation/check is repeated as the final write/check after this record update.
Primary manual-document local file links passed (232 links across 14 documents;
anchors and the known generated-map links excluded). `git diff --check` passed.

Support-agent docs impact: Required, completed. ADR-0350 records the durable
source/continuity decision; ADR-0347's forecast and geometry decisions remain.
No new application troubleshooting root cause requires a TSR. Original failed
and inconclusive evidence remains available and is not relabeled as passed.

Deployment remains outside this work. Before using the new executable with an
existing active configuration, remove retired `ssn_recent_flux_weight`. Monitor
state reseeds automatically; valid generation-2 forecasts retain their actual
SSN, original provenance and original expiry. Live glyph impact remains
unmeasured until deployment and observation.

## Local integration follow-up, 2026-11-13

The owner's explicit follow-up authorizes the previously excluded commit/rebase
and local fast-forward, following the concrete integration sequence discussed
with the owner. GitHub publication and production restart remain excluded.

- Original `main` baseline: `14c04f5d5d295306bd59f9f466660896057e4401`.
- Isolated SSN implementation committed as `10dbed5f`; rebased candidate
  `98ddcc51` includes the two existing spot-comment commits as ancestors.
- Go changes applied without conflicts. Resolved only the decision-index text
  conflict, retaining ADRs 0348/0349/0350 and the partial supersession of 0347;
  regenerated the conflicting runtime code map from the combined tree. The
  troubleshooting index auto-merge preserves both support sections.
- Git content comparison proves all 20 SSN source/config paths identical to
  the validated implementation commit and all 21 comment Go files identical
  to the original main baseline. Raw file hashes can differ after Windows
  checkout line-ending conversion. See `integration/source-preservation.log` and
  `integration/rebase-range-diff.txt` under the existing evidence directory.
- Combined ordinary validation passed all 74 root packages: 59 executed,
  15 build-only, 13039 passed cases and 96 conditional skips
  (`integration/windows-ordinary.log`).
- Combined race validation passed all nine affected Go packages: 8828 passed
  cases and 83 conditional skips (`integration/windows-race.log`). Their exact
  selectors are recorded in `integration/race-packages.txt`. The original full
  74-package SSN race evidence remains applicable to its unchanged source;
  combined race results are reported separately and are not described as a new
  full-root race run. No data race was reported.
- Combined vet, Staticcheck and lint passed; lint reported zero issues. YAML,
  support-critical comments and support-agent checks passed. All 238 local file
  links across the 14 merged manual documents passed; anchors and the known
  generated-map links remain excluded. Raw outputs are under `integration/`.
- The short count-source native pipeline is enabled only against the existing
  isolated engine installation and passed in both combined ordinary and race
  runs. No performance campaign or production node is run.
- Final review confirms the rebase changed no Go/config implementation from
  the preserved component versions. Only this execution record is amended after
  validation; maps are regenerated/checked as the final document write/check.
  No new product-policy or scientific decision is introduced by integration.
- The original clean `main` is advanced with `git merge --ff-only` after these
  checks. Handoff verifies matching branch tips, preserved comment ancestry and
  worktree cleanliness; checkout-specific confirmation is retained in
  `integration/final-integration.log`. Publication and restart remain excluded.
