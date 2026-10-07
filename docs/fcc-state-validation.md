# FCC State Implementation and Validation

The implementation follows the user's `Approved v2` authorization. The first
phase covers FCC mailing states and territories; Canadian provinces remain a
separate source project. [ADR-0253](decisions/ADR-0253-fcc-state-enrichment-and-filtering.md)
records the accepted architecture.

## Approved Scope to Evidence

| Approved behavior | Implementation | Evidence |
| --- | --- | --- |
| Retain only state from EN, joined by license ID and callsign | `uls/state_import.go`, `uls/loader.go` | Generated EN fixtures, all 60 codes, duplicate identity order permutations, missing/blank/conflicting state, full FCC sample histogram comparison; literal fixture follow-up described below |
| Preserve license membership when state is unknown | `uls/state_import.go`, `uls/license_check.go` | Import failure and ambiguity tests; factual found/unknown lookup tests |
| Upgrade legacy schema and retain last good database | `uls/downloader.go`, `uls/loader.go`, `uls/extract.go` | Missing/old startup builds, unchanged failed-processing retry, failed publication retry, cancellable replacement, extraction cleanup, actual Windows sharing denial |
| Separate reference data from enforcement | `uls/license_check.go`, `internal/cluster/main_runtime.go`, `cmd/rbn_replay/runner.go` | Disabled-enforcement startup/build tests; central delivery/archive tests; sequential replay setup registers local databases and direct lookups read their state without HTTP; replay spots are not enriched |
| One bounded factual cache with generation isolation | `uls/cache.go`, `uls/license_check.go` | Concurrent reset/lookup race tests, old-result publication barrier, bounded CLOCK churn, prompt stats while DB owner is held, no unavailable-answer caching |
| DE enrichment at ingest and DX after final corrections | `internal/cluster/ingest_validation.go`, `internal/cluster/bootstrap.go`, `internal/cluster/output_pipeline_stages.go` | Same-identity DE CTY refresh; immediate/delayed propagation after manually changed DX identity; foreign/missing state clearing; actual broadcast queue and decoded archive checks; resolver integration follow-up described below |
| Expand spot FCC jurisdiction and preserve exemptions | `spot/state.go`, `uls/allowlist.go`, central DE/DX gates | Literal 17-entity consumer coverage, excluded 105/134, TEST/beacon exemptions, US/default allowlist coverage and explicit ADIF isolation |
| Preserve existing login jurisdiction | `telnet/server.go` remains on its prior login predicate | `TestStateExpansionPreservesLoginLicensePolicy` |
| PASS/REJECT DESTATE/DXSTATE and existing composition/reset rules | `filter/state.go`, `filter/filter.go`, `telnet/state_commands.go`, command HELP | Independent truth matrix, both dialects, atomic invalid input, unknown state, ALL, NOFILTER, category AND and rejection precedence |
| NEARBY suspension, locking and restoration | `filter/filter.go`, telnet configuration handling | Snapshot isolation, restore/reset tests, hidden-state v1 PUT under NEARBY |
| Exact disk migration and protected malformed/future profiles | `filter/configuration_storage.go`, `filter/user_record.go`, `filter/presets.go` | Legacy/v1/v2 fixtures, false entries/zero flags, nested baselines, merge/alias decoding oracles, unchanged bytes on protected writes, pre-clone cardinality bounds |
| Keep YAML v1 and explicitly expose v2 | `telnet/machine_versions.go`, schema/readback/transaction code | Literal v1 golden output, hidden state preservation and revision conflict, full v2 required fields, PATCH omissions/replacement, malformed transport recovery, native/ziutek framing, near-limit v1 and oversized v2 atomicity |
| Archive v6 with v2-v5 compatibility and stored-state history | `archive/archive.go`, telnet history snapshots | Fixed historical bytes, v6 snapshot roundtrip/truncation checks, mixed-version history PASS/REJECT tests, state digest/cursor invalidation |
| Cancel and join the refresh worker | `uls/downloader.go`, runtime close | Actual HTTP download blocked until cancellation, completion-channel joins, teardown before temporary directory removal |
| Operator and support documentation | Root/package/config READMEs, operator/domain contracts, `customgpt/support-cards/configuration-readback.md`, ADR/index | Reviewed command examples, config enforcement wording, schema migration/downgrade guidance, support-critical crawler headers and generated code maps |

Support-agent impact is required: state readbacks, unknown-state behavior,
NEARBY, enforcement-disabled refresh and machine/disk/archive versions all
change troubleshooting answers. The configuration readback support card now
contains these rules.

## Full Sample

The downloaded FCC amateur archive was imported with the final state importer
through the ordinary `uls.Refresh` path using a local HTTP fixture serving the
unaltered downloaded bytes. Original and state-enabled databases were kept
separately outside the repository.

- Active HD and AM rows: **823,141** each.
- Known state: **823,101**; unknown state: **40**.
- All **60** normalized codes and their counts match the original EN analysis,
  including normalization of lowercase codes.
- SQLite `user_version`: **1**; `PRAGMA quick_check`: **ok**.
- Permanent tables: **HD and AM only**; no EN addresses or scratch tables.
- Final sample import: approximately **5 minutes** on this local environment.
- Existing HD date-column mapping and active-status criteria were deliberately
  left unchanged.

The local archive is `/tmp/gocluster-uls-review-GTKnvuuQ/l_amat.zip`; final
import output is `/tmp/gocluster-fcc-state-validation/full-import.log`. These
scratch artifacts are local evidence, not deployed reference data.

## Original Implementation Checks

For the original implementation (`daccf00`), the recorded final checks all exited
successfully:

```text
go test ./...
go vet ./...
staticcheck ./...
golangci-lint run ./... --config=.golangci.yaml
go test -race ./...
```

Go 1.27.1, Staticcheck 2026.2.1 and golangci-lint 2.14.0 were used. Lint reported
zero issues. Native Windows Go 1.27.1 passed ULS/download, filter, spot, archive,
telnet, commands and replay package tests, plus central FCC/ingest tests. The
Windows replacement test holds a real file handle that denies deletion,
verifies cancellation leaves old bytes intact, releases the handle and verifies
successful replacement.

Bounded fuzzing passed for EN import evidence, human state lists, machine YAML
and archive decoding. Archive fuzzing completed 51,520 executions; the final
import fuzz run completed 629; state-list and machine-schema runs completed
11,211 and 16,665 respectively. The original EN fixtures were generated with
positional assignments and could share a column-index mistake with the importer.
The archive compatibility fixtures use fixed historical bytes. Additional EN
and integration evidence is described in the follow-up below.

Earlier broad race runs exposed two unchanged asynchronous assertion gaps:
a logging test can read yesterday's archive after one-day cleanup deletes it;
a toxicity test can receive a result before the deferred pending decrement.
The toxicity failure reproduced on an untouched HEAD snapshot. Five focused
logging reruns passed. These tests were not modified; the final full race run
passed with no race report. Prior analyzer findings and native sharing/teardown
failures were corrected and rechecked.

## Local Performance Evidence and Limits

| Measurement | Observed result |
| --- | --- |
| Warm factual lookup | 229.1 ns/op; 0 B/op; 0 allocations |
| Cold factual lookup | 23.801 us/op; 544 B/op; 18 allocations |
| Warm DE/DX central consumer benchmark | 3.959 us/op; 448 B/op; 4 allocations, including existing callsign/CTY work |
| State-filter matcher | Approximately 396-417 ns/op; 0 B/op; 0 allocations |
| Local production runtime | 500/500 state-filtered TCP lines and decoded archive rows, enforcement off |
| Local complete-line delivery latency | p99 7.016 ms; maximum 15.351 ms |

The focused runtime uses one local client, 500 distinct known US license
records, local reference data, correction disabled and immediate fanout instead
of the shipped 200 ms broadcast timer. Its timing begins before central enqueue
and ends after reading the complete TCP line. It establishes functional
reachability and a local measurement, not a deployed first-byte SLA, full
fanout qualification or a measured improvement over the old implementation.

CPU, heap and mutex profiles were captured and inspected for the warm consumer
benchmark and the focused production runtime. Whole-run profiles include
startup and cleanup; retained-memory samples are not a steady-state leak
verdict. Cache ownership/cardinality claims use source review and targeted
churn/generation tests as well as profiles.

An optional broad PC92 peer-load smoke failed with a broken connection after
827 generated inputs. It does not provide full qualification evidence. The
focused FCC runtime check above completed successfully. Local logs, binaries
and profiles are under `/tmp/gocluster-fcc-state-validation/`.

Original review was lead-owned with bounded read-only worker reviews. Those
reviews used inherited context and are not claimed as independent evidence.

## Documentation And Coverage Follow-up

The follow-up on `states_provinces` verifies the behavior already approved in
v2. It changes documentation, the replay setup comment and tests; production
behavior, including unknown lookups during extraction/rebuilding, is unchanged.

| Audit gap | New evidence |
| --- | --- |
| Generated EN fixtures could share the importer's column assumptions | `uls/testdata/state-column-en.dat` contains hand-written 27-field records based on the [FCC ENTITY specification](https://wireless.fcc.gov/wtbfiles/pa_ddef51.pdf), with different city/state/ZIP values. `TestImportLiteralFCCStateColumnAndIdentity` builds the database and checks literal expected states, licensee/contact separation, conflicting evidence, identity joins, missing evidence and inactive membership. HD/AM setup still uses the existing generator. |
| Tests manually changed the corrected callsign | `TestFCCResolverCorrectionDeliveryArchive` seeds a live resolver with independent reporters, lets the production pipeline correct K1ABC (CA) to K1ABD (TX), and checks the broadcast queue and decoded archive. It covers immediate and stabilizer-release paths, with enforcement both enabled and disabled, while preserving DE state AP. It does not add a TCP or contest-load test. |
| Concurrent cache tests reset unchanged database contents | `TestRefreshChangesStateDuringConcurrentLookups` performs an actual CA-to-TX `Refresh` while four readers run, checks unknown lookups during rebuild, and checks TX after publication. A real completed CA SQL result held with the old cache owner is rejected by `finishLookup` after publication. This barrier check does not claim to stall an SQL query across the switch. |
| Cancellation did not reach substantial extraction/import work | `TestRefreshCancellationAfterSubstantialExtraction` cancels after 262,144 extracted EN bytes and checks directory cleanup, last-good CA data, failed processing status and refresh-flag recovery. `TestStateImportCancellationAfterSubstantialRows` cancels after 4,096 complete matching EN rows (477,018 bytes), returns valid tail data with more input unread, and checks rollback, scratch-table removal, connection reuse, integrity, closed-file rename and retained CA facts. The EN test exercises the importer directly, rather than a full worker shutdown during import. |

The human category list, schema-2 writable-state row and NEARBY edit-lock list
are complete in `telnet/README.md`. The FCC section and configuration-readback
support card explain the refresh interruption and permanently empty archived
states. Replay setup only registers a local database; replay spots do not acquire
state metadata.

Isolated Go overlays outside the repository confirmed that the new tests reject
incorrect implementations: selecting EN index 16 or 18 fails the literal-state
test, using the ULS file number as the license identity fails the EN join, and
keeping CA on the resolver-corrected TX identity fails all four delivery/archive
cases. Removing the old-cache-owner guard fails at the held CA result after TX
publication. These are expected negative-test failures; the actual production
files were not edited.

The final follow-up source passed these complete repository checks with Go
1.27.1, Staticcheck 2026.2.1 and golangci-lint 2.14.0:

```text
go test ./...
go test -race ./...
go vet ./...
staticcheck ./...
golangci-lint run ./... --config=.golangci.yaml
```

Lint reported zero issues. The four new ULS tests also passed 20 ordinary
repetitions and five race-enabled repetitions. The resolver integration test
passed 20 race-enabled repetitions. On native Windows Go 1.27.1, all five new
tests passed three repetitions. The crawler-entry check inspected the changed
replay file and passed. Changed documentation terms and the local validation
reference were checked directly. Final review made the literal fixture's ULS
file numbers distinct from its system IDs; the affected ULS suite, ULS race
suite and native Windows coverage were rechecked after that refinement. Final
check logs are under
`/tmp/gocluster-fcc-coverage-validation/`.

No full FCC download/import, fuzz campaign, benchmark or load profile was rerun
for this follow-up. Production parsers, performance and refresh semantics did
not change. The original sample and performance results above remain historical
evidence, rather than new measurements.

No architecture decision changed, so ADR-0253 remains authoritative. No new
ADR or TSR is needed for these documentation and evidence corrections.
Support-agent documentation impact is required and is addressed by the updated
configuration-readback support card. Final verification is lead-owned; the ULS
worker used inherited context and is not claimed as an independent reviewer.
