# FCC State Implementation and Validation

The implementation follows the user's `Approved v2` authorization. The first
phase covers FCC mailing states and territories; Canadian provinces remain a
separate source project. [ADR-0253](decisions/ADR-0253-fcc-state-enrichment-and-filtering.md)
records the accepted architecture.

## Approved Scope to Evidence

| Approved behavior | Implementation | Evidence |
| --- | --- | --- |
| Retain only state from EN, joined by license ID and callsign | `uls/state_import.go`, `uls/loader.go` | Literal EN fixtures, all 60 codes, duplicate identity order permutations, missing/blank/conflicting state, full FCC sample histogram comparison |
| Preserve license membership when state is unknown | `uls/state_import.go`, `uls/license_check.go` | Import failure and ambiguity tests; factual found/unknown lookup tests |
| Upgrade legacy schema and retain last good database | `uls/downloader.go`, `uls/loader.go`, `uls/extract.go` | Missing/old startup builds, unchanged failed-processing retry, failed publication retry, cancellable replacement, extraction cleanup, actual Windows sharing denial |
| Separate reference data from enforcement | `uls/license_check.go`, `internal/cluster/main_runtime.go`, `cmd/rbn_replay/runner.go` | Disabled-enforcement startup/build tests; central delivery/archive tests; sequential offline replay reads local state without HTTP |
| One bounded factual cache with generation isolation | `uls/cache.go`, `uls/license_check.go` | Concurrent reset/lookup race tests, old-result publication barrier, bounded CLOCK churn, prompt stats while DB owner is held, no unavailable-answer caching |
| DE enrichment at ingest and DX after final corrections | `internal/cluster/ingest_validation.go`, `internal/cluster/bootstrap.go`, `internal/cluster/output_pipeline_stages.go` | Same-identity DE CTY refresh, immediate/delayed corrected DX, foreign/missing state clearing, actual broadcast queue and decoded archive checks |
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

## Final Checks

On the final relevant production/test state, all of these exited successfully:

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
11,211 and 16,665 respectively. Parser fixtures and literal historical records
provide the test oracles rather than copies of the new implementation.

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

Final review was lead-owned with bounded read-only worker reviews. Those
reviews used inherited context and are not claimed as independent evidence.
