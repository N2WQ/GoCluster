# Frequency-change secondary dedupe: approved v6 and validation

Authority: the owner's exact `Approved v6`, following adoption of earlier
reported changes with more repeats and report exceptions at every frequency.
The owner subsequently authorized exact `Approved v7`, changing only the
shadow retained-memory lab target described below; every other v6 rule and
validation requirement remains in force.
Baseline: `main` at `3838ad631948f338ca209edc094aadd7cbeed870`.
The unrelated `data/bulletins/history.json` change is outside this work.

This record covers implementation and local validation. The first shipment
keeps `state: off`. Activation and production acceptance remain separate.
See [ADR-0334](decisions/ADR-0334-frequency-change-aware-secondary-dedupe.md)
for the durable decision and [operator config](../data/config/README.md#secondary-frequency-change-policy)
for required settings.

## Approved boundary

- Keep historical key/value behavior and baseline-only construction in off.
- Use candidate-only construction in on and separate baseline/candidate state
  in shadow. Baseline drives all shadow spot output.
- Pair decisions and cleanup in 64 historical-key shards, with one owned,
  joined cleanup timer. No sequencer queue or independent child cleanup.
- In the candidate, add a bounded canonical mode identity at key offset 21 for
  skimmers only. Human keys remain mode-free. Do not intern incoming strings.
- Evaluate report exceptions before tolerance/minute/budget checks. Outside
  tolerance, an exception moves anchor frequency and time without incrementing
  the count. A normal frequency-change admission increments count, replaces
  report state and leaves `when` unchanged. Only expiry resets count.
- Round operational kHz to nearest whole Hz. Tolerances start at 500/3000/1000/
  2000 Hz for skimmer CW/RTTY, skimmer FT8/FT4/FT2, other skimmers, and humans.
  The configured change limit starts at two; it is not a total-output ceiling.
- Require the new YAML settings, reject null/missing/invalid values and
  nonpositive numeric values, and enforce CW/RTTY tolerance against actual
  configured averaging tolerance. No private config edits or runtime fallback.
- Carry immutable live diagnostic evidence through spot cloning and async
  handoff. Off/shadow retain old tag bytes; archive encoding/history do not
  acquire persisted anchor fields.
- Expose fixed shadow counters and both cache cardinalities from the existing
  stats loop through ordinary logging. Include run/start/interval identity;
  do not reset counters on report. Zero denominators are N/A.
- Preserve disabled-policy fallback, SELF, stabilized fanout and peer content.

Sample capture, reservoirs, persistence, shared logger code/stream/config
changes, primary dedupe, averaging, stabilizer policy, user filtering, archive
schema and workflow changes are outside v6. Reported changes are heuristic;
this work makes no verified-QSY or scientific accuracy claim.

## Implementation and validation traceability

The targets below identify ownership. Validation is planned until a command
and its observed result are entered in the execution record.

| Approved item | Implementation owner | Falsifying evidence |
| --- | --- | --- |
| Historical baseline preserved | `dedup/secondary.go` and existing key/window tests | Literal keys, original entry layout, report/window boundaries, disabled and missing-country behavior; no candidate allocation in off |
| Candidate transitions and reasons | `dedup/` candidate/controller and tests | Hand-specified state/forward vectors, both frequency directions, tolerance/minute/expiry boundaries, exhausted-budget and earlier-minute report exceptions |
| Faithful shadow concurrency/cleanup | `dedup/` paired shard owner | Forced interleaving with identical algorithms yields zero additions/losses; paired cleanup uses one timestamp; another shard progresses while one is paused; timer joins on stop |
| Bounded canonical mode identities | `spot/taxonomy.go` and tests | Custom/hidden modes, aliases, USB/LSB, unknown bucket, ordering independence and registry cap; humans remain mode-free |
| Required config and startup wiring | `config/`, `data/config/dedupe.yaml`, runtime construction | Missing/null/state/nonpositive rejection, averaging cross-check and consumer-level settings propagation |
| Stable live diagnostic evidence | `spot/` clone, output delivery, `telnet/server.go` | Change/delete the cache after capture, then format queued evidence; effective-policy fallback and SELF; old bytes in off/shadow |
| Shared output behavior | `internal/cluster/` fanout and `telnet/` tests | End-to-end off/shadow spot equivalence for telnet, ring, archive and peer; candidate admissions reach the intended lanes in on |
| Exact fixed comparison counters | `dedup/` statistics, existing runtime stats loop | Three-addition/one-loss golden trace; exclusive reasons; all step-4 admissions; limit-blocked eligibility; current-input cohorts and bypass forwards |
| Bounded disposable summaries | Runtime stats summary helper/tests | Interval/cumulative arithmetic and run reset, N/A values, dashboard/headless visibility, worst-width records within 2 KiB, locks released before logging, no extra reporter goroutine |
| Resource/performance contract | Targeted benchmarks and comparable profiles | Off within 3% measurement noise; established-entry on paths zero allocations and within 20% matched baseline; insertion separate; shadow mixed-mode memory/cleanup/contention measured |

Golden lifetime trace, with a 179-second window and stronger-SNR preference
off: A reported at 0; B unreported at 10; B reported at 20, 180 and 200.
A/B are outside tolerance. Baseline forwards at 0 and 180. Candidate forwards
at 0, 10, 20 and 200. Shadow has three additions (frequency change, report
exception, new entry) and one loss at 180.

Counters cover 12 fixed policy/class cohorts: FAST/MED/SLOW, each split into
human, skimmer CW/RTTY, skimmer FT and other skimmer inputs. They count
secondary admission decisions, not completed delivery. Added forwards have
one candidate reason: new entry, report exception, or frequency change.
Frequency-change admissions include cases where baseline also forwards.
Limit-blocked counts only otherwise eligible step-4 attempts. Read-only stats
do not mutate dedupe state; run identity distinguishes restart resets.

The 2026-11-10 CI repair uses an immutable process-local engine number in
snapshots and the `run=<UTC-start>-<number>` log identifier instead of treating
a creation timestamp as unique. See [TSR-0117](troubleshooting/TSR-0117-ci-fixtures-and-shadow-run-identity.md)
for the forced-collision regression, isolated executable fixture, and repair
validation. This does not change admission rules or the resource/performance
acceptance limits below.

## Performance and production evidence limits

The original 2.5-times shadow memory target failed measurement. Approved v7
replaces only that lab target with at most 4 times the matched off fixture,
separately at 100,000 and 200,000 historical keys plus 20% additional mode
keys. Include both caches and fixed comparison state. This is not a total
process bound or a production memory multiplier.
Report baseline/candidate cardinalities, retained heap, cleanup
and contended latency together. Existing-key allocation measurements include
decision snapshots and counters. Map growth has a separate insertion result.
Microbenchmarks do not establish production latency or a fixed memory ceiling.

Counters can measure added/lost volume and budget pressure. They cannot
establish the owner's 80% usefulness gate for additions or review losses for
harm: example capture is deferred. No production gate is declared passed and
there is no automatic activation. Existing shared-logger resource qualification
also remains outside this work.

## Execution record

### Observed implementation checks (2026-11-10)

- Native targeted ordinary: four packages, 54 passed cases, zero skips.
- Native targeted race: five packages, 117 passed cases, zero skips. These
  runs cover the feature matrix and real telnet/peer consumers; the subsequent
  deterministic overlapping-alias correction also passed its spot tests and
  remains covered by the final lanes below.
- Config worker: full `go test ./config -count=1` passed (32.192 s).
- Go quality review found one alias-order nondeterminism issue. Alias owners
  now resolve against immutable declared identities; a repeated overlapping
  alias-chain regression passed. No further material correctness issue was
  reported by that review.
- Vet and Staticcheck passed. Lint initially identified three readability
  findings; all were corrected and lint then reported `0 issues`.
- One intermediate full-test attempt stopped on a syntax error introduced
  during the benchmark lint correction. That error was corrected; a fresh
  complete ordinary run subsequently passed. The stopped attempt is not
  counted as a passing full-lane result.
- Established-entry candidate allocation checks passed at zero allocations.
- Complete native ordinary lane passed: 74 packages (59 executed, 15
  build-only), 12,269 passed cases, 93 skipped cases. Complete native race
  passed with the same counts and no race reports. Skipped cases remain
  skipped; these runs do not establish gated live-service behavior.
- Final-source vet, Staticcheck and lint passed, with lint reporting `0 issues`.
- An isolated six-repetition, single-CPU, 500-ms matched comparison used the
  original HEAD secondary source through the temporary overlay. Candidate
  repeat/report/change medians were 99.57/119.3/131.7 ns versus original
  116.6/139.55/117.9 ns, all at zero candidate allocations. The frequency
  change median was 11.7% slower, within the 20% budget. Off repeat/change
  medians were 118.8/118.45 ns. Off report initially measured 145.2 ns versus
  original 139.55 ns, so it received an isolated alternating-order recheck:
  16 observations each, single CPU, 500 ms. Benchstat reported original
  139.1 ns and off 140.0 ns (about 0.6% higher, p=0.108), with identical
  8 bytes/one allocation. This clears the 3% off budget; all three candidate
  established-entry paths clear their 20% time and zero-allocation budgets.
- Controlled mixed-mode retained-heap observations at 100,000 historical keys
  plus 20% additional mode keys measured baseline 10,457,968 bytes and shadow
  35,996,840 bytes (3.442 times). At 200,000 keys, baseline 20,986,968 bytes
  and shadow 73,851,952 bytes (3.519 times). These are cache/component
  observations, not complete-process or production memory ceilings. The
  original measurement test's PASS did **not** satisfy the 2.5-times budget.
- The owner authorized exact `Approved v7` for a 4-times lab target on both
  declared workloads. The observed retained-heap ratios meet that amended
  target. No CPU/allocation, functionality, logger or activation condition
  changed. The final test now asserts both cache cardinalities and the
  4-times ratio explicitly. Native ordinary and race reruns passed. Ordinary:
  100,000/120,000 baseline/candidate keys, off 10,453,496 bytes versus shadow
  36,630,752 (3.504 times); 200,000/240,000 keys, off 20,987,016 versus shadow
  73,745,552 (3.514 times). Race observations were 3.499 and 3.454 times.
- Separate insertion benchmarks (two CPUs, 200 ms) measured off/on/shadow
  416.6/304.9/612.7 ns, 320/344/761 bytes and 2/1/1 allocations per insertion.
  These include map growth and are not established-entry allocation claims.
  Contended shadow repeats measured 175.9 ns and zero allocations. A populated
  100,000-key non-expiring sweep measured off/on/shadow 1.802/1.933/4.264 ms
  with zero allocations. Expiry/compaction correctness is tested separately.
- Final profiles passed: established on/change measured 134.0 ns with zero
  allocations. A two-producer, 100,000-historical-key/120,000-candidate-key
  shadow stress fixture with a deliberately aggressive 2-ms sweep measured
  778.9 ns per repeat and zero allocations. Cleanup accounted for 61.7% of
  sampled CPU; mutex profiling attributed 4.03 aggregate seconds of waiter
  delay to cleanup and 1.01 seconds to producer unlocks. Block profiling
  recorded 4.97 aggregate seconds at producer mutex acquisition. This
  confirms cleanup contention under stress; it is not a per-spot maximum,
  production p99 result, or forecast for the runtime's 60-second sweep.

Local ignored artifacts are under `.tmp/qsy-v6-*` and `.tmp/qsy-v7-*`.
The temporary legacy Go
overlay reads `HEAD:dedup/secondary.go` without modifying the working package,
so matched timing can compare the original implementation to the candidate.

All production behavior changes preceded the complete ordinary/race lanes.
The later explicit v7 memory/cardinality assertions received targeted ordinary
and race reruns; later benchmark-only changes received their own executions.
Baseline checks reported before approval are not final-change validation.

| Check | Command/evidence source | Current status |
| --- | --- | --- |
| Targeted checks | Native runner with feature filters across dedup/spot/config/cluster/telnet | Earlier focused ordinary 54/race 117 cases passed; subsequent behavior fixes covered by full lanes |
| Full ordinary | `pwsh -NoProfile -File scripts/test-windows.ps1 -Timeout 10m` | Passed: 74 packages; 12,269 passed, 93 skipped; `.tmp/qsy-v6-full-ordinary.txt` |
| Full race | `pwsh -NoProfile -File scripts/test-windows.ps1 -Race -Timeout 10m` | Passed: 74 packages; 12,269 passed, 93 skipped; `.tmp/qsy-v6-full-race.txt` |
| Vet | `go vet ./...` | Passed again after final benchmark/assertion changes; `.tmp/qsy-v7-vet.txt` |
| Staticcheck | `staticcheck ./...` | Passed again after final benchmark/assertion changes; `.tmp/qsy-v7-staticcheck.txt` |
| Lint | `golangci-lint run ./... --config=.golangci.yaml` | Passed again, 0 issues; `.tmp/qsy-v7-lint.txt` |
| Matched timings | `go test ./dedup -run '^$' -bench '^BenchmarkSecondaryFrequencyEstablished$' -benchmem -cpu 1 -count 6 -benchtime 500ms`, plus legacy overlay and isolated off/report recheck (16 each) | Passed time/allocation budgets; matched logs and benchstat under `.tmp/qsy-v6-matched-*` and `.tmp/qsy-v7-off-report-*` |
| Insertion/contention/cleanup | `go test ./dedup -run '^$' -bench '^(BenchmarkSecondaryFrequencyInsertion\|BenchmarkSecondaryFrequencyPopulatedCleanup\|BenchmarkSecondaryFrequencyContended)$' -benchmem -cpu 2 -count 1 -benchtime 200ms` | Passed; `.tmp/qsy-v7-structural-benchmarks.txt` |
| Amended memory assertion | Native runner `-Packages ./dedup -Run '^TestSecondaryFrequencyRetainedHeapMeasurement$' -Timeout 180s`, ordinary and `-Race` | Passed one case each; `.tmp/qsy-v7-memory-ordinary.txt` and `-race.txt` |
| Profiles | `go test ./dedup -run '^$' -bench` established on/change (`-cpu 1`) and MixedCleanup (`-cpu 2`), `-benchtime 3s`, CPU/mutex/block/memory profile flags | Passed; `.tmp/qsy-v7-*-profile.txt`, `.cpu`, `.mutex`, `.block`, `.mem`; limits above |
| Generated maps | `go run ./cmd/codemap generate -all`, then `go run ./cmd/codemap check -all` | Passed; runtime-ingest-fanout map regenerated, all maps fresh |
| Final diff | `git diff --check` and direct approved-scope review | Passed; scoped implementation/docs/tests, no shared logger or private config changes; unrelated bulletin history preserved |

Use the native Windows runner and tool versions in
[the development runbook](dev-runbook.md). Record command scope, exit status,
key evidence, failures/skips and material limits before closeout. A fresh final
Go quality review completed with the alias correction verified. The final
lead-owned verification checks approved scope, actual diff, validation and
claim limits; it is not described as independent review. Support-agent
documentation impact: **Required**. Updated operator documentation, ADR-0334,
`customgpt/source-map.md` and `customgpt/developer-guide-index.md` describe the
new required config, diagnostics and shadow limitations.
