# Whole-runtime profiling route after S7

Preparation under approved v4-v6, inspected 2026-10-15. This document proposes a
controlled experiment; it does not authorize implementation, builds, listeners,
runtime launches, deployment, production restart, host tuning or firewall edits.
S7 has an accepted local source checkpoint on `voacap`. Bind its actual
commit with `git rev-parse HEAD` when preparing the target build; the checkpoint
contains ADR-0309 and this plan. The concrete final harness, inputs,
source overlays and launch/capture controller still require lead/owner review.
No executable, suite, runtime, port probe or host inspection was run for this
preparation. The engineering investigation was steered, not independent.

The requested whole-cluster assessment starts with a full configured-runtime
profile of accepted final S7. The controlled comparison below then isolates S4;
its intentionally disabled external features cannot stand in for that first
assessment.

The recommended S4 comparison is **final S7 versus the same final S7 with only the
old shard selector restored in an experimental build**. This holds the S5 zero
report fix and S6/S7 evidence semantics constant. An original-S4-versus-final
comparison would also change those semantics and cannot isolate S4's effect.
The S4 exception and its failed/inconclusive historical results remain intact.

## Full Configured-Runtime Pass

First freeze the intended runtime configuration and a finite workload with
normal and peak feed rates, client counts, client filters and path settings.
The current shipped files are source evidence, not proof of the production
configuration or load. Preserve every intended enabled subsystem, its startup,
maintenance and shutdown owners, while redirecting writable state and network
dependencies to the reviewed private experiment. Record which features actually
do work during each window; an enabled but idle feature is not exercised cost.

Collect target CPU, retained heap, allocation churn, contention, queues, drops
and complete ingress-to-client delivery timing across the whole runtime before
focusing on the path store. Separate cold startup from steady operation, and
use an unprofiled matched delivery pass to measure observer overhead. A short
warm capture cannot establish hourly maintenance or long-running retention.

When VOACAP is part of that configuration, use a private engine home, cache,
run/lock paths and controlled SSN/reference inputs. Record every external
engine child's executable, PID, start/exit times, CPU and memory separately.
Go pprof describes the Go target; it cannot attribute external engine CPU.
Absence of a private, verified engine or a required local dependency is a
declared coverage gap, not permission to call a disabled-feature result a
complete configured-runtime profile.

Then use the matched old/new shard selector experiment to explain any relevant
path-store cost. Hold S5-S7 semantics, configuration and completed workload
constant within each pair. The primary isolated comparison specified below
disables external VOACAP and publication; report that narrower coverage plainly.
An additional matched pass with the full intended feature set is required if
those components materially change the workload or the latency conclusion.
No arbitrary latency verdict or owner service limit is invented in this plan:
report absolute delivery delays and saturation, then judge practical impact.

## What current source establishes

- `internal/cluster/bootstrap.go:193-225`: `loadClusterConfig` tries nonblank
  `DXC_CONFIG_PATH` first, then relative `data/config` only for a not-found
  error. Other load errors are terminal. Missing override contents can therefore
  fall through; an environment variable alone is not an isolation guarantee.
- `config/config.go:1556` and `config/config_files.go`: the override must name a
  complete directory, not a YAML file or patch. The registered merged files,
  feature-root path/solar files and required reference tables are validated.
  `LoadedFrom` records the chosen directory. File-valued runtime settings are
  not generally rebased to that directory; ordinary relative files use CWD.
- `internal/cluster/bootstrap.go:266` and `main_runtime.go:199`: the real runtime
  owns config, runtime tuning, stores, pipeline, telnet, feeds and maintenance.
  `initialize` followed by `shutdown` and `close` is the source-grounded target
  lifecycle for a dedicated, bounded in-package test child. `Run` waits for an
  OS signal, so invoking it unchanged is unsuitable for deterministic test
  retirement on Windows.
- `pathreliability/keys.go:10`: one selector owns update and capture placement.
  Actual S3 checkpoint `9145209` used `key % len(shards)` at both sites. Replacing
  only the current selector body with that expression retains final S7 keys,
  admission, selection, support, compaction and persistence code.
- `scripts/test-windows.ps1`: native builds run from saved full-import-path
  executables under `.tmp/windows-tests`, guarded by `runner.lock`. The already
  used cluster path is
  `C:\src\gocluster\.tmp\windows-tests\dxcluster\internal\cluster\package.test.exe`.
  Saved comparison copies are evidence, not executable launch paths.
- `scripts/run-with-profiling.ps1` hardcodes root `gocluster.exe`, root
  `data/config`, root `logs`, port 6061 and visible `Normal` windows. Do **not**
  launch it unchanged for this experiment. Its collection categories are useful
  orientation, not an isolated controller implementation.

## A real prelaunch gap: loopback binding

`telnet/server.go:2299` builds `:%d`, a wildcard bind. Current config/options
expose a port but no bind host. Its held-listener `startWithListener` is private
to package telnet; existing telnet-package fixtures use it with loopback, but
an internal/cluster test cannot call it. Selecting an unused port and connecting
only local clients does not make that wildcard listener private.

The smallest proposed experiment adapter is a **common test-build overlay**
binding telnet `Start` to literal `127.0.0.1:<fixed port>` in both variants, with
all following worker/accept/Stop code unchanged. This is not a production patch
or an existing capability. Review its exact diff and test its refusal/cleanup
before launch; alternatively review a bounded held-listener seam. Do not use
reflection, unsafe private-field injection, a firewall change, or a wildcard
listener justified solely by assumed firewall protection. If the binding gap
is not resolved within a separately reviewed experiment, stop before startup.

## Target, driver and ownership

Use two dedicated opt-in test-child entries in the cluster package, to be
implemented only after the concrete route is reviewed. Both run from the same
stable `internal/cluster/package.test.exe` pathname, with exact distinct test
selectors, `-test.count=1`, `-test.paniconexit0`, finite test deadlines and a
larger bounded controller deadline. There is no such whole-runtime child yet.

The target child loads the isolated configuration with the actual loader,
asserts the exact `LoadedFrom`, applies the actual YAML runtime controls and
uses the real runtime initialization/pipeline/telnet/feed/maintenance owners.
The driver child owns recorded feed servers, local client sockets, transmission
schedule, complete-line receipt parsing and exhaustive correlation outside the
target process. It waits for an explicit bounded release after every feed and
client has completed startup. Readiness is not inferred from a listening port.
Driver CPU/memory is recorded separately and excluded from target pprof.

The parent owns the existing exclusive
`.tmp/windows-tests/runner.lock` throughout preparation/build/restore/launch/
capture/retirement. No unrelated run may replace the stable executable. Restore
the chosen saved bytes to that path, verify hash, launch driver and target,
then join **both** before restoring the next variant. Record each process's PID,
parent PID, executable path/hash, command line, creation UTC and exit UTC/code.
PID reuse must not pass identity checks. Background `Start-Process` helpers use
`-WindowStyle Hidden`; no interactive production console is involved.

Proposed fixed local endpoint table, subject to actual collision preflight:

| Owner | Literal endpoint |
|---|---|
| target telnet, after reviewed loopback adapter | `127.0.0.1:18300` |
| driver RBN CW/RTTY | `127.0.0.1:17000` |
| driver RBN digital | `127.0.0.1:17001` |
| driver PSKR MQTT | `127.0.0.1:11883` |
| driver reference HTTP fixtures, and DXSummit only if in corpus | `127.0.0.1:18081` |

No pprof TCP endpoint is needed: the target test owns direct `runtime/pprof`
captures. Before any child starts, inspect IPv4/IPv6 listeners, their owning
PIDs and all active processes at the stable binary path. Refuse collisions;
never terminate a production process to make room. Where possible the driver
holds its bound sockets through startup, eliminating release/rebind races.
Immediately verify every actual listener is loopback and owned by the expected
PID. A bind/ownership/loopback failure aborts, cancels and joins the experiment.
No port availability or existing firewall rule was verified in this task.

## Configuration and every writable owner

Create fresh, non-reparse run directories under
`.tmp/path-reliability-first-wave/whole-runtime/<run-id>/`, refusing existing
evidence directories. Keep immutable source/config/corpus manifests separate
from per-process mutable state. Clone initial databases/assets by an offline or
supported consistent snapshot; never copy a live Pebble directory piecemeal.
Give every run the same initial contents and a new mutable copy. Do not open a
production DB even for the apparent convenience of an existing cache.

Launch from the canonical package directory as usual; the isolated child must
change CWD to its own run work directory **before** any runtime initialization,
record that CWD and keep it fixed. This confines hardcoded relative output too.
The sandbox's fallback `data/config` must be absent so a missing override fails
closed. All configured paths are additionally resolved and checked before use;
each writable path must stay below that run's mutable root. Reparse points and
aliases to production data are refused. This is a proposed child lifecycle,
not something the current runner automatically provides.

| Owner/settings | Required treatment in isolated configuration |
|---|---|
| complete `DXC_CONFIG_PATH` directory | copied required merged YAML, feature-root YAML and reference tables; run actual validation; hash all files, record effective config and permitted per-run path substitutions |
| `grid_db` and its sibling recovery/checkpoint files | independent Pebble root and checkpoint/restore output; keep grid cache/write/flush settings identical between variants |
| `archive.db_path` | independent archive Pebble root; retain ordinary archive queue behavior; recovery/deletion may affect only this root |
| `call_correction.custom_scp.path` | independent SCP Pebble root, cleanup and ancillary files; identical initial state |
| `filter.UserDataDir` | current default is CWD-relative `data/users`, not a config-directory child; isolate users/settings/presets and local login history there |
| `fcc_uls.{db_path,archive_path,temp_dir,allowlist_path}` | isolated DB/archive/temp/allowlist copies, including metadata, extraction/build files and SQLite auxiliary files |
| `ised.{db_path,archive_path,special_archive_path,temp_dir}` | isolated paired reference snapshots, both archives and metadata/build auxiliaries |
| `cty.file`, `skew.file` | hashed local copies, with any download/replacement/metadata writes confined to the run |
| `h3_table_path`, correction reliability/model files | immutable hashed copies; same H3/taxonomy/assets for both builds, no selection-dependent population construction |
| `logging.dir` and every logging sub-directory | independent system, drop, login, reputation, ingest, telnet, peer and propagation logs; include rotation/retention output |
| reputation `ipinfo_pebble_path`, snapshot/download paths, `reputation_dir` | independent copies; all cleanup/import/generation markers confined to that run |
| `peering.topology.db_path` | disabled together with peering; if later included, independent DB and loopback peers only |
| VOACAP `ssn_state_path`, `forecast_cache_db_path`, `voacap_home` | primary isolation experiment disables external VOACAP execution; enabling it later needs a private engine/run/lock directory as well as private state/cache, not the production VOACAP home |
| CWD-relative `logs/peering_overlong.log`, `data/diagnostics`, `data/reports`, propagation scheduler defaults | sandbox CWD catches these hardcoded outputs; disable report generation and built-in dump/log tickers for the primary experiment |

This inventory names inspected writable families, not a claim that an unopened
optional feature cannot own another file. The future preflight must reconcile
the final effective config/path inventory and observed startup writes before
measurement. Any unexpected write destination stops the route.

## No external feeds, downloads or publication

Use recorded RBN and actual MQTT PSKR input through their existing clients,
including normal and path-only modes. Enable a source only when its configured
destination is one of the literal loopback endpoints above. Disable all human
upstreams without recorded local counterparts; disable DXSummit or point it to
the fixed local HTTP corpus. Disable peering and peer forwarding, scheduled
propagation reports, optional toxicity/OpenAI requests, solar HTTP fetching and
external VOACAP for the primary comparison. Native/local fallback settings and
all changed S5-S7 policies otherwise stay identical and recorded. This measures
the whole local ingest-to-delivery runtime under a declared feature profile;
it does not measure every shipped external service or VOACAP engine cost.

**FCC/ISED enabled=false is not an offline switch.**
`main_runtime.go:547`, `uls/downloader.go:34` and `uls/ised_refresh.go:30` start
reference refresh independently of enforcement; ISED attempts reconciliation
at startup even with a ready DB. Supply validated private reference fixtures
and literal local HTTP URLs for both archive routes, serving the required
conditional-download semantics. CTY and skew can also fetch/refresh; their URLs
must be local or their entire feature intentionally disabled. Audit all URL
consumers before startup rather than relying on an old snapshot being fresh.

Disable reputation live API, download/import and Team Cymru DNS fallback for
the local feature profile. Do not inherit API tokens, proxy settings or debug
credentials. Clear process-local proxy variables or freeze an explicitly local
proxy policy; never alter system proxy/firewall settings. Inventory socket
destinations and fail on a non-loopback attempt/connection. Observed socket
snapshots supplement, rather than replace, the prelaunch consumer/config audit.
Local clients issue only frozen setup/read commands; no external publication.

## Frozen input and fair comparison

Freeze the exact accepted S7 commit and full build-input hashes. Archive both
overlay documents and resulting source hashes: (a) common loopback experiment
adapter, identical in both variants; (b) old selector body, present only in the
control. No other production change is allowed. Freeze toolchain, CGO/CC
executable hash/version, GOFLAGS, GOWORK, GOOS/GOARCH and build flags. Preserve
saved executable SHA256, `go tool buildid` and `go version -m` for each build,
then recheck restored stable bytes before each process launch and at closeout.
Test-only overlays require explicit manifests; VCS metadata alone is insufficient.
Extract each profile's mappings/build IDs when present and compare them with
the saved target identity. A Windows profile may omit an embedded build ID;
record that absence explicitly and bind it through the PID/start-time/capture
manifest plus saved binary SHA256/build ID. The reused stable filename alone
cannot identify a variant or authorize profile attribution.

Recorded feeds must declare finite frame count/bytes, hash, timestamp fields,
ordering, topic/mode/SNR distribution, receiver identities, grids, bands, burst
schedule and intended path-only versus ordinary routes. Include actual shared
and distinct geographic populations; do not choose keys using either selector.
The local client manifest fixes callsigns, grids, noise, PATHSAMPLES, filters,
dialects, login pacing, number of clients and any deliberate slow reader.
Respect actual prelogin rate limits; do not silently increase queue/worker
capacities to achieve a cleaner run. Slow-client stress is a separate declared
cohort from normally draining readers.

An old recording cannot simply be replayed with historical timestamps: S7
ages support from observation time. Review and freeze a deterministic relative
age/timestamp translation. Store both original and materialized input hashes,
run anchor, scheduled release times and actual release/arrival ages. Sequential
variants using the same absolute old timestamp would receive different aging.
Require matched intended age populations and classify unmatched delay/aging as
a comparability failure. No freshening of timestamps after a failed run and no
producer-selected expected glyphs.

Before timing, a bounded unscored schema/lifecycle preflight must verify actual
Go JSON output, full-corpus accounting, client setup/readiness, route counts,
independent selected-support/glyph expectations and complete TCP lines. Verify
the one-line selector overlay preserves those semantics; do not run a selector
distribution test and treat its intentional old-selector failure as a product
failure, or disable ordinary correctness oracles with it.

Proposed first feature profile is the current shipped `enforce` mode, with
paired off/shadow follow-ups only if separately frozen. Freeze run length,
release rate, warmup, drain deadline, sample cardinality, pair count and AB/BA
orders after the actual finite corpus is selected. These values are not supplied
by the current artifacts, so this preparation does not invent an executable
measurement schedule or a new acceptance threshold. Use same-code A/A controls
to characterize repeatability; preserve every observation and stop on a failed
completeness/comparability check. Whole-runtime diagnostics do not retroactively
turn deferred/failed S4 qualification into a pass or waive any S7 guard.

## Capture boundaries and meaningful measurements

Keep an observer-light delivery/throughput lane separate from diagnostic lanes
with CPU, allocation, block/mutex sampling or execution traces. Both variants
get identical observer settings/windows. A target observer may take bounded
existing API snapshots outside the per-item hot path; it cannot add per-item
checker scans/maps, report rebuilding or unbounded history to the target.
The driver retains all finite correlation samples or exact bounded histograms,
checks every required delivery, and reports its own CPU/scheduling saturation.

| Evidence | Boundary and interpretation |
|---|---|
| CPU | target-only warm workload interval; cold startup separate; report profile sample duration, process CPU seconds, workload counts and symbols; include runtime/GC/ingest/telnet totals before attributing path-store symbols |
| allocation churn | same-window allocs-before/after profiles, analyzed using `alloc_space`/`alloc_objects` and explicit base; exact MemStats Mallocs/TotalAlloc deltas as supplementary totals; inclusive pprof rows overlap and must not be summed |
| live retained memory | steady workload and post-drain two-GC captures, separately labeled; `inuse_space`/`inuse_objects`, exact heap/object counters, live store/key/sidecar cardinalities and retained owners; allocation totals are not retained memory |
| lock/wait | matched `SetMutexProfileFraction` and `SetBlockProfileRate` diagnostic windows, restored on cleanup; blocking is accumulated goroutine wait, not request elapsed or automatically a shard bottleneck |
| queues/drops | `PSKReporter.HealthSnapshot`, `RBN.HealthSnapshot`, ingest-health fields, telnet `BroadcastMetricSnapshot`, reject/prelogin counters, dedup/stats and archive diagnostics as available; counters are stage-specific, not additive unique missing-spot counts |
| delivery | driver send/scheduled-release to complete correlated TCP line, per client/cohort; exact expected/received/missing/duplicate/filtered/dropped counts; report p50/p95/p99/p99.9/max and full denominator only for complete matched populations |
| process resources | PID-bound CPU, working/private bytes, threads, handles and socket owners at fixed cadence; process start time/path/hash checked with every sample; driver process reported separately |

Use explicit sampling settings rather than inheriting them. Exact existing
environment names are `DXC_CONFIG_PATH`, `DXC_GRID_DB_CHECK_ON_MISS`,
`DXC_PPROF_ADDR`, `DXC_BLOCK_PROFILE_RATE`, `DXC_MUTEX_PROFILE_FRACTION`,
`DXC_HEAP_LOG_INTERVAL`, `DXC_MAP_LOG_INTERVAL`, `DXC_PSKR_MQTT_DEBUG`,
`GOMEMLIMIT`, `GOGC`, `GOMAXPROCS`. YAML `go_runtime` positive values override
the environment through `applyGoRuntimeTuning`; shipped current values are
1536MiB/50/2, so setting GOMAXPROCS=4 alone does not establish a 4-thread run.
Freeze effective values after startup. A future child-opt-in variable is a new
test interface and must be named/reviewed then, not confused with an existing
runtime switch. Leave DXC_PPROF_ADDR/heap/map logger variables unset when the
test owner performs captures: the current HTTP server and periodic loggers
are process-lifetime helpers without runtime-owned cancel/join handles.

The existing telnet latency API has a material limitation:
`telnet/latency.go` retains only 512 atomic samples per tracker; Snapshot allocates
and sorts an approximate concurrent view. Its N is retained slots, at most 512,
not all completed deliveries. It supplies p50/p99 only, not a campaign p99.9.
`enqueue` is broadcast-envelope to accepted client queue. `firstByte` is
actually successful batch-flush completion minus envelope enqueue time, observed
for each spot in the flushed batch; it is not an instrumented first byte at the
client. `writeStall` times the flush call once per batch. None includes complete
feed-to-pipeline delay or proves client receipt. Keep these as recent diagnostic
snapshots; never promote them into exhaustive end-to-end latency evidence.
Current queue-length snapshots similarly miss intervening spikes, and the
30-second ingest-health logger primarily reports state transitions rather than
a complete queue time series. Raw counts/high-water claims need an actual
reviewed observer, not inference from these logs.

Use a supported monotonic cross-process Windows clock for the external driver,
recording source/frequency/resolution and zero counts. Existing QPC helper
syscall overhead must stay out of target per-item work. Do not subtract overhead,
clip/replace zero durations or reuse a retired testing.B domain. An unavailable
clock/endpoint/denominator is an evidence gap, never numeric zero latency.

The real predictor maintenance is minute purge and first eligible minute
compaction followed by hourly compaction (`main_runtime.go:426`). Do not
accelerate it in a claim about production cadence. Short warm captures may
exclude compaction entirely; report actual events/windows. The accepted S4
66-minute soak remains separate evidence, not proof that a new full-runtime
capture observed another hourly cycle.

## Retirement, receipts and unresolved prerequisites

Stop/release feed producers, finish the finite declared drain, then stop/join
driver sockets and target observers/profilers according to the reviewed owner
sequence. Exercise the target's real `shutdown` then `close`; preserve output
retirement semantics and feed/telnet/store cleanup. `close` cancels context and
joins license refresh/VOACAP owners, but not every current background task has
an explicit join handle. Do not assert all goroutines retired merely because
those functions returned. Preserve bounded shutdown evidence and final process
exit, no orphan child or listener at any experiment endpoint. Forced termination
is failure evidence, never a normal success/heap-capture boundary.

Each run receipt binds source/config/corpus/controller/overlay/tool hashes,
both process identities, effective CWD/env, endpoints, windows, completed work,
delivery denominator, profile file hashes and their exact matching binary.
Retain exact Go child RUN/PASS, package PASS, normal exit and successful joins;
no skip, timeout, crash or best-effort profile warning may produce an accepted
receipt. Preserve failed artifacts without unchanged retries, pooling or edits.
Recheck immutable inputs and stable binary identities at the end.

Outstanding prerequisites before any launch: bind the accepted final S7 hash; reviewed
test children/controller and loopback adapter; finite recorded corpus and age
translation; full isolated config/reference snapshot and conditional HTTP
fixtures; concrete phase counts/durations/orders; actual PID/port/path checks;
schema/lifecycle/completeness preflight; and the lead/owner's concrete route
review under the retained v4 boundary. No new approval token is implied here.

Inspected sources: `main.go`; `internal/cluster/{bootstrap.go,main_runtime.go,
go_runtime_tuning.go,ingest_health.go,output_pipeline.go,path_sharding_profile_test.go,
path_sharding_fixture_test.go,runtime_output_retirement_test.go,
runtime_output_listener_windows_test.go,pc92_runtime_profile_test.go}`;
`config/{config.go,config_files.go}`; `pathreliability/keys.go` and the actual
S3 store selector; `telnet/{server.go,latency.go,command_fixture_test.go}`;
`filter/filter.go`; `uls/{downloader.go,ised_refresh.go}`;
`internal/voacap/runner.go`; `solarweather/config.go`; source configuration
`{app,runtime,ingest,data,archive,peering,pipeline,reputation,prop_report,
path_reliability,solarweather}.yaml`; `scripts/{test-windows.ps1,
run-with-profiling.ps1}`; the existing S5 stable-path wire controller and
`approved-scope-v4.md`. Earlier memory was used only to locate the stable-path
and profiling history; all material route claims above were checked in current
workspace source.
