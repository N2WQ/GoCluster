# TSR-0025: P50 Merge CPU And Heap Pressure

- Status: Resolved
- Date opened: 2026-06-04
- Status date: 2026-06-04

## Trigger

Production cluster CPU rose sharply after the p50 branch was merged into main.
Profiling data was collected overnight from the production cluster for CPU,
heap, allocation, mutex, block, trace, goroutine, and OS process evidence.

## Symptoms and impact

The cluster appeared CPU-pegged during the incident window. The overnight run
with `GOMEMLIMIT=1024MiB` showed lower host CPU but private bytes approached the
memory limit, leaving little heap headroom.

## Hypotheses tested

1. Active p50 introduced a CPU busy loop in path prediction.
2. Active p50 retained enough additional bucket state to push the process into
   GC pressure under the previous memory limit.
3. Diagnostic/reporting collection was allocating on hot ingest paths.
4. The incident was primarily lock contention, blocked goroutines, or network
   stalls.

## Evidence

- Raw CPU pprof samples were dominated by Windows console/tcell `runtime.cgocall`
  and did not show a p50 busy loop.
- OS process samples from the overnight run averaged about 26 percent of one
  core, with p99 about 55.5 percent of one core, while private bytes reached
  about 984.5 MiB under a 1024 MiB memory limit.
- Heap profiles showed `pathreliability.(*Store).updateBucket` retaining about
  62-63 MiB in the final overnight capture.
- Local retained-size evidence showed the p50-era bucket at 640 bytes before the
  fix and 440 bytes after retaining only the active histogram lane.
- `pathReportMetrics.Observe` benchmarked at 24 B/op and 2 allocs/op before the
  reporting-key change, then 0 B/op and 0 allocs/op after it.
- Mutex, block, trace, and goroutine profiles did not identify a p50 contention
  storm or goroutine leak.

## Root cause or best current explanation

The p50 merge added material retained heap by keeping both raw and capped SNR
histograms in every path bucket. Under tighter memory limits, that retained heap
reduced headroom enough to plausibly drive GC pressure and the observed CPU
symptom. The overnight run with a higher memory limit mitigated CPU pressure but
confirmed the retained-heap risk remained.

## Fix or mitigation

The code fix retains only the active p50 histogram lane for the configured
receiver-contribution mode and removes avoidable allocation from path reporting
metrics. Operationally, a higher `GOMEMLIMIT` can provide short-term headroom,
but it is not the durable fix.

Follow-up Custom SCP profiling showed additional allocation and write churn that
was not the p50 root cause: edit-neighbor support probes generated synthetic
calls that missed the global callsign normalization cache, and unchanged static
membership observations still attempted static persistence. ADR-0143 records the
behavior-preserving follow-up optimization.

## Why an ADR was or was not required

- ADR required because the fix changes retained-state resource bounds in the
  active path reliability subsystem while preserving active p50 behavior.

## Links

- Related ADRs: ADR-0139, ADR-0143
- Related issues/PRs/commits:
- Related tests: `pathreliability/active_p50_contract_test.go`,
  `pathreliability/snr_histogram_test.go`, `pathreliability/receiver_test.go`,
  `pathreliability/store_bench_test.go`,
  `internal/cluster/path_report_metrics_test.go`,
  `internal/cluster/path_report_metrics_bench_test.go`
- Related docs: `pathreliability/README.md`,
  `data/config/PATH_PREDICTIONS.md`
