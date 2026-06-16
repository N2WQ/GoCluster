# ADR-0183: PSKReporter T3 Worker Budget

- Status: Accepted
- Date: 2026-06-16
- Decision Origin: Design

## Context

The production cluster target is an AWS `t3.medium` instance with 2 vCPUs and
4 GiB of RAM. T3 medium baseline CPU is 20%, so sustained or frequent spikes
above that baseline can consume or bill surplus CPU credits. Recent local
profiles showed that PSKReporter is the dominant path-evidence source and a
major contributor to allocation volume. The checked-in ingest config also
started PSKReporter with eight processing workers and eight MQTT inbound
workers, which is wider than the target host's CPU budget.

ADR-0141 already made process scheduler width YAML-owned through
`go_runtime.max_procs`. PSKReporter auto worker sizing should respect that
budget instead of independently forcing burst parallelism.

## Decision

Set the checked-in PSKReporter runtime profile to one processing worker and one
MQTT inbound worker for the 2-vCPU deployment target. This deliberately favors
CPU smoothing over maximum burst throughput because the observed PSKReporter
message handling cost is small relative to the incoming rate.

Change PSKReporter auto worker sizing so `pskreporter.workers: 0` follows the
effective `GOMAXPROCS` value, with a minimum of one worker. Explicit
`pskreporter.workers` values remain authoritative for operators who prefer
higher burst throughput.

## Alternatives considered

1. Keep eight workers and rely on `go_runtime.max_procs: 2`.
   - Rejected because it still widens runnable work and queue drain bursts on a
     host where the operating goal is smooth CPU below the T3 baseline.
2. Hard-code one worker in PSKReporter.
   - Rejected because larger hosts and private deployments still need an
     explicit way to trade CPU burst width for throughput.
3. Set auto workers to `runtime.NumCPU()` with a minimum of four.
   - Rejected because it ignores the operator-owned scheduler budget from
     ADR-0141 and is too aggressive for small burstable instances.

## Consequences

### Benefits

- PSKReporter concurrency is narrower than the 2-vCPU production target by
  default in checked-in config, which reduces runnable burst width.
- Auto mode is aligned with `go_runtime.max_procs`/`GOMAXPROCS`, so one CPU
  budget controls both scheduler width and PSKReporter worker width.
- Operators retain the ability to raise worker counts when queue depth or drops
  show that throughput matters more than smoothing.

### Risks

- Lower worker counts can increase PSKReporter processing queue depth during
  large incoming bursts.
- If the queue fills, payloads are dropped by the existing bounded queue policy
  rather than allowing unbounded CPU or memory growth.
- Live CloudWatch CPU, CPU credit, queue-depth, and drop counters are still
  required before claiming production improvement.

### Operational impact

- On the T3 medium target, `data/config/ingest.yaml` starts PSKReporter with
  one processing worker and one MQTT inbound dispatch worker.
- `pskreporter.workers: 0` follows the process's effective `GOMAXPROCS` value.
- The existing PSKReporter queue/drop logs remain the guardrail for detecting
  insufficient worker capacity.

## Links

- Related issues/PRs/commits:
- Related tests:
  - `pskreporter/client_test.go`
- Related docs:
  - `data/config/ingest.yaml`
  - `data/config/README.md`
  - `docs/decisions/ADR-0141-go-runtime-max-procs-yaml-control.md`
  - `docs/decisions/ADR-0145-pskreporter-fast-payload-parser.md`
- Related TSRs:
  - `docs/troubleshooting/TSR-0025-p50-merge-cpu-and-heap-pressure.md`
- Supersedes / superseded by:
  - Related: ADR-0141
