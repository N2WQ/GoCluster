# ADR-0182: VOACAP Runtime SSN State Persistence

- Status: Accepted
- Date: 2026-06-16
- Decision Origin: Design

## Context

The production VOACAP fallback uses `SunspotMonitor` to fetch NOAA SWPC
`sunspot_report.json`, maintain an 8-hour EWMA SSN, and expose the rounded SSN
generation used in VOACAP cache keys and deck generation.

Before this decision, that monitor retained state only in memory. A restart
lost the last NOAA validators, last observed raw SSN, EWMA, and current rounded
forecast SSN generation. The experiment commands had JSON state files, but
those files were explicitly experiment-owned and not part of production path
reliability.

ADR-0162 intentionally kept VOACAP hourly forecast-window cache records
memory-only. This decision addresses only the SSN monitor continuity state, not
forecast cache restore.

## Decision

Add a required production config setting,
`path_reliability.voacap_fallback.ssn_state_path`, for runtime SSN monitor
state. When VOACAP fallback is enabled, startup restores that file before the
SSN polling goroutine starts. A missing file is a cold start. Malformed or
unreadable state is logged as a warning and ignored so path reliability can
still start and fetch fresh NOAA data.

Persist only the monitor continuity subset:

- NOAA `ETag` and `Last-Modified` validators;
- last fetch and last observation timestamps;
- last raw SSN;
- EWMA and EWMA initialization state;
- current rounded forecast SSN generation and related diagnostic metadata.

Do not persist live `LastError`. Do not persist VOACAP hourly forecast-window
cache entries, delay queues, inflight work, or worker state. The state file is a
per-node runtime artifact and must not be shared by multiple running cluster
processes.

Runtime saves use bounded versioned JSON and replace the state file through a
same-directory temporary file. State save failures are warned about and recorded
as monitor errors but do not stop future NOAA polling.

## Alternatives considered

1. Keep SSN state memory-only.
   - Rejected because restarts unnecessarily lose the EWMA baseline and can
     keep VOACAP fallback in `vssn`/SSN-unavailable state until NOAA fetches
     fresh usable data.
2. Reuse the experiment command `ForecastState`.
   - Rejected because experiment state also carries command-specific forecast
     output fields and success semantics that are not production runtime state.
3. Persist VOACAP forecast-window cache records too.
   - Rejected because ADR-0162 deliberately keeps that cache bounded and
     memory-only; restoring it would require separate lifecycle, TTL, and
     compatibility decisions.
4. Store SSN state in Pebble.
   - Rejected as unnecessary for one small per-node continuity record.

## Consequences

### Benefits

- Restarts can reuse the last rounded SSN generation immediately when the state
  file is present.
- NOAA conditional request validators survive restarts.
- The operator-visible state path is explicit in production YAML rather than a
  hidden runtime default.

### Risks

- Corrupt state files cause a cold SSN start after a warning. This is safer
  than aborting the cluster for a continuity optimization, but operators should
  inspect repeated restore warnings.
- Sharing the same state path between running processes is unsupported; the file
  is per runtime instance.
- Forecast-window cache misses can still occur after restart because only the
  SSN monitor state is persisted.

### Operational impact

Production configs must include `voacap_fallback.ssn_state_path`. The shipped
example writes to ignored runtime state under `data/voacap/`. Operators should
keep that file with the node's local runtime data and deploy binary/config
changes together.

## Links

- Related issues/PRs/commits: none
- Related code: `internal/voacap/sunspot_monitor.go`,
  `internal/voacap/sunspot_monitor_state.go`,
  `pathreliability/config.go`, `internal/cluster/main_runtime.go`
- Related config: `data/config/path_reliability.yaml`
- Related tests: `internal/voacap/sunspot_monitor_test.go`,
  `pathreliability/config_test.go`
- Related docs: `data/config/README.md`, `pathreliability/README.md`,
  `docs/OPERATOR_GUIDE.md`, `customgpt/support-cards/path-reliability.md`
- Related TSRs: none
- Supersedes / superseded by: extends ADR-0161 SSN runtime behavior while
  preserving ADR-0162 memory-only VOACAP forecast-window cache behavior
