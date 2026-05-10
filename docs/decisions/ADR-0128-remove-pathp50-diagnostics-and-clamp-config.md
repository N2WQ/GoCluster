# ADR-0128: Remove PATHP50 Diagnostics And Clamp Config

- Status: Accepted
- Date: 2026-05-10
- Decision Origin: Design

## Context
ADR-0126 made fixed-bin p50 the active path statistic for glyphs and PATH
filters. After that change, `SET DIAG PATHP50` and the `Path p50 diag (5m)`
aggregate no longer provide a separate shadow comparison; they duplicate active
scoring visibility while adding command surface, retained counters, parser
surface, and support documentation.

The `clamp_min` and `clamp_max` path config keys are also dead settings. The
active fixed histogram owns the effective SNR range and does not read those
config values.

## Decision
Remove the `SET DIAG PATHP50` telnet mode, the PATHP50 diagnostic counters, the
five-minute `Path p50 diag` propagation-log emission, and propagation-report
parsing/reporting for those diagnostic aggregates.

Remove `clamp_min` and `clamp_max` from checked-in path configuration,
configuration loading, required-key validation, defaults, and propagation-report
model context.

Keep active p50 scoring unchanged. The fixed histogram bucket geometry,
retained p50 fields, PATH filters, `SET DIAG PATH`, receiver-cap diagnostics,
noise penalties, and propagation report prediction metrics remain in force.

## Alternatives considered
1. Keep PATHP50 as an alias for active p50 visibility.
   Rejected because it adds support surface without distinct behavior.
2. Keep `Path p50 diag (5m)` as a legacy report input.
   Rejected because it is diagnostic-observed only and no longer needed for
   current production review.
3. Leave `clamp_min` and `clamp_max` as ignored compatibility keys.
   Rejected because ignored required keys make operator configuration look
   tunable when it is not.

## Consequences
### Benefits
- Removes dead diagnostic state from the telnet server.
- Reduces current support and report surface to the active path diagnostics
  operators still use.
- Removes stale config knobs that no longer affect runtime behavior.

### Risks
- Operators cannot request the compact `p<db>n<count>` comment view.
- Historical propagation logs can still contain `Path p50 diag` lines, but
  current reports no longer summarize them.
- Existing local override config files that still contain clamp keys will fail
  strict YAML decoding and must remove them.

### Operational impact
- Current active p50 glyphs and PATH filters are unchanged.
- `SET DIAG PATH` remains the supported per-spot path diagnostic.
- Runtime memory is reduced by removing PATHP50 diagnostic atomics; no new
  retained state is introduced.
- The agreed retained histogram heap for active p50 scoring remains unchanged.

## Links
- Related issues/PRs/commits:
- Related tests: `telnet/diag_command_test.go`, `telnet/server_prediction_stats_test.go`, `pathreliability/snr_histogram_test.go`, `pathreliability/store_bench_test.go`, `internal/propreport/report_test.go`, `commands/processor_test.go`
- Related docs: `README.md`, `docs/OPERATOR_GUIDE.md`, `pathreliability/README.md`, `data/config/path_reliability.yaml`, `customgpt/`
- Related TSRs:
- Supersedes / superseded by: Supersedes the PATHP50 diagnostic portions of ADR-0126 and the `Path p50 diag` aggregate portion of ADR-0123.
