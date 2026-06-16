# ADR-0180: VOACAP Runtime Output Cleanup

- Status: Accepted
- Date: 2026-06-16
- Decision Origin: Troubleshooting chat

## Context

The VOACAP process wrapper reads output from the shared VOACAP run directory
after invoking `Voacapw.exe`. Runtime path-reliability fallback jobs then parse
those bytes into in-memory hourly records and cache the records, not the file.

After VOACAP fallback became part of the long-running runtime path, successful
receive/transmit `.out` files remained in the run directory after parsing. That
created unbounded disk growth for a server-lifetime background worker.

The same runner is also used by experiment and sample commands that intentionally
report or persist `RunResult.OutputPath` as a local artifact. A global default
change would silently alter those tools.

## Decision

Keep generic `Runner.Run` artifact retention as the default, but add an explicit
`RunRequest.RemoveOutputAfterRead` option. When enabled, the runner removes the
output file after it has read bounded output bytes into `RunResult.Output`.

Runtime path-reliability VOACAP fallback opts into that cleanup for each
directional receive/transmit run. The fallback continues to parse from
`RunResult.Output` and cache in-memory records. `RunResult.OutputPath` remains
populated as the path that was used for the run, but the file is not expected to
remain after a successful runtime fallback read.

If post-read removal fails, the runner returns an error with the in-memory result
attached. Runtime fallback treats that as a failed run through the existing
failure path instead of silently retaining a file.

This decision does not introduce a cleanup sweep for historical `.out` files.

## Alternatives considered

1. Delete every successful runner output by default.
   - Rejected because experiment and sample commands intentionally expose
     output files as local artifacts.
2. Delete files only after `ParsePredictions` succeeds.
   - Rejected for the runtime fallback because the file is already no longer
     needed once the bounded bytes are copied into memory; parse failures should
     not preserve server-lifetime disk artifacts by default.
3. Add a periodic run-directory cleanup job.
   - Rejected for this slice because prompt cleanup at the ownership boundary is
     simpler, bounded, and avoids deleting artifacts owned by other tools or
     non-Go VOACAP users.

## Consequences

### Benefits

- Successful runtime fallback runs no longer accumulate `.out` files in the
  shared VOACAP run directory.
- Experiment and sample commands keep their existing output-artifact behavior.
- Cleanup happens while the runner still owns the serialized VOACAP run
  boundary and before the lock is released.

### Risks

- Runtime fallback no longer leaves successful `.out` files behind for ad hoc
  manual inspection. Operators should use parsed diagnostics, logs, or targeted
  experiment commands when they need a retained output artifact.
- If Windows refuses file removal, the fallback records a run failure even
  though bytes were read. This favors visible cleanup failure over silent disk
  growth.
- Existing accumulated files still require manual cleanup or a separately
  approved maintenance sweep.

### Operational impact

- No YAML/config schema, worker count, queue, delay, cache key, SSN, parser,
  glyph, or telnet command behavior changes.
- The live VOACAP run directory should stop growing from successful runtime
  fallback jobs after deployment.
- Local experiment commands continue to produce retained `.out` files unless
  they explicitly opt into cleanup in a future change.

## Links

- Related issues/PRs/commits: none
- Related code: `internal/voacap/runner.go`,
  `pathreliability/voacap_fallback.go`
- Related tests: `internal/voacap/runner_test.go`,
  `pathreliability/voacap_fallback_test.go`
- Related docs: `docs/decisions/ADR-0158-voacap-process-wrapper-experiment.md`,
  `docs/decisions/ADR-0169-bidirectional-noise-aware-voacap-fallback.md`
- Related TSRs: TSR-0030
- Supersedes / superseded by: extends ADR-0158's process-wrapper cleanup
  contract and ADR-0169's bidirectional runtime fallback output behavior
