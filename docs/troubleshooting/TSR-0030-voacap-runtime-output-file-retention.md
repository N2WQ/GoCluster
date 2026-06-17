# TSR-0030: VOACAP Runtime Output File Retention

- Status: Resolved
- Date opened: 2026-06-16
- Status date: 2026-06-16

## RCA Summary

- What happened: Runtime VOACAP fallback left successful `.out` files in the
  shared run directory after parsing them into memory, causing unbounded
  long-running disk growth.
- Why: The generic runner removed stale output before a run and read the new
  output file, but had no post-read cleanup contract; runtime fallback no
  longer needed the file after copying bytes into `RunResult.Output`.
- What fixed it: ADR-0180 added `RunRequest.RemoveOutputAfterRead`, kept the
  generic default artifact-retention behavior, enabled cleanup for runtime
  fallback, and made cleanup failure visible as a run error.
- How we know: Source inspection separated runtime fallback from experiment
  commands that intentionally use `OutputPath`, and tests cover both default
  runner retention and runtime fallback cleanup.
- Operator/support answer: For growing VOACAP run directories, distinguish
  runtime fallback outputs from intentional experiment artifacts; this fix does
  not delete files accumulated before deployment.

## Trigger

The user reported that VOACAP `.out` files were not cleaned up after the
runtime fallback had ingested the results into memory, causing the VOACAP run
directory to grow without bound.

## Symptoms and impact

Successful runtime VOACAP fallback jobs wrote receive and transmit `.out` files
into the shared VOACAP run directory. The fallback parsed those bytes into
in-memory forecast records, but the output files remained on disk. A long-running
cluster could therefore accumulate one pair of `.out` files per successful
fallback job until an operator manually cleaned the directory.

## Hypotheses tested

1. The generic VOACAP runner might already delete successful outputs.
   - Disproved by inspecting `internal/voacap/runner.go`: it removed stale
     output before launch and read the new file, but did not remove it after
     reading.
2. The path-reliability fallback might need the output file after parsing.
   - Disproved by inspecting `pathreliability/voacap_fallback.go`: runtime
     fallback uses `RunResult.Output` for parsing and stores parsed hourly
     records in memory.
3. All runner callers could safely lose the output artifact.
   - Disproved by inspecting experiment/sample commands: they still report or
     size `RunResult.OutputPath` as an intentional local artifact.

## Evidence

- `Runner.Run` wrote `voacapx.dat`, removed stale output, invoked VOACAP, read
  the bounded output file into `RunResult.Output`, and returned without deleting
  the just-read file.
- `VOACAPRunnerClosedForecaster.runDirectionalForecast` parsed
  `RunResult.Output` into `VOACAPClosedForecast` records; no runtime fallback
  caller reread the `.out` file.
- `cmd/voacap_run_sample` prints `RunResult.OutputPath`.
- `cmd/voacap_ssn_forecast_watch` stores and sizes `RunResult.OutputPath`.
- Focused regression tests now cover both the generic runner retention default
  and the runtime fallback cleanup path.

## Root cause or best current explanation

The original process wrapper made stale-output removal deterministic before a
run, but did not define a post-read cleanup contract. Once the wrapper became
part of the long-running runtime fallback, successful output files became
server-lifetime disk artifacts even though the runtime only needed their bytes
long enough to parse records into memory.

## Fix or mitigation

- Add an explicit `RunRequest.RemoveOutputAfterRead` option to the generic
  VOACAP runner.
- Leave the default runner behavior unchanged so experiment/sample commands can
  keep intentional output artifacts.
- Enable post-read removal for the path-reliability runtime fallback, after the
  `.out` bytes have been copied into `RunResult.Output`.
- Treat cleanup failures as run errors so they are visible through the existing
  fallback failure path instead of silently retaining files.

This does not remove already accumulated files from the VOACAP run directory.

## Why an ADR was or was not required

- ADR required because the fix changes the durable runtime cleanup contract for
  successful VOACAP fallback output files while preserving the shared runner's
  default artifact-retention contract.

## Links

- Related ADRs: ADR-0180
- Related issues/PRs/commits: none
- Related tests: `internal/voacap/runner_test.go`,
  `pathreliability/voacap_fallback_test.go`
- Related docs: `docs/decisions/ADR-0158-voacap-process-wrapper-experiment.md`,
  `docs/decisions/ADR-0169-bidirectional-noise-aware-voacap-fallback.md`
