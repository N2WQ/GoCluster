# ADR-0158: VOACAP Process Wrapper Experiment

- Status: Accepted
- Date: 2026-06-08
- Decision Origin: Design

## Context

The VOACAP experiment needs a Go-owned way to invoke the installed VOACAP
engine before deck generation, output parsing, H3 endpoint selection, caching,
or path-reliability integration can be evaluated in the repository.

The PowerShell experiment invokes `C:\itshfbc\bin_win\Voacapw.exe` with:

```text
silent C:\itshfbc voacapx.dat <output-name>
```

The engine reads from and writes to the shared VOACAP run directory,
`C:\itshfbc\run`. The conventional input filename is `voacapx.dat`, which
means concurrent runs can collide unless access is serialized.

## Decision

Add an isolated Go wrapper for one VOACAP run at a time.

The wrapper validates the engine and run directory, writes the deck into the
run directory, removes stale output before launch, invokes VOACAP with a
context timeout, checks for nonzero exit and missing/empty output, and reads
the output with a size limit. Access to the shared run directory is serialized
inside the Go process and guarded with a lock file so separate Go experiment
processes can cooperate.

This decision does not add runtime integration, deck generation, output metric
parsing, H3 endpoint handling, caching, YAML configuration, or path-reliability
fallback behavior.

## Alternatives considered

1. Keep invoking VOACAP only through PowerShell scripts.
2. Allow parallel Go invocations using unique output names only.
3. Build deck generation, process launch, output parsing, and path reliability
   fallback in one broad change.

## Consequences

### Benefits

- Gives VOACAP experimentation a tested Go process boundary.
- Makes timeout, nonzero exit, missing output, and stale output behavior
  deterministic in tests.
- Keeps live cluster behavior unchanged.

### Risks

- The lock file is cooperative. Non-Go scripts that ignore it can still collide
  with the fixed `voacapx.dat` input file.
- A crashed process can leave a stale lock file that must be removed manually.
- Live smoke tests still depend on a local VOACAP install.

### Operational impact

None for the live cluster. The wrapper is experiment-only and is not called by
the runtime path reliability code.

## Links

- Related issues/PRs/commits: pending
- Related tests: `internal/voacap/runner_test.go`
- Related docs: `docs/decisions/ADR-0157-voacap-ssn-moving-average-experiment.md`
- Related TSRs: none
- Supersedes / superseded by: none
