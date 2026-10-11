# ADR-0294: Independent Profiling Controller

- Status: Accepted
- Date: 2026-10-11
- Decision Origin: Troubleshooting chat

## Context

The local profiling script launched GoCluster in a separate PowerShell window,
but kept the profile-capture loop in the PowerShell process that invoked the
script. During a reported run, the cluster and its pprof endpoint remained
available while the controller process was absent and no captures had been
logged. The reason that controller process exited was not established.

## Decision

Start the profiling controller in its own visible PowerShell window. The
controller owns both the GoCluster launch and the capture loop. The original
launch window may close without stopping profiling. Closing the controller
window ends profile collection; the GoCluster console may continue running.
Controller startup and capture outcomes are written to the per-run controller
log under `logs/`.

## Alternatives considered

1. Keep the controller in the invoking PowerShell session. This preserves the
   observed process ownership and allows closing that session to stop captures
   while the separate cluster window remains active.
2. Run the controller without a visible window. This would make process status
   and capture failures harder to see during local profiling.

## Consequences

### Benefits

- The profile loop does not depend on the lifetime of the launch window.
- One visible controller owns cluster startup and capture, so a failed
  controller startup does not start an unmonitored cluster.
- The per-run log records controller startup and profile capture outcomes.

### Risks

- Closing the controller intentionally stops profile collection but does not
  necessarily stop the cluster process.
- The controller and cluster still require separate window and process
  lifecycle awareness.

### Operational impact

Keep the profiling controller window open while collecting data. Use
`logs/profiling-run-<timestamp>.log` to confirm startup and capture results.
See [the local runtime profiling guide](../../scripts/README.md#local-runtime-profiling).

## Links

- Related tests: `scripts/test-run-with-profiling.ps1`
- Related docs: `scripts/README.md`, `customgpt/developer-guide-index.md`,
  `customgpt/source-map.md`
- Related TSRs: [TSR-0084](../troubleshooting/TSR-0084-profiling-controller-lifetime.md)
- Supersedes / superseded by: none
