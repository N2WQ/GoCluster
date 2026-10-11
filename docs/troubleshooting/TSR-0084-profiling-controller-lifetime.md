# TSR-0084 - Profiling Controller Lifetime

Status: Monitoring
Date Opened: 2026-10-11
Date Resolved: n/a
Owner: Codex
Technical Area: local profiling script, PowerShell process lifecycle, pprof captures
Trigger Source: Chat request
Led To ADR(s): ADR-0294
Tags: profiling, pprof, PowerShell, controller lifetime, logs

## RCA Summary

- What happened: GoCluster remained running while the profiling controller was
  absent; the run log contained only startup entries and no profile captures.
- Why: the script ran its capture loop in the invoking PowerShell session but
  launched GoCluster in a separate window. The reason that session ended was
  not established.
- What fixed it: launch a visible independent controller that owns both cluster
  startup and the capture loop.
- How we know: the controller fixture starts the production process-launch
  helper, observes the child after its caller exits, releases it to finish, and
  verifies missing-engine failure. A live end-to-end capture was not performed.
- Operator/support answer: keep the profiling controller window open; inspect
  `logs/profiling-run-<timestamp>.log` for startup and capture results. Closing
  the controller ends capture while the cluster may remain running.

## Triggering Request

- Request date: 2026-10-11
- Request summary: fix the local profile runner that left a running cluster
  without visible profile captures.
- Request reference: approved Scope Ledger v1 in this chat.

## Symptoms and Impact

The cluster process and `localhost:6061/debug/pprof/` endpoint were observed
running. The controller process was absent. Its log had startup entries only,
and no profile files for that run were present. The controller's exit cause was
unknown; the observation does not establish whether the user closed its shell
or another failure ended it.

## Timeline

1. 2026-10-11 - Confirmed the cluster and pprof endpoint were available, while
   the controller process was absent and the run had no capture entries.
2. 2026-10-11 - User selected an independent visible controller window.
3. 2026-10-11 - Added a detached-process fixture and updated the script's
   operational guidance; a live cluster capture remains unverified.

## Hypotheses and Tests

1. The controller ended because the original PowerShell window was closed.
   - Evidence/commands: process inspection showed the controller process was
     absent; no close event or exit status was available.
   - Outcome: Inconclusive.
2. The pprof endpoint was unavailable and blocked capture.
   - Evidence/commands: `Invoke-WebRequest` to the pprof index returned HTTP
     200; a one-second CPU profile probe completed.
   - Outcome: Rejected as the explanation for the missing captures in the
     inspected state; it does not identify why the controller ended earlier.
3. Separate controller lifetime preserves profiling after the caller exits.
   - Evidence/commands: `pwsh -NoProfile -File scripts/test-run-with-profiling.ps1`.
   - Outcome: Supported for the tested PowerShell process-launch path; live
     profiling remains unverified.

## Findings

- Root cause (or best current explanation): the prior execution topology made
  capture lifetime depend on the invoking PowerShell process, but the reason
  that process exited in the observed run is unknown.
- Contributing factors: GoCluster was launched separately and could outlive the
  controller; controller logs stopped after initial startup messages.
- Why this did or did not require a durable decision: controller ownership and
  close behavior are durable operator-visible process-lifecycle behavior; see
  ADR-0294.

## Decision Linkage

- ADR created/updated: ADR-0294.
- Decision delta summary: an independent visible controller owns cluster
  launch and capture; the original launch window may close without ending the
  capture loop.
- Contract/behavior changes: closing the controller ends profiling while the
  cluster may remain alive.

## Verification and Monitoring

- Validation steps run: script syntax parsing; detached-process positive and
  missing-engine negative fixture; record and diff checks. No live end-to-end
  capture was performed.
- Signals to monitor: controller startup, capture success/failure, and final
  exit entries in `logs/profiling-run-<timestamp>.log`; generated `cpu-*.pprof`
  and related files.
- Rollback triggers: controller fails to launch reliably, starts GoCluster
  without owning capture, or captures fail without a corresponding log entry.

## References

- Issue(s): none.
- PR(s): none.
- Commit(s): none.
- Related ADR(s): ADR-0294.
- Related docs: `scripts/README.md`,
  `customgpt/developer-guide-index.md`, `customgpt/source-map.md`.
