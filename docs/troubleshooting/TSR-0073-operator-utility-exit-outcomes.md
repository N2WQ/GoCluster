# TSR-0073 - Operator Utility Exit Outcomes

Status: Monitoring
Date Opened: 2026-10-10
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: operator tools, failure feedback and partial output
Trigger Source: Chat request
Led To ADR(s): none
Tags: tools, CLI, exit-status, output, persistence

## RCA Summary
- What happened: rbnskewfetch logged shared fetch/filter/write failures then
  returned success. Its success-report write was unchecked.
- Why: returning from main after a logged error implies successful process exit.
- What fixed it: exit unsuccessfully on those errors and check report writes,
  while preserving existing output order and partial effects.
- How we know: actual-main private config/HTTP fixtures observe successful
  literal rows and failed fetch/body/CSV/filter/marshal/file/report paths.
  Checkpoint 24 qualification is retained in the execution record.
- Operator/support answer: use command status and causal stderr. Failure does
  not imply no effects; stdout reporting follows the durable JSON write.

## Triggering Request
- Request date: 2026-10-10.
- Request summary: implement approved v3 operator-tool outcome repairs.
- Request reference: `docs/command-fix-scope-ledger-v3.md`, checkpoint 24.

## Symptoms and Impact
- Automation may treat a failed refresh as successful.
- An existing table survives fetch/parse/filter/marshal failures. A later
  stdout failure can leave a successfully written new table.

## Hypotheses and Tests
1. Shared FetchAndWrite returns failures correctly.
   - Evidence: current source returns fetch, empty-selection and write errors.
   - Outcome: confirmed; repair the command boundary, not the shared API.
2. Any nonzero fixture status proves the intended operation failed.
   - Evidence: invalid config fails before HTTP; actual-main tests therefore
     check request counts, causal diagnostics, bounds and output witnesses.
   - Outcome: require stage-specific evidence paired with genuine success.

## Findings
- Root cause: log-and-return command behavior, plus unchecked success output.
- Inclusive absolute-skew selection, CSV normalization and factor-1 retention
  remain as prescribed by ADR-0031; no scientific calculation changed.
- Existing direct writes remain direct writes; failure feedback is not an
  atomic replacement guarantee.

## Decision Linkage
- ADR created/updated: none; output order and established selection preserved.
- Contract changes: command failures now reach automation through exit status.

## Verification and Monitoring
- Validation steps: checkpoint 24 in `docs/command-fix-execution.md`.
- Signals: command status, causal stderr, request witnesses and decoded table.
- Rollback triggers: failed operation exits zero or selection/output format drifts.
- Limits: private fixtures do not establish external feed health or calibration.

## References
- Issue(s): approved v3 operator-tool checkpoints.
- PR(s): none.
- Commit(s): checkpoint 24 on `fix_commands`; see execution history.
- Related ADR(s): [ADR-0031](../decisions/ADR-0031-skew-selection-min-abs-skew.md).
- Related docs: `data/config/README.md`, `cmd/rbnskewfetch/main.go`.
