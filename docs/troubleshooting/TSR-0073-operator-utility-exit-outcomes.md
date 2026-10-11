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
- What happened: rbnskewfetch and prop_report logged shared operation failures
  then returned success. Their success-report writes were unchecked.
- Why: returning from main after a logged error implies successful process exit.
- What fixed it: exit unsuccessfully on those errors and check report writes,
  while preserving existing output order and partial effects.
- How we know: actual-main private config/HTTP fixtures observe successful
  literal rows and failed fetch/body/CSV/filter/marshal/file/report paths.
  Propagation-report fixtures cover hard inputs, early/late output faults and
  local optional-request fallback. Checkpoint 24/25 qualification is retained
  in the execution record.
- Operator/support answer: use command status and causal stderr. Failure does
  not imply no effects; stdout reporting follows the durable JSON write.

## Triggering Request
- Request date: 2026-10-10.
- Request summary: implement approved v3 operator-tool outcome repairs.
- Request reference: `docs/command-fix-scope-ledger-v3.md`, checkpoints 24/25.

## Symptoms and Impact
- Automation may treat a failed refresh as successful.
- An existing table survives fetch/parse/filter/marshal failures. A later
  stdout failure can leave a successfully written new table.
- Propagation JSON precedes optional narrative and Markdown. Later failure can
  leave JSON; stdout reporting failure can leave both outputs. Enabled invalid
  optional configuration fails before output, while a request failure deliberately
  warns and retains the local report. No-LLM bypass remains available.

## Hypotheses and Tests
1. Shared FetchAndWrite returns failures correctly.
   - Evidence: current source returns fetch, empty-selection and write errors.
   - Outcome: confirmed; repair the command boundary, not the shared API.
2. Any nonzero fixture status proves the intended operation failed.
   - Evidence: invalid config fails before HTTP; actual-main tests therefore
     check request counts, causal diagnostics, bounds and output witnesses.
   - Outcome: require stage-specific evidence paired with genuine success.
3. Every optional narrative error should fail report generation.
   - Evidence: Generate explicitly returns config errors before output but logs
     request errors and writes its local fallback report.
   - Outcome: preserve the distinction; actual-main fixtures use loopback
     responses and compare fallback bytes against equivalent no-LLM reports.

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
- Validation steps: checkpoints 24/25 in `docs/command-fix-execution.md`.
- Signals: command status, causal stderr, request witnesses and decoded table.
- Rollback triggers: failed operation exits zero or selection/output format drifts.
- Limits: private fixtures do not establish external feed health or calibration.
  Narrative fixtures use synthetic credentials and private loopback requests;
  they do not establish external LLM availability or report model validity.

### V4 blocked-parent test portability

The original prop_report output test used IsNotExist when a regular file was
the requested output's parent. Linux reports ENOTDIR for this child lookup;
Windows commonly reports an absent path. That difference failed the assertion
after the command had already produced its intended nonzero status.

Only the two regular-file-parent cases now admit absence or ENOTDIR. Literal
error controls reject a successful lookup, permission failure, I/O failure and
invalid-argument error. Both actual-main cases require the blocker to remain a
regular file with unchanged bytes. JSON-parent failure produces neither output;
report-parent failure retains independently parsed JSON and no Markdown, as
required by the existing write order. Other absence assertions stay strict.

The complete local prop_report suite passed on native Windows and Linux with
cgo enabled and disabled; the execution record links the observed artifacts.
No production command, output ordering, fallback or model contract changed.

## References
- Issue(s): approved v3 operator-tool checkpoints.
- PR(s): none.
- Commit(s): checkpoints 24/25 on `fix_commands`; see execution history.
- Related ADR(s): [ADR-0031](../decisions/ADR-0031-skew-selection-min-abs-skew.md).
- Related docs: `data/config/README.md`, `cmd/rbnskewfetch/main.go`,
  `cmd/prop_report/main.go`, `internal/propreport/report.go`.
