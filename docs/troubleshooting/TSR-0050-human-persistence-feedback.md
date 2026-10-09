# TSR-0050 - Human Persistence Feedback

Status: Monitoring
Date Opened: 2026-10-09
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: human commands and configuration persistence
Trigger Source: Chat request
Led To ADR(s): none

## RCA Summary

Filter, COMMENT, reset and changed-DIALECT consumers applied accepted edits,
attempted saving and logged failures, but returned only the success
acknowledgement to the client. Several other settings already reported save
failures. An acknowledgement could therefore imply durable success even when
the next login would restore the old settings.

These consumers now append one explicit warning on its own line. The live edit
remains active and the old stored bytes remain intact when preparation or atomic
replacement fails. Protected temporary sessions keep the refused record during
commands and logout. Machine changes and LOAD retain their persistence-before-
publication contract.

## Checker Boundaries

A replacement callback can verify error propagation, but cannot prove that
production storage preparation or commit failed. The Windows checkout fixture
opens a valid owned record with read/write sharing and without delete sharing:
loading succeeds while the production MoveFileEx replacement fails. Its control
checks replacement succeeds after closing the handle.

Keep the fault installed through complete child retirement. Checking only after
disconnect, or releasing the fault before unregister's final save, can allow
autosave to repair the defect and produce misleading evidence. Inspect live
readbacks and old stored bytes while connected, then replace the server process.

Preparation failures are exercised separately by changing authoritative storage
to malformed or future-version YAML after valid login. Fresh login through those
bytes must report temporary defaults and preserve them through retirement.

## Decision Disposition And Verification

This completes the established live-human-update and truthful-failure feedback
contract; it adds no new policy, schema or retry mechanism, so no ADR is required.
The [execution record](../command-fix-execution.md#checkpoint-03-truthful-human-persistence-warnings)
contains commands and observed results. Checkpoint 04 must rerun these cases
against its storage admission/preparation changes.

Support should explain that the edit is active but unsaved, and investigate the
stated storage failure. An unchanged DIALECT or idempotent COMMENT operation does
not retry saving; ordinary filter commands retain their existing save attempts.
Do not remove or overwrite a refused user record as an automatic repair.
