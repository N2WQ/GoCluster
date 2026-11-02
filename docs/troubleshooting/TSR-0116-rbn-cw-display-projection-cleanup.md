# TSR-0116 - RBN CW Display Projection Cleanup

Status: Monitoring
Date Opened: 2026-11-01
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: RBN parsing, CW display, archive history
Trigger Source: Chat request
Led To ADR(s): none
Tags: RBN, CW speed, comment projection, archive

## RCA Summary

- What happened: An RBN spot with speed-like text before its signal report
  could display the wrong ordinary comment text in live output and history.
- Why: The display projection ran a second cleanup parse after removing speed
  tokens. That parse could associate a different number with the report.
- What fixed it: The projection uses mode, report, and time cleanup decisions
  from the original canonical parse and recognizes raw speed pairs separately.
- How we know: Fixed raw-input and archive-reopen regression cases exercise
  signed text, ordering, overlapping report/speed roles, and time boundaries.
- Operator/support answer: New spots use the corrected projection. Existing
  archived projections are unchanged and cannot be reconstructed reliably from
  the saved record; allow them to expire or reset the spot archive to remove
  them sooner, recognizing that a reset deletes all its spot history.

## Triggering Request

- Request date: 2026-11-01.
- Request summary: Correct the reported display defect and investigate reuse of
  the existing formatted-line cache for the default OFF preference.
- Request reference: Review of commit `ab7d8da20be4cfd93acdc2df89fc8f701d6ceabd`.

## Symptoms and Impact

For `CW +22 WPM 5 dB CQ`, the affected projection discarded `+22` and left
the report's `5` as ordinary comment text. The canonical comment and stored
report followed the original parse; display and saved display projection were
affected. No production incidence or load impact was measured.

## Timeline

1. 2026-11-01 - Source review identified a mismatched projection parse.
2. 2026-11-01 - The owner approved the bounded correction and cache shortcut.
3. 2026-11-01 - Regression coverage was added for raw input and archive reopen.

## Hypotheses and Tests

1. The report value itself was changed by the projection.
   - Evidence: The canonical parse supplies the stored report before projection.
   - Outcome: Rejected for the reported example.
2. Removing speed tokens before reparsing changed report cleanup decisions.
   - Evidence: The original pending-number association and the display reparse
     follow different token streams; fixed expected-output cases exercise both.
   - Outcome: Supported.
3. Restarting the server repairs earlier archived projections.
   - Evidence: The archive stores the projection and does not retain the raw
     token positions needed to reconstruct it.
   - Outcome: Rejected.

## Findings

- Root cause: Display cleanup did not reuse the canonical parse's decisions.
- Contributing factors: Earlier tests put the signal report before speed-like
  text and checked metadata without asserting all surviving ordinary text.
- Durable decision: No change to ADR-0332; this restores its display contract.

## Decision Linkage

- ADR created/updated: none.
- Decision delta summary: No new durable architecture decision.
- Contract/behavior changes: Correct display projection; canonical matching,
  archive encoding, and saved preferences remain unchanged.

## Verification and Monitoring

- Validation steps: See the CW projection, archive-reopen, and formatter tests
  in `rbn`, `spot`, `telnet`, and `internal/cluster`.
- Signals to monitor: Incorrect ordinary comment text on unusual RBN payloads;
  check the binary version and whether a history record predates the correction.
- Rollback triggers: Incorrect canonical matching or new display corruption.

## References

- Related ADR: [ADR-0332](../decisions/ADR-0332-per-user-rbn-cw-speed-display.md).
- Operator guidance: [RBN CW speed](../../telnet/README.md#rbn-cw-speed).
